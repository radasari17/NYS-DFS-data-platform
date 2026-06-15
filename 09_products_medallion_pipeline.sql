-- ==========================================
-- BRONZE LAYER (STAGE) — Products
-- ==========================================
-- PURPOSE: Raw ingestion layer. All VARCHAR to prevent any type-casting failures.
-- Special handling: ISO-8859-1 encoding for special characters (Nestlé, etc.)
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- Raw staging table — no type enforcement, accepts anything from CSV
CREATE OR REPLACE TABLE products (
    product_id       VARCHAR,
    product_category VARCHAR,
    product_name     VARCHAR,
    category_id      VARCHAR,
    supplier_id      VARCHAR,
    price            VARCHAR
);

-- Stream created BEFORE COPY so it captures the initial load as INSERT actions
CREATE OR REPLACE STREAM products_stm ON TABLE products;

-- Ingest with ISO-8859-1 encoding to handle special characters (é, ñ, ü)
-- that exist in brand names like "Nestlé"
COPY INTO products
FROM @NYS_DFS_RETAIL.STAGE.csv_stage/products.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ENCODING = 'ISO-8859-1'
);

-- ==========================================
-- SILVER LAYER (CLEAN) — Products
-- ==========================================
-- PURPOSE: Type-safe, validated, deduplicated product data.
-- DECIMAL(10,2) for price ensures financial precision through to Gold.
-- No referential integrity subqueries — avoids race condition with parallel pipelines.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- Governed table with strict types and financial constraint
CREATE OR REPLACE TABLE products (
    product_id       INT PRIMARY KEY,        -- Uniqueness + NOT NULL enforced natively
    product_category VARCHAR,                 -- Product grouping (preserved casing from source)
    product_name     VARCHAR NOT NULL,        -- Brand names — never empty, casing preserved
    category_id      INT NOT NULL,            -- FK to categories (no enforcement to avoid race conditions)
    supplier_id      INT NOT NULL,            -- FK to suppliers (no enforcement to avoid race conditions)
    price            DECIMAL(10,2),           -- Financial precision — no floating-point drift
    dw_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- GOVERNANCE: Prevents negative prices from corrupting revenue calculations
    -- A single -10.99 multiplied by quantity would subtract from executive dashboards
    CHECK (price >= 0)
);

-- Stream BEFORE insert — captures baseline for Gold layer MERGE
CREATE OR REPLACE STREAM products_stm ON TABLE products;

-- Type casting and whitespace normalization
-- No INITCAP — brand names (Nestlé, MamyPoko, FarmFresh) have intentional casing
INSERT INTO products (product_id, product_category, product_name, category_id, supplier_id, price)
SELECT
    -- Safe integer cast — NULL on failure, rejected by WHERE clause below
    TRY_CAST(product_id AS INT),

    -- Category: Collapse double-spaces, preserve original casing
    -- Categories like "Bakery, Bread & Cakes" have commas and & symbols
    TRIM(REGEXP_REPLACE(product_category, '\\s+', ' ')),

    -- Product name: Strip whitespace artifacts, preserve brand casing
    TRIM(REGEXP_REPLACE(product_name, '\\s+', ' ')),

    -- Foreign keys: Cast to INT (no referential check to avoid pipeline race conditions)
    TRY_CAST(category_id AS INT),
    TRY_CAST(supplier_id AS INT),

    -- DECIMAL cast ensures ₹1084.50 stays ₹1084.50, not ₹1084.4999999
    TRY_CAST(price AS DECIMAL(10,2))

FROM NYS_DFS_RETAIL.STAGE.products_stm

-- ROW-LEVEL FILTERS: Reject invalid rows silently
WHERE METADATA$ACTION = 'INSERT'
    AND TRY_CAST(product_id AS INT) IS NOT NULL       -- No valid ID = no identity
    AND TRY_CAST(price AS DECIMAL(10,2)) >= 0         -- No negative prices allowed

-- DEDUPLICATION: Keep one row per product_id if source has duplicates
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) = 1;

-- Verify Silver layer
SELECT * FROM NYS_DFS_RETAIL.CLEAN.products LIMIT 10;

-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Products
-- ==========================================
-- PURPOSE: Enterprise dimension table with hybrid SCD tracking.
-- Price, category_id, and supplier_id changes are business-critical events
-- that affect financial reporting — these trigger SCD2 (historical versioning).
-- Name/category label spelling fixes are cosmetic — these trigger SCD1 (overwrite).
-- Price tier is pre-computed in the CTE to avoid NULL trap and code repetition.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- Dimension table with DECIMAL precision and pre-computed segmentation
CREATE OR REPLACE TABLE products_dim (
    product_sk           INT AUTOINCREMENT PRIMARY KEY,  -- Surrogate key (fact tables join here)
    product_id           INT NOT NULL,                   -- Source business key (stream matching)
    product_category     VARCHAR,                        -- Product grouping for dashboards
    product_name         VARCHAR NOT NULL,               -- Display name (brand casing preserved)
    category_id          INT NOT NULL,                   -- FK to categories_dim
    supplier_id          INT NOT NULL,                   -- FK to suppliers_dim
    price                DECIMAL(10,2),                  -- Financial precision (no FLOAT drift)
    price_tier           VARCHAR,                        -- Pre-computed segmentation for reports
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- HYBRID SCD MERGE: Routes changes based on business impact
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.products_dim AS target
USING (
    -- CTE: Detect change type AND compute derived columns in one pass.
    -- Price tier computed here with NULL handling — prevents NULL prices
    -- from accidentally landing in the 'Luxury' bucket.
    WITH stream_changes AS (
        SELECT
            new_row.product_id,
            new_row.product_category,
            new_row.product_name,
            new_row.category_id,
            new_row.supplier_id,
            new_row.price,

            -- PRICE TIER: Calculated once per row, NULL-safe.
            -- 'Unknown' for NULL prevents silent misclassification.
            CASE
                WHEN new_row.price IS NULL THEN 'Unknown'
                WHEN new_row.price < 250 THEN 'Budget (< 250)'
                WHEN new_row.price BETWEEN 250 AND 1000 THEN 'Mid-Range (250-1000)'
                WHEN new_row.price BETWEEN 1001 AND 2500 THEN 'Premium (1001-2500)'
                ELSE 'Luxury (2500+)'
            END AS price_tier,

            -- CHANGE DETECTION ENGINE: Classifies each stream record.
            -- NVL converts NULLs to sentinel values for safe != comparison.
            CASE
                -- No DELETE pair = brand new product (never existed before)
                WHEN old_row.product_id IS NULL THEN 'INSERT'

                -- Financial/hierarchy columns changed = business event (SCD2)
                -- Price change → affects revenue calculations
                -- Category change → affects product mix analytics
                -- Supplier change → affects procurement reporting
                WHEN NVL(new_row.price, -1) != NVL(old_row.price, -1)
                  OR NVL(new_row.category_id, -1) != NVL(old_row.category_id, -1)
                  OR NVL(new_row.supplier_id, -1) != NVL(old_row.supplier_id, -1)
                THEN 'SCD2'

                -- Everything else = cosmetic correction (SCD1)
                -- Name typo fix, category label update — no historical significance
                ELSE 'SCD1'
            END AS change_type

        FROM NYS_DFS_RETAIL.CLEAN.products_stm new_row
        -- Self-join: INSERT action = new values, DELETE action = old values
        -- Snowflake streams represent UPDATEs as a DELETE + INSERT pair
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.products_stm old_row
            ON new_row.product_id = old_row.product_id
            AND old_row.METADATA$ACTION = 'DELETE'
        WHERE new_row.METADATA$ACTION = 'INSERT'
    )

    -- ROW DUPLICATION TRICK: For SCD2, we need two actions per change:
    -- 1. UPDATE the old row (close it)
    -- 2. INSERT a new row (the new version)
    -- A single MERGE row can only trigger ONE action, so we duplicate.

    -- SCD1 corrections and fresh inserts — pass through to MATCHED or NOT MATCHED
    SELECT *, 'SCD1_OR_INSERT' AS merge_action
    FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')

    UNION ALL

    -- SCD2 copy 1: Will MATCH the existing current row and close it
    SELECT *, 'CLOSE_OLD' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'

    UNION ALL

    -- SCD2 copy 2: Forced to NOT MATCH (by ON clause hack), inserts as new version
    SELECT *, 'INSERT_NEW' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'

) AS source

-- ON CLAUSE: Three conditions work together:
-- 1. Match on business key (product_id)
-- 2. Only match the CURRENT active row (don't touch closed historical versions)
-- 3. Force INSERT_NEW rows to never match → they fall into NOT MATCHED → INSERT
ON target.product_id = source.product_id
    AND target.is_current = TRUE
    AND source.merge_action != 'INSERT_NEW'

-- SCENARIO A: SCD1 — Cosmetic fix (product name typo, category label correction)
-- Action: Overwrite in place. No new version created. Row stays current.
WHEN MATCHED
    AND source.change_type = 'SCD1'
THEN UPDATE SET
    target.product_name = source.product_name,
    target.product_category = source.product_category,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- SCENARIO B: SCD2 — Business event (price change, supplier switch, re-categorization)
-- Action: Close the old version. Stamp end date. Mark as no longer current.
-- The old row is preserved forever for historical reporting and auditing.
WHEN MATCHED
    AND source.change_type = 'SCD2'
    AND source.merge_action = 'CLOSE_OLD'
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO C: New record (brand new product, or new active version from SCD2)
-- Action: Insert with is_current = TRUE, far-future end date, and pre-computed tier.
-- Both brand new products AND the "new version" of SCD2 changes land here.
WHEN NOT MATCHED
THEN INSERT (product_id, product_category, product_name, category_id, supplier_id, price, price_tier, effective_start_date, effective_end_date, is_current, dw_loaded_at)
VALUES (
    source.product_id,
    source.product_category,
    source.product_name,
    source.category_id,
    source.supplier_id,
    source.price,
    source.price_tier,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- Verify Gold layer
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.products_dim LIMIT 10;