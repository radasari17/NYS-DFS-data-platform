
-- ==========================================
-- SILVER LAYER (CLEAN) — Order Items
-- ==========================================
-- PURPOSE: Transform raw VARCHAR order item data into typed, validated records.
-- Architecture: Read stream ONCE → fork into Clean table OR Dead-Letter Queue.
-- MERGE ensures idempotency — safe to re-run without duplicating data.
-- Derived UNIT_PRICE captures actual price-at-sale for accurate revenue reporting.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- 1. Build the governed clean table with strict constraints
CREATE OR REPLACE TABLE clean_order_items (
    ORDER_ITEM_ID     INT PRIMARY KEY,         -- Unique line item identity
    ORDER_ID          INT NOT NULL,            -- FK to orders (every item belongs to an order)
    PRODUCT_ID        INT NOT NULL,            -- FK to products_dim
    PRODUCT_NAME      VARCHAR NOT NULL,        -- Cleaned name (redundant P-tag stripped)
    QTY               INT NOT NULL,            -- Quantity ordered (must be positive)
    TOTAL_TRANSACTION DECIMAL(10,2) NOT NULL,  -- Line total (financial precision)
    UNIT_PRICE        DECIMAL(10,2),           -- Derived: TOTAL_TRANSACTION / QTY
    dw_loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Dead-Letter Queue: Catches invalid records for auditing
-- All columns remain VARCHAR to accept whatever garbage the source sent.
-- rejection_reason tells you WHY it failed — root cause analysis without guessing.
CREATE OR REPLACE TABLE clean_order_items_dlq (
    ORDER_ITEM_ID     VARCHAR,
    ORDER_ID          VARCHAR,
    PRODUCT_ID        VARCHAR,
    PRODUCT_NAME      VARCHAR,
    QTY               VARCHAR,
    TOTAL_TRANSACTION VARCHAR,
    quarantined_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rejection_reason  VARCHAR
);

-- 3. Attach stream BEFORE consuming to capture baseline for Gold layer
CREATE OR REPLACE STREAM clean_order_items_stm ON TABLE clean_order_items;

-- ==========================================
-- 4. SINGLE-READ STAGING: Snapshot the stream ONCE
-- ==========================================
-- CRITICAL: Snowflake streams can only be consumed once per transaction.
-- We read the entire stream into a temp table, then fork the data into
-- either Clean or DLQ. This prevents the "empty stream on second read" trap.
CREATE OR REPLACE TEMPORARY TABLE order_items_staging AS
SELECT *, METADATA$ACTION AS action
FROM NYS_DFS_RETAIL.STAGE.raw_order_items_stm;

-- ==========================================
-- 5. FORK — Route INVALID records to Dead-Letter Queue
-- ==========================================
-- Any row failing validation lands here with a human-readable reason.
-- Finance can audit this table to find source system issues.
-- Nothing is silently dropped — every record is accounted for.
INSERT INTO clean_order_items_dlq (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, QTY, TOTAL_TRANSACTION, rejection_reason)
SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    PRODUCT_ID,
    PRODUCT_NAME,
    QTY,
    TOTAL_TRANSACTION,

    -- REJECTION REASON: First failing check gets stamped as the reason
    CASE
        WHEN TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL THEN 'INVALID_ORDER_ITEM_ID'
        WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
        WHEN TRY_CAST(PRODUCT_ID AS INT) IS NULL THEN 'INVALID_PRODUCT_ID'
        WHEN TRY_CAST(QTY AS INT) IS NULL OR TRY_CAST(QTY AS INT) <= 0 THEN 'INVALID_QUANTITY'
        WHEN TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) IS NULL
          OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) <= 0 THEN 'INVALID_TRANSACTION_AMOUNT'
        ELSE 'UNKNOWN_REJECTION'
    END

FROM order_items_staging
WHERE action = 'INSERT'
    AND (
        TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL
        OR TRY_CAST(ORDER_ID AS INT) IS NULL
        OR TRY_CAST(PRODUCT_ID AS INT) IS NULL
        OR TRY_CAST(QTY AS INT) IS NULL OR TRY_CAST(QTY AS INT) <= 0
        OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) IS NULL
        OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) <= 0
    );

-- ==========================================
-- 6. FORK — MERGE VALID records into Clean table
-- ==========================================
-- MERGE guarantees idempotency: re-running this script never duplicates data.
-- WHEN MATCHED handles late-arriving corrections (source system fixes a record).
-- WHEN NOT MATCHED inserts brand new clean records.
MERGE INTO clean_order_items target
USING (
    SELECT
        -- Safe type casting — NULL on failure, caught by WHERE clause
        TRY_CAST(ORDER_ITEM_ID AS INT) AS ORDER_ITEM_ID,
        TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,
        TRY_CAST(PRODUCT_ID AS INT) AS PRODUCT_ID,

        -- ADVANCED STRING CLEANING:
        -- Strip redundant "(P-XXX)" system tag from product names
        -- COALESCE ensures NULL names become 'UNKNOWN' instead of failing NOT NULL
        COALESCE(
            TRIM(REGEXP_REPLACE(PRODUCT_NAME, '\\s*\\(P-\\d+\\)$', '')),
            'UNKNOWN'
        ) AS PRODUCT_NAME,

        TRY_CAST(QTY AS INT) AS QTY,

        -- Financial precision: DECIMAL(10,2) prevents floating-point drift
        TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) AS TOTAL_TRANSACTION,

        -- DERIVED COLUMN: Unit price at time of sale
        -- Captures actual price paid (may differ from current catalog price
        -- due to promotions, bulk discounts, or historical price changes)
        -- DIV0NULL returns NULL instead of error if QTY is somehow zero
        DIV0NULL(
            TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)),
            TRY_CAST(QTY AS INT)
        ) AS UNIT_PRICE

    FROM order_items_staging
    WHERE action = 'INSERT'
        -- Only process rows that PASS all validation checks
        AND TRY_CAST(ORDER_ITEM_ID AS INT) IS NOT NULL
        AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
        AND TRY_CAST(PRODUCT_ID AS INT) IS NOT NULL
        AND TRY_CAST(QTY AS INT) > 0
        AND TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) > 0

    -- DEDUPLICATION: If source has duplicate ORDER_ITEM_IDs, keep only one
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY ORDER_ITEM_ID) = 1

) source
ON target.ORDER_ITEM_ID = source.ORDER_ITEM_ID

-- MATCHED: Record already exists — update with latest values (late corrections)
WHEN MATCHED THEN UPDATE SET
    target.ORDER_ID = source.ORDER_ID,
    target.PRODUCT_ID = source.PRODUCT_ID,
    target.PRODUCT_NAME = source.PRODUCT_NAME,
    target.QTY = source.QTY,
    target.TOTAL_TRANSACTION = source.TOTAL_TRANSACTION,
    target.UNIT_PRICE = source.UNIT_PRICE,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- NOT MATCHED: Brand new record — insert into clean table
WHEN NOT MATCHED THEN INSERT (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, QTY, TOTAL_TRANSACTION, UNIT_PRICE)
VALUES (
    source.ORDER_ITEM_ID,
    source.ORDER_ID,
    source.PRODUCT_ID,
    source.PRODUCT_NAME,
    source.QTY,
    source.TOTAL_TRANSACTION,
    source.UNIT_PRICE
);

-- ==========================================
-- 7. VERIFY
-- ==========================================
-- Clean data: Should show 600K rows with stripped product names and UNIT_PRICE
SELECT * FROM clean_order_items LIMIT 10;

-- DLQ audit: Should be 0 if source data is clean (which profiling confirmed)
SELECT COUNT(*) AS quarantined_rows FROM clean_order_items_dlq;