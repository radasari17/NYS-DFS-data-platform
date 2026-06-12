
-- ==========================================
-- BRONZE LAYER (STAGE) — Categories
-- ==========================================
-- PURPOSE: Raw ingestion of category reference data.
-- All VARCHAR to prevent any ingestion failures from source CSV.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- Raw staging table — no type enforcement at this layer
CREATE OR REPLACE TABLE categories (
    category_id   VARCHAR,
    category_name VARCHAR
);

-- Stream captures all changes for downstream consumption
CREATE OR REPLACE STREAM categories_stm ON TABLE categories;

-- Ingest raw CSV data into bronze
COPY INTO categories
FROM @NYS_DFS_RETAIL.STAGE.csv_stage/categories.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;

-- ==========================================
-- SILVER LAYER (CLEAN) — Categories
-- ==========================================
-- PURPOSE: Type-safe, deduplicated, standardized reference data.
-- UNIQUE on category_name prevents duplicate naming confusion in dashboards.
-- ==========================================
USE SCHEMA CLEAN;

CREATE OR REPLACE TABLE categories (
    category_id   INT PRIMARY KEY,
    category_name VARCHAR NOT NULL UNIQUE,
    dw_loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stream BEFORE insert to capture baseline for Gold layer
CREATE OR REPLACE STREAM categories_stm ON TABLE categories;

-- Consume Stage Stream: cast types, standardize casing, deduplicate
INSERT INTO categories (category_id, category_name)
SELECT
    TRY_CAST(category_id AS INT),

    -- INITCAP safe here — category names are generic terms (Electronics, Clothing)
    -- No "McDonald" problem with product categories
    INITCAP(TRIM(REGEXP_REPLACE(category_name, '\\s+', ' ')))
FROM NYS_DFS_RETAIL.STAGE.categories_stm
WHERE METADATA$ACTION = 'INSERT'
    AND TRY_CAST(category_id AS INT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY category_id) = 1;

-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Categories
-- ==========================================
-- PURPOSE: Dimension table for star schema joins with fact tables.
-- SCD2 tracks category name changes (e.g., "Electronics" renamed to
-- "Consumer Electronics") so historical fact records still join correctly
-- to the category name that was active at the time of the transaction.
-- ==========================================
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE TABLE categories_dim (
    category_sk          INT AUTOINCREMENT PRIMARY KEY,  -- Surrogate key for fact table joins
    category_id          INT NOT NULL,                   -- Source business key
    category_name        VARCHAR NOT NULL,               -- Display name for reports
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Standard SCD Type 2 MERGE
-- No hybrid needed — categories only have one meaningful column (name)
-- Any change to category_name = new historical version
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.categories_dim AS target
USING NYS_DFS_RETAIL.CLEAN.categories_stm AS source
ON target.category_id = source.category_id AND target.is_current = TRUE

-- SCENARIO A: Category was renamed (close the old version)
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE'
    AND source.METADATA$ISUPDATE = TRUE
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO B: New category arrives (or new version of renamed category)
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT'
THEN INSERT (category_id, category_name, effective_start_date, effective_end_date, is_current, dw_loaded_at)
VALUES (
    source.category_id,
    source.category_name,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- Verify
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.categories_dim;