
-- ==========================================
-- BRONZE LAYER (STAGE) — Promotions
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

CREATE OR REPLACE TABLE promotions (
    promotion_id VARCHAR,
    discount     VARCHAR
);

CREATE OR REPLACE STREAM promotions_stm ON TABLE promotions;

COPY INTO promotions
FROM @NYS_DFS_RETAIL.STAGE.csv_stage/promotions.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;

-- ==========================================
-- SILVER LAYER (CLEAN) — Promotions
-- ==========================================
USE SCHEMA CLEAN;

-- 1. Build the fully governed Clean table
CREATE OR REPLACE TABLE promotions (
    promotion_id INT PRIMARY KEY,
    discount     INT,
    dw_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CHECK (discount BETWEEN 0 AND 100)
);

-- 2. Attach the stream BEFORE inserting to capture the baseline
CREATE OR REPLACE STREAM promotions_stm ON TABLE promotions;

-- 3. Consume the Stage Stream
INSERT INTO promotions (promotion_id, discount)
SELECT
    TRY_CAST(promotion_id AS INT),
    TRY_CAST(discount AS INT)
FROM NYS_DFS_RETAIL.STAGE.promotions_stm
WHERE METADATA$ACTION = 'INSERT'
    AND TRY_CAST(promotion_id AS INT) IS NOT NULL
    AND TRY_CAST(discount AS INT) BETWEEN 0 AND 100
QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_id ORDER BY promotion_id) = 1;

-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Promotions
-- ==========================================
USE SCHEMA CONSUMPTION;

-- 1. Build the Gold Dimension Table
CREATE OR REPLACE TABLE promotions_dim (
    promotion_sk         INT AUTOINCREMENT PRIMARY KEY,
    promotion_id         INT NOT NULL,
    discount             INT,
    discount_tier        VARCHAR,
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. The Automated Incremental MERGE (SCD Type 2)
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.promotions_dim AS target
USING (
    SELECT 
        promotion_id,
        discount,
        -- DYNAMIC MAPPING: Paving the cow paths and handling NULLs proactively
        CASE
            WHEN discount IS NULL THEN 'Unknown'
            WHEN discount <= 10 THEN 'Low (1-10%)'
            WHEN discount <= 25 THEN 'Medium (11-25%)'
            ELSE 'High (26%+)'
        END AS discount_tier,
        METADATA$ACTION AS action,
        METADATA$ISUPDATE AS is_update
    FROM NYS_DFS_RETAIL.CLEAN.promotions_stm
) AS source
ON target.promotion_id = source.promotion_id AND target.is_current = TRUE

WHEN MATCHED
    AND source.action = 'DELETE'
    AND source.is_update = TRUE
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

WHEN NOT MATCHED
    AND source.action = 'INSERT'
THEN INSERT (
    promotion_id, 
    discount, 
    discount_tier,
    effective_start_date, 
    effective_end_date, 
    is_current, 
    dw_loaded_at
)
VALUES (
    source.promotion_id,
    source.discount,
    source.discount_tier,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- 3. Verify
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.promotions_dim LIMIT 10;