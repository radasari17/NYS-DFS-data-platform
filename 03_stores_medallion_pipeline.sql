USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- 1. Create the Stage table for Stores
CREATE TABLE stores (
    store_id VARCHAR,
    city VARCHAR,
    address VARCHAR,
    pincode VARCHAR,
    store_manager VARCHAR,
    no_of_employees VARCHAR
);

-- 2. Create the Stream (The Security Camera)
CREATE STREAM stores_stm ON TABLE stores;

-- 3. Unpack the box from the root loading dock
COPY INTO stores
FROM @csv_stage/stores.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;


USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- 1. Build the fully governed Clean table
CREATE OR REPLACE TABLE stores (
    store_id        INT          PRIMARY KEY,
    city            VARCHAR      NOT NULL,
    address         VARCHAR,
    pincode         INT          CHECK (pincode BETWEEN 110000 AND 999999),
    store_manager   VARCHAR,
    no_of_employees INT          CHECK (no_of_employees >= 0),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (address, pincode)
);

-- 2. Create the Stream BEFORE inserting data to capture the initial baseline
CREATE OR REPLACE STREAM stores_stm ON TABLE stores;

-- 3. Consume the Stage Stream with dynamic data manipulation
INSERT INTO stores (store_id, city, address, pincode, store_manager, no_of_employees)
SELECT
    TRY_CAST(store_id AS INT),
    INITCAP(TRIM(city)),
    TRIM(address),
    TRY_CAST(pincode AS INT),
    COALESCE(INITCAP(TRIM(store_manager)), 'Unassigned'),
    TRY_CAST(no_of_employees AS INT)
FROM NYS_DFS_RETAIL.STAGE.stores_stm
WHERE METADATA$ACTION = 'INSERT' 
  AND TRY_CAST(store_id AS INT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) = 1;
2. The Gold Layer (Consumption)
Copy this exact block and save it into your 05_build_consumption_layer.sql file:
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- 1. Build the highly governed Gold Dimension Table
CREATE OR REPLACE TABLE stores_dim (
    store_sk INT AUTOINCREMENT PRIMARY KEY, 
    store_id INT,                           
    city VARCHAR NOT NULL,                  
    address VARCHAR,
    pincode INT,
    store_manager VARCHAR,
    no_of_employees INT,
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,         
    dw_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()                
);

-- 2. The Automated Incremental MERGE (SCD Type 2)
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.stores_dim AS target
USING NYS_DFS_RETAIL.CLEAN.stores_stm AS source
ON target.store_id = source.store_id AND target.is_current = TRUE

-- SCENARIO A: A record was updated in the source (SCD2 End-Dating)
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE'
    AND source.METADATA$ISUPDATE = TRUE
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO B: A brand new record arrives (or the new version of an updated record)
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT'
THEN INSERT (
    store_id, 
    city, 
    address, 
    pincode, 
    store_manager, 
    no_of_employees, 
    effective_start_date, 
    effective_end_date, 
    is_current, 
    dw_loaded_at
)
VALUES (
    source.store_id,
    source.city,
    source.address,
    source.pincode,
    source.store_manager,
    source.no_of_employees,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);0303