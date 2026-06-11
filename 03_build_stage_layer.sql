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