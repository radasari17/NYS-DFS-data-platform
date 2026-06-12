-- ==========================================
-- Suppliers
-- BRONZE LAYER (STAGE)
-- ==========================================



USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- 1. Build the raw Stage table
CREATE OR REPLACE TABLE suppliers (
    supplier_id    VARCHAR,
    country        VARCHAR,
    vendor_name    VARCHAR,
    vendor_contact VARCHAR,
    email_address  VARCHAR,
    address        VARCHAR
);

-- 2. Attach the stream before loading
CREATE STREAM suppliers_stm ON TABLE suppliers;

-- 3. Load raw data
COPY INTO suppliers
FROM @csv_stage/suppliers.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;

-- 4. Verify
SELECT * FROM suppliers LIMIT 5;
SELECT * FROM suppliers_stm LIMIT 5;


-- ==========================================
-- SILVER LAYER (CLEAN)
-- ==========================================

USE SCHEMA CLEAN;

-- 1. Build the fully governed Clean table
CREATE OR REPLACE TABLE suppliers (
    supplier_id    INT          PRIMARY KEY,
    country        VARCHAR      NOT NULL,
    vendor_name    VARCHAR      NOT NULL,
    vendor_contact VARCHAR,
    email_address  VARCHAR      UNIQUE,
    address        VARCHAR,
    dw_loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CHECK (email_address REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$')
);

-- 2. CRITICAL: Attach the stream BEFORE inserting to capture the baseline!
CREATE OR REPLACE STREAM suppliers_stm ON TABLE suppliers;

-- 3. Consume the Stage Stream with Proactive Data Manipulation
INSERT INTO suppliers (supplier_id, country, vendor_name, vendor_contact, email_address, address)
SELECT
    TRY_CAST(supplier_id AS INT),
    UPPER(TRIM(country)),

    -- Strip hidden double-spaces but preserve original casing for company names
    TRIM(REGEXP_REPLACE(vendor_name, '\\s+', ' ')),

    -- Normalize phone: keep only digits and leading '+'
    CASE
        WHEN TRIM(vendor_contact) IN ('', 'None', 'NA', 'N/A') THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(TRIM(vendor_contact), '[^0-9]', '')) < 10 THEN NULL
        ELSE REGEXP_REPLACE(TRIM(vendor_contact), '[^0-9+]', '')
    END,

    -- Deep Regex validation before casting to lowercase
    CASE
        WHEN LOWER(TRIM(email_address)) REGEXP '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}$'
        THEN LOWER(TRIM(email_address))
        ELSE NULL
    END,

    -- Strip double-spaces and null out junk values
    CASE
        WHEN TRIM(address) IN ('', 'None', 'NA', 'N/A', 'null') THEN NULL
        ELSE TRIM(REGEXP_REPLACE(address, '\\s+', ' '))
    END
FROM NYS_DFS_RETAIL.STAGE.suppliers_stm
WHERE METADATA$ACTION = 'INSERT'
    AND TRY_CAST(supplier_id AS INT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY supplier_id) = 1;


SELECT * FROM NYS_DFS_RETAIL.CLEAN.suppliers_stm

SELECT * FROM NYS_DFS_RETAIL.CLEAN.suppliers;



-- ==========================================
-- Gold LAYER (Consumption)
-- ==========================================

USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- 1. Build the Gold Supplier Dimension (SCD Type 1 & 2 Hybrid)
CREATE OR REPLACE TABLE suppliers_dim (
    supplier_sk          INT AUTOINCREMENT PRIMARY KEY,
    supplier_id          INT NOT NULL,
    country              VARCHAR NOT NULL,
    country_code         VARCHAR(2) NOT NULL,
    is_domestic          BOOLEAN NOT NULL,
    vendor_name          VARCHAR NOT NULL,
    vendor_contact       VARCHAR,
    email_address        VARCHAR,
    address              VARCHAR,
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. The Hybrid SCD1 & SCD2 MERGE
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.suppliers_dim AS target
USING (
    WITH stream_changes AS (
        SELECT
            new_row.supplier_id,
            new_row.country,
            CASE
                WHEN new_row.country = 'INDIA' THEN 'IN'
                WHEN new_row.country = 'USA'   THEN 'US'
                WHEN new_row.country = 'CHINA' THEN 'CN'
                ELSE 'XX'
            END AS country_code,
            CASE 
            WHEN new_row.country = 'INDIA' THEN TRUE ELSE FALSE END AS is_domestic,
            new_row.vendor_name,
            new_row.vendor_contact,
            new_row.email_address,
            new_row.address,
            CASE
                WHEN old_row.supplier_id IS NULL THEN 'INSERT'
                WHEN new_row.vendor_name != old_row.vendor_name
                  OR new_row.country != old_row.country
                  OR NVL(new_row.address, '') != NVL(old_row.address, '')
                THEN 'SCD2'
                ELSE 'SCD1'
            END AS change_type
        FROM NYS_DFS_RETAIL.CLEAN.suppliers_stm new_row
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.suppliers_stm old_row
            ON new_row.supplier_id = old_row.supplier_id
            AND old_row.METADATA$ACTION = 'DELETE'
        WHERE new_row.METADATA$ACTION = 'INSERT'
    )

    -- SCD1 updates and fresh inserts
    SELECT *, 'SCD1_OR_INSERT' AS merge_action
    FROM stream_changes
    WHERE change_type IN ('SCD1', 'INSERT')

    UNION ALL

    -- SCD2: close old version
    SELECT *, 'CLOSE_OLD' AS merge_action
    FROM stream_changes
    WHERE change_type = 'SCD2'

    UNION ALL

    -- SCD2: insert new version
    SELECT *, 'INSERT_NEW' AS merge_action
    FROM stream_changes
    WHERE change_type = 'SCD2'

) AS source
ON target.supplier_id = source.supplier_id
    AND target.is_current = TRUE
    AND source.merge_action != 'INSERT_NEW'

-- SCENARIO A: SCD1 (phone/email changed) -> Overwrite in place
WHEN MATCHED
    AND source.change_type = 'SCD1'
THEN UPDATE SET
    target.vendor_contact = source.vendor_contact,
    target.email_address = source.email_address,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- SCENARIO B: SCD2 (name/country/address changed) -> Close old record
WHEN MATCHED
    AND source.change_type = 'SCD2'
    AND source.merge_action = 'CLOSE_OLD'
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO C: New record (brand new supplier or new SCD2 version)
WHEN NOT MATCHED
THEN INSERT (
    supplier_id, country, country_code, is_domestic, vendor_name,
    vendor_contact, email_address, address,
    effective_start_date, effective_end_date, is_current, dw_loaded_at
)
VALUES (
    source.supplier_id,
    source.country,
    source.country_code,
    source.is_domestic,
    source.vendor_name,
    source.vendor_contact,
    source.email_address,
    source.address,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- 3. Verify
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.suppliers_dim LIMIT 10;
