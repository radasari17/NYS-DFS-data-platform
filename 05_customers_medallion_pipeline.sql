-- ==========================================
-- Customers
-- BRONZE LAYER (STAGE)
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- 1. Build the raw Stage table
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR,
    city VARCHAR,
    signup_date VARCHAR,
    customer_name VARCHAR,
    dob VARCHAR,
    phone_number VARCHAR,
    address VARCHAR
);

-- 2. Attach the security camera BEFORE loading
CREATE OR REPLACE STREAM customers_stm ON TABLE customers;

-- 3. Ingest the raw data from the CSV
COPY INTO customers
FROM @NYS_DFS_RETAIL.STAGE.csv_stage/customers.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;


-- ==========================================
-- SILVER LAYER (CLEAN) — Customers (Fixed)
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- 1. Build the fully governed Clean table with PII awareness
CREATE OR REPLACE TABLE customers (
    customer_id    INT          PRIMARY KEY,
    city           VARCHAR      NOT NULL,
    signup_date    DATE         NOT NULL,
    customer_name  VARCHAR      NOT NULL,
    dob            DATE,
    phone_number   VARCHAR,
    address        VARCHAR,
    dw_loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- VALIDATION: Indian phone numbers are exactly 10 digits (Deterministic)
    CHECK (phone_number IS NULL OR LENGTH(phone_number) = 10)
);

-- 2. PII GOVERNANCE: Tag sensitive columns for downstream masking policies
COMMENT ON COLUMN customers.dob IS 'PII:SENSITIVE - Date of Birth';
COMMENT ON COLUMN customers.phone_number IS 'PII:SENSITIVE - Personal Phone Number';
COMMENT ON COLUMN customers.address IS 'PII:SENSITIVE - Residential Address';
COMMENT ON COLUMN customers.customer_name IS 'PII:IDENTIFIER - Full Name';

-- 3. CRITICAL: Attach the stream BEFORE inserting to capture the baseline!
CREATE OR REPLACE STREAM customers_stm ON TABLE customers;

-- 4. Consume the Stage Stream with Proactive Data Manipulation
INSERT INTO customers (customer_id, city, signup_date, customer_name, dob, phone_number, address)
SELECT
    TRY_CAST(customer_id AS INT),

    -- City: Standardize casing
    INITCAP(TRIM(city)),

    -- Signup Date: Enforce DD-MM-YYYY format
    TRY_TO_DATE(TRIM(signup_date), 'DD-MM-YYYY'),

    -- Name: Strip double-spaces but DO NOT use INITCAP to preserve complex names like "McDonald"
    TRIM(REGEXP_REPLACE(customer_name, '\\s+', ' ')),

    -- DOB: Enforce DD-MM-YYYY format
    TRY_TO_DATE(TRIM(dob), 'DD-MM-YYYY'),

    -- Phone: Keep only digits, validate exactly 10 digits for Indian numbers
    CASE
        WHEN TRIM(phone_number) IN ('', 'None', 'NA', 'N/A', 'null') THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(TRIM(phone_number), '[^0-9]', '')) != 10 THEN NULL
        ELSE REGEXP_REPLACE(TRIM(phone_number), '[^0-9]', '')
    END,

    -- Address: Strip double-spaces, null out junk
    CASE
        WHEN TRIM(address) IN ('', 'None', 'NA', 'N/A', 'null') THEN NULL
        ELSE TRIM(REGEXP_REPLACE(address, '\\s+', ' '))
    END

FROM NYS_DFS_RETAIL.STAGE.customers_stm
WHERE METADATA$ACTION = 'INSERT'
    AND TRY_CAST(customer_id AS INT) IS NOT NULL
    
    -- REJECT rows where the critical signup date fails to parse
    AND TRY_TO_DATE(TRIM(signup_date), 'DD-MM-YYYY') IS NOT NULL
    
    -- ADVANCED GOVERNANCE: Mathematically reject future signup dates
    AND TRY_TO_DATE(TRIM(signup_date), 'DD-MM-YYYY') <= CURRENT_DATE()
    
    -- ADVANCED GOVERNANCE: Customer must be born before they can sign up
    AND TRY_TO_DATE(TRIM(dob), 'DD-MM-YYYY') < TRY_TO_DATE(TRIM(signup_date), 'DD-MM-YYYY')
    
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1;

-- 5. Data Validation Check
SELECT * FROM NYS_DFS_RETAIL.CLEAN.customers LIMIT 10;
SELECT * FROM NYS_DFS_RETAIL.CLEAN.customers_stm  LIMIT 10;



-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Customers
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- 1. Build the Gold Customer Dimension Table
CREATE OR REPLACE TABLE customers_dim (
    customer_sk          INT AUTOINCREMENT PRIMARY KEY,
    customer_id          INT NOT NULL,
    city                 VARCHAR NOT NULL,
    signup_date          DATE NOT NULL,
    customer_name        VARCHAR NOT NULL,
    dob                  DATE,
    phone_number         VARCHAR,
    address              VARCHAR,
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. The Hybrid SCD1 & SCD2 MERGE
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.customers_dim AS target
USING (
    WITH stream_changes AS (
        SELECT
            new_row.customer_id,
            new_row.city,
            new_row.signup_date,
            new_row.customer_name,
            new_row.dob,
            new_row.phone_number,
            new_row.address,
            CASE
                WHEN old_row.customer_id IS NULL THEN 'INSERT'
                WHEN new_row.city != old_row.city
                  OR NVL(new_row.address, '') != NVL(old_row.address, '')
                THEN 'SCD2'
                ELSE 'SCD1'
            END AS change_type
        FROM NYS_DFS_RETAIL.CLEAN.customers_stm new_row
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.customers_stm old_row
            ON new_row.customer_id = old_row.customer_id
            AND old_row.METADATA$ACTION = 'DELETE'
        WHERE new_row.METADATA$ACTION = 'INSERT'
    )

    SELECT *, 'SCD1_OR_INSERT' AS merge_action
    FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')
    UNION ALL
    SELECT *, 'CLOSE_OLD' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'
    UNION ALL
    SELECT *, 'INSERT_NEW' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'

) AS source
ON target.customer_id = source.customer_id
    AND target.is_current = TRUE
    AND source.merge_action != 'INSERT_NEW'

-- SCENARIO A: SCD1 Update (phone/name/dob corrected) -> Overwrite in place
WHEN MATCHED
    AND source.change_type = 'SCD1'
THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.dob = source.dob,
    target.phone_number = source.phone_number,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- SCENARIO B: SCD2 Update (City/Address changed) -> Close the old record
WHEN MATCHED
    AND source.change_type = 'SCD2'
    AND source.merge_action = 'CLOSE_OLD'
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO C: New Record Arrives
WHEN NOT MATCHED
THEN INSERT (customer_id, city, signup_date, customer_name, dob, phone_number, address, effective_start_date, effective_end_date, is_current, dw_loaded_at)
VALUES (
    source.customer_id,
    source.city,
    source.signup_date,
    source.customer_name,
    source.dob,
    source.phone_number,
    source.address,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- ==========================================
-- 3. RBAC SECURE VIEW FOR DASHBOARDING
-- ==========================================
CREATE OR REPLACE SECURE VIEW customers_segmentation_vw AS
WITH Age_Calculated AS (
    SELECT 
        *,
        -- ACCURATE AGE: Calculated exactly ONCE to prevent massive code repetition
        DATEDIFF('YEAR', dob, CURRENT_DATE())
        - CASE
            WHEN MONTH(CURRENT_DATE()) < MONTH(dob)
              OR (MONTH(CURRENT_DATE()) = MONTH(dob) AND DAY(CURRENT_DATE()) < DAY(dob))
            THEN 1
            ELSE 0
          END AS exact_age
    FROM customers_dim
    WHERE is_current = TRUE 
)
SELECT
    customer_sk,
    customer_id,
    city,
    signup_date,

    -- DYNAMIC RBAC MASKING
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE') THEN customer_name
        ELSE '***MASKED_NAME***'
    END AS secure_customer_name,

    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE') THEN phone_number
        ELSE CONCAT('***-***-', RIGHT(phone_number, 4))
    END AS secure_phone_number,

    -- Pass the cleanly calculated age to the dashboard
    exact_age AS current_age,

    -- DYNAMIC SEGMENTATION: Proactively handling NULLs to prevent skewed reporting!
    CASE
        WHEN exact_age IS NULL THEN 'Unknown'
        WHEN exact_age < 18 THEN 'Under 18 (Minor)'
        WHEN exact_age BETWEEN 18 AND 25 THEN '18-25 (Gen Z)'
        WHEN exact_age BETWEEN 26 AND 40 THEN '26-40 (Millennials)'
        WHEN exact_age BETWEEN 41 AND 60 THEN '41-60 (Gen X)'
        ELSE '60+ (Seniors)'
    END AS age_segment,

    is_current
FROM Age_Calculated;

-- 4. Verify
SELECT * FROM customers_segmentation_vw LIMIT 10;