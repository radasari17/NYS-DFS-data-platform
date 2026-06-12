USE DATABASE NYS_DFS_RETAIL;

-- ==========================================
-- BRONZE LAYER (STAGE) — Employees
-- ==========================================
USE SCHEMA STAGE;

-- 1. Build the raw Stage table
CREATE OR REPLACE TABLE employees (
    employee_id VARCHAR,
    store_id VARCHAR,
    salary VARCHAR,
    employee_name VARCHAR,
    address VARCHAR,
    pan_no VARCHAR,
    phone_no VARCHAR,
    department VARCHAR,
    role VARCHAR
);

-- 2. Attach the security camera BEFORE loading
CREATE OR REPLACE STREAM employees_stm ON TABLE employees;

-- 3. Ingest the raw data from the CSV
COPY INTO employees
FROM @NYS_DFS_RETAIL.STAGE.csv_stage/employees.csv
FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;


-- ==========================================
-- SILVER LAYER (CLEAN) — Employees
-- ==========================================
-- PURPOSE: Transform raw VARCHAR stage data into typed, validated, PII-tagged
-- records ready for downstream consumption. This layer acts as the data quality
-- gatekeeper — only clean, deduplicated, format-validated records pass through.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- TABLE DESIGN: Strict types + constraints enforce data quality at the DDL level.
-- Any INSERT that violates these rules will be rejected by Snowflake automatically.
CREATE OR REPLACE TABLE employees (
    employee_id   INT PRIMARY KEY,       -- Uniqueness + NOT NULL enforced natively
    store_id      INT NOT NULL,          -- FK to stores — every employee must belong to a store
    salary        INT,                   -- Nullable: new hires may not have salary assigned yet
    employee_name VARCHAR NOT NULL,      -- Identity column — can never be empty
    address       VARCHAR,               -- Nullable: some employees may not disclose
    pan_no        VARCHAR,               -- Nullable: contract workers may not have PAN
    phone_no      VARCHAR,               -- Nullable: some may opt out of sharing
    department    VARCHAR,               -- Organizational grouping
    role          VARCHAR,               -- Job title within department
    dw_loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- Audit: when ETL wrote this row

    -- CONSTRAINT: Salary can never be negative (logical impossibility)
    CHECK (salary >= 0),

    -- CONSTRAINT: PAN must match Indian govt format — 5 uppercase letters,
    -- 4 digits (0-9), 1 uppercase letter. Example: ABCDE1234F
    -- This catches garbage data that merely "looks like" a 10-char string
    CHECK (pan_no IS NULL OR pan_no REGEXP '^[A-Z]{5}[0-9]{4}[A-Z]$'),

    -- CONSTRAINT: Indian mobile numbers are exactly 10 digits after normalization
    CHECK (phone_no IS NULL OR LENGTH(phone_no) = 10)
);

-- PII TAGGING: Column-level metadata for automated masking policy detection.
-- Downstream, a dynamic masking policy can scan these comments and auto-apply
-- masking rules based on sensitivity level without manual column-by-column config.
COMMENT ON COLUMN employees.employee_name IS 'PII:IDENTIFIER - Full Name';
COMMENT ON COLUMN employees.pan_no IS 'PII:HIGHLY_SENSITIVE - Government Tax ID';
COMMENT ON COLUMN employees.salary IS 'PII:SENSITIVE - Compensation Data';
COMMENT ON COLUMN employees.phone_no IS 'PII:SENSITIVE - Personal Phone Number';
COMMENT ON COLUMN employees.address IS 'PII:SENSITIVE - Residential Address';

-- STREAM: Created BEFORE the INSERT so it captures the initial baseline load.
-- This stream feeds the Gold layer MERGE for incremental SCD processing.
CREATE OR REPLACE STREAM employees_stm ON TABLE employees;

-- DATA TRANSFORMATION: Each column gets purpose-built cleaning logic.
-- Philosophy: Fix what's fixable, NULL what's invalid, reject what's dangerous.
INSERT INTO employees (employee_id, store_id, salary, employee_name, address, pan_no, phone_no, department, role)
SELECT
    -- IDENTITY: Safe cast — if employee_id isn't a valid integer, TRY_CAST
    -- returns NULL and the WHERE clause below rejects the entire row
    TRY_CAST(employee_id AS INT),

    -- FOREIGN KEY: Must be a valid integer pointing to an existing store
    TRY_CAST(store_id AS INT),

    -- COMPENSATION: Cast to integer — salary is always whole rupees in source
    TRY_CAST(salary AS INT),

    -- NAME CLEANING: Collapse multiple spaces ("Alok  Pathak" → "Alok Pathak")
    -- but preserve original casing (no INITCAP — avoids "McDonald" → "Mcdonald")
    TRIM(REGEXP_REPLACE(employee_name, '\\s+', ' ')),

    -- ADDRESS CLEANING: Two-step defense:
    -- Step 1: If the value is a known placeholder string, convert to proper NULL
    -- Step 2: If valid, collapse double-spaces for consistency
    CASE
        WHEN TRIM(address) IN ('', 'None', 'NA', 'N/A', 'null') THEN NULL
        ELSE TRIM(REGEXP_REPLACE(address, '\\s+', ' '))
    END,

    -- PAN VALIDATION: Strict regex enforces the Indian Income Tax format.
    -- Format: [A-Z]{5}[0-9]{4}[A-Z] (e.g., QAHPP2768C)
    -- Invalid PANs are NULLed — better to have no data than wrong data
    -- for a government tax identifier
    CASE
        WHEN UPPER(TRIM(pan_no)) REGEXP '^[A-Z]{5}[0-9]{4}[A-Z]$'
        THEN UPPER(TRIM(pan_no))
        ELSE NULL
    END,

    -- PHONE NORMALIZATION: Three-tier validation:
    -- Tier 1: Reject known placeholder strings → NULL
    -- Tier 2: Strip all non-digit chars, check if exactly 10 digits remain
    -- Tier 3: If valid, store only the 10 clean digits (no +91, no dashes)
    CASE
        WHEN TRIM(phone_no) IN ('', 'None', 'NA', 'N/A', 'null') THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(TRIM(phone_no), '[^0-9]', '')) != 10 THEN NULL
        ELSE REGEXP_REPLACE(TRIM(phone_no), '[^0-9]', '')
    END,

    -- DEPARTMENT: Forced UPPERCASE ensures "Sales", "sales", "SALES" all
    -- become one consistent group — prevents dashboard fragmentation
    UPPER(TRIM(REGEXP_REPLACE(department, '\\s+', ' '))),

    -- ROLE: INITCAP for readability in reports ("Key Account Manager")
    -- Safe here because job titles don't have the "McDonald" problem
    INITCAP(TRIM(REGEXP_REPLACE(role, '\\s+', ' ')))

FROM NYS_DFS_RETAIL.STAGE.employees_stm

-- ROW-LEVEL FILTERS: These act as the final gatekeeper.
-- Any row failing these conditions is silently excluded — no error, no partial insert.
WHERE METADATA$ACTION = 'INSERT'                    -- Only process new stream records
    AND TRY_CAST(employee_id AS INT) IS NOT NULL    -- Reject: no valid ID = no identity
    AND TRY_CAST(store_id AS INT) IS NOT NULL       -- Reject: orphan employees
    AND TRY_CAST(salary AS INT) >= 0                -- Reject: negative salary = corrupt data

-- DEDUPLICATION: If the source accidentally has two rows with the same employee_id,
-- keep only one. PARTITION BY employee_id ensures uniqueness without failing the PK.
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY employee_id) = 1;


-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Employees
-- ==========================================
-- PURPOSE: Enterprise-grade dimension table with hybrid SCD tracking and
-- role-based access control. Provides historical versioning for business-critical
-- changes while allowing operational corrections without cluttering history.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- DIMENSION TABLE: The single source of truth for employee analytics.
-- Surrogate key (employee_sk) is the join key for fact tables — never the business key,
-- because SCD2 creates multiple rows per employee.
CREATE OR REPLACE TABLE employees_dim (
    employee_sk          INT AUTOINCREMENT PRIMARY KEY, -- Warehouse identity (fact tables join here)
    employee_id          INT NOT NULL,                  -- Source business key (for stream matching)
    store_id             INT NOT NULL,                  -- FK to stores_dim (which store they work at)
    salary               INT,                           -- Compensation (SCD2 tracked — raises matter)
    employee_name        VARCHAR NOT NULL,              -- Identity (SCD1 — spelling fixes don't need history)
    address              VARCHAR,                       -- Residential (SCD2 — relocation is significant)
    pan_no               VARCHAR,                       -- Tax ID (SCD1 — corrections overwrite)
    phone_no             VARCHAR,                       -- Contact (SCD1 — updates overwrite)
    department           VARCHAR,                       -- Org unit (SCD2 — transfers are tracked)
    role                 VARCHAR,                       -- Job title (SCD2 — promotions are tracked)
    effective_start_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- HYBRID SCD MERGE: Intelligently routes changes to either:
-- SCD1 (overwrite) for operational corrections that don't need audit trail
-- SCD2 (version) for business events that finance/HR needs to track historically
MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.employees_dim AS target
USING (
    -- CTE: Self-join the stream to compare old vs new values and classify the change
    WITH stream_changes AS (
        SELECT
            new_row.employee_id,
            new_row.store_id,
            new_row.salary,
            new_row.employee_name,
            new_row.address,
            new_row.pan_no,
            new_row.phone_no,
            new_row.department,
            new_row.role,

            -- CHANGE DETECTION ENGINE: Compares every column to determine change type
            -- NVL handles NULL-safe comparison (NULL != NULL would always be true without it)
            CASE
                -- No old row exists = brand new employee
                WHEN old_row.employee_id IS NULL THEN 'INSERT'

                -- Business-critical columns changed = preserve history (SCD2)
                -- These changes have financial, legal, or organizational significance
                WHEN NVL(new_row.salary, -1) != NVL(old_row.salary, -1)          -- Pay raise/cut
                  OR NVL(new_row.department, '') != NVL(old_row.department, '')   -- Department transfer
                  OR NVL(new_row.role, '') != NVL(old_row.role, '')               -- Promotion/demotion
                  OR NVL(new_row.store_id, -1) != NVL(old_row.store_id, -1)      -- Store reassignment
                  OR NVL(new_row.address, '') != NVL(old_row.address, '')         -- Relocation
                THEN 'SCD2'

                -- Everything else (name typo fix, phone update, PAN correction) = overwrite (SCD1)
                ELSE 'SCD1'
            END AS change_type

        FROM NYS_DFS_RETAIL.CLEAN.employees_stm new_row
        -- Self-join: Match the INSERT (new values) with the DELETE (old values)
        -- Snowflake streams represent an UPDATE as a DELETE + INSERT pair
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.employees_stm old_row
            ON new_row.employee_id = old_row.employee_id
            AND old_row.METADATA$ACTION = 'DELETE'
        WHERE new_row.METADATA$ACTION = 'INSERT'
    )

    -- ROW DUPLICATION TRICK: A single MERGE row can only trigger one action.
    -- For SCD2, we need TWO actions (close old + insert new), so we duplicate the row.

    -- Pass-through: SCD1 corrections and fresh inserts go directly to matching
    SELECT *, 'SCD1_OR_INSERT' AS merge_action
    FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')

    UNION ALL

    -- Copy 1 of SCD2: This will MATCH the existing row and close it
    SELECT *, 'CLOSE_OLD' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'

    UNION ALL

    -- Copy 2 of SCD2: This will NOT MATCH (forced by ON clause) and insert as new version
    SELECT *, 'INSERT_NEW' AS merge_action
    FROM stream_changes WHERE change_type = 'SCD2'

) AS source

-- ON CLAUSE: Three conditions work together:
-- 1. Match on business key (employee_id)
-- 2. Only match the CURRENT active row (don't touch historical versions)
-- 3. Force INSERT_NEW rows to never match (they must fall to NOT MATCHED)
ON target.employee_id = source.employee_id
    AND target.is_current = TRUE
    AND source.merge_action != 'INSERT_NEW'

-- SCENARIO A: SCD1 — Operational correction (name typo, phone update, PAN fix)
-- Action: Overwrite in place. No history needed. Row stays current.
WHEN MATCHED
    AND source.change_type = 'SCD1'
THEN UPDATE SET
    target.employee_name = source.employee_name,
    target.pan_no = source.pan_no,
    target.phone_no = source.phone_no,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- SCENARIO B: SCD2 — Business event (raise, promotion, transfer, relocation)
-- Action: Close the old version. Mark as historical. Stamp end date.
WHEN MATCHED
    AND source.change_type = 'SCD2'
    AND source.merge_action = 'CLOSE_OLD'
THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE

-- SCENARIO C: New record (brand new hire, or the new active version from SCD2)
-- Action: Insert with is_current = TRUE and far-future end date
WHEN NOT MATCHED
THEN INSERT (employee_id, store_id, salary, employee_name, address, pan_no, phone_no, department, role, effective_start_date, effective_end_date, is_current, dw_loaded_at)
VALUES (
    source.employee_id,
    source.store_id,
    source.salary,
    source.employee_name,
    source.address,
    source.pan_no,
    source.phone_no,
    source.department,
    source.role,
    CURRENT_TIMESTAMP(),
    '9999-12-31'::TIMESTAMP_NTZ,
    TRUE,
    CURRENT_TIMESTAMP()
);

-- ==========================================
-- 3. HIGHLY SECURE RBAC VIEW FOR DASHBOARDING
-- ==========================================
-- PURPOSE: Presentation layer that dynamically masks PII based on the querying
-- user's role. Analysts see masked data. HR/Compliance see full PII.
-- This replaces the need for separate tables or manual access requests.
-- ==========================================
CREATE OR REPLACE SECURE VIEW employees_secure_vw AS
SELECT
    employee_sk,
    employee_id,
    store_id,
    department,
    role,

    -- NAME MASKING: Full name visible only to privileged roles
    -- Operations managers see masked — they don't need individual identity
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE', 'HR_ROLE')
        THEN employee_name
        ELSE '***MASKED_NAME***'
    END AS secure_employee_name,

    -- ADDRESS MASKING: Residential address is sensitive PII
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE', 'HR_ROLE')
        THEN address
        ELSE '***MASKED_ADDRESS***'
    END AS secure_address,

    -- PHONE MASKING: Show last 4 digits for partial identification
    -- NULL handling prevents CONCAT from producing garbage output
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE', 'HR_ROLE')
        THEN phone_no
        WHEN phone_no IS NULL THEN 'NOT_ON_FILE'
        ELSE CONCAT('***-***-', RIGHT(phone_no, 4))
    END AS secure_phone_no,

    -- PAN MASKING: Show first 2 + last 2 chars for partial verification
    -- This lets HR confirm "is this the right person?" without full exposure
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE', 'HR_ROLE')
        THEN pan_no
        WHEN pan_no IS NULL THEN 'NOT_ON_FILE'
        ELSE CONCAT(LEFT(pan_no, 2), '******', RIGHT(pan_no, 2))
    END AS secure_pan_no,

    -- SALARY MASKING: Completely hidden from non-privileged roles
    -- Even partial salary data can cause workplace friction — total blackout
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_ROLE', 'HR_ROLE')
        THEN salary
        ELSE NULL
    END AS secure_salary,

    -- SALARY BANDING: Visible to ALL roles — safe for workforce planning
    -- Operations managers can see distribution without exact figures
    -- No PII risk: knowing "60% of staff are Band B" reveals nothing individual
    CASE
        WHEN salary IS NULL THEN 'Unknown'
        WHEN salary < 25000 THEN 'Band A (< 25K)'
        WHEN salary BETWEEN 25000 AND 50000 THEN 'Band B (25-50K)'
        WHEN salary BETWEEN 50001 AND 75000 THEN 'Band C (50-75K)'
        ELSE 'Band D (75K+)'
    END AS salary_band,

    is_current
FROM employees_dim
-- ACTIVE FILTER: Analysts only see current records
-- Historical versions are accessible via the base table for auditors
WHERE is_current = TRUE;

-- View the secure RBAC view (shows masked/unmasked data based on your current role)
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.employees_secure_vw LIMIT 20;

-- View the raw Gold dimension table (all versions including historical SCD2 records)
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.employees_dim LIMIT 20;

USE ROLE SYSADMIN;
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.employees_secure_vw LIMIT 20;