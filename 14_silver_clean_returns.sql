-- ==========================================
-- SILVER LAYER (CLEAN) — Returns (with DLQ)
-- ==========================================
-- PURPOSE: Transform raw VARCHAR return data into typed, validated records.
-- Financial precision: DECIMAL(10,2) for refund amounts.
-- Strict validation: Only returns with positive refund amounts reach clean table.
-- Zero/negative refunds routed to DLQ for finance team audit.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- CLEAN TABLE: Validated return records with financial precision.
-- ORDER_ITEM_ID links back to the specific line item that was returned.
CREATE OR REPLACE TABLE clean_returns (
    RETURN_ID     INT PRIMARY KEY,             -- Unique return identity
    ORDER_ITEM_ID INT NOT NULL,                -- FK to clean_order_items (which item was returned)
    REFUND        DECIMAL(10,2) NOT NULL,      -- Refund amount (must be positive)
    dw_loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- DLQ TABLE: Catches invalid return records.
-- Common issues: POS glitch sending $0 refund, corrupted IDs from network timeout.
CREATE OR REPLACE TABLE clean_returns_dlq (
    RETURN_ID      VARCHAR,
    ORDER_ITEM_ID  VARCHAR,
    REFUND         VARCHAR,
    quarantined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rejection_reason VARCHAR
);

-- STREAM: Captures baseline for Gold fact table consumption
CREATE OR REPLACE STREAM clean_returns_stm ON TABLE clean_returns;

-- ==========================================
-- 4. SINGLE-READ STAGING: Snapshot the stream ONCE
-- ==========================================
-- Read the entire Bronze stream into a temp table.
-- Both DLQ and MERGE read from this snapshot — prevents stream double-consumption.
CREATE OR REPLACE TEMPORARY TABLE returns_staging AS
SELECT *, METADATA$ACTION AS action
FROM NYS_DFS_RETAIL.STAGE.raw_returns_stm;

-- ==========================================
-- 5. FORK — Route INVALID records to DLQ
-- ==========================================
-- Financial records are never silently dropped.
-- If a POS glitch sends a $0 refund or a corrupted ID,
-- it lands here with a reason for the finance team to investigate.
INSERT INTO clean_returns_dlq (RETURN_ID, ORDER_ITEM_ID, REFUND, rejection_reason)
SELECT
    RETURN_ID,
    ORDER_ITEM_ID,
    REFUND,

    -- REJECTION REASON: Identifies the specific validation failure
    CASE
        WHEN TRY_CAST(RETURN_ID AS INT) IS NULL THEN 'INVALID_RETURN_ID'
        WHEN TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL THEN 'INVALID_ORDER_ITEM_ID'
        WHEN TRY_CAST(REFUND AS DECIMAL(10,2)) IS NULL THEN 'NON_NUMERIC_REFUND'
        WHEN TRY_CAST(REFUND AS DECIMAL(10,2)) <= 0 THEN 'ZERO_OR_NEGATIVE_REFUND'
        ELSE 'UNKNOWN_REJECTION'
    END

FROM returns_staging
WHERE action = 'INSERT'
    AND (
        TRY_CAST(RETURN_ID AS INT) IS NULL
        OR TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL
        OR TRY_CAST(REFUND AS DECIMAL(10,2)) IS NULL
        OR TRY_CAST(REFUND AS DECIMAL(10,2)) <= 0
    );

-- ==========================================
-- 6. FORK — MERGE VALID records into Clean table
-- ==========================================
-- MERGE guarantees idempotency: safe to re-run without duplicating returns.
-- Only returns with valid IDs and positive refund amounts pass through.
MERGE INTO clean_returns target
USING (
    SELECT
        TRY_CAST(RETURN_ID AS INT) AS RETURN_ID,
        TRY_CAST(ORDER_ITEM_ID AS INT) AS ORDER_ITEM_ID,

        -- Financial precision: DECIMAL(10,2) ensures ₹500.50 stays ₹500.50
        TRY_CAST(REFUND AS DECIMAL(10,2)) AS REFUND

    FROM returns_staging
    WHERE action = 'INSERT'
        -- Identity validation
        AND TRY_CAST(RETURN_ID AS INT) IS NOT NULL
        AND TRY_CAST(ORDER_ITEM_ID AS INT) IS NOT NULL
        -- Financial validation: only positive refunds are legitimate
        AND TRY_CAST(REFUND AS DECIMAL(10,2)) IS NOT NULL
        AND TRY_CAST(REFUND AS DECIMAL(10,2)) > 0

    -- DEDUPLICATION: If source has duplicate RETURN_IDs, keep only one
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST(RETURN_ID AS INT)
        ORDER BY TRY_CAST(RETURN_ID AS INT)
    ) = 1

) source
ON target.RETURN_ID = source.RETURN_ID

-- MATCHED: Return already exists — late-arriving correction
WHEN MATCHED THEN UPDATE SET
    target.ORDER_ITEM_ID = source.ORDER_ITEM_ID,
    target.REFUND = source.REFUND,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- NOT MATCHED: Brand new return record
WHEN NOT MATCHED THEN INSERT (RETURN_ID, ORDER_ITEM_ID, REFUND)
VALUES (
    source.RETURN_ID,
    source.ORDER_ITEM_ID,
    source.REFUND
);

-- ==========================================
-- 7. VERIFY
-- ==========================================
-- Expected: 30K clean returns, 0 quarantined
SELECT 'CLEAN_RETURNS' AS table_name, COUNT(*) AS row_count FROM clean_returns
UNION ALL SELECT 'QUARANTINE_DLQ', COUNT(*) FROM clean_returns_dlq;