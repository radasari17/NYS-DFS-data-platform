-- ==========================================
-- SILVER LAYER (CLEAN) — Payments (with DLQ)
-- ==========================================
-- PURPOSE: Transform raw VARCHAR payment data into typed, validated records.
-- Financial precision: DECIMAL(10,2) prevents floating-point drift in revenue calcs.
-- Strict validation: Only payments with positive amounts reach the clean table.
-- Zero/negative amounts routed to DLQ for finance team audit.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- CLEAN TABLE: Financial records with strict precision.
-- PAYMENT_AMOUNT uses DECIMAL(10,2) — same standard as products.price
-- and order_items.TOTAL_TRANSACTION for consistent downstream joins.
CREATE OR REPLACE TABLE clean_payments (
    PAYMENT_ID     INT PRIMARY KEY,            -- Unique payment identity
    ORDER_ID       INT NOT NULL,               -- FK to clean_orders (every payment ties to an order)
    PAYMENT_AMOUNT DECIMAL(10,2) NOT NULL,     -- Revenue amount (must be positive)
    dw_loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- Audit timestamp
);

-- DLQ TABLE: Catches payments that fail validation.
-- Common POS glitches: $0.00 payments, negative refunds mis-routed as payments,
-- or corrupted IDs from network timeouts during card processing.
CREATE OR REPLACE TABLE clean_payments_dlq (
    PAYMENT_ID       VARCHAR,
    ORDER_ID         VARCHAR,
    PAYMENT_AMOUNT   VARCHAR,
    quarantined_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rejection_reason VARCHAR
);

-- STREAM: Captures baseline for Gold fact table consumption
CREATE OR REPLACE STREAM clean_payments_stm ON TABLE clean_payments;

-- ==========================================
-- 4. SINGLE-READ STAGING: Snapshot the stream ONCE
-- ==========================================
-- Read the entire Bronze stream into a temp table.
-- Both DLQ and MERGE read from this snapshot — prevents stream double-consumption.
CREATE OR REPLACE TEMPORARY TABLE payments_staging AS
SELECT *, METADATA$ACTION AS action
FROM NYS_DFS_RETAIL.STAGE.raw_payments_stm;

-- ==========================================
-- 5. FORK — Route INVALID records to DLQ
-- ==========================================
-- Financial records are never silently dropped.
-- If a POS glitch sends a $0 payment or a corrupted ID,
-- it lands here with a reason for the finance team to investigate.
INSERT INTO clean_payments_dlq (PAYMENT_ID, ORDER_ID, PAYMENT_AMOUNT, rejection_reason)
SELECT
    PAYMENT_ID,
    ORDER_ID,
    PAYMENT_AMOUNT,

    -- REJECTION REASON: Identifies the specific validation failure
    CASE
        WHEN TRY_CAST(PAYMENT_ID AS INT) IS NULL THEN 'INVALID_PAYMENT_ID'
        WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
        WHEN TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) IS NULL THEN 'NON_NUMERIC_AMOUNT'
        WHEN TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) <= 0 THEN 'ZERO_OR_NEGATIVE_AMOUNT'
        ELSE 'UNKNOWN_REJECTION'
    END

FROM payments_staging
WHERE action = 'INSERT'
    AND (
        TRY_CAST(PAYMENT_ID AS INT) IS NULL
        OR TRY_CAST(ORDER_ID AS INT) IS NULL
        OR TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) IS NULL
        OR TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) <= 0
    );

-- ==========================================
-- 6. FORK — MERGE VALID records into Clean table
-- ==========================================
-- MERGE guarantees idempotency: safe to re-run without duplicating payments.
-- Only payments with valid IDs and positive amounts pass through.
MERGE INTO clean_payments target
USING (
    SELECT
        TRY_CAST(PAYMENT_ID AS INT) AS PAYMENT_ID,
        TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,

        -- Financial precision: DECIMAL(10,2) ensures ₹1084.50 stays ₹1084.50
        TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) AS PAYMENT_AMOUNT

    FROM payments_staging
    WHERE action = 'INSERT'
        -- Identity validation
        AND TRY_CAST(PAYMENT_ID AS INT) IS NOT NULL
        AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
        -- Financial validation: only positive amounts are real payments
        AND TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) IS NOT NULL
        AND TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) > 0

    -- DEDUPLICATION: If source has duplicate PAYMENT_IDs, keep only one
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST(PAYMENT_ID AS INT)
        ORDER BY TRY_CAST(PAYMENT_ID AS INT)
    ) = 1

) source
ON target.PAYMENT_ID = source.PAYMENT_ID

-- MATCHED: Payment already exists — late-arriving correction from POS system
WHEN MATCHED THEN UPDATE SET
    target.ORDER_ID = source.ORDER_ID,
    target.PAYMENT_AMOUNT = source.PAYMENT_AMOUNT,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- NOT MATCHED: Brand new payment record
WHEN NOT MATCHED THEN INSERT (PAYMENT_ID, ORDER_ID, PAYMENT_AMOUNT)
VALUES (
    source.PAYMENT_ID,
    source.ORDER_ID,
    source.PAYMENT_AMOUNT
);

-- ==========================================
-- 7. VERIFY
-- ==========================================
-- Expected: 300K clean payments, 0 quarantined
SELECT 'CLEAN_PAYMENTS' AS table_name, COUNT(*) AS row_count FROM clean_payments
UNION ALL SELECT 'QUARANTINE_DLQ', COUNT(*) FROM clean_payments_dlq;