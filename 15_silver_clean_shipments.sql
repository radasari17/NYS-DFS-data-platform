-- ==========================================
-- SILVER LAYER (CLEAN) — Shipments (with DLQ)
-- ==========================================
-- PURPOSE: Transform raw shipment tracking data into validated records.
-- STATUS standardized to UPPERCASE for consistent dashboard filtering.
-- Whitelist validation: only SHIPPED, DELIVERED, LATE are accepted.
-- Unknown statuses (API glitches, error codes) routed to DLQ.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

-- CLEAN TABLE: Validated shipment records.
-- STATUS constrained to VARCHAR(20) — statuses are short categorical values.
-- UPPER standardization ensures "delivered", "Delivered", "DELIVERED" all match.
CREATE OR REPLACE TABLE clean_shipments (
    SHIPMENT_ID  INT PRIMARY KEY,              -- Unique shipment identity
    ORDER_ID     INT NOT NULL,                 -- FK to clean_orders (every shipment ties to an order)
    STATUS       VARCHAR(20) NOT NULL,         -- Standardized: SHIPPED, DELIVERED, LATE
    dw_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- DLQ TABLE: Catches invalid or unknown-status shipment records.
-- If the source system starts sending new statuses like "CANCELLED" or "PENDING",
-- they land here instead of silently polluting the clean table.
-- Operations team audits this to decide if new statuses should be whitelisted.
CREATE OR REPLACE TABLE clean_shipments_dlq (
    SHIPMENT_ID    VARCHAR,
    ORDER_ID       VARCHAR,
    STATUS         VARCHAR,
    quarantined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rejection_reason VARCHAR
);

-- STREAM: Captures baseline for Gold fact table consumption
CREATE OR REPLACE STREAM clean_shipments_stm ON TABLE clean_shipments;

-- ==========================================
-- 4. SINGLE-READ STAGING: Snapshot the stream ONCE
-- ==========================================
-- Read the entire Bronze stream into a temp table.
-- Both DLQ and MERGE read from this snapshot — prevents stream double-consumption.
CREATE OR REPLACE TEMPORARY TABLE shipments_staging AS
SELECT *, METADATA$ACTION AS action
FROM NYS_DFS_RETAIL.STAGE.raw_shipments_stm;

-- ==========================================
-- 5. FORK — Route INVALID records to DLQ
-- ==========================================
-- Catches: NULL IDs, empty statuses, unknown status values (API error codes).
-- Every rejected record gets a specific reason — no guessing during audits.
INSERT INTO clean_shipments_dlq (SHIPMENT_ID, ORDER_ID, STATUS, rejection_reason)
SELECT
    SHIPMENT_ID,
    ORDER_ID,
    STATUS,

    -- REJECTION REASON: Identifies the specific validation failure
    CASE
        WHEN TRY_CAST(SHIPMENT_ID AS INT) IS NULL THEN 'INVALID_SHIPMENT_ID'
        WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
        WHEN TRIM(STATUS) IN ('', 'None', 'NA', 'null') OR STATUS IS NULL THEN 'MISSING_STATUS'
        WHEN UPPER(TRIM(STATUS)) NOT IN ('SHIPPED', 'DELIVERED', 'LATE') THEN 'UNKNOWN_STATUS_VALUE'
        ELSE 'UNKNOWN_REJECTION'
    END

FROM shipments_staging
WHERE action = 'INSERT'
    AND (
        TRY_CAST(SHIPMENT_ID AS INT) IS NULL
        OR TRY_CAST(ORDER_ID AS INT) IS NULL
        OR TRIM(STATUS) IN ('', 'None', 'NA', 'null')
        OR STATUS IS NULL
        OR UPPER(TRIM(STATUS)) NOT IN ('SHIPPED', 'DELIVERED', 'LATE')
    );

-- ==========================================
-- 6. FORK — MERGE VALID records into Clean table
-- ==========================================
-- MERGE guarantees idempotency: safe to re-run without duplicating shipments.
-- WHEN MATCHED handles status updates (e.g., SHIPPED → DELIVERED over time).
MERGE INTO clean_shipments target
USING (
    SELECT
        TRY_CAST(SHIPMENT_ID AS INT) AS SHIPMENT_ID,
        TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,

        -- STANDARDIZE: Force UPPERCASE for consistent filtering in dashboards
        -- "delivered" → "DELIVERED", "Shipped" → "SHIPPED", "late" → "LATE"
        UPPER(TRIM(STATUS)) AS STATUS

    FROM shipments_staging
    WHERE action = 'INSERT'
        -- Identity validation
        AND TRY_CAST(SHIPMENT_ID AS INT) IS NOT NULL
        AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
        -- Whitelist validation: only known statuses pass through
        -- Future unknown values (CANCELLED, PENDING, ERROR_99) hit DLQ
        AND UPPER(TRIM(STATUS)) IN ('SHIPPED', 'DELIVERED', 'LATE')

    -- DEDUPLICATION: One record per shipment
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST(SHIPMENT_ID AS INT)
        ORDER BY TRY_CAST(SHIPMENT_ID AS INT)
    ) = 1

) source
ON target.SHIPMENT_ID = source.SHIPMENT_ID

-- MATCHED: Shipment status updated (e.g., SHIPPED → DELIVERED)
WHEN MATCHED THEN UPDATE SET
    target.ORDER_ID = source.ORDER_ID,
    target.STATUS = source.STATUS,
    target.dw_loaded_at = CURRENT_TIMESTAMP()

-- NOT MATCHED: Brand new shipment record
WHEN NOT MATCHED THEN INSERT (SHIPMENT_ID, ORDER_ID, STATUS)
VALUES (
    source.SHIPMENT_ID,
    source.ORDER_ID,
    source.STATUS
);

-- ==========================================
-- 7. VERIFY
-- ==========================================
-- Expected: 300K clean shipments, 0 quarantined
SELECT 'CLEAN_SHIPMENTS' AS table_name, COUNT(*) AS row_count FROM clean_shipments
UNION ALL SELECT 'QUARANTINE_DLQ', COUNT(*) FROM clean_shipments_dlq;

-- Bonus: Verify status distribution matches source (~100K each)
SELECT STATUS, COUNT(*) AS cnt FROM clean_shipments GROUP BY STATUS;