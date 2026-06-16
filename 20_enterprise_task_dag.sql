-- ==============================================================================
-- AUTOMATION — Enterprise-Grade Stream-Aware Task DAG
-- ==============================================================================
-- PURPOSE: Fully autonomous, self-healing pipeline that monitors ALL streams
-- (both transaction and dimension) and triggers transformations ONLY when new
-- data arrives. Zero wasted compute.
--
-- DAG STRUCTURE:
--   silver_layer_task (ROOT — scheduled daily 6 AM UTC)
--     ↓ checks: do any of the 12 bronze streams have new data?
--     ↓ if YES → fires CALL sp_bronze_to_silver()
--   gold_dimensions_task (CHILD 1 — fires AFTER silver completes)
--     ↓ fires CALL sp_silver_to_gold_dims()
--   gold_facts_task (CHILD 2 — fires AFTER dimensions complete)
--     ↓ fires CALL sp_gold_facts()
--
-- ADVANCED FEATURES:
--   1. DIMENSION STREAM MONITORING: If business uploads new stores.csv or
--      promotions.csv but no transactions arrive, DAG still fires to update dims.
--      Without this, dimensions fall out of sync — a data trust issue.
--   2. USER_TASK_TIMEOUT_MS: Hard timeout kills runaway queries before they
--      burn through compute credits. Prevents silent SLA breaches.
--   3. QUERY_TAG: FinOps tagging allows finance to isolate pipeline costs
--      from other warehouse activity in billing reports.
--   4. IDEMPOTENT SPs: All stored procedures use MERGE — safe to retry on failure.
--
-- COST OPTIMIZATION:
--   - WHEN clause checks all 12 streams — if no new data, warehouse stays asleep
--   - Timeouts cap runaway compute at 20-30 minutes max
--   - Child tasks only fire after parent succeeds — no wasted compute on failures
--
-- ROLE: ACCOUNTADMIN (trial account — has EXECUTE TASK privilege natively)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- ==============================================================================
-- ROOT TASK: Bronze → Silver
-- ==============================================================================
-- Scheduled: daily at 6 AM UTC
-- Timeout: 30 minutes (1,800,000 ms) — kills the query if it hangs
-- WHEN clause: checks ALL 12 streams (5 transaction + 7 dimension)
--   - Transaction streams: detect new orders/payments/returns/shipments from Azure
--   - Dimension streams: detect new master data uploads (stores, products, etc.)
--   If ALL streams are empty → warehouse never wakes (zero cost)
--   If ANY stream has data → fires and processes the entire Silver layer
CREATE OR REPLACE TASK silver_layer_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    USER_TASK_TIMEOUT_MS = 1800000
    WHEN
        -- Transaction streams (Azure Blob → Bronze)
        SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.raw_orders_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.raw_order_items_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.raw_payments_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.raw_shipments_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.raw_returns_stm')
        -- Dimension streams (CSV internal stage → Bronze)
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.stores_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.customers_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.products_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.promotions_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.suppliers_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.employees_stm')
        OR SYSTEM$STREAM_HAS_DATA('NYS_DFS_RETAIL.STAGE.categories_stm')
AS
    -- Fully qualified: SP lives in CLEAN schema, task lives in STAGE schema
    CALL NYS_DFS_RETAIL.CLEAN.sp_bronze_to_silver();

-- ==============================================================================
-- CHILD TASK 1: Silver → Gold Dimensions
-- ==============================================================================
-- Fires automatically AFTER silver_layer_task completes successfully.
-- Timeout: 20 minutes — dimension MERGEs are lightweight (small tables)
-- Dimensions MUST be updated BEFORE facts (facts join to dimension surrogate keys).
-- If silver fails, this never fires — blast radius contained.
CREATE OR REPLACE TASK gold_dimensions_task
    WAREHOUSE = COMPUTE_WH
    USER_TASK_TIMEOUT_MS = 1200000
    AFTER NYS_DFS_RETAIL.STAGE.silver_layer_task
AS
    -- Fully qualified: SP lives in CONSUMPTION schema
    CALL NYS_DFS_RETAIL.CONSUMPTION.sp_silver_to_gold_dims();

-- ==============================================================================
-- CHILD TASK 2: Silver → Gold Facts
-- ==============================================================================
-- Fires automatically AFTER gold_dimensions_task completes successfully.
-- Timeout: 30 minutes — fact MERGEs process up to 600K rows with complex joins
-- Facts depend on dimensions being current (surrogate key resolution).
-- If dimensions fail, facts never fire — prevents orphaned surrogate keys.
CREATE OR REPLACE TASK gold_facts_task
    WAREHOUSE = COMPUTE_WH
    USER_TASK_TIMEOUT_MS = 1800000
    AFTER NYS_DFS_RETAIL.STAGE.gold_dimensions_task
AS
    -- Fully qualified: SP lives in CONSUMPTION schema
    CALL NYS_DFS_RETAIL.CONSUMPTION.sp_gold_facts();

-- ==============================================================================
-- FINOPS: Query Tags for Cost Tracking
-- ==============================================================================
-- Tags allow finance to isolate pipeline compute costs in ACCOUNT_USAGE views.
-- Query: SELECT QUERY_TAG, SUM(CREDITS_USED) FROM QUERY_HISTORY GROUP BY QUERY_TAG
-- Without tags, pipeline costs blend into all other COMPUTE_WH activity.
ALTER TASK silver_layer_task SET QUERY_TAG = 'MEDALLION_PIPELINE_SILVER';
ALTER TASK gold_dimensions_task SET QUERY_TAG = 'MEDALLION_PIPELINE_GOLD_DIMS';
ALTER TASK gold_facts_task SET QUERY_TAG = 'MEDALLION_PIPELINE_GOLD_FACTS';

-- ==============================================================================
-- ACTIVATE THE DAG
-- ==============================================================================
-- Tasks are created in SUSPENDED state by default (safety measure).
-- CRITICAL: Resume children FIRST, then root task LAST.
-- If root resumes first, it may fire before children are active and skip them.
ALTER TASK gold_facts_task RESUME;
ALTER TASK gold_dimensions_task RESUME;
ALTER TASK silver_layer_task RESUME;

-- ==============================================================================
-- VERIFY DAG STATUS
-- ==============================================================================
-- All 3 tasks should show state = 'started'
SHOW TASKS IN SCHEMA NYS_DFS_RETAIL.STAGE;