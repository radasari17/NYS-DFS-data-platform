-- ==============================================================================
-- BRONZE LAYER (STAGE) — Transaction Tables
-- ==============================================================================
-- PURPOSE: Raw ingestion of 5 transaction tables from Azure Blob Storage.
-- All VARCHAR columns to prevent any type-casting failures during bulk load.
-- Data arrives from SQL Server → Python ETL → Azure Blob → Snowflake External Stage.
--
-- SOURCE: @azure_blob_stage (Azure Blob: nysdfsretailstorage/bronze-layer)
-- TABLES: raw_orders, raw_payments, raw_returns, raw_shipments, raw_order_items
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- ==========================================
-- 1. BUILD BRONZE TABLES
-- ==========================================
-- All columns are VARCHAR — raw data lands exactly as it arrived from source.
-- No type enforcement at this layer. This is our recovery point if anything
-- goes wrong downstream in Silver or Gold.

-- ORDERS: One row per customer order (300K records)
-- Links to: customers (CUSTOMER_ID), stores (STORE_ID), promotions (PROMOTION_ID)
CREATE OR REPLACE TABLE raw_orders (
    ORDER_ID     VARCHAR,    -- Business key for the order
    CUSTOMER_ID  VARCHAR,    -- FK to customers dimension
    STORE_ID     VARCHAR,    -- FK to stores dimension
    ORDER_DATE   VARCHAR,    -- Date of purchase (DD-MM-YYYY format from source)
    PROMOTION_ID VARCHAR     -- FK to promotions dimension (discount applied)
);

-- PAYMENTS: One row per payment against an order (300K records)
-- Links to: orders (ORDER_ID)
CREATE OR REPLACE TABLE raw_payments (
    PAYMENT_ID     VARCHAR,  -- Business key for the payment
    ORDER_ID       VARCHAR,  -- FK to orders
    PAYMENT_AMOUNT VARCHAR   -- Amount paid (will cast to DECIMAL in Silver)
);

-- RETURNS: One row per returned item (30K records)
-- Links to: order_items (ORDER_ITEM_ID)
CREATE OR REPLACE TABLE raw_returns (
    RETURN_ID     VARCHAR,   -- Business key for the return
    ORDER_ITEM_ID VARCHAR,   -- FK to order_items (which specific item was returned)
    REFUND        VARCHAR    -- Refund amount (will cast to DECIMAL in Silver)
);

-- SHIPMENTS: One row per shipment tracking record (300K records)
-- Links to: orders (ORDER_ID)
CREATE OR REPLACE TABLE raw_shipments (
    SHIPMENT_ID VARCHAR,     -- Business key for the shipment
    ORDER_ID    VARCHAR,     -- FK to orders
    STATUS      VARCHAR      -- Delivery status (delivered, in-transit, etc.)
);

-- ORDER ITEMS: One row per product in an order (600K records — largest table)
-- Links to: orders (ORDER_ID), products (PRODUCT_ID)
-- This is the grain of the final fact table
CREATE OR REPLACE TABLE raw_order_items (
    ORDER_ITEM_ID     VARCHAR,  -- Business key (unique per line item)
    ORDER_ID          VARCHAR,  -- FK to orders
    PRODUCT_ID        VARCHAR,  -- FK to products dimension
    PRODUCT_NAME      VARCHAR,  -- Denormalized product name from source
    QTY               VARCHAR,  -- Quantity ordered (will cast to INT in Silver)
    TOTAL_TRANSACTION VARCHAR   -- Line total = qty * unit_price (will cast to DECIMAL)
);

-- ==========================================
-- 2. ATTACH CDC STREAMS
-- ==========================================
-- CRITICAL: Streams must be created BEFORE data is loaded.
-- They capture all INSERT/UPDATE/DELETE actions for downstream Silver processing.
-- Without streams, the Silver layer has no way to detect what's new.

CREATE OR REPLACE STREAM raw_orders_stm ON TABLE raw_orders;
CREATE OR REPLACE STREAM raw_payments_stm ON TABLE raw_payments;
CREATE OR REPLACE STREAM raw_returns_stm ON TABLE raw_returns;
CREATE OR REPLACE STREAM raw_shipments_stm ON TABLE raw_shipments;
CREATE OR REPLACE STREAM raw_order_items_stm ON TABLE raw_order_items;

-- ==========================================
-- 3. BULK INGESTION FROM AZURE BLOB
-- ==========================================
-- COPY INTO reads directly from the external stage (Azure Blob).
-- Snowflake warehouse only spins up for the seconds it takes to load —
-- no compute cost during extraction or upload phases.
-- FILE_FORMAT handles: CSV parsing, comma delimiter, header skip, quote handling.

COPY INTO raw_orders FROM @azure_blob_stage/raw_orders/ FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;
COPY INTO raw_payments FROM @azure_blob_stage/raw_payments/ FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;
COPY INTO raw_returns FROM @azure_blob_stage/raw_returns/ FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;
COPY INTO raw_shipments FROM @azure_blob_stage/raw_shipments/ FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;
COPY INTO raw_order_items FROM @azure_blob_stage/raw_order_items/ FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format;

-- ==========================================
-- 4. VERIFY INGESTION
-- ==========================================
-- Quick row count sanity check across all 5 tables.
-- Expected: orders=300K, payments=300K, returns=30K, shipments=300K, order_items=600K

SELECT 'raw_orders' AS tbl, COUNT(*) AS row_count FROM raw_orders
UNION ALL SELECT 'raw_payments', COUNT(*) FROM raw_payments
UNION ALL SELECT 'raw_returns', COUNT(*) FROM raw_returns
UNION ALL SELECT 'raw_shipments', COUNT(*) FROM raw_shipments
UNION ALL SELECT 'raw_order_items', COUNT(*) FROM raw_order_items;