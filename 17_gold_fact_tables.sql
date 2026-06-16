-- ==============================================================================
-- GOLD LAYER (CONSUMPTION) — Order Item Fact Table
-- ==============================================================================
-- ARCHITECTURAL DECISION: Two-Fact Design
--
-- We are building TWO separate fact tables instead of one because:
--
-- 1. GRAIN MISMATCH: Orders have 2.3 items on average. Payments and shipments
--    are 1:1 with orders. If we join payments at the item level, PAYMENT_AMOUNT
--    duplicates across every line item. A SUM() in Power BI would inflate revenue
--    by 2.3x — a catastrophic data trust issue for the finance team.
--
-- 2. SILENT DROP PREVENTION: 40,767 orders have no line items but DO have
--    payments and shipments. An order-item-grain fact drops these entirely.
--    Finance's General Ledger includes them. A separate order_fact captures
--    ALL 300K orders so payment totals reconcile perfectly.
--
-- FACT 1: order_item_fact (THIS TABLE)
--   Grain: One row per line item (600K rows)
--   Measures: QTY, TOTAL_TRANSACTION, REFUND_AMOUNT, NET_REVENUE
--   Use case: Product performance, return rates, category analysis
--
-- FACT 2: order_fact (BUILT SEPARATELY)
--   Grain: One row per order (300K rows)
--   Measures: PAYMENT_AMOUNT, SHIPMENT_STATUS
--   Use case: Revenue reconciliation, logistics tracking, GL tie-out
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- FACT TABLE: Order Item grain — the most granular level of transaction detail.
-- Surrogate keys from dimensions enable star schema joins.
-- Nullable SK columns because LEFT JOINs may not resolve (e.g., deleted customer).
CREATE OR REPLACE TABLE order_item_fact (
    ORDER_ITEM_FACT_ID INT AUTOINCREMENT PRIMARY KEY,  -- Fact table surrogate key

    -- DIMENSION SURROGATE KEYS: Star schema join points
    DATE_ID            INT NOT NULL,    -- FK to dim_date (YYYYMMDD integer key)
    CUSTOMER_SK        INT,            -- FK to customers_dim (nullable: LEFT JOIN)
    STORE_SK           INT,            -- FK to stores_dim (nullable: LEFT JOIN)
    PRODUCT_SK         INT,            -- FK to products_dim (nullable: LEFT JOIN)
    PROMOTION_SK       INT,            -- FK to promotions_dim (nullable: no promo = NULL)

    -- DEGENERATE DIMENSIONS: Business IDs for drill-through to source
    ORDER_ID           INT NOT NULL,   -- Parent order (links to order_fact)
    ORDER_ITEM_ID      INT NOT NULL,   -- Grain of this fact (unique per row)

    -- MEASURES: Quantity and Revenue
    QTY                INT,            -- Units ordered
    TOTAL_TRANSACTION  DECIMAL(10,2),  -- Gross line item revenue (from source, no rounding)
    REFUND_AMOUNT      DECIMAL(10,2),  -- Refund if returned, 0 otherwise
    NET_REVENUE        DECIMAL(10,2),  -- TOTAL_TRANSACTION - REFUND_AMOUNT

    -- FLAGS: For dashboard filtering
    IS_RETURNED        BOOLEAN,        -- Quick slicer for return analysis

    -- AUDIT
    dw_loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- POPULATE: Join Silver transaction tables with Gold dimension surrogate keys.
-- Base: clean_order_items (600K rows — the grain)
-- INNER JOIN orders: Every item must belong to an order (0 orphans confirmed)
-- LEFT JOIN returns: Only 30K of 600K items are returned (item-level = safe)
-- LEFT JOIN dimensions: Resolve business keys to surrogate keys (current version only)
INSERT INTO order_item_fact (
    DATE_ID, CUSTOMER_SK, STORE_SK, PRODUCT_SK, PROMOTION_SK,
    ORDER_ID, ORDER_ITEM_ID, QTY, TOTAL_TRANSACTION,
    REFUND_AMOUNT, NET_REVENUE, IS_RETURNED
)
SELECT
    -- DATE: Convert ORDER_DATE to integer key matching dim_date.DATE_ID
    TO_CHAR(o.ORDER_DATE, 'YYYYMMDD')::INT AS DATE_ID,

    -- DIMENSION SURROGATE KEYS: Resolved via LEFT JOIN on business key + IS_CURRENT
    c.CUSTOMER_SK,
    s.STORE_SK,
    p.PRODUCT_SK,
    promo.PROMOTION_SK,

    -- DEGENERATE DIMENSIONS: Kept for drill-through and order_fact linkage
    oi.ORDER_ID,
    oi.ORDER_ITEM_ID,

    -- QUANTITY: Units sold per line item
    oi.QTY,

    -- GROSS REVENUE: Using TOTAL_TRANSACTION directly from source
    -- NOT recalculating UNIT_PRICE * QTY (avoids floating-point rounding drift)
    oi.TOTAL_TRANSACTION,

    -- REFUND: COALESCE converts NULL (not returned) to 0 for clean arithmetic
    COALESCE(r.REFUND, 0) AS REFUND_AMOUNT,

    -- NET REVENUE: The true bottom-line revenue per line item
    -- If returned: gross minus refund. If not returned: gross minus 0 = gross.
    (oi.TOTAL_TRANSACTION - COALESCE(r.REFUND, 0)) AS NET_REVENUE,

    -- IS_RETURNED: Boolean flag — TRUE if this item has a return record
    IFF(r.RETURN_ID IS NOT NULL, TRUE, FALSE) AS IS_RETURNED

-- BASE: Order items at fact grain (600K rows)
FROM NYS_DFS_RETAIL.CLEAN.clean_order_items oi

-- INNER JOIN: Every item must have a parent order (0 orphans validated)
INNER JOIN NYS_DFS_RETAIL.CLEAN.clean_orders o
    ON oi.ORDER_ID = o.ORDER_ID

-- LEFT JOIN RETURNS: Only 30K items returned — safe at item grain, no fan-out
LEFT JOIN NYS_DFS_RETAIL.CLEAN.clean_returns r
    ON oi.ORDER_ITEM_ID = r.ORDER_ITEM_ID

-- DIMENSION LOOKUPS: Resolve business keys to surrogate keys
-- IS_CURRENT = TRUE ensures we join to the active SCD2 version
LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.customers_dim c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID AND c.IS_CURRENT = TRUE

LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s
    ON o.STORE_ID = s.STORE_ID AND s.IS_CURRENT = TRUE

LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.products_dim p
    ON oi.PRODUCT_ID = p.PRODUCT_ID AND p.IS_CURRENT = TRUE

LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.promotions_dim promo
    ON o.PROMOTION_ID = promo.PROMOTION_ID AND promo.IS_CURRENT = TRUE;

-- ==============================================================================
-- VERIFY
-- ==============================================================================
-- Row count: Expected 600K (one per order item)
SELECT COUNT(*) AS total_fact_rows FROM order_item_fact;

-- Revenue sanity check: Gross - Refunds = Net
SELECT
    SUM(TOTAL_TRANSACTION) AS total_gross_revenue,
    SUM(REFUND_AMOUNT) AS total_refunds,
    SUM(NET_REVENUE) AS total_net_revenue,
    SUM(CASE WHEN IS_RETURNED THEN 1 ELSE 0 END) AS returned_items,
    COUNT(*) AS total_items,
    ROUND(SUM(CASE WHEN IS_RETURNED THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM order_item_fact;

-- Dimension resolution check: How many items couldn't resolve to a dimension?
SELECT
    COUNT(CASE WHEN CUSTOMER_SK IS NULL THEN 1 END) AS missing_customer,
    COUNT(CASE WHEN STORE_SK IS NULL THEN 1 END) AS missing_store,
    COUNT(CASE WHEN PRODUCT_SK IS NULL THEN 1 END) AS missing_product,
    COUNT(CASE WHEN PROMOTION_SK IS NULL THEN 1 END) AS missing_promotion
FROM order_item_fact;

-- Sample data
SELECT * FROM order_item_fact LIMIT 15;