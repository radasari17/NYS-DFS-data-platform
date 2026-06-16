View	Type	RBAC
v_annual_revenue	Financial	All roles
v_monthly_revenue	Financial	All roles
v_daily_revenue_kpi	Operational	All roles
v_product_performance	Analytics	All roles
v_category_analysis	Analytics	All roles
v_customer_lifetime_value	Analytics + PII	Managers only see names
v_store_performance	Operations	All roles
v_promotional_lift	Marketing	All roles
v_labor_efficiency_kpi	HR/Finance	Managers only (salary data)
v_return_analysis	Operations	All roles
v_shipment_performance	Logistics	All roles
v_executive_kpi	C-Suite summary	All roles


-- ==============================================================================
-- CERTIFIED VIEW: v_executive_kpi (C-Suite Single-Row Summary)
-- ==============================================================================
-- PURPOSE: One-row snapshot of the entire business for executive dashboards.
-- Designed for Power BI direct-connect or Streamlit real-time KPI tiles.
--
-- ARCHITECTURE:
--   - Two isolated CTEs prevent Cartesian fan-out between order-grain and item-grain
--   - CROSS JOIN is safe because both CTEs produce exactly ONE row (aggregates)
--   - NULLIF on all denominators prevents divide-by-zero crashes
--   - SECURE VIEW hides query plan from unauthorized roles
--
-- PERFORMANCE OPTIMIZATIONS:
--   - No COUNT(DISTINCT) — order_fact grain is guaranteed unique (UNIQUE constraint)
--   - COUNT_IF() — Snowflake-native columnar function (faster than CASE WHEN)
--   - No unnecessary TRIM/UPPER on clean data (already standardized in Silver)
--
-- BUSINESS LOGIC:
--   - true_delivery_success_pct: denominator excludes in-transit orders
--     (only counts resolved shipments: DELIVERED + LATE)
--   - revenue_vs_payment_variance: instant audit flag if GL doesn't reconcile
--   - return_rate_pct: items returned / total items (not orders returned)
--
-- RBAC: Non-sensitive (aggregated metrics only — no PII exposed)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_executive_kpi AS
WITH order_level_metrics AS (
    SELECT
        -- Total orders: no DISTINCT needed — order_fact has UNIQUE on ORDER_ID
        COUNT(ORDER_ID) AS total_orders,

        -- Total payments collected (GL reconciliation number)
        SUM(PAYMENT_AMOUNT) AS total_payments,

        -- Logistics: Snowflake-native COUNT_IF for columnar optimization
        COUNT_IF(UPPER(TRIM(SHIPMENT_STATUS)) = 'DELIVERED') AS total_delivered,
        COUNT_IF(UPPER(TRIM(SHIPMENT_STATUS)) = 'LATE') AS total_late,

        -- Resolved shipments: only orders that finished their journey
        -- Excludes SHIPPED (in-transit) and PENDING (not yet dispatched)
        COUNT_IF(UPPER(TRIM(SHIPMENT_STATUS)) IN ('DELIVERED', 'LATE')) AS total_resolved_shipments
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact
),
item_level_metrics AS (
    SELECT
        -- Revenue metrics: gross, refunds, and net (all DECIMAL precision)
        SUM(TOTAL_TRANSACTION) AS total_gross_revenue,
        SUM(REFUND_AMOUNT) AS total_refunds,
        SUM(NET_REVENUE) AS total_net_revenue,

        -- Volume metrics
        COUNT(ORDER_ITEM_ID) AS total_items,
        SUM(QTY) AS total_units_sold,

        -- Returns: COUNT_IF for Snowflake-native performance
        COUNT_IF(IS_RETURNED = TRUE) AS total_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact
)
SELECT
    -- REVENUE BLOCK: The finance team's primary numbers
    ROUND(i.total_gross_revenue, 2) AS gross_revenue,
    ROUND(i.total_refunds, 2) AS total_refunds,
    ROUND(i.total_net_revenue, 2) AS net_revenue,

    -- VOLUME BLOCK: Operational scale indicators
    o.total_orders,
    i.total_units_sold,

    -- BASKET METRICS: Marketing effectiveness indicators
    ROUND(i.total_net_revenue / NULLIF(o.total_orders, 0), 2) AS avg_basket_size,
    ROUND(i.total_units_sold / NULLIF(o.total_orders, 0), 1) AS avg_items_per_order,

    -- RETURNS: Product quality and customer satisfaction signal
    ROUND((i.total_returns / NULLIF(i.total_items, 0)) * 100, 2) AS return_rate_pct,

    -- LOGISTICS: True delivery success (only among resolved shipments)
    -- Excludes in-transit orders from penalizing the delivery team
    ROUND((o.total_delivered / NULLIF(o.total_resolved_shipments, 0)) * 100, 2) AS true_delivery_success_pct,
    ROUND((o.total_late / NULLIF(o.total_resolved_shipments, 0)) * 100, 2) AS late_shipment_pct,

    -- GL RECONCILIATION: Finance audit numbers
    ROUND(o.total_payments, 2) AS total_payments_collected,

    -- VARIANCE: If non-zero, item revenue doesn't match order payments — investigate
    ROUND(i.total_net_revenue - o.total_payments, 2) AS revenue_vs_payment_variance

FROM order_level_metrics o
CROSS JOIN item_level_metrics i;

-- VERIFY
SELECT * FROM v_executive_kpi;


-- ==============================================================================
-- CERTIFIED VIEW: v_labor_efficiency_kpi (Store Workforce Analytics)
-- ==============================================================================
-- PURPOSE: Measures revenue productivity per employee at each store.
-- Enables operations to identify underperforming locations and optimize staffing.
--
-- ARCHITECTURE:
--   - Three CTEs isolate different grains before joining:
--     1. store_revenue: from order_item_fact (item grain → SUM revenue per store)
--     2. store_orders: from order_fact (order grain → COUNT orders per store, no DISTINCT needed)
--     3. employee_metrics: from employees_dim (headcount + payroll per store)
--   - Uses BOTH fact tables per our two-fact design (avoids COUNT DISTINCT on 600K rows)
--
-- RBAC POLICY:
--   - All roles: revenue_per_employee, efficiency_tier, revenue_rank, total_orders
--   - Managers/HR/Finance only: store_manager name, payroll costs, labor_roi_ratio
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) anywhere — each CTE queries the correct grain table
--   - COUNT_IF / COUNT on pre-deduplicated tables only
--   - COALESCE prevents NULL display for stores with no data
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_labor_efficiency_kpi AS
WITH store_revenue AS (
    -- Revenue aggregated from order_item_fact (600K rows → grouped by store)
    -- This is the only place we touch the item-grain table
    SELECT
        s.STORE_ID,
        SUM(f.NET_REVENUE) AS total_store_revenue
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s ON f.STORE_SK = s.STORE_SK
    GROUP BY s.STORE_ID
),
store_orders AS (
    -- Order count from order_fact (300K rows — already 1:1 grain, no DISTINCT needed)
    -- This is WHY we built two fact tables — avoids expensive hash-aggregation
    SELECT
        s.STORE_ID,
        COUNT(f.ORDER_ID) AS total_orders
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s ON f.STORE_SK = s.STORE_SK
    GROUP BY s.STORE_ID
),
employee_metrics AS (
    -- Headcount and payroll per store (current employees only — IS_CURRENT filter)
    SELECT
        STORE_ID,
        COUNT(EMPLOYEE_ID) AS head_count,
        SUM(SALARY) AS total_payroll
    FROM NYS_DFS_RETAIL.CONSUMPTION.employees_dim
    WHERE IS_CURRENT = TRUE
    GROUP BY STORE_ID
)
SELECT
    s.STORE_ID,
    s.CITY,

    -- PII MASKING: Store manager name restricted to authorized roles
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'HR_ROLE', 'FINANCE_ROLE', 'MANAGER_ROLE'),
        s.STORE_MANAGER, '*** MASKED ***') AS store_manager,

    -- WORKFORCE METRICS: Visible to all roles (no sensitive data)
    COALESCE(e.head_count, 0) AS active_employees,
    COALESCE(ROUND(r.total_store_revenue, 2), 0) AS total_store_revenue,
    COALESCE(o.total_orders, 0) AS total_orders,

    -- LABOR EFFICIENCY: Revenue generated per employee (key operations KPI)
    ROUND(COALESCE(r.total_store_revenue, 0) / NULLIF(e.head_count, 0), 2) AS revenue_per_employee,

    -- EFFICIENCY TIER: Dashboard slicer for quick store segmentation
    CASE
        WHEN e.head_count IS NULL OR e.head_count = 0 THEN 'No Staff Data'
        WHEN r.total_store_revenue / NULLIF(e.head_count, 0) < 500000 THEN 'Underperforming'
        WHEN r.total_store_revenue / NULLIF(e.head_count, 0) < 1000000 THEN 'Average'
        ELSE 'High Performing'
    END AS efficiency_tier,

    -- REVENUE RANK: Store leaderboard for executive reviews
    RANK() OVER (ORDER BY COALESCE(r.total_store_revenue, 0) DESC) AS revenue_rank,

    -- FINANCIAL MASKING: Payroll data restricted to authorized roles
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'HR_ROLE', 'FINANCE_ROLE', 'MANAGER_ROLE'),
        ROUND(e.total_payroll, 2), NULL) AS total_payroll_cost,

    -- FINANCIAL MASKING: Labor ROI (rupees generated per rupee spent on labor)
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'HR_ROLE', 'FINANCE_ROLE', 'MANAGER_ROLE'),
        ROUND(COALESCE(r.total_store_revenue, 0) / NULLIF(e.total_payroll, 0), 2), NULL) AS labor_roi_ratio

FROM NYS_DFS_RETAIL.CONSUMPTION.stores_dim s
LEFT JOIN store_revenue r ON s.STORE_ID = r.STORE_ID
LEFT JOIN store_orders o ON s.STORE_ID = o.STORE_ID
LEFT JOIN employee_metrics e ON s.STORE_ID = e.STORE_ID
WHERE s.IS_CURRENT = TRUE;

-- VERIFY
SELECT * FROM v_labor_efficiency_kpi ORDER BY revenue_rank LIMIT 15;

-- ==============================================================================
-- CERTIFIED VIEW: v_customer_lifetime_value (Marketing & CRM Analytics)
-- ==============================================================================
-- PURPOSE: Calculates customer lifetime value metrics for marketing segmentation.
-- Aggregates across all historical orders per CUSTOMER_ID (natural key).
--
-- ARCHITECTURE:
--   - customer_revenue CTE: from order_item_fact (item grain → revenue per customer)
--   - customer_orders CTE: from order_fact (order grain → count, no DISTINCT)
--   - GROUP BY CUSTOMER_ID consolidates across SCD2 versions
--     (customer who moved cities still has ONE lifetime value, not two)
--
-- RBAC POLICY:
--   - All roles: LTV metrics, ltv_tier, customer_rank, city, tenure
--   - Managers/Compliance/Marketing: customer_name, phone_number
--   - Marketing needs PII access to execute targeted SMS/cross-sell campaigns
--
-- SEGMENTATION LOGIC:
--   - Platinum: >= ₹10,000 lifetime spend
--   - Gold: >= ₹5,000
--   - Silver: >= ₹1,000
--   - Bronze: > ₹0 (has purchased at least once)
--   - Prospect / Zero-Spend: signed up but never ordered (re-engagement target)
--
-- PERFORMANCE:
--   - Two-fact architecture: order counts from order_fact (no COUNT DISTINCT)
--   - COALESCE on all LEFT JOIN outputs prevents NULL arithmetic
--   - COUNT_IF for Snowflake-native columnar optimization
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_customer_lifetime_value AS
WITH customer_revenue AS (
    -- Revenue and return metrics from item-grain fact
    -- GROUP BY CUSTOMER_ID (natural key) consolidates across SCD2 versions
    SELECT
        c.CUSTOMER_ID,
        SUM(f.NET_REVENUE) AS total_lifetime_spend,
        SUM(f.QTY) AS total_lifetime_items,
        COUNT_IF(f.IS_RETURNED = TRUE) AS total_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.customers_dim c ON f.CUSTOMER_SK = c.CUSTOMER_SK
    GROUP BY c.CUSTOMER_ID
),
customer_orders AS (
    -- Order count from order-grain fact (1:1 grain, no DISTINCT needed)
    SELECT
        c.CUSTOMER_ID,
        COUNT(f.ORDER_ID) AS lifetime_order_count
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.customers_dim c ON f.CUSTOMER_SK = c.CUSTOMER_SK
    GROUP BY c.CUSTOMER_ID
)
SELECT
    curr.CUSTOMER_ID,

    -- PII MASKING: Includes MARKETING_ROLE so campaigns can actually execute
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'MANAGER_ROLE', 'COMPLIANCE_ROLE', 'MARKETING_ROLE'),
        curr.CUSTOMER_NAME, '*** MASKED ***') AS customer_name,

    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'MANAGER_ROLE', 'COMPLIANCE_ROLE', 'MARKETING_ROLE'),
        curr.PHONE_NUMBER, '*** MASKED ***') AS phone_number,

    curr.CITY,

    -- TENURE: Months since signup — correlates with LTV for retention analysis
    DATEDIFF('MONTH', curr.SIGNUP_DATE, CURRENT_DATE()) AS tenure_months,

    -- LIFETIME METRICS: Volume and revenue
    COALESCE(o.lifetime_order_count, 0) AS lifetime_order_count,
    COALESCE(ROUND(r.total_lifetime_spend, 2), 0) AS total_lifetime_spend,
    COALESCE(r.total_lifetime_items, 0) AS total_lifetime_items,

    -- BEHAVIORAL RATIOS: Spend patterns and quality signals
    ROUND(COALESCE(r.total_lifetime_spend, 0) / NULLIF(o.lifetime_order_count, 0), 2) AS avg_order_value,
    ROUND((COALESCE(r.total_returns, 0) / NULLIF(r.total_lifetime_items, 0)) * 100, 2) AS lifetime_return_rate_pct,

    -- LTV SEGMENTATION: Zero-spend customers isolated as 'Prospect' (not Bronze)
    -- Prevents inactive signups from inflating active customer marketing cohorts
    CASE
        WHEN COALESCE(r.total_lifetime_spend, 0) >= 10000 THEN 'Platinum'
        WHEN COALESCE(r.total_lifetime_spend, 0) >= 5000 THEN 'Gold'
        WHEN COALESCE(r.total_lifetime_spend, 0) >= 1000 THEN 'Silver'
        WHEN COALESCE(r.total_lifetime_spend, 0) > 0 THEN 'Bronze'
        ELSE 'Prospect / Zero-Spend'
    END AS ltv_tier,

    -- RANKING: Highest value customers at the top for VIP targeting
    RANK() OVER (ORDER BY COALESCE(r.total_lifetime_spend, 0) DESC) AS customer_rank

FROM NYS_DFS_RETAIL.CONSUMPTION.customers_dim curr
LEFT JOIN customer_revenue r ON curr.CUSTOMER_ID = r.CUSTOMER_ID
LEFT JOIN customer_orders o ON curr.CUSTOMER_ID = o.CUSTOMER_ID
WHERE curr.IS_CURRENT = TRUE;

-- VERIFY
SELECT * FROM v_customer_lifetime_value ORDER BY customer_rank LIMIT 15;

-- ==============================================================================
-- CERTIFIED VIEW: v_product_performance (Merchandising & Supply Chain)
-- ==============================================================================
-- PURPOSE: Revenue and return performance per product for merchandising teams.
-- Identifies top-performing SKUs and flags products with high return risk.
--
-- ARCHITECTURE:
--   - CTE groups by PRODUCT_ID (natural key) not PRODUCT_SK
--     Prevents SCD2 fragmentation (price changes would split one product's revenue)
--   - Anchored to IS_CURRENT = TRUE for display attributes (current name/price)
--   - SUM(...) OVER() window function enables Pareto analysis without subquery
--
-- SEGMENTATION:
--   - return_health_status: Critical (>10%), Watch (>5%), Healthy (<5%), No Data
--   - product_revenue_rank: Top sellers ranked for merchandising decisions
--   - revenue_contribution_pct: What % of total business does this product drive
--
-- RBAC: Non-sensitive (no PII — product data is public catalog information)
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — item_fact is the correct grain for product metrics
--   - COUNT_IF for Snowflake-native columnar optimization
--   - COALESCE prevents NULL display for products with no sales
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_product_performance AS
WITH product_metrics AS (
    -- Aggregate at PRODUCT_ID (natural key) to prevent SCD2 fragmentation
    -- A product with 3 price changes still shows ONE consolidated revenue total
    SELECT
        p.PRODUCT_ID,
        SUM(f.NET_REVENUE) AS total_revenue,
        SUM(f.QTY) AS total_units_sold,
        COUNT(f.ORDER_ITEM_ID) AS total_order_items,
        COUNT_IF(f.IS_RETURNED = TRUE) AS total_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.products_dim p ON f.PRODUCT_SK = p.PRODUCT_SK
    GROUP BY p.PRODUCT_ID
)
SELECT
    curr.PRODUCT_ID,
    curr.PRODUCT_NAME,
    curr.PRODUCT_CATEGORY,
    curr.PRICE AS current_catalog_price,

    -- SALES METRICS: Volume and revenue
    COALESCE(m.total_units_sold, 0) AS total_units_sold,
    COALESCE(ROUND(m.total_revenue, 2), 0) AS total_lifetime_revenue,

    -- ACTUAL SELLING PRICE: Reveals discount impact
    -- If lower than catalog price, promotions are eating into margin
    ROUND(COALESCE(m.total_revenue, 0) / NULLIF(m.total_units_sold, 0), 2) AS avg_actual_selling_price,

    -- REVENUE CONTRIBUTION: Pareto analysis in one column
    -- Top 20% of products likely drive 80% of total revenue
    ROUND(COALESCE(m.total_revenue, 0) / NULLIF(SUM(m.total_revenue) OVER (), 0) * 100, 2) AS revenue_contribution_pct,

    -- RETURN METRICS: Absolute count alongside percentage for context
    -- 5% of 10 sales vs 5% of 10,000 sales are very different situations
    COALESCE(m.total_returns, 0) AS total_returns,
    ROUND((COALESCE(m.total_returns, 0) / NULLIF(m.total_order_items, 0)) * 100, 2) AS return_rate_pct,

    -- RETURN HEALTH: Flags products for supply chain investigation
    -- Critical: >10% returns (defect likely from upstream supplier)
    -- Watch: 5-10% (monitor — may need quality audit)
    -- Healthy: <5% (normal retail return baseline)
    CASE
        WHEN m.total_order_items IS NULL OR m.total_order_items = 0 THEN 'No Sales Data'
        WHEN (m.total_returns / m.total_order_items) >= 0.10 THEN 'Critical Return Risk'
        WHEN (m.total_returns / m.total_order_items) >= 0.05 THEN 'Watch / Moderate Risk'
        ELSE 'Healthy'
    END AS return_health_status,

    -- RANKING: Top revenue-driving products for merchandising prioritization
    RANK() OVER (ORDER BY COALESCE(m.total_revenue, 0) DESC) AS product_revenue_rank

FROM NYS_DFS_RETAIL.CONSUMPTION.products_dim curr
LEFT JOIN product_metrics m ON curr.PRODUCT_ID = m.PRODUCT_ID
WHERE curr.IS_CURRENT = TRUE;

-- VERIFY
SELECT * FROM v_product_performance ORDER BY product_revenue_rank LIMIT 15;




-- ==============================================================================
-- CERTIFIED VIEW: v_category_analysis (Merchandising & Category Management)
-- ==============================================================================
-- PURPOSE: Revenue and return performance aggregated at the product category level.
-- Enables category managers to identify top-performing departments and flag
-- categories with systemic return issues for supplier/quality investigation.
--
-- ARCHITECTURE:
--   - Single CTE aggregates from order_item_fact joined to products_dim
--   - Groups by PRODUCT_CATEGORY (string from dimension, not category_id)
--   - SUM(...) OVER() window function for Pareto contribution without subquery
--   - COALESCE handles orphan/NULL categories from unmapped upstream SKUs
--
-- SEGMENTATION:
--   - category_health_status: High Risk (>8%), Moderate (>4%), Healthy (<4%)
--   - Thresholds are LOWER than product-level (10%/5%) because categories
--     aggregate many products — 8% at category level means systemic issues
--   - category_rank: Top revenue-driving categories for budget allocation
--
-- RBAC: Non-sensitive (category data is public catalog information, no PII)
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — item_fact is the correct grain
--   - COUNT_IF for Snowflake-native columnar optimization
--   - No dimension self-join needed — category name comes directly from products_dim
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_category_analysis AS
WITH category_metrics AS (
    -- Aggregate at category level via product dimension join
    -- Each order item resolves to its category through the product SK
    SELECT
        p.PRODUCT_CATEGORY,
        SUM(f.NET_REVENUE) AS category_revenue,
        SUM(f.QTY) AS category_units_sold,
        COUNT(f.ORDER_ITEM_ID) AS total_category_items,
        COUNT_IF(f.IS_RETURNED = TRUE) AS total_category_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.products_dim p ON f.PRODUCT_SK = p.PRODUCT_SK
    GROUP BY p.PRODUCT_CATEGORY
)
SELECT
    -- ORPHAN HANDLING: NULL categories become visible instead of silently disappearing
    -- This catches unmapped SKUs that slipped through without a category assignment
    COALESCE(PRODUCT_CATEGORY, 'Unassigned / Orphan SKU') AS product_category,

    -- SALES METRICS: Volume and revenue at category level
    COALESCE(category_units_sold, 0) AS total_units_sold,
    COALESCE(ROUND(category_revenue, 2), 0) AS total_revenue,

    -- PARETO CONTRIBUTION: What % of total business does this category drive
    -- Enables quick identification of "hero categories" vs underperformers
    ROUND(COALESCE(category_revenue, 0) / NULLIF(SUM(category_revenue) OVER (), 0) * 100, 2) AS category_revenue_contribution_pct,

    -- AVERAGE REVENUE PER UNIT: Tracks the categorical price positioning
    -- High value = premium category, low value = volume/commodity category
    ROUND(COALESCE(category_revenue, 0) / NULLIF(category_units_sold, 0), 2) AS avg_revenue_per_unit,

    -- RETURN METRICS: Absolute count + percentage for proper context
    COALESCE(total_category_returns, 0) AS total_returns,
    ROUND((COALESCE(total_category_returns, 0) / NULLIF(total_category_items, 0)) * 100, 2) AS category_return_rate_pct,

    -- CATEGORY HEALTH: Flags problematic categories for merchandising review
    -- Thresholds lower than product-level because category aggregation smooths outliers
    -- A category at 8% means MULTIPLE products within it have quality issues
    CASE
        WHEN total_category_items IS NULL OR total_category_items = 0 THEN 'No Data'
        WHEN (total_category_returns / total_category_items) >= 0.08 THEN 'High Return Risk'
        WHEN (total_category_returns / total_category_items) >= 0.04 THEN 'Moderate Return Risk'
        ELSE 'Healthy'
    END AS category_health_status,

    -- RANKING: Orders categories by revenue for budget allocation decisions
    RANK() OVER (ORDER BY COALESCE(category_revenue, 0) DESC) AS category_rank

FROM category_metrics;

-- VERIFY: Should show all 30 categories ranked by revenue
SELECT * FROM v_category_analysis ORDER BY category_rank;



-- ==============================================================================
-- CERTIFIED VIEW: v_promotional_lift (Marketing Campaign Analytics)
-- ==============================================================================
-- PURPOSE: Measures the revenue impact of each promotion by comparing
-- promoted Average Selling Price (ASP) against full-price baseline.
-- Quantifies whether a promotion drives enough volume to offset margin erosion.
--
-- ARCHITECTURE:
--   - promo_metrics CTE: aggregates from order_item_fact WHERE PROMOTION_SK IS NOT NULL
--   - baseline CTE: aggregates from order_item_fact WHERE PROMOTION_SK IS NULL
--   - CROSS JOIN is safe — baseline produces exactly ONE row (single aggregate)
--   - Comparison reveals true per-unit promotional lift or drag
--
-- CRITICAL DENOMINATOR DECISION:
--   - ASP calculated as REVENUE / SUM(QTY), NOT REVENUE / COUNT(ORDER_ITEM_ID)
--   - COUNT(ORDER_ITEM_ID) = line items (1 line can have QTY=10)
--   - SUM(QTY) = actual units sold (the true physical volume)
--   - Per-unit ASP is the only mathematically valid metric for price comparison
--
-- BUSINESS LOGIC:
--   - lift_per_unit_vs_baseline: positive = promo drives higher-value purchases
--     negative = promo erodes per-unit margin (expected, but must offset with volume)
--   - promoted_return_rate_pct: high returns on promo items = impulse buy signal
--
-- RBAC: Non-sensitive (promotion data is operational, no PII)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_promotional_lift AS
WITH promo_metrics AS (
    -- Revenue and volume from orders WHERE a promotion was actively applied
    -- GROUP BY natural PROMOTION_ID to prevent SCD2 fragmentation
    SELECT
        p.PROMOTION_ID,
        p.DISCOUNT,
        p.DISCOUNT_TIER,
        SUM(f.TOTAL_TRANSACTION) AS promoted_gross_revenue,
        SUM(f.NET_REVENUE) AS promoted_net_revenue,
        -- The correct volume denominator: actual physical units moved
        SUM(f.QTY) AS promoted_units_sold,
        COUNT(f.ORDER_ITEM_ID) AS promoted_item_count,
        COUNT_IF(f.IS_RETURNED = TRUE) AS promoted_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.promotions_dim p ON f.PROMOTION_SK = p.PROMOTION_SK
    WHERE f.PROMOTION_SK IS NOT NULL
    GROUP BY p.PROMOTION_ID, p.DISCOUNT, p.DISCOUNT_TIER
),
baseline AS (
    -- Revenue from full-price orders (no promotion applied)
    -- This is the "control group" for measuring promotional impact
    SELECT
        SUM(f.TOTAL_TRANSACTION) AS baseline_revenue,
        SUM(f.QTY) AS baseline_units,
        -- TRUE ASP: Revenue divided by actual units (not line items)
        -- A line item with QTY=10 at $50 each = $500/10 = $50 per unit
        -- Using COUNT would give $500/1 = $500 per item (completely wrong)
        ROUND(SUM(f.TOTAL_TRANSACTION) / NULLIF(SUM(f.QTY), 0), 2) AS avg_baseline_per_unit
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    WHERE f.PROMOTION_SK IS NULL
)
SELECT
    m.PROMOTION_ID,
    m.DISCOUNT AS discount_percent,
    m.DISCOUNT_TIER,

    -- VOLUME METRICS: How many physical units and transactions this promo drove
    COALESCE(m.promoted_units_sold, 0) AS total_promoted_units,
    COALESCE(m.promoted_item_count, 0) AS total_promoted_transactions,

    -- REVENUE METRICS: Gross and net revenue generated by this promotion
    COALESCE(ROUND(m.promoted_gross_revenue, 2), 0) AS gross_revenue_driven,
    COALESCE(ROUND(m.promoted_net_revenue, 2), 0) AS net_revenue_driven,

    -- AVERAGE SELLING PRICE (ASP): True per-unit comparison
    -- promoted_per_unit vs baseline_per_unit reveals exact margin impact
    ROUND(m.promoted_gross_revenue / NULLIF(m.promoted_units_sold, 0), 2) AS avg_promoted_per_unit,
    b.avg_baseline_per_unit,

    -- TRUE PROMOTIONAL LIFT: Per-unit margin comparison against full-price baseline
    -- Positive = promo customers spending MORE per unit (upsell/premium mix working)
    -- Negative = promo eroding per-unit margin (expected for discounts, but must offset with volume)
    ROUND(
        (m.promoted_gross_revenue / NULLIF(m.promoted_units_sold, 0)) - b.avg_baseline_per_unit, 2
    ) AS lift_per_unit_vs_baseline,

    -- RETURN RATE: Are promoted items returned more often?
    -- High return rate on promos signals impulse buying — customers regret the purchase
    ROUND((m.promoted_returns / NULLIF(m.promoted_item_count, 0)) * 100, 2) AS promoted_return_rate_pct,

    -- REVENUE CONTRIBUTION: What % of total promotional revenue does this specific promo drive
    ROUND(m.promoted_net_revenue / NULLIF(SUM(m.promoted_net_revenue) OVER (), 0) * 100, 2) AS promo_revenue_contribution_pct,

    -- RANKING: Most financially successful promotions for budget reallocation decisions
    RANK() OVER (ORDER BY m.promoted_net_revenue DESC) AS promotion_rank

FROM promo_metrics m
CROSS JOIN baseline b;

-- VERIFY
SELECT * FROM v_promotional_lift ORDER BY promotion_rank;



-- ==============================================================================
-- CERTIFIED VIEW: v_store_performance (Operations & Regional Analytics)
-- ==============================================================================
-- PURPOSE: Revenue, logistics, and quality performance per physical store location.
-- Enables operations to identify top-performing stores and geographic bottlenecks.
--
-- ARCHITECTURE:
--   - Two isolated CTEs prevent Cartesian fan-out between item and order grains:
--     1. store_item_metrics: from order_item_fact (600K → revenue, returns per store)
--     2. store_order_metrics: from order_fact (300K → order count, logistics per store)
--   - GROUP BY natural STORE_ID prevents SCD2 fragmentation
--   - LEFT JOIN to stores_dim for display attributes (city, pincode, manager)
--
-- DATA MODELING DECISION:
--   - NO_OF_EMPLOYEES intentionally excluded from this view
--   - Headcount is a dynamic metric (hires/fires change it daily)
--   - The accurate, dynamically calculated headcount lives in v_labor_efficiency_kpi
--   - Including a stale snapshot here would create conflicting sources of truth
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — each CTE queries the correct grain table
--   - COUNT_IF for Snowflake-native columnar optimization
--   - Resolved shipment denominator (excludes in-transit from penalizing delivery rate)
--
-- RBAC: Store manager name masked for non-authorized roles
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_store_performance AS
WITH store_item_metrics AS (
    -- Revenue and returns from item-grain fact (600K rows)
    -- GROUP BY natural STORE_ID consolidates across SCD2 versions
    SELECT
        s.STORE_ID,
        SUM(i.NET_REVENUE) AS total_revenue,
        SUM(i.QTY) AS total_units_sold,
        COUNT(i.ORDER_ITEM_ID) AS total_items,
        COUNT_IF(i.IS_RETURNED = TRUE) AS total_returns
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact i
    JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s ON i.STORE_SK = s.STORE_SK
    GROUP BY s.STORE_ID
),
store_order_metrics AS (
    -- Order count and logistics from order-grain fact (300K rows, no DISTINCT needed)
    -- Resolved shipment denominator excludes in-transit orders
    SELECT
        s.STORE_ID,
        COUNT(o.ORDER_ID) AS total_orders,
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) = 'LATE') AS total_late_shipments,
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) IN ('DELIVERED', 'LATE')) AS total_resolved_shipments
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact o
    JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s ON o.STORE_SK = s.STORE_SK
    GROUP BY s.STORE_ID
)
SELECT
    curr.STORE_ID,
    curr.CITY,
    curr.PINCODE,

    -- PII MASKING: Store manager name restricted to authorized roles
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'MANAGER_ROLE', 'HR_ROLE'),
        curr.STORE_MANAGER, '*** MASKED ***') AS store_manager,

    -- REVENUE & VOLUME METRICS
    COALESCE(ROUND(im.total_revenue, 2), 0) AS total_revenue,
    COALESCE(im.total_units_sold, 0) AS total_units_sold,
    COALESCE(om.total_orders, 0) AS total_orders,

    -- OPERATIONAL EFFICIENCY: Average order value per store
    ROUND(COALESCE(im.total_revenue, 0) / NULLIF(om.total_orders, 0), 2) AS avg_order_value,

    -- QUALITY METRICS: Return rate signals merchandise handling issues at specific locations
    ROUND((COALESCE(im.total_returns, 0) / NULLIF(im.total_items, 0)) * 100, 2) AS return_rate_pct,

    -- LOGISTICS: Late shipment rate (resolved denominator — excludes in-transit)
    -- Only counts orders that finished their journey (DELIVERED + LATE)
    ROUND((COALESCE(om.total_late_shipments, 0) / NULLIF(om.total_resolved_shipments, 0)) * 100, 2) AS late_shipment_pct,

    -- STORE HEALTH: Combined signal from returns and logistics
    -- Flags stores with EITHER high return rates OR poor delivery performance
    -- A store hitting both thresholds has compounding operational issues
    CASE
        WHEN im.total_items IS NULL OR im.total_items = 0 THEN 'No Data'
        WHEN (im.total_returns / im.total_items) >= 0.08
          OR (om.total_late_shipments / NULLIF(om.total_resolved_shipments, 0)) >= 0.40
        THEN 'Needs Attention'
        ELSE 'Healthy'
    END AS store_health_status,

    -- RANKING: Top-performing stores by revenue for executive reviews
    RANK() OVER (ORDER BY COALESCE(im.total_revenue, 0) DESC) AS store_revenue_rank

FROM NYS_DFS_RETAIL.CONSUMPTION.stores_dim curr
LEFT JOIN store_item_metrics im ON curr.STORE_ID = im.STORE_ID
LEFT JOIN store_order_metrics om ON curr.STORE_ID = om.STORE_ID
WHERE curr.IS_CURRENT = TRUE;

-- VERIFY
SELECT * FROM v_store_performance ORDER BY store_revenue_rank LIMIT 15;


-- ==============================================================================
-- CERTIFIED VIEW: v_shipment_performance (Logistics & Supply Chain Analytics)
-- ==============================================================================
-- PURPOSE: Delivery performance metrics per store location for logistics teams.
-- Identifies geographic bottlenecks where shipments consistently arrive late.
-- Enables operations to optimize routing, carrier selection, and warehouse allocation.
--
-- ARCHITECTURE:
--   - Single CTE queries ONLY the order_fact table (300K rows, 1:1 grain)
--   - Completely bypasses order_item_fact — shipments are order-level, not item-level
--   - GROUP BY natural STORE_ID prevents SCD2 fragmentation
--   - Resolved shipment denominator: excludes in-transit (SHIPPED) and PENDING
--     orders from success/failure rate calculations
--
-- BUSINESS LOGIC:
--   - on_time_delivery_rate_pct + late_delivery_rate_pct = 100% (of resolved)
--   - geographic_delivery_health: Critical (>30% late), Moderate (>15%), Efficient (<15%)
--   - bottleneck_severity_rank: ranked by VOLUME of late shipments (not rate)
--     because a store with 1000 late shipments is worse than one with 3, even if rates differ
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — order_fact grain is guaranteed unique
--   - COUNT_IF for Snowflake-native columnar optimization
--   - Single CTE (no cross-grain joins) — fastest possible execution
--
-- RBAC: Store manager masked — LOGISTICS_ROLE added for escalation workflows
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_shipment_performance AS
WITH logistics_metrics AS (
    -- Exclusively queries order_fact (1:1 grain with orders)
    -- Zero Cartesian fan-out risk — no item-level table involved
    -- GROUP BY natural STORE_ID prevents SCD2 historical fragmentation
    SELECT
        s.STORE_ID,
        -- Total pipeline volume
        COUNT(o.ORDER_ID) AS total_orders_processed,
        -- Current state: orders still moving through the logistics pipeline
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) = 'SHIPPED') AS total_in_transit,
        -- Resolved: orders that completed their journey (success or failure)
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) IN ('DELIVERED', 'LATE')) AS total_resolved_shipments,
        -- Success: arrived on time
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) = 'DELIVERED') AS total_delivered_on_time,
        -- Failure: arrived but missed the SLA window
        COUNT_IF(UPPER(TRIM(o.SHIPMENT_STATUS)) = 'LATE') AS total_late_shipments
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact o
    JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s ON o.STORE_SK = s.STORE_SK
    GROUP BY s.STORE_ID
)
SELECT
    curr.STORE_ID,
    curr.CITY,
    curr.PINCODE,

    -- PII MASKING: Store manager name restricted to operations/logistics roles
    -- Logistics team needs manager names to escalate delivery issues
    IFF(CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'MANAGER_ROLE', 'LOGISTICS_ROLE'),
        curr.STORE_MANAGER, '*** MASKED ***') AS store_manager,

    -- PIPELINE VOLUME: Total throughput and current in-flight orders
    COALESCE(m.total_orders_processed, 0) AS total_orders_processed,
    COALESCE(m.total_in_transit, 0) AS active_in_transit_shipments,
    COALESCE(m.total_resolved_shipments, 0) AS total_resolved_shipments,

    -- LOGISTICS KPIs: Calculated strictly against RESOLVED shipments only
    -- Excludes in-transit orders from penalizing the delivery success rate
    COALESCE(m.total_late_shipments, 0) AS total_late_shipments,

    -- ON-TIME RATE: % of resolved shipments that arrived within SLA
    ROUND((COALESCE(m.total_delivered_on_time, 0) / NULLIF(m.total_resolved_shipments, 0)) * 100, 2) AS on_time_delivery_rate_pct,

    -- LATE RATE: % of resolved shipments that missed SLA
    -- on_time + late = 100% of resolved (mathematical guarantee)
    ROUND((COALESCE(m.total_late_shipments, 0) / NULLIF(m.total_resolved_shipments, 0)) * 100, 2) AS late_delivery_rate_pct,

    -- SUPPLY CHAIN RISK SEGMENTATION: Actionable slicer for logistics dashboards
    -- Critical (>30%): systemic routing failure — needs carrier/warehouse review
    -- Moderate (>15%): emerging pattern — investigate before it worsens
    -- Efficient (<15%): within acceptable retail delivery norms
    CASE
        WHEN m.total_resolved_shipments IS NULL OR m.total_resolved_shipments = 0 THEN 'No Resolved Data'
        WHEN (m.total_late_shipments / m.total_resolved_shipments) >= 0.30 THEN 'Critical Bottleneck'
        WHEN (m.total_late_shipments / m.total_resolved_shipments) >= 0.15 THEN 'Moderate Delay Risk'
        ELSE 'Efficient Routing'
    END AS geographic_delivery_health,

    -- BOTTLENECK RANKING: Ranked by VOLUME of late shipments (not rate)
    -- A store with 1000 late orders has more operational impact than one with 3
    -- Even if the small store has a higher late %, the large store costs more to fix
    RANK() OVER (ORDER BY COALESCE(m.total_late_shipments, 0) DESC) AS bottleneck_severity_rank

FROM NYS_DFS_RETAIL.CONSUMPTION.stores_dim curr
LEFT JOIN logistics_metrics m ON curr.STORE_ID = m.STORE_ID
WHERE curr.IS_CURRENT = TRUE;

-- VERIFY: Shows worst-performing stores first (highest late volume)
SELECT * FROM v_shipment_performance ORDER BY bottleneck_severity_rank LIMIT 15;

-- ==============================================================================
-- CERTIFIED VIEW: v_annual_revenue (Finance & Executive Reporting)
-- ==============================================================================
-- PURPOSE: Year-over-year revenue trends for executive financial reviews.
-- Provides gross, refunds, and net revenue with automated YoY growth calculation.
--
-- ARCHITECTURE:
--   - Two isolated CTEs prevent Cartesian fan-out:
--     1. annual_item_metrics: from order_item_fact (revenue, refunds, units)
--     2. annual_order_metrics: from order_fact (order count, no DISTINCT)
--   - FULL OUTER JOIN safely combines both grains
--   - LAG() window function calculates YoY growth without expensive self-joins
--
-- VERIFIED SCHEMA REFERENCES:
--   - Table: NYS_DFS_RETAIL.CONSUMPTION.DIM_DATE (not date_dim)
--   - Join key: DATE_ID (not DATE_SK)
--   - Year column: CALENDAR_YEAR (not YEAR)
--
-- BUSINESS LOGIC:
--   - yoy_revenue_growth_pct: (current - previous) / previous * 100
--   - First year shows NULL for YoY (no prior year — correct behavior)
--   - NULLIF protects against divide-by-zero if prior year revenue was $0
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — order_fact grain is 1:1
--   - LAG() is a single-pass window function (no self-join overhead)
--   - dim_date join resolves YYYYMMDD integer key to calendar year
--
-- RBAC: Non-sensitive (aggregated financial metrics, no PII)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_annual_revenue AS
WITH annual_item_metrics AS (
    -- Revenue metrics from item-grain fact joined to date dimension
    -- GROUP BY CALENDAR_YEAR for annual aggregation
    SELECT
        d.CALENDAR_YEAR AS reporting_year,
        SUM(f.TOTAL_TRANSACTION) AS total_gross_revenue,
        SUM(f.REFUND_AMOUNT) AS total_refund_amount,
        SUM(f.NET_REVENUE) AS total_net_revenue,
        SUM(f.QTY) AS total_units_sold
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_YEAR
),
annual_order_metrics AS (
    -- Order count from order-grain fact (1:1, no DISTINCT needed)
    SELECT
        d.CALENDAR_YEAR AS reporting_year,
        COUNT(f.ORDER_ID) AS total_orders
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_YEAR
),
annual_summary AS (
    -- FULL OUTER JOIN: handles edge case where a year has orders but no items or vice versa
    SELECT
        COALESCE(i.reporting_year, o.reporting_year) AS reporting_year,
        COALESCE(i.total_gross_revenue, 0) AS gross_revenue,
        COALESCE(i.total_refund_amount, 0) AS total_refunds,
        COALESCE(i.total_net_revenue, 0) AS net_revenue,
        COALESCE(i.total_units_sold, 0) AS total_units_sold,
        COALESCE(o.total_orders, 0) AS total_orders
    FROM annual_item_metrics i
    FULL OUTER JOIN annual_order_metrics o ON i.reporting_year = o.reporting_year
)
SELECT
    reporting_year,

    -- FINANCIAL METRICS: The three numbers finance cares about
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(total_refunds, 2) AS total_refunds,
    ROUND(net_revenue, 2) AS net_revenue,

    -- VOLUME METRICS: Scale of business operations
    total_orders,
    total_units_sold,

    -- AVERAGE ORDER VALUE: Blended financial performance signal
    ROUND(net_revenue / NULLIF(total_orders, 0), 2) AS avg_order_value,

    -- YEAR-OVER-YEAR: LAG() window function avoids expensive self-joins
    -- Pulls prior year's revenue in a single pass over the sorted result
    ROUND(LAG(net_revenue) OVER (ORDER BY reporting_year), 2) AS previous_year_revenue,

    -- YOY GROWTH: (Current - Previous) / Previous * 100
    -- First year returns NULL (no prior year to compare — correct behavior)
    -- NULLIF protects against divide-by-zero if prior year was $0
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY reporting_year))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY reporting_year), 0) * 100
    , 2) AS yoy_revenue_growth_pct

FROM annual_summary;

-- VERIFY: Most recent years first
SELECT * FROM v_annual_revenue ORDER BY reporting_year DESC;

-- ==============================================================================
-- CERTIFIED VIEW: v_monthly_revenue (Finance & Trend Analysis)
-- ==============================================================================
-- PURPOSE: Month-over-month revenue trends for financial planning and forecasting.
-- Provides gross, refunds, and net revenue with automated MoM growth calculation.
-- Enables finance to spot seasonal patterns and revenue acceleration/deceleration.
--
-- ARCHITECTURE:
--   - Two isolated CTEs prevent Cartesian fan-out:
--     1. monthly_item_metrics: from order_item_fact (revenue, refunds, units)
--     2. monthly_order_metrics: from order_fact (order count, no DISTINCT)
--   - FULL OUTER JOIN safely combines both grains
--   - LAG() window function calculates MoM growth without expensive self-joins
--   - Ordered by (YEAR, MONTH) to ensure correct chronological LAG comparison
--
-- VERIFIED SCHEMA REFERENCES:
--   - Table: NYS_DFS_RETAIL.CONSUMPTION.DIM_DATE
--   - Join key: DATE_ID
--   - Group columns: CALENDAR_YEAR, CALENDAR_MONTH
--
-- BUSINESS LOGIC:
--   - mom_revenue_growth_pct: (current_month - previous_month) / previous_month * 100
--   - First month shows NULL for MoM (no prior month — correct behavior)
--   - NULLIF protects against divide-by-zero if prior month revenue was $0
--   - Seasonal patterns visible: Dec spike (holidays), Jan dip (post-holiday)
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — order_fact grain is 1:1
--   - LAG() is a single-pass window function (no self-join overhead)
--   - dim_date join resolves YYYYMMDD integer key to year + month
--
-- RBAC: Non-sensitive (aggregated financial metrics, no PII)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_monthly_revenue AS
WITH monthly_item_metrics AS (
    -- Revenue metrics from item-grain fact joined to date dimension
    -- GROUP BY YEAR + MONTH for monthly aggregation
    SELECT
        d.CALENDAR_YEAR AS reporting_year,
        d.CALENDAR_MONTH AS reporting_month,
        SUM(f.TOTAL_TRANSACTION) AS total_gross_revenue,
        SUM(f.REFUND_AMOUNT) AS total_refund_amount,
        SUM(f.NET_REVENUE) AS total_net_revenue,
        SUM(f.QTY) AS total_units_sold
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_YEAR, d.CALENDAR_MONTH
),
monthly_order_metrics AS (
    -- Order count from order-grain fact (1:1, no DISTINCT needed)
    SELECT
        d.CALENDAR_YEAR AS reporting_year,
        d.CALENDAR_MONTH AS reporting_month,
        COUNT(f.ORDER_ID) AS total_orders
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_YEAR, d.CALENDAR_MONTH
),
monthly_summary AS (
    -- FULL OUTER JOIN: handles edge case where a month has orders but no items or vice versa
    SELECT
        COALESCE(i.reporting_year, o.reporting_year) AS reporting_year,
        COALESCE(i.reporting_month, o.reporting_month) AS reporting_month,
        COALESCE(i.total_gross_revenue, 0) AS gross_revenue,
        COALESCE(i.total_refund_amount, 0) AS total_refunds,
        COALESCE(i.total_net_revenue, 0) AS net_revenue,
        COALESCE(i.total_units_sold, 0) AS total_units_sold,
        COALESCE(o.total_orders, 0) AS total_orders
    FROM monthly_item_metrics i
    FULL OUTER JOIN monthly_order_metrics o
        ON i.reporting_year = o.reporting_year
        AND i.reporting_month = o.reporting_month
)
SELECT
    reporting_year,
    reporting_month,

    -- FINANCIAL METRICS: The three numbers finance cares about at monthly grain
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(total_refunds, 2) AS total_refunds,
    ROUND(net_revenue, 2) AS net_revenue,

    -- VOLUME METRICS: Monthly operational scale
    total_orders,
    total_units_sold,

    -- AVERAGE ORDER VALUE: Monthly blended performance signal
    ROUND(net_revenue / NULLIF(total_orders, 0), 2) AS avg_order_value,

    -- MONTH-OVER-MONTH: LAG() window function avoids expensive self-joins
    -- ORDER BY (year, month) ensures correct chronological comparison
    -- e.g., Jan 2023 compares to Dec 2022 (not Jan 2022)
    ROUND(LAG(net_revenue) OVER (ORDER BY reporting_year, reporting_month), 2) AS previous_month_revenue,

    -- MOM GROWTH: (Current - Previous) / Previous * 100
    -- First month returns NULL (no prior month — correct behavior)
    -- NULLIF protects against divide-by-zero if prior month was $0
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY reporting_year, reporting_month))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY reporting_year, reporting_month), 0) * 100
    , 2) AS mom_revenue_growth_pct

FROM monthly_summary;

-- VERIFY: Most recent 24 months (2 years of monthly trends)
SELECT * FROM v_monthly_revenue ORDER BY reporting_year DESC, reporting_month DESC LIMIT 24;


-- ==============================================================================
-- CERTIFIED VIEW: v_daily_revenue_kpi (Operations & Real-Time Monitoring)
-- ==============================================================================
-- PURPOSE: Daily revenue metrics for operational dashboards and real-time KPI tiles.
-- Includes rolling 7-day trend and day-over-day growth for anomaly detection.
-- Enables operations to spot revenue dips/spikes within hours, not weeks.
--
-- ARCHITECTURE:
--   - Two isolated CTEs prevent Cartesian fan-out:
--     1. daily_item_metrics: from order_item_fact (revenue, refunds, units per day)
--     2. daily_order_metrics: from order_fact (order count per day, no DISTINCT)
--   - FULL OUTER JOIN safely combines both grains
--   - RANGE BETWEEN for rolling window (calendar-aware, not row-based)
--   - LAG() for day-over-day growth without self-joins
--
-- VERIFIED SCHEMA REFERENCES:
--   - Table: NYS_DFS_RETAIL.CONSUMPTION.DIM_DATE
--   - Join key: DATE_ID
--   - Date column: CALENDAR_DATE
--
-- BUSINESS LOGIC:
--   - rolling_7_day_revenue: RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING
--     Uses calendar-aware window (not ROWS BETWEEN which ignores date gaps)
--     If a day has no sales, the window still spans exactly 7 calendar days
--   - dod_revenue_growth_pct: (today - yesterday) / yesterday * 100
--     Volatile metric — best used with the rolling average for context
--   - First day shows NULL for DoD (no prior day — correct behavior)
--
-- PERFORMANCE:
--   - No COUNT(DISTINCT) — order_fact grain is 1:1
--   - Window functions (SUM OVER, LAG) are single-pass operations
--   - dim_date join resolves YYYYMMDD integer key to native DATE for window math
--
-- RBAC: Non-sensitive (aggregated daily metrics, no PII)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE SECURE VIEW v_daily_revenue_kpi AS
WITH daily_item_metrics AS (
    -- Revenue metrics from item-grain fact at daily granularity
    -- CALENDAR_DATE from dim_date enables proper date-based window functions
    SELECT
        d.CALENDAR_DATE AS reporting_date,
        SUM(f.TOTAL_TRANSACTION) AS total_gross_revenue,
        SUM(f.REFUND_AMOUNT) AS total_refund_amount,
        SUM(f.NET_REVENUE) AS total_net_revenue,
        SUM(f.QTY) AS total_units_sold
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_item_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_DATE
),
daily_order_metrics AS (
    -- Order count from order-grain fact (1:1, no DISTINCT needed)
    SELECT
        d.CALENDAR_DATE AS reporting_date,
        COUNT(f.ORDER_ID) AS total_orders
    FROM NYS_DFS_RETAIL.CONSUMPTION.order_fact f
    JOIN NYS_DFS_RETAIL.CONSUMPTION.dim_date d ON f.DATE_ID = d.DATE_ID
    GROUP BY d.CALENDAR_DATE
),
daily_summary AS (
    -- FULL OUTER JOIN: handles days where orders exist but no items or vice versa
    SELECT
        COALESCE(i.reporting_date, o.reporting_date) AS reporting_date,
        COALESCE(i.total_gross_revenue, 0) AS gross_revenue,
        COALESCE(i.total_refund_amount, 0) AS total_refunds,
        COALESCE(i.total_net_revenue, 0) AS net_revenue,
        COALESCE(i.total_units_sold, 0) AS total_units_sold,
        COALESCE(o.total_orders, 0) AS total_orders
    FROM daily_item_metrics i
    FULL OUTER JOIN daily_order_metrics o ON i.reporting_date = o.reporting_date
)
SELECT
    reporting_date,

    -- DAILY FINANCIAL METRICS
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(total_refunds, 2) AS total_refunds,
    ROUND(net_revenue, 2) AS net_revenue,

    -- VOLUME METRICS: Daily operational throughput
    total_orders,
    total_units_sold,

    -- AVERAGE ORDER VALUE: Daily operational performance signal
    ROUND(net_revenue / NULLIF(total_orders, 0), 2) AS daily_avg_order_value,

    -- ROLLING 7-DAY REVENUE: Smooths daily volatility for trend detection
    -- RANGE BETWEEN uses calendar days (not row count) — correctly handles
    -- date gaps where no transactions occurred (weekends, holidays, outages)
    -- A ROWS-based window would incorrectly pull data from 7+ calendar days ago
    ROUND(
        SUM(net_revenue) OVER (
            ORDER BY reporting_date
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW
        )
    , 2) AS rolling_7_day_revenue,

    -- DAY-OVER-DAY GROWTH: (Today - Yesterday) / Yesterday * 100
    -- Highly volatile metric — best paired with rolling_7_day for context
    -- A -50% DoD drop with stable rolling_7_day = one bad day (not a trend)
    -- A -50% DoD drop with declining rolling_7_day = systemic issue
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY reporting_date))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY reporting_date), 0) * 100
    , 2) AS dod_revenue_growth_pct

FROM daily_summary;

-- VERIFY: Most recent 30 days of daily performance
SELECT * FROM v_daily_revenue_kpi ORDER BY reporting_date DESC LIMIT 30;
