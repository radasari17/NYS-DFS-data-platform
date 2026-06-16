-- ==========================================
-- GOLD LAYER (CONSUMPTION) — Date Dimension
-- ==========================================
-- PURPOSE: Foundational calendar dimension for all time-based analytics.
-- Generated dynamically from earliest order date to today using GENERATOR.
-- Includes fiscal year mapping (April start), quarter flags, weekend/month-end indicators.
-- GENERATOR approach avoids Snowflake's 100-iteration recursive CTE limit.
-- ==========================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

-- DIMENSION TABLE: Comprehensive calendar for time-based analytics.
-- DATE_ID is an integer key in YYYYMMDD format for fast fact table joins.
CREATE OR REPLACE TABLE dim_date (
    DATE_ID          INT PRIMARY KEY,          -- Smart key: YYYYMMDD (e.g., 20210826)
    CALENDAR_DATE    DATE NOT NULL,            -- Native date for date math functions
    CALENDAR_YEAR    INT NOT NULL,             -- 2019, 2020, 2021...
    CALENDAR_QUARTER INT NOT NULL,             -- 1, 2, 3, 4
    CALENDAR_MONTH   INT NOT NULL,             -- 1-12
    MONTH_NAME       VARCHAR(10) NOT NULL,     -- Jan, Feb, Mar...
    DAY_OF_MONTH     INT NOT NULL,             -- 1-31
    DAY_OF_WEEK      INT NOT NULL,             -- 0=Sun, 6=Sat (Snowflake default)
    DAY_NAME         VARCHAR(10) NOT NULL,     -- Mon, Tue, Wed...
    DAY_OF_YEAR      INT NOT NULL,             -- 1-365 (YoY same-day comparison)
    IS_WEEKEND       BOOLEAN NOT NULL,         -- TRUE for Sat/Sun
    IS_MONTH_END     BOOLEAN NOT NULL,         -- TRUE on last day of month (GL closing)
    FISCAL_YEAR      INT NOT NULL,             -- Indian fiscal: Apr start (Apr 2021 = FY2022)
    FISCAL_QUARTER   INT NOT NULL              -- FQ1=Apr-Jun, FQ2=Jul-Sep, FQ3=Oct-Dec, FQ4=Jan-Mar
);

-- POPULATE: GENERATOR creates sequential integers (0,1,2,3...) added to start date.
-- ROWCOUNT 3650 covers ~10 years. WHERE clause trims to exactly today.
INSERT INTO dim_date
WITH date_range AS (
    -- SEQ4() produces 0-based sequence, added to the earliest order date
    SELECT
        DATEADD(DAY, SEQ4(), (SELECT MIN(ORDER_DATE) FROM NYS_DFS_RETAIL.CLEAN.clean_orders)) AS dte
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    -- INTEGER KEY: Enables fast numeric joins vs slower DATE comparisons
    TO_CHAR(dte, 'YYYYMMDD')::INT AS DATE_ID,

    -- RAW DATE: For DATEDIFF, DATEADD, and native Snowflake date math
    dte AS CALENDAR_DATE,

    -- CALENDAR YEAR: Standard grouping for annual reports
    EXTRACT(YEAR FROM dte) AS CALENDAR_YEAR,

    -- CALENDAR QUARTER: Standard Q1-Q4 for quarterly business reviews
    EXTRACT(QUARTER FROM dte) AS CALENDAR_QUARTER,

    -- CALENDAR MONTH: 1-12 for monthly trend analysis
    EXTRACT(MONTH FROM dte) AS CALENDAR_MONTH,

    -- MONTH NAME: Human-readable for dashboard labels
    MONTHNAME(dte) AS MONTH_NAME,

    -- DAY OF MONTH: 1-31 for daily granularity
    EXTRACT(DAY FROM dte) AS DAY_OF_MONTH,

    -- DAY OF WEEK: 0=Sunday through 6=Saturday
    DAYOFWEEK(dte) AS DAY_OF_WEEK,

    -- DAY NAME: Mon, Tue, Wed... for dashboard slicers
    DAYNAME(dte) AS DAY_NAME,

    -- DAY OF YEAR: Enables "day 150 of 2023 vs day 150 of 2022" YoY comparisons
    EXTRACT(DAYOFYEAR FROM dte) AS DAY_OF_YEAR,

    -- WEEKEND FLAG: Retail sales patterns shift significantly on weekends
    IFF(DAYOFWEEK(dte) IN (0, 6), TRUE, FALSE) AS IS_WEEKEND,

    -- MONTH-END FLAG: Finance uses for GL closing period reconciliation
    IFF(dte = LAST_DAY(dte), TRUE, FALSE) AS IS_MONTH_END,

    -- FISCAL YEAR: Indian retail fiscal year starts April
    -- Apr 2021 through Mar 2022 = FY2022
    IFF(EXTRACT(MONTH FROM dte) >= 4,
        EXTRACT(YEAR FROM dte) + 1,
        EXTRACT(YEAR FROM dte)
    ) AS FISCAL_YEAR,

    -- FISCAL QUARTER: Maps calendar months to Indian fiscal quarters
    -- FQ1=Apr-Jun, FQ2=Jul-Sep, FQ3=Oct-Dec, FQ4=Jan-Mar
    CASE
        WHEN EXTRACT(MONTH FROM dte) IN (4, 5, 6) THEN 1
        WHEN EXTRACT(MONTH FROM dte) IN (7, 8, 9) THEN 2
        WHEN EXTRACT(MONTH FROM dte) IN (10, 11, 12) THEN 3
        ELSE 4
    END AS FISCAL_QUARTER

FROM date_range
-- Only generate dates up to today — no future dates in the dimension
WHERE dte <= CURRENT_DATE();

-- ==========================================
-- VERIFY (Advanced Governance Checks)
-- ==========================================
-- CONTINUITY CHECK: Total days must equal DATEDIFF + 1
-- If they don't match, there's a gap — sales on that day would vanish from dashboards
-- UNIQUENESS CHECK: Total rows must equal distinct dates
-- If they don't match, duplicates exist — would double-count revenue on that day
SELECT
    COUNT(*) AS total_days,
    MIN(CALENDAR_DATE) AS earliest_date,
    MAX(CALENDAR_DATE) AS latest_date,
    IFF(
        COUNT(*) = DATEDIFF(DAY, MIN(CALENDAR_DATE), MAX(CALENDAR_DATE)) + 1,
        'PASS',
        'FAIL: MISSING DATES'
    ) AS continuous_calendar_check,
    IFF(
        COUNT(*) = COUNT(DISTINCT CALENDAR_DATE),
        'PASS',
        'FAIL: DUPLICATES FOUND'
    ) AS uniqueness_check
FROM NYS_DFS_RETAIL.CONSUMPTION.dim_date;

-- VISUAL INSPECTION: Verify most recent dates look correct
SELECT * FROM NYS_DFS_RETAIL.CONSUMPTION.dim_date ORDER BY CALENDAR_DATE DESC LIMIT 15;