-- ==============================================================================
-- STORED PROCEDURE 1: sp_bronze_to_silver()
-- ==============================================================================
-- PURPOSE: Consumes all 5 Bronze streams within a single atomic transaction.
-- Routes invalid records to DLQ, MERGEs valid records into Silver.
--
-- ARCHITECTURE:
-- - NO temporary tables (DDL auto-commits and breaks transaction isolation)
-- - Explicit BEGIN TRANSACTION ensures all 5 tables succeed or fail together
-- - EXCEPTION block with ROLLBACK prevents stream offset from advancing on failure
-- - Streams queried directly (repeatable read within transaction = same data each read)
--
-- CALLED BY: silver_layer_task (daily at 6 AM UTC)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CLEAN;

CREATE OR REPLACE PROCEDURE sp_bronze_to_silver()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Explicit transaction: all 5 tables commit together or roll back together.
    -- Stream offsets only advance on COMMIT — ROLLBACK preserves the data for retry.
    BEGIN TRANSACTION;

    -- ==========================================
    -- 1. CLEAN ORDERS
    -- ==========================================
    -- DLQ: Route invalid records (stream supports repeatable read — same data on re-query)
    INSERT INTO NYS_DFS_RETAIL.CLEAN.clean_orders_dlq
        (ORDER_ID, CUSTOMER_ID, STORE_ID, ORDER_DATE, PROMOTION_ID, rejection_reason)
    SELECT ORDER_ID, CUSTOMER_ID, STORE_ID, ORDER_DATE, PROMOTION_ID,
        CASE
            WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
            WHEN TRY_CAST(CUSTOMER_ID AS INT) IS NULL THEN 'INVALID_CUSTOMER_ID'
            WHEN TRY_CAST(STORE_ID AS INT) IS NULL THEN 'INVALID_STORE_ID'
            WHEN TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') IS NULL THEN 'INVALID_ORDER_DATE_FORMAT'
            WHEN TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') < '2018-01-01'
              OR TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') > DATEADD(DAY, 1, CURRENT_DATE())
            THEN 'OUT_OF_RANGE_ORDER_DATE'
            ELSE 'UNKNOWN_REJECTION'
        END
    FROM NYS_DFS_RETAIL.STAGE.raw_orders_stm
    WHERE METADATA$ACTION = 'INSERT'
        AND (
            TRY_CAST(ORDER_ID AS INT) IS NULL
            OR TRY_CAST(CUSTOMER_ID AS INT) IS NULL
            OR TRY_CAST(STORE_ID AS INT) IS NULL
            OR TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') IS NULL
            OR TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') < '2018-01-01'
            OR TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') > DATEADD(DAY, 1, CURRENT_DATE())
        );

    -- MERGE valid orders
    MERGE INTO NYS_DFS_RETAIL.CLEAN.clean_orders target
    USING (
        SELECT
            TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,
            TRY_CAST(CUSTOMER_ID AS INT) AS CUSTOMER_ID,
            TRY_CAST(STORE_ID AS INT) AS STORE_ID,
            TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') AS ORDER_DATE,
            TRY_CAST(PROMOTION_ID AS INT) AS PROMOTION_ID
        FROM NYS_DFS_RETAIL.STAGE.raw_orders_stm
        WHERE METADATA$ACTION = 'INSERT'
            AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
            AND TRY_CAST(CUSTOMER_ID AS INT) IS NOT NULL
            AND TRY_CAST(STORE_ID AS INT) IS NOT NULL
            AND TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') IS NOT NULL
            AND TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') >= '2018-01-01'
            AND TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') <= DATEADD(DAY, 1, CURRENT_DATE())
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(ORDER_ID AS INT) ORDER BY TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY') DESC) = 1
    ) source
    ON target.ORDER_ID = source.ORDER_ID
    WHEN MATCHED THEN UPDATE SET
        target.CUSTOMER_ID = source.CUSTOMER_ID, target.STORE_ID = source.STORE_ID,
        target.ORDER_DATE = source.ORDER_DATE, target.PROMOTION_ID = source.PROMOTION_ID,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (ORDER_ID, CUSTOMER_ID, STORE_ID, ORDER_DATE, PROMOTION_ID)
    VALUES (source.ORDER_ID, source.CUSTOMER_ID, source.STORE_ID, source.ORDER_DATE, source.PROMOTION_ID);

    -- ==========================================
    -- 2. CLEAN ORDER ITEMS
    -- ==========================================
    INSERT INTO NYS_DFS_RETAIL.CLEAN.clean_order_items_dlq
        (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, QTY, TOTAL_TRANSACTION, rejection_reason)
    SELECT ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, QTY, TOTAL_TRANSACTION,
        CASE
            WHEN TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL THEN 'INVALID_ORDER_ITEM_ID'
            WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
            WHEN TRY_CAST(PRODUCT_ID AS INT) IS NULL THEN 'INVALID_PRODUCT_ID'
            WHEN TRY_CAST(QTY AS INT) IS NULL OR TRY_CAST(QTY AS INT) <= 0 THEN 'INVALID_QUANTITY'
            WHEN TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) IS NULL
              OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) <= 0 THEN 'INVALID_TRANSACTION_AMOUNT'
            ELSE 'UNKNOWN_REJECTION'
        END
    FROM NYS_DFS_RETAIL.STAGE.raw_order_items_stm
    WHERE METADATA$ACTION = 'INSERT'
        AND (
            TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL
            OR TRY_CAST(ORDER_ID AS INT) IS NULL
            OR TRY_CAST(PRODUCT_ID AS INT) IS NULL
            OR TRY_CAST(QTY AS INT) IS NULL OR TRY_CAST(QTY AS INT) <= 0
            OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) IS NULL
            OR TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) <= 0
        );

    MERGE INTO NYS_DFS_RETAIL.CLEAN.clean_order_items target
    USING (
        SELECT
            TRY_CAST(ORDER_ITEM_ID AS INT) AS ORDER_ITEM_ID,
            TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,
            TRY_CAST(PRODUCT_ID AS INT) AS PRODUCT_ID,
            COALESCE(TRIM(REGEXP_REPLACE(PRODUCT_NAME, '\\s*\\(P-\\d+\\)$', '')), 'UNKNOWN') AS PRODUCT_NAME,
            TRY_CAST(QTY AS INT) AS QTY,
            TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) AS TOTAL_TRANSACTION,
            DIV0NULL(TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)), TRY_CAST(QTY AS INT)) AS UNIT_PRICE
        FROM NYS_DFS_RETAIL.STAGE.raw_order_items_stm
        WHERE METADATA$ACTION = 'INSERT'
            AND TRY_CAST(ORDER_ITEM_ID AS INT) IS NOT NULL
            AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
            AND TRY_CAST(PRODUCT_ID AS INT) IS NOT NULL
            AND TRY_CAST(QTY AS INT) > 0
            AND TRY_CAST(TOTAL_TRANSACTION AS DECIMAL(10,2)) > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY ORDER_ITEM_ID) = 1
    ) source
    ON target.ORDER_ITEM_ID = source.ORDER_ITEM_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID = source.ORDER_ID, target.PRODUCT_ID = source.PRODUCT_ID,
        target.PRODUCT_NAME = source.PRODUCT_NAME, target.QTY = source.QTY,
        target.TOTAL_TRANSACTION = source.TOTAL_TRANSACTION, target.UNIT_PRICE = source.UNIT_PRICE,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, QTY, TOTAL_TRANSACTION, UNIT_PRICE)
    VALUES (source.ORDER_ITEM_ID, source.ORDER_ID, source.PRODUCT_ID, source.PRODUCT_NAME, source.QTY, source.TOTAL_TRANSACTION, source.UNIT_PRICE);

    -- ==========================================
    -- 3. CLEAN PAYMENTS
    -- ==========================================
    INSERT INTO NYS_DFS_RETAIL.CLEAN.clean_payments_dlq
        (PAYMENT_ID, ORDER_ID, PAYMENT_AMOUNT, rejection_reason)
    SELECT PAYMENT_ID, ORDER_ID, PAYMENT_AMOUNT,
        CASE
            WHEN TRY_CAST(PAYMENT_ID AS INT) IS NULL THEN 'INVALID_PAYMENT_ID'
            WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
            WHEN TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) IS NULL THEN 'NON_NUMERIC_AMOUNT'
            WHEN TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) <= 0 THEN 'ZERO_OR_NEGATIVE_AMOUNT'
            ELSE 'UNKNOWN_REJECTION'
        END
    FROM NYS_DFS_RETAIL.STAGE.raw_payments_stm
    WHERE METADATA$ACTION = 'INSERT'
        AND (
            TRY_CAST(PAYMENT_ID AS INT) IS NULL
            OR TRY_CAST(ORDER_ID AS INT) IS NULL
            OR TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) IS NULL
            OR TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) <= 0
        );

    MERGE INTO NYS_DFS_RETAIL.CLEAN.clean_payments target
    USING (
        SELECT
            TRY_CAST(PAYMENT_ID AS INT) AS PAYMENT_ID,
            TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,
            TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) AS PAYMENT_AMOUNT
        FROM NYS_DFS_RETAIL.STAGE.raw_payments_stm
        WHERE METADATA$ACTION = 'INSERT'
            AND TRY_CAST(PAYMENT_ID AS INT) IS NOT NULL
            AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
            AND TRY_CAST(PAYMENT_AMOUNT AS DECIMAL(10,2)) > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(PAYMENT_ID AS INT) ORDER BY TRY_CAST(PAYMENT_ID AS INT)) = 1
    ) source
    ON target.PAYMENT_ID = source.PAYMENT_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID = source.ORDER_ID, target.PAYMENT_AMOUNT = source.PAYMENT_AMOUNT,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (PAYMENT_ID, ORDER_ID, PAYMENT_AMOUNT)
    VALUES (source.PAYMENT_ID, source.ORDER_ID, source.PAYMENT_AMOUNT);

    -- ==========================================
    -- 4. CLEAN RETURNS
    -- ==========================================
    INSERT INTO NYS_DFS_RETAIL.CLEAN.clean_returns_dlq
        (RETURN_ID, ORDER_ITEM_ID, REFUND, rejection_reason)
    SELECT RETURN_ID, ORDER_ITEM_ID, REFUND,
        CASE
            WHEN TRY_CAST(RETURN_ID AS INT) IS NULL THEN 'INVALID_RETURN_ID'
            WHEN TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL THEN 'INVALID_ORDER_ITEM_ID'
            WHEN TRY_CAST(REFUND AS DECIMAL(10,2)) IS NULL THEN 'NON_NUMERIC_REFUND'
            WHEN TRY_CAST(REFUND AS DECIMAL(10,2)) <= 0 THEN 'ZERO_OR_NEGATIVE_REFUND'
            ELSE 'UNKNOWN_REJECTION'
        END
    FROM NYS_DFS_RETAIL.STAGE.raw_returns_stm
    WHERE METADATA$ACTION = 'INSERT'
        AND (
            TRY_CAST(RETURN_ID AS INT) IS NULL
            OR TRY_CAST(ORDER_ITEM_ID AS INT) IS NULL
            OR TRY_CAST(REFUND AS DECIMAL(10,2)) IS NULL
            OR TRY_CAST(REFUND AS DECIMAL(10,2)) <= 0
        );

    MERGE INTO NYS_DFS_RETAIL.CLEAN.clean_returns target
    USING (
        SELECT
            TRY_CAST(RETURN_ID AS INT) AS RETURN_ID,
            TRY_CAST(ORDER_ITEM_ID AS INT) AS ORDER_ITEM_ID,
            TRY_CAST(REFUND AS DECIMAL(10,2)) AS REFUND
        FROM NYS_DFS_RETAIL.STAGE.raw_returns_stm
        WHERE METADATA$ACTION = 'INSERT'
            AND TRY_CAST(RETURN_ID AS INT) IS NOT NULL
            AND TRY_CAST(ORDER_ITEM_ID AS INT) IS NOT NULL
            AND TRY_CAST(REFUND AS DECIMAL(10,2)) > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(RETURN_ID AS INT) ORDER BY TRY_CAST(RETURN_ID AS INT)) = 1
    ) source
    ON target.RETURN_ID = source.RETURN_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ITEM_ID = source.ORDER_ITEM_ID, target.REFUND = source.REFUND,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (RETURN_ID, ORDER_ITEM_ID, REFUND)
    VALUES (source.RETURN_ID, source.ORDER_ITEM_ID, source.REFUND);

    -- ==========================================
    -- 5. CLEAN SHIPMENTS
    -- ==========================================
    INSERT INTO NYS_DFS_RETAIL.CLEAN.clean_shipments_dlq
        (SHIPMENT_ID, ORDER_ID, STATUS, rejection_reason)
    SELECT SHIPMENT_ID, ORDER_ID, STATUS,
        CASE
            WHEN TRY_CAST(SHIPMENT_ID AS INT) IS NULL THEN 'INVALID_SHIPMENT_ID'
            WHEN TRY_CAST(ORDER_ID AS INT) IS NULL THEN 'INVALID_ORDER_ID'
            WHEN TRIM(STATUS) IN ('', 'None', 'NA', 'null') OR STATUS IS NULL THEN 'MISSING_STATUS'
            WHEN UPPER(TRIM(STATUS)) NOT IN ('SHIPPED', 'DELIVERED', 'LATE') THEN 'UNKNOWN_STATUS_VALUE'
            ELSE 'UNKNOWN_REJECTION'
        END
    FROM NYS_DFS_RETAIL.STAGE.raw_shipments_stm
    WHERE METADATA$ACTION = 'INSERT'
        AND (
            TRY_CAST(SHIPMENT_ID AS INT) IS NULL
            OR TRY_CAST(ORDER_ID AS INT) IS NULL
            OR TRIM(STATUS) IN ('', 'None', 'NA', 'null')
            OR STATUS IS NULL
            OR UPPER(TRIM(STATUS)) NOT IN ('SHIPPED', 'DELIVERED', 'LATE')
        );

    MERGE INTO NYS_DFS_RETAIL.CLEAN.clean_shipments target
    USING (
        SELECT
            TRY_CAST(SHIPMENT_ID AS INT) AS SHIPMENT_ID,
            TRY_CAST(ORDER_ID AS INT) AS ORDER_ID,
            UPPER(TRIM(STATUS)) AS STATUS
        FROM NYS_DFS_RETAIL.STAGE.raw_shipments_stm
        WHERE METADATA$ACTION = 'INSERT'
            AND TRY_CAST(SHIPMENT_ID AS INT) IS NOT NULL
            AND TRY_CAST(ORDER_ID AS INT) IS NOT NULL
            AND UPPER(TRIM(STATUS)) IN ('SHIPPED', 'DELIVERED', 'LATE')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(SHIPMENT_ID AS INT) ORDER BY TRY_CAST(SHIPMENT_ID AS INT)) = 1
    ) source
    ON target.SHIPMENT_ID = source.SHIPMENT_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID = source.ORDER_ID, target.STATUS = source.STATUS,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (SHIPMENT_ID, ORDER_ID, STATUS)
    VALUES (source.SHIPMENT_ID, source.ORDER_ID, source.STATUS);

    -- All 5 tables succeeded — commit advances all stream offsets atomically
    COMMIT;
    RETURN 'SUCCESS: Bronze to Silver pipeline executed successfully.';

-- EXCEPTION: Rollback on any failure — stream offsets stay in place for retry
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILURE: ' || SQLERRM;
END;
$$;




-- ==============================================================================
-- STORED PROCEDURE 2: sp_silver_to_gold_dims()
-- ==============================================================================
-- PURPOSE: Consumes all 7 Silver dimension streams and MERGEs into Gold dimension
-- tables using SCD Type 2 (historical versioning) and Hybrid SCD1/SCD2 logic.
--
-- DIMENSIONS PROCESSED:
--   1. stores_dim       — SCD2 (any change creates new version)
--   2. suppliers_dim    — Hybrid: SCD2 for name/country/address, SCD1 for phone/email
--   3. customers_dim    — Hybrid: SCD2 for city/address, SCD1 for name/phone/dob
--   4. employees_dim    — Hybrid: SCD2 for salary/dept/role/store/address, SCD1 for name/pan/phone
--   5. products_dim     — Hybrid: SCD2 for price/category_id/supplier_id, SCD1 for name/category label
--   6. categories_dim   — SCD2 (any change creates new version)
--   7. promotions_dim   — SCD2 (any change creates new version)
--
-- ADVANCED FEATURES:
--   - Stream deduplication via QUALIFY ROW_NUMBER() prevents multiple-match errors
--     when the same entity is updated twice in one batch
--   - Explicit BEGIN TRANSACTION ensures all 7 dimensions commit or rollback together
--   - Stream offsets only advance on COMMIT
--   - EXCEPTION block with ROLLBACK preserves stream data on failure
--   - Hybrid SCD: Row duplication trick (UNION ALL) handles close + insert in one MERGE
--
-- CALLED BY: gold_dimensions_task (runs AFTER silver_layer_task completes)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE PROCEDURE sp_silver_to_gold_dims()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- All 7 dimension updates succeed together or fail together.
    -- Stream offsets only advance on COMMIT.
    BEGIN TRANSACTION;

    -- ==========================================
    -- 1. STORES DIMENSION (Pure SCD Type 2)
    -- ==========================================
    -- Any change to a store record creates a new historical version.
    -- QUALIFY deduplicates: if same store updates twice in one batch,
    -- only the latest change is processed (prevents MERGE multiple-match error).
    -- ON clause isolates DELETE actions for closing, NOT MATCHED handles INSERTs.
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.stores_dim target
    USING (
        -- Stream pre-processing: deduplicate per STORE_ID per action type
        SELECT STORE_ID, CITY, ADDRESS, PINCODE, STORE_MANAGER, NO_OF_EMPLOYEES,
            METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update
        FROM NYS_DFS_RETAIL.CLEAN.stores_stm
        -- Keep only the latest change per store per action (DELETE or INSERT)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID, METADATA$ACTION ORDER BY METADATA$ROW_ID DESC) = 1
    ) source
    -- Match ONLY on DELETE actions to safely close the old version
    ON target.STORE_ID = source.STORE_ID AND target.IS_CURRENT = TRUE
        AND source.action = 'DELETE' AND source.is_update = TRUE

    -- MATCHED: Close old version (end-date it, mark no longer current)
    WHEN MATCHED THEN UPDATE SET
        target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- NOT MATCHED: INSERT actions create new current version (brand new or updated)
    WHEN NOT MATCHED AND source.action = 'INSERT'
    THEN INSERT (STORE_ID, CITY, ADDRESS, PINCODE, STORE_MANAGER, NO_OF_EMPLOYEES, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.STORE_ID, source.CITY, source.ADDRESS, source.PINCODE, source.STORE_MANAGER, source.NO_OF_EMPLOYEES, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 2. SUPPLIERS DIMENSION (Hybrid SCD1/SCD2)
    -- ==========================================
    -- SCD2 triggers: vendor_name, country, address (business-critical changes)
    -- SCD1 triggers: vendor_contact, email_address (operational corrections, overwrite)
    -- Derived columns: COUNTRY_CODE (IN/US/CN), IS_DOMESTIC (TRUE if India)
    -- CTE self-joins stream to detect WHICH columns changed (INSERT vs DELETE pair)
    -- Row duplication trick: SCD2 needs close + insert from one change event
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.suppliers_dim target
    USING (
        WITH stream_changes AS (
            SELECT new_row.SUPPLIER_ID, new_row.COUNTRY, new_row.VENDOR_NAME,
                new_row.VENDOR_CONTACT, new_row.EMAIL_ADDRESS, new_row.ADDRESS,
                -- Derived: ISO country code for standardized reporting
                CASE WHEN new_row.COUNTRY = 'INDIA' THEN 'IN' WHEN new_row.COUNTRY = 'USA' THEN 'US' WHEN new_row.COUNTRY = 'CHINA' THEN 'CN' ELSE 'XX' END AS COUNTRY_CODE,
                -- Derived: Domestic flag for procurement analytics
                CASE WHEN new_row.COUNTRY = 'INDIA' THEN TRUE ELSE FALSE END AS IS_DOMESTIC,
                -- Change detection: compare old vs new to classify update type
                CASE
                    WHEN old_row.SUPPLIER_ID IS NULL THEN 'INSERT'
                    WHEN new_row.VENDOR_NAME != old_row.VENDOR_NAME OR new_row.COUNTRY != old_row.COUNTRY OR NVL(new_row.ADDRESS, '') != NVL(old_row.ADDRESS, '') THEN 'SCD2'
                    ELSE 'SCD1'
                END AS change_type
            FROM NYS_DFS_RETAIL.CLEAN.suppliers_stm new_row
            LEFT JOIN NYS_DFS_RETAIL.CLEAN.suppliers_stm old_row
                ON new_row.SUPPLIER_ID = old_row.SUPPLIER_ID AND old_row.METADATA$ACTION = 'DELETE'
            WHERE new_row.METADATA$ACTION = 'INSERT'
        )
        -- SCD1 + fresh inserts pass through directly
        SELECT *, 'SCD1_OR_INSERT' AS merge_action FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')
        UNION ALL
        -- SCD2 copy 1: will MATCH existing row and close it
        SELECT *, 'CLOSE_OLD' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
        UNION ALL
        -- SCD2 copy 2: forced to NOT MATCH by ON clause, inserts as new version
        SELECT *, 'INSERT_NEW' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
    ) source
    ON target.SUPPLIER_ID = source.SUPPLIER_ID AND target.IS_CURRENT = TRUE AND source.merge_action != 'INSERT_NEW'

    -- SCD1: Phone/email changed — overwrite in place, no history needed
    WHEN MATCHED AND source.change_type = 'SCD1'
    THEN UPDATE SET target.VENDOR_CONTACT = source.VENDOR_CONTACT, target.EMAIL_ADDRESS = source.EMAIL_ADDRESS, target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- SCD2: Name/country/address changed — close old version
    WHEN MATCHED AND source.change_type = 'SCD2' AND source.merge_action = 'CLOSE_OLD'
    THEN UPDATE SET target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- New record (brand new supplier or new SCD2 version)
    WHEN NOT MATCHED
    THEN INSERT (SUPPLIER_ID, COUNTRY, COUNTRY_CODE, IS_DOMESTIC, VENDOR_NAME, VENDOR_CONTACT, EMAIL_ADDRESS, ADDRESS, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.SUPPLIER_ID, source.COUNTRY, source.COUNTRY_CODE, source.IS_DOMESTIC, source.VENDOR_NAME, source.VENDOR_CONTACT, source.EMAIL_ADDRESS, source.ADDRESS, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 3. CUSTOMERS DIMENSION (Hybrid SCD1/SCD2)
    -- ==========================================
    -- SCD2 triggers: city, address (relocation is a significant business event)
    -- SCD1 triggers: customer_name, dob, phone_number (corrections, overwrite)
    -- Same CTE pattern: self-join to detect what changed, row duplication for SCD2
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.customers_dim target
    USING (
        WITH stream_changes AS (
            SELECT new_row.CUSTOMER_ID, new_row.CITY, new_row.SIGNUP_DATE, new_row.CUSTOMER_NAME,
                new_row.DOB, new_row.PHONE_NUMBER, new_row.ADDRESS,
                CASE
                    WHEN old_row.CUSTOMER_ID IS NULL THEN 'INSERT'
                    WHEN new_row.CITY != old_row.CITY OR NVL(new_row.ADDRESS, '') != NVL(old_row.ADDRESS, '') THEN 'SCD2'
                    ELSE 'SCD1'
                END AS change_type
            FROM NYS_DFS_RETAIL.CLEAN.customers_stm new_row
            LEFT JOIN NYS_DFS_RETAIL.CLEAN.customers_stm old_row
                ON new_row.CUSTOMER_ID = old_row.CUSTOMER_ID AND old_row.METADATA$ACTION = 'DELETE'
            WHERE new_row.METADATA$ACTION = 'INSERT'
        )
        SELECT *, 'SCD1_OR_INSERT' AS merge_action FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')
        UNION ALL SELECT *, 'CLOSE_OLD' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
        UNION ALL SELECT *, 'INSERT_NEW' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
    ) source
    ON target.CUSTOMER_ID = source.CUSTOMER_ID AND target.IS_CURRENT = TRUE AND source.merge_action != 'INSERT_NEW'

    -- SCD1: Name/DOB/phone corrected — overwrite, no history
    WHEN MATCHED AND source.change_type = 'SCD1'
    THEN UPDATE SET target.CUSTOMER_NAME = source.CUSTOMER_NAME, target.DOB = source.DOB, target.PHONE_NUMBER = source.PHONE_NUMBER, target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- SCD2: City/address changed — close old version (customer relocated)
    WHEN MATCHED AND source.change_type = 'SCD2' AND source.merge_action = 'CLOSE_OLD'
    THEN UPDATE SET target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- New customer or new SCD2 version after relocation
    WHEN NOT MATCHED
    THEN INSERT (CUSTOMER_ID, CITY, SIGNUP_DATE, CUSTOMER_NAME, DOB, PHONE_NUMBER, ADDRESS, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.CUSTOMER_ID, source.CITY, source.SIGNUP_DATE, source.CUSTOMER_NAME, source.DOB, source.PHONE_NUMBER, source.ADDRESS, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 4. EMPLOYEES DIMENSION (Hybrid SCD1/SCD2)
    -- ==========================================
    -- SCD2 triggers: salary, department, role, store_id, address
    --   (raises, promotions, transfers, relocations — all business events with financial impact)
    -- SCD1 triggers: employee_name, pan_no, phone_no
    --   (PII corrections that don't need audit trail)
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.employees_dim target
    USING (
        WITH stream_changes AS (
            SELECT new_row.EMPLOYEE_ID, new_row.STORE_ID, new_row.SALARY, new_row.EMPLOYEE_NAME,
                new_row.ADDRESS, new_row.PAN_NO, new_row.PHONE_NO, new_row.DEPARTMENT, new_row.ROLE,
                CASE
                    WHEN old_row.EMPLOYEE_ID IS NULL THEN 'INSERT'
                    WHEN NVL(new_row.SALARY, -1) != NVL(old_row.SALARY, -1)
                      OR NVL(new_row.DEPARTMENT, '') != NVL(old_row.DEPARTMENT, '')
                      OR NVL(new_row.ROLE, '') != NVL(old_row.ROLE, '')
                      OR NVL(new_row.STORE_ID, -1) != NVL(old_row.STORE_ID, -1)
                      OR NVL(new_row.ADDRESS, '') != NVL(old_row.ADDRESS, '') THEN 'SCD2'
                    ELSE 'SCD1'
                END AS change_type
            FROM NYS_DFS_RETAIL.CLEAN.employees_stm new_row
            LEFT JOIN NYS_DFS_RETAIL.CLEAN.employees_stm old_row
                ON new_row.EMPLOYEE_ID = old_row.EMPLOYEE_ID AND old_row.METADATA$ACTION = 'DELETE'
            WHERE new_row.METADATA$ACTION = 'INSERT'
        )
        SELECT *, 'SCD1_OR_INSERT' AS merge_action FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')
        UNION ALL SELECT *, 'CLOSE_OLD' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
        UNION ALL SELECT *, 'INSERT_NEW' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
    ) source
    ON target.EMPLOYEE_ID = source.EMPLOYEE_ID AND target.IS_CURRENT = TRUE AND source.merge_action != 'INSERT_NEW'

    -- SCD1: Name/PAN/phone corrected — overwrite PII fixes in place
    WHEN MATCHED AND source.change_type = 'SCD1'
    THEN UPDATE SET target.EMPLOYEE_NAME = source.EMPLOYEE_NAME, target.PAN_NO = source.PAN_NO, target.PHONE_NO = source.PHONE_NO, target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- SCD2: Salary/dept/role/store/address changed — close old version for audit
    WHEN MATCHED AND source.change_type = 'SCD2' AND source.merge_action = 'CLOSE_OLD'
    THEN UPDATE SET target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- New employee or new SCD2 version (promotion, raise, transfer)
    WHEN NOT MATCHED
    THEN INSERT (EMPLOYEE_ID, STORE_ID, SALARY, EMPLOYEE_NAME, ADDRESS, PAN_NO, PHONE_NO, DEPARTMENT, ROLE, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.EMPLOYEE_ID, source.STORE_ID, source.SALARY, source.EMPLOYEE_NAME, source.ADDRESS, source.PAN_NO, source.PHONE_NO, source.DEPARTMENT, source.ROLE, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 5. PRODUCTS DIMENSION (Hybrid SCD1/SCD2)
    -- ==========================================
    -- SCD2 triggers: price, category_id, supplier_id (financial/hierarchy changes)
    -- SCD1 triggers: product_name, product_category (cosmetic spelling fixes)
    -- Derived: price_tier computed with NULL-safe logic in CTE
    --   (prevents NULL prices from falling into 'Luxury' bucket)
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.products_dim target
    USING (
        WITH stream_changes AS (
            SELECT new_row.PRODUCT_ID, new_row.PRODUCT_CATEGORY, new_row.PRODUCT_NAME,
                new_row.CATEGORY_ID, new_row.SUPPLIER_ID, new_row.PRICE,
                -- Derived: Price tier with NULL handling
                CASE
                    WHEN new_row.PRICE IS NULL THEN 'Unknown'
                    WHEN new_row.PRICE < 250 THEN 'Budget (< 250)'
                    WHEN new_row.PRICE BETWEEN 250 AND 1000 THEN 'Mid-Range (250-1000)'
                    WHEN new_row.PRICE BETWEEN 1001 AND 2500 THEN 'Premium (1001-2500)'
                    ELSE 'Luxury (2500+)'
                END AS PRICE_TIER,
                -- Change detection
                CASE
                    WHEN old_row.PRODUCT_ID IS NULL THEN 'INSERT'
                    WHEN NVL(new_row.PRICE, -1) != NVL(old_row.PRICE, -1)
                      OR NVL(new_row.CATEGORY_ID, -1) != NVL(old_row.CATEGORY_ID, -1)
                      OR NVL(new_row.SUPPLIER_ID, -1) != NVL(old_row.SUPPLIER_ID, -1) THEN 'SCD2'
                    ELSE 'SCD1'
                END AS change_type
            FROM NYS_DFS_RETAIL.CLEAN.products_stm new_row
            LEFT JOIN NYS_DFS_RETAIL.CLEAN.products_stm old_row
                ON new_row.PRODUCT_ID = old_row.PRODUCT_ID AND old_row.METADATA$ACTION = 'DELETE'
            WHERE new_row.METADATA$ACTION = 'INSERT'
        )
        SELECT *, 'SCD1_OR_INSERT' AS merge_action FROM stream_changes WHERE change_type IN ('SCD1', 'INSERT')
        UNION ALL SELECT *, 'CLOSE_OLD' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
        UNION ALL SELECT *, 'INSERT_NEW' AS merge_action FROM stream_changes WHERE change_type = 'SCD2'
    ) source
    ON target.PRODUCT_ID = source.PRODUCT_ID AND target.IS_CURRENT = TRUE AND source.merge_action != 'INSERT_NEW'

    -- SCD1: Name/category label fixed — overwrite cosmetic corrections
    WHEN MATCHED AND source.change_type = 'SCD1'
    THEN UPDATE SET target.PRODUCT_NAME = source.PRODUCT_NAME, target.PRODUCT_CATEGORY = source.PRODUCT_CATEGORY, target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- SCD2: Price/category_id/supplier_id changed — close old version for financial history
    WHEN MATCHED AND source.change_type = 'SCD2' AND source.merge_action = 'CLOSE_OLD'
    THEN UPDATE SET target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- New product or new SCD2 version (price change, supplier switch)
    WHEN NOT MATCHED
    THEN INSERT (PRODUCT_ID, PRODUCT_CATEGORY, PRODUCT_NAME, CATEGORY_ID, SUPPLIER_ID, PRICE, PRICE_TIER, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.PRODUCT_ID, source.PRODUCT_CATEGORY, source.PRODUCT_NAME, source.CATEGORY_ID, source.SUPPLIER_ID, source.PRICE, source.PRICE_TIER, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 6. CATEGORIES DIMENSION (Pure SCD Type 2)
    -- ==========================================
    -- Simple reference table — any name change creates new version.
    -- Historical joins preserve the category name active at time of sale.
    -- QUALIFY deduplicates: prevents multiple-match if same category updated twice in batch.
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.categories_dim target
    USING (
        SELECT CATEGORY_ID, CATEGORY_NAME,
            METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update
        FROM NYS_DFS_RETAIL.CLEAN.categories_stm
        -- Deduplicate: keep only latest change per category per action type
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CATEGORY_ID, METADATA$ACTION ORDER BY METADATA$ROW_ID DESC) = 1
    ) source
    -- Match ONLY DELETE actions to safely close old version
    ON target.CATEGORY_ID = source.CATEGORY_ID AND target.IS_CURRENT = TRUE
        AND source.action = 'DELETE' AND source.is_update = TRUE

    -- Close old version when category is renamed
    WHEN MATCHED THEN UPDATE SET
        target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- INSERT actions create new current version (brand new or updated category)
    WHEN NOT MATCHED AND source.action = 'INSERT'
    THEN INSERT (CATEGORY_ID, CATEGORY_NAME, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.CATEGORY_ID, source.CATEGORY_NAME, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- ==========================================
    -- 7. PROMOTIONS DIMENSION (SCD Type 2 with derived DISCOUNT_TIER)
    -- ==========================================
    -- Derived: discount_tier computed in USING subquery with NULL-safe logic.
    -- When a discount value changes, old tier preserved in historical row for audit.
    -- QUALIFY deduplicates: prevents multiple-match errors in batch processing.
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.promotions_dim target
    USING (
        SELECT PROMOTION_ID, DISCOUNT,
            -- Derived: Discount tier with NULL handling
            CASE
                WHEN DISCOUNT IS NULL THEN 'Unknown'
                WHEN DISCOUNT <= 10 THEN 'Low (1-10%)'
                WHEN DISCOUNT <= 25 THEN 'Medium (11-25%)'
                ELSE 'High (26%+)'
            END AS DISCOUNT_TIER,
            METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update
        FROM NYS_DFS_RETAIL.CLEAN.promotions_stm
        -- Deduplicate: keep only latest change per promotion per action type
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PROMOTION_ID, METADATA$ACTION ORDER BY METADATA$ROW_ID DESC) = 1
    ) source
    -- Match ONLY DELETE actions to safely close old version
    ON target.PROMOTION_ID = source.PROMOTION_ID AND target.IS_CURRENT = TRUE
        AND source.action = 'DELETE' AND source.is_update = TRUE

    -- Close old version when discount changes
    WHEN MATCHED THEN UPDATE SET
        target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE

    -- Insert new or updated promotion with derived tier
    WHEN NOT MATCHED AND source.action = 'INSERT'
    THEN INSERT (PROMOTION_ID, DISCOUNT, DISCOUNT_TIER, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, DW_LOADED_AT)
    VALUES (source.PROMOTION_ID, source.DISCOUNT, source.DISCOUNT_TIER, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP());

    -- All 7 dimensions succeeded — commit advances all stream offsets atomically
    COMMIT;
    RETURN 'SUCCESS: Silver to Gold Dimensions pipeline executed successfully.';

-- EXCEPTION: Rollback on any failure — stream offsets stay in place for retry
-- No corrupted data enters the Gold layer. Safe to re-run.
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILURE: ' || SQLERRM;
END;
$$;


-- ==============================================================================
-- STORED PROCEDURE 3: sp_gold_facts()
-- ==============================================================================
-- PURPOSE: Populates the two Gold fact tables from Silver STREAMS (delta only).
-- Uses MERGE for idempotency and point-in-time SCD2 dimension joins.
--
-- CRITICAL OPTIMIZATION: Queries STREAMS (not base tables) to process only
-- NEW records since last run. As data grows from 600K to 6M+ rows, this ensures
-- the daily 6 AM task only processes yesterday's delta (e.g., 5K new items)
-- instead of scanning the entire historical table every run.
--
-- FACT TABLES:
--   1. order_item_fact — Stream: clean_order_items_stm (item-level delta)
--   2. order_fact — Stream: clean_orders_stm (order-level delta)
--
-- TRANSACTION CONTROL:
--   - Explicit BEGIN TRANSACTION ensures both facts commit or rollback together
--   - Stream offsets only advance on COMMIT
--   - EXCEPTION block with ROLLBACK preserves stream data on failure
--
-- CALLED BY: gold_facts_task (runs AFTER gold_dimensions_task completes)
-- ==============================================================================
USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA CONSUMPTION;

CREATE OR REPLACE PROCEDURE sp_gold_facts()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Both fact tables succeed together or fail together.
    -- Stream offsets only advance on COMMIT.
    BEGIN TRANSACTION;

    -- ==========================================
    -- 1. ORDER ITEM FACT (Item-Level Grain)
    -- ==========================================
    -- BASE: clean_order_items_stm (STREAM — only new items since last run)
    -- NOT the base table — prevents full table scan on 600K+ rows every morning.
    -- JOINs to clean base tables for header attributes and returns.
    -- Dimension joins: point-in-time SCD2 (date between effective_start and effective_end)
    -- MERGE on ORDER_ITEM_ID: idempotent, safe to re-run
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.order_item_fact target
    USING (
        SELECT
            -- DATE DIMENSION: Convert ORDER_DATE to YYYYMMDD integer key
            TO_CHAR(o.ORDER_DATE, 'YYYYMMDD')::INT AS DATE_ID,

            -- DIMENSION SURROGATE KEYS: Point-in-time SCD2 resolution
            -- Gets the SK that was active ON THE DATE the order was placed
            c.CUSTOMER_SK,
            s.STORE_SK,
            p.PRODUCT_SK,
            promo.PROMOTION_SK,

            -- DEGENERATE DIMENSIONS: Business IDs for drill-through
            oi_stm.ORDER_ID,
            oi_stm.ORDER_ITEM_ID,

            -- MEASURES: Quantity and Revenue
            oi_stm.QTY,
            oi_stm.TOTAL_TRANSACTION,

            -- REFUND: COALESCE NULL to 0 for non-returned items
            COALESCE(r.REFUND, 0) AS REFUND_AMOUNT,

            -- NET REVENUE: Gross minus refund — true bottom-line per line item
            (oi_stm.TOTAL_TRANSACTION - COALESCE(r.REFUND, 0)) AS NET_REVENUE,

            -- FLAG: Quick boolean for return analysis dashboards
            IFF(r.RETURN_ID IS NOT NULL, TRUE, FALSE) AS IS_RETURNED

        -- STREAM BASE: Only new order items since last consumption (delta processing)
        -- This is the key optimization — NOT a full table scan
        FROM NYS_DFS_RETAIL.CLEAN.clean_order_items_stm oi_stm

        -- JOIN to orders base table for header attributes (date, customer, store, promo)
        INNER JOIN NYS_DFS_RETAIL.CLEAN.clean_orders o
            ON oi_stm.ORDER_ID = o.ORDER_ID

        -- LEFT JOIN to returns base table (item may or may not be returned)
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.clean_returns r
            ON oi_stm.ORDER_ITEM_ID = r.ORDER_ITEM_ID

        -- CUSTOMER DIMENSION: Point-in-time join (order date between effective dates)
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.customers_dim c
            ON o.CUSTOMER_ID = c.CUSTOMER_ID
            AND o.ORDER_DATE >= c.EFFECTIVE_START_DATE::DATE
            AND o.ORDER_DATE < COALESCE(c.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- STORE DIMENSION: Point-in-time join
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s
            ON o.STORE_ID = s.STORE_ID
            AND o.ORDER_DATE >= s.EFFECTIVE_START_DATE::DATE
            AND o.ORDER_DATE < COALESCE(s.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- PRODUCT DIMENSION: Point-in-time join
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.products_dim p
            ON oi_stm.PRODUCT_ID = p.PRODUCT_ID
            AND o.ORDER_DATE >= p.EFFECTIVE_START_DATE::DATE
            AND o.ORDER_DATE < COALESCE(p.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- PROMOTION DIMENSION: Point-in-time join (nullable — full-price orders)
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.promotions_dim promo
            ON o.PROMOTION_ID = promo.PROMOTION_ID
            AND o.ORDER_DATE >= promo.EFFECTIVE_START_DATE::DATE
            AND o.ORDER_DATE < COALESCE(promo.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- STREAM FILTER: Only process newly inserted items from the delta
        WHERE oi_stm.METADATA$ACTION = 'INSERT'

    ) source
    -- MERGE anchor: one row per order item (unique business key)
    ON target.ORDER_ITEM_ID = source.ORDER_ITEM_ID

    -- MATCHED: Late-arriving correction or return processed after initial load
    WHEN MATCHED THEN UPDATE SET
        target.DATE_ID = source.DATE_ID,
        target.CUSTOMER_SK = source.CUSTOMER_SK,
        target.STORE_SK = source.STORE_SK,
        target.PRODUCT_SK = source.PRODUCT_SK,
        target.PROMOTION_SK = source.PROMOTION_SK,
        target.ORDER_ID = source.ORDER_ID,
        target.QTY = source.QTY,
        target.TOTAL_TRANSACTION = source.TOTAL_TRANSACTION,
        target.REFUND_AMOUNT = source.REFUND_AMOUNT,
        target.NET_REVENUE = source.NET_REVENUE,
        target.IS_RETURNED = source.IS_RETURNED,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- NOT MATCHED: Brand new order item from yesterday's sales
    WHEN NOT MATCHED THEN INSERT (
        DATE_ID, CUSTOMER_SK, STORE_SK, PRODUCT_SK, PROMOTION_SK,
        ORDER_ID, ORDER_ITEM_ID, QTY, TOTAL_TRANSACTION,
        REFUND_AMOUNT, NET_REVENUE, IS_RETURNED
    )
    VALUES (
        source.DATE_ID, source.CUSTOMER_SK, source.STORE_SK, source.PRODUCT_SK, source.PROMOTION_SK,
        source.ORDER_ID, source.ORDER_ITEM_ID, source.QTY, source.TOTAL_TRANSACTION,
        source.REFUND_AMOUNT, source.NET_REVENUE, source.IS_RETURNED
    );

    -- ==========================================
    -- 2. ORDER FACT (Order-Level / Header Grain)
    -- ==========================================
    -- BASE: clean_orders_stm (STREAM — only new/updated orders since last run)
    -- Captures ALL orders including 40K without line items (no silent drops)
    -- Payments and shipments at 1:1 grain (no fan-out risk)
    -- Point-in-time SCD2 dimension joins
    MERGE INTO NYS_DFS_RETAIL.CONSUMPTION.order_fact target
    USING (
        SELECT
            -- DATE DIMENSION: YYYYMMDD integer key
            TO_CHAR(o_stm.ORDER_DATE, 'YYYYMMDD')::INT AS DATE_ID,

            -- DIMENSION SURROGATE KEYS: Point-in-time SCD2
            c.CUSTOMER_SK,
            s.STORE_SK,
            promo.PROMOTION_SK,

            -- DEGENERATE DIMENSION: Order business key (MERGE anchor)
            o_stm.ORDER_ID,

            -- PAYMENT: 1:1 with orders — safe join, no fan-out
            COALESCE(pay.PAYMENT_AMOUNT, 0) AS PAYMENT_AMOUNT,

            -- SHIPMENT STATUS: UPPER to match clean_shipments standard
            UPPER(COALESCE(ship.STATUS, 'PENDING')) AS SHIPMENT_STATUS

        -- STREAM BASE: Only new/updated orders since last consumption
        FROM NYS_DFS_RETAIL.CLEAN.clean_orders_stm o_stm

        -- PAYMENT: 1:1 relationship (join to base table for payment amount)
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.clean_payments pay
            ON o_stm.ORDER_ID = pay.ORDER_ID

        -- SHIPMENT: 1:1 relationship (join to base table for status)
        LEFT JOIN NYS_DFS_RETAIL.CLEAN.clean_shipments ship
            ON o_stm.ORDER_ID = ship.ORDER_ID

        -- CUSTOMER DIMENSION: Point-in-time SCD2 join
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.customers_dim c
            ON o_stm.CUSTOMER_ID = c.CUSTOMER_ID
            AND o_stm.ORDER_DATE >= c.EFFECTIVE_START_DATE::DATE
            AND o_stm.ORDER_DATE < COALESCE(c.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- STORE DIMENSION: Point-in-time SCD2 join
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.stores_dim s
            ON o_stm.STORE_ID = s.STORE_ID
            AND o_stm.ORDER_DATE >= s.EFFECTIVE_START_DATE::DATE
            AND o_stm.ORDER_DATE < COALESCE(s.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- PROMOTION DIMENSION: Point-in-time SCD2 join
        LEFT JOIN NYS_DFS_RETAIL.CONSUMPTION.promotions_dim promo
            ON o_stm.PROMOTION_ID = promo.PROMOTION_ID
            AND o_stm.ORDER_DATE >= promo.EFFECTIVE_START_DATE::DATE
            AND o_stm.ORDER_DATE < COALESCE(promo.EFFECTIVE_END_DATE, '9999-12-31')::DATE

        -- STREAM FILTER: Only process newly inserted/updated orders
        WHERE o_stm.METADATA$ACTION = 'INSERT'

    ) source
    -- MERGE anchor: one row per order (UNIQUE constraint enforces this)
    ON target.ORDER_ID = source.ORDER_ID

    -- MATCHED: Shipment status updated (SHIPPED → DELIVERED → LATE)
    WHEN MATCHED THEN UPDATE SET
        target.DATE_ID = source.DATE_ID,
        target.CUSTOMER_SK = source.CUSTOMER_SK,
        target.STORE_SK = source.STORE_SK,
        target.PROMOTION_SK = source.PROMOTION_SK,
        target.PAYMENT_AMOUNT = source.PAYMENT_AMOUNT,
        target.SHIPMENT_STATUS = source.SHIPMENT_STATUS,
        target.DW_LOADED_AT = CURRENT_TIMESTAMP()

    -- NOT MATCHED: Brand new order
    WHEN NOT MATCHED THEN INSERT (
        DATE_ID, CUSTOMER_SK, STORE_SK, PROMOTION_SK,
        ORDER_ID, PAYMENT_AMOUNT, SHIPMENT_STATUS
    )
    VALUES (
        source.DATE_ID, source.CUSTOMER_SK, source.STORE_SK, source.PROMOTION_SK,
        source.ORDER_ID, source.PAYMENT_AMOUNT, source.SHIPMENT_STATUS
    );

    -- Both fact tables succeeded — commit advances stream offsets atomically
    COMMIT;
    RETURN 'SUCCESS: Gold Facts pipeline executed successfully.';

-- EXCEPTION: Rollback on any failure — no corrupted facts, streams preserved for retry
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILURE: ' || SQLERRM;
END;
$$;