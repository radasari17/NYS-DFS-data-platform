-- ==============================================================================
-- BUSINESS LOGIC: Creating the OLTP (Online Transaction Processing) Environment
-- GOVERNANCE: Landing data in its "Raw/Bronze" state for strict audit lineage.
-- ==============================================================================

-- 1.Create the Database
CREATE DATABASE StateRetail_OLTP;
GO

-- 2. Open the Filing Cabinet (Tell the system to use this specific database)
USE StateRetail_OLTP;
GO

-- 3. Create the first manila folder (Create the raw_orders table)
CREATE TABLE raw_orders (
    -- 'order_id' is our Primary Key (The unique barcode on the receipt). 
    -- INT means it will only accept whole numbers.
    order_id VARCHAR(50) PRIMARY KEY,
    
    -- 'customer_id' tells us who bought it.
    customer_id VARCHAR(50),
    
    -- 'store_id' tells us which of our locations sold it.
    store_id VARCHAR(50),
    
    -- 'order_date' is the day it happened. 
    -- GOVERNANCE NOTE: We are using VARCHAR (Text) instead of DATE right now. 
    -- The raw CSV has dates like "26-08-2021". If we force a DATE type now, it might crash. 
    -- We load it as raw text first, and cast it safely later in Snowflake.
    order_date VARCHAR(50),
    
    -- 'promotion_id' tracks if a discount was applied.
    promotion_id VARCHAR(50)
);
GO


-- ==============================================================================
-- BUSINESS LOGIC: Validating the First Data Ingestion
-- GOVERNANCE: Checking row counts and data integrity in our Bronze layer.
-- We must prove the CSV data landed safely in the relational table without corruption.
-- ==============================================================================

-- Tell the system which database to look at
USE StateRetail_OLTP;
GO

Select COUNT(*) from raw_orders; --Check the count of all the data

-- Pull the top 10 receipts from our manila folder to ensure it worked
SELECT TOP 10 * 
FROM raw_orders;
GO

-- ==============================================================================
-- BUSINESS LOGIC: Creating the "Line Items" folder for our OLTP Environment.
-- GOVERNANCE: Landing the exact products and quantities sold in a Bronze layer.
-- We must capture the exact product names and totals to prevent inventory shrinkage.
-- ==============================================================================

-- Tell the system to open our specific digital filing cabinet
USE StateRetail_OLTP;
GO

-- Create the second manila folder (Create the raw_order_items table)
CREATE TABLE raw_order_items (
    -- 'order_item_id' is our Primary Key (The unique barcode for this specific row).
    order_item_id VARCHAR(50) PRIMARY KEY,
    
    -- 'order_id' is our Foreign Key. This links the item back to the receipt header!
    order_id VARCHAR(50),
    
    -- 'product_id' is the manufacturer's code for the item.
    product_id VARCHAR(50),
    
    -- 'product_name' is the actual text description (e.g., 'Lifebuoy Soap').
    -- GOVERNANCE NOTE: We use VARCHAR(255) here to ensure long product 
    -- names are not accidentally chopped off or corrupted when ingested.
    product_name VARCHAR(255),
    
    -- 'qty' is how many units the customer bought.
    qty VARCHAR(50),
    
    -- 'total_transaction' is the raw revenue for this specific item.
    -- We use DECIMAL(10,2) to safely handle money (up to 10 digits, 2 decimal places).
    total_transaction VARCHAR(50)
);
GO


-- 3. Create the payments transaction table
CREATE TABLE raw_payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    amount VARCHAR(50)
);
GO

-- 4. Create the shipments transaction table
CREATE TABLE raw_shipments (
    shipment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    status VARCHAR(50)
);
GO

-- 5. Create the returns transaction table
CREATE TABLE raw_returns (
    return_id VARCHAR(50) PRIMARY KEY,
    order_item_id VARCHAR(50),
    refund VARCHAR(50)
);
GO
