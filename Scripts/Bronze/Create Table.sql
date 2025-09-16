/*
    Script Name     : Create Bronze Tables (CRM & ERP)
    Description     : This script creates raw tables under the Bronze schema.
                      It first drops the table if it exists, then recreates it.
                      These tables store data directly extracted from CSV (no transformation).
    Layer           : Bronze (Raw Data Layer)
*/

---------------------------
-- CRM TABLES
---------------------------

-- Drop existing table if already created (safety for re-run)
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

-- Customer Information table from CRM
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,                     -- Customer unique ID
    cst_key NVARCHAR(50),           -- Business key
    cst_firstname NVARCHAR(50),     -- First name
    cst_lastname NVARCHAR(50),      -- Last name
    cst_marital_status NVARCHAR(50),-- Marital status (Single/Married/etc.)
    cst_gndr NVARCHAR(50),          -- Gender
    cst_create_date DATE            -- Customer creation/registration date
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

-- Product Information table from CRM
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                     -- Product unique ID
    prd_key NVARCHAR(50),           -- Product key (business reference)
    prd_nm NVARCHAR(50),            -- Product name
    prd_cost INT,                   -- Cost of product
    prd_line NVARCHAR(50),          -- Product line/category
    prd_start_dt DATETIME,          -- Start availability date
    prd_end_dt DATETIME             -- End availability date
);

IF OBJECT_ID('bronze.crm_sls_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sls_details;

-- Sales Details table from CRM
CREATE TABLE bronze.crm_sls_details (
    sls_ord_num NVARCHAR(50),       -- Order number
    sls_prd_key NVARCHAR(50),       -- Product key (foreign ref to product table)
    sls_cust_id INT,                -- Customer ID (foreign ref to customer table)
    sls_order_dt INT,               -- Order date (in integer format, e.g., YYYYMMDD)
    sls_ship_dt INT,                -- Shipping date (in integer format)
    sls_due_dt INT,                 -- Due date for delivery (in integer format)
    sls_sales INT,                  -- Sales amount
    sls_quantity INT,               -- Quantity ordered
    sls_price INT                   -- Unit price
);

---------------------------
-- ERP TABLES
---------------------------

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

-- Customer Location table from ERP
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),               -- Customer ID
    cntry NVARCHAR(50)              -- Country of the customer
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

-- Customer master data from ERP
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),               -- Customer ID
    bdate DATE,                     -- Birthdate
    gen NVARCHAR(50)                -- Gender
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;

-- Product Category table from ERP
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),                -- Product ID
    cat NVARCHAR(50),               -- Main category
    subcat NVARCHAR(50),            -- Sub-category
    maintance NVARCHAR(50)          -- Maintenance/Status info
);
