/*
    Script Name     : Create Silver Tables
    Description     : This script creates the Silver schema tables. 
                      These tables store cleaned, standardized, and 
                      business-ready data from Bronze schema. 
                      Each table includes an audit column (dwh_create_date) 
                      for tracking ETL load time.
*/

------------------------------
-- Table CRM: Customer Info
------------------------------
-- Drop existing table if exists to avoid conflict
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

-- Create cleaned CRM customer table
CREATE TABLE silver.crm_cust_info (
    cst_id INT,                        -- Customer ID (Primary Business Key)
    cst_key NVARCHAR(50),              -- Customer Natural Key
    cst_firstname NVARCHAR(50),        -- Cleaned First Name
    cst_lastname NVARCHAR(50),         -- Cleaned Last Name
    cst_marital_status NVARCHAR(50),   -- Standardized marital status (e.g., Married/Single)
    cst_gndr NVARCHAR(50),             -- Standardized gender (Male/Female/n/a)
    cst_create_date DATE,              -- Creation Date (latest record per customer)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- ETL Load Timestamp
);

------------------------------
-- Table CRM: Product Info
------------------------------
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,                        -- Product ID
    cat_id NVARCHAR(50),               -- Extracted Category ID (standardized)
    prd_key NVARCHAR(50),              -- Cleaned Product Key
    prd_nm NVARCHAR(50),               -- Product Name (trimmed & standardized)
    prd_cost INT,                      -- Cleaned product cost (no null/negative)
    prd_line NVARCHAR(50),             -- Standardized product line (Mountain/Road/etc.)
    prd_start_dt DATE,                 -- Valid start date
    prd_end_dt DATE,                   -- Valid end date
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------------------------------
-- Table CRM: Sales Details
------------------------------
IF OBJECT_ID ('silver.crm_sls_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sls_details;

CREATE TABLE silver.crm_sls_details (
    sls_ord_num NVARCHAR(50),          -- Sales Order Number
    sls_prd_key NVARCHAR(50),          -- Product Key (FK to Product Info)
    sls_cust_id INT,                   -- Customer ID (FK to Customer Info)
    sls_order_dt DATE,                 -- Cleaned Order Date
    sls_ship_dt DATE,                  -- Cleaned Ship Date
    sls_due_dt DATE,                   -- Cleaned Due Date
    sls_sales INT,                     -- Validated Sales Amount
    sls_quantity INT,                  -- Quantity Sold
    sls_price INT,                     -- Standardized Price
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------------------------------
-- Table ERP: Location Info
------------------------------
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),                  -- Customer ID (FK to CRM Customer)
    cntry NVARCHAR(50),                -- Standardized Country Name
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------------------------------
-- Table ERP: Customer Info
------------------------------
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),                  -- Customer ID
    bdate DATE,                        -- Cleaned Birthdate (validated, realistic range)
    gen NVARCHAR(50),                  -- Standardized Gender
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------------------------------
-- Table ERP: Product Category
------------------------------
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),                   -- Product Category ID
    cat NVARCHAR(50),                  -- Category
    subcat NVARCHAR(50),               -- Subcategory
    maintance NVARCHAR(50),            -- Maintenance Type (standardized)
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
