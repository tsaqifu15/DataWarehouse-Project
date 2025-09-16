-- ========================================================
-- Project: Data Warehouse (Gold Schema Views)
-- Purpose: Create Dimension & Fact views in the Gold layer
-- ========================================================

-- ========================================================
-- DIM_CUSTOMERS VIEW
-- This view consolidates customer-related data into a 
-- dimension table for analysis. It joins CRM customer info 
-- with ERP customer and location data, ensuring enriched 
-- attributes such as gender and country.
-- ========================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS -- Dimension View
SELECT  
    ROW_NUMBER() OVER (ORDER BY cci.cst_id) AS Customer_Key,   -- Surrogate Key for DW
    cci.cst_id AS Customer_id,                                -- Customer ID from CRM
    cci.cst_key AS Customer_Number,                           -- Customer number from CRM
    cci.cst_firstname AS Customer_FirstName,                  -- First name
    cci.cst_lastname AS Customer_LastName,                    -- Last name
    ec.bdate AS Birthdate,                                    -- Birthdate from ERP
    CASE WHEN cci.cst_gndr != 'n/a' 
         THEN cci.cst_gndr 
         ELSE COALESCE(ec.gen,'n/a') END AS Gender,           -- Gender handling missing values
    cci.cst_marital_status AS Marital_Status,                 -- Marital status
    el.cntry AS Country,                                      -- Country from ERP location
    cci.cst_create_Date AS Create_Date                        -- Customer creation date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 ec
    ON cci.cst_key = ec.cid
LEFT JOIN silver.erp_loc_a101 el
    ON cci.cst_key = el.cid;
GO


-- ========================================================
-- DIM_PRODUCT VIEW
-- This view provides product-related attributes enriched 
-- with category information from ERP. Only active products 
-- (no end date) are included.
-- ========================================================

IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS -- Dimension View
SELECT 
    ROW_NUMBER() OVER(ORDER BY prd_start_dt) AS Product_Key, -- Surrogate Key
    cp.prd_id AS Product_Id,                                -- Product ID from CRM
    cp.prd_key AS Product_Number,                           -- Product number
    cp.prd_nm AS Product_Name,                              -- Product name
    cp.cat_id AS Category_Id,                               -- Category ID
    ep.cat AS Category,                                     -- Category name
    ep.subcat AS Subcategory,                               -- Subcategory
    ep.maintance AS Maintance,                              -- Maintenance attribute
    cp.prd_cost AS Cost,                                    -- Product cost
    cp.prd_line AS Product_Line,                            -- Product line
    cp.prd_start_dt AS Start_Date                           -- Start date
FROM silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep
    ON cp.cat_id = ep.id
WHERE cp.prd_end_dt IS NULL;                                -- Filter active products only
GO


-- ========================================================
-- FACT_SALES_ORDER VIEW
-- This view forms the fact table containing sales orders. 
-- It joins CRM sales details with dimension tables for 
-- products and customers, allowing analysis across measures.
-- ========================================================

IF OBJECT_ID('gold.fact_sales_order', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales_order;
GO

CREATE VIEW gold.fact_sales_order AS -- Fact View
SELECT
    cs.sls_ord_num AS Order_Number,                        -- Sales order number
    dp.Product_Key,                                        -- Linked product dimension key
    dc.Customer_Key,                                       -- Linked customer dimension key
    cs.sls_order_dt AS Order_Date,                         -- Order date
    cs.sls_ship_dt AS Ship_Date,                           -- Shipping date
    cs.sls_due_dt AS Due_Date,                             -- Due date
    cs.sls_sales AS Sales,                                 -- Sales amount
    cs.sls_quantity AS Quantity,                           -- Quantity sold
    cs.sls_price AS Price                                  -- Unit price
FROM silver.crm_sls_details cs
LEFT JOIN gold.dim_customers dc
    ON cs.sls_cust_id = dc.Customer_id
LEFT JOIN gold.dim_product dp
    ON cs.sls_prd_key = dp.Product_Number;
GO
