/*
    Stored Procedure : silver.load_silver
    Description      : Transforms and loads data from Bronze to Silver layer.
                       - Cleans raw data (remove duplicates, trim strings, handle nulls)
                       - Standardizes formats (dates, categories, codes)
                       - Applies business rules (validation & recalculation)
                       - Prepares data for downstream analytics (Gold layer)
    Layer            : Silver (Cleaned & Standardized Data)
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN

    /* ============================
       CRM: Customer Info
       ============================ */
    PRINT 'Truncate Table : silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    PRINT 'Inserting Data Into : silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,   -- Remove leading/trailing spaces
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN cst_gndr = 'M' THEN 'Married' -- Decode marital status
             WHEN cst_gndr = 'S' THEN 'Single'
             ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN cst_gndr = 'M' THEN 'Male'    -- Decode gender
             WHEN cst_gndr = 'F' THEN 'Female'
             ELSE 'n/a'
        END cst_gndr,
        cst_create_date
    FROM (
        -- Deduplicate by taking the latest create_date per customer
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rank_Last_Create
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) r
    WHERE Rank_Last_Create = 1;


    /* ============================
       CRM: Product Info
       ============================ */
    PRINT 'Truncate Table : silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    PRINT 'Inserting Data Into : silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,  -- Extract category ID
        SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,         -- Clean product key
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,                      -- Replace NULL cost with 0
        CASE WHEN prd_line = 'M' THEN 'Mountain'              -- Decode product line
             WHEN prd_line = 'R' THEN 'Road'
             WHEN prd_line = 'S' THEN 'Other Sales'
             WHEN prd_line = 'T' THEN 'Touring'
             ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,           -- Convert to DATE
        CAST(LEAD(prd_start_dt) 
             OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;


    /* ============================
       CRM: Sales Details
       ============================ */
    PRINT 'Truncate Table : silver.crm_sls_details';
    TRUNCATE TABLE silver.crm_sls_details;

    PRINT 'Inserting Data Into : silver.crm_sls_details';
    INSERT INTO silver.crm_sls_details (
          sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          sls_order_dt,
          sls_ship_dt,
          sls_due_dt,
          sls_sales,
          sls_quantity,
          sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Validate and convert date columns
        CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        -- Recalculate sales if inconsistent or missing
        CASE WHEN sls_sales != sls_quantity * ABS(sls_price) 
                   OR sls_sales IS NULL OR sls_sales <= 0
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Fix invalid price values
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sls_details;


    /* ============================
       ERP: Customer Data
       ============================ */
    PRINT 'Truncate Table : silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    PRINT 'Inserting Data Into : silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid,4, LEN(Cid)) -- Clean ID prefix
             ELSE cid
        END cid,
        CASE WHEN bdate > GETDATE() THEN NULL                      -- Remove invalid future dates
             ELSE bdate 
        END AS bdate,
        CASE WHEN gen IN ('F', 'Female') THEN 'Female'             -- Standardize gender values
             WHEN gen IN ('M', 'Male')   THEN 'Male'
             ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;


    /* ============================
       ERP: Customer Location
       ============================ */
    PRINT 'Truncate Table : silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    PRINT 'Inserting Data Into : silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-','') AS cid,                              -- Remove dashes from ID
        CASE WHEN cntry = 'DE' THEN 'Germany'                     -- Standardize country codes
             WHEN cntry IN ('US','USA') THEN 'United States'
             WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
             ELSE cntry
        END AS cntry
    FROM bronze.erp_loc_a101;


    /* ============================
       ERP: Product Category
       ============================ */
    PRINT 'Truncate Table : silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    PRINT 'Inserting Data Into : silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintance)
    SELECT 
        id,
        cat,
        subcat,
        maintance
    FROM bronze.erp_px_cat_g1v2;

END;
