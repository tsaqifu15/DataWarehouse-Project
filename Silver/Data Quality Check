/*
    Script Name     : Data Quality Check - Bronze Layer
    Description     : Validate and profile raw data in Bronze tables before 
                      transformation into Silver schema. Ensures data quality 
                      (duplicates, nulls, spacing, standardization, consistency, 
                      business rules, referential integrity).
*/

------------------------------
-- Table CRM: Customer Info
------------------------------

-- 1. Check for NULLs or duplicate values in primary key (cst_id)
SELECT
    cst_id,
    COUNT(*) AS Total_Duplicate
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Check leading/trailing spacing issues in customer first name
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 3. Check data standardization & consistency of gender values
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


------------------------------
-- Table CRM: Product Info
------------------------------

-- 4. Check for NULLs or duplicates in primary key (prd_id)
SELECT
    prd_id,
    COUNT(*) AS Total_Duplicate
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 5. Check spacing issues in product names
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 6. Check numeric validity of product cost (no negative or NULL values allowed)
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 7. Check consistency of product line codes (should match standardized set)
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- 8. Validate product start/end date (end date must be after start date)
SELECT DISTINCT 
    prd_key,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


------------------------------
-- Table CRM: Sales Details
------------------------------

-- 9. Check spacing in sales order numbers
SELECT sls_ord_num
FROM bronze.crm_sls_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- 10. Check referential integrity (product key in sales must exist in Product Info - Silver)
SELECT sls_cust_id
FROM bronze.crm_sls_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
-- Alternative check for customer ID against CRM customers:
-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- 11. Check validity of order date (format, length, realistic range)
SELECT sls_order_dt
FROM bronze.crm_sls_details
WHERE sls_order_dt <= 0 
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

-- 12. Identify invalid date relationships (order date must be before ship/due dates)
SELECT *
FROM bronze.crm_sls_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- 13. Check business rules (sales = quantity * price, all > 0)
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sls_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


------------------------------
-- Table ERP: Customer Info
------------------------------

-- 14. Check referential integrity: ERP customer IDs must exist in CRM customers
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM bronze.crm_cust_info);

-- 15. Check date validity (birthdate must be realistic)
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- 16. Check gender standardization & consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;


------------------------------
-- Table ERP: Location Info
------------------------------

-- 17. Check referential integrity: location customer IDs must exist in CRM customer table
SELECT cid
FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- 18. Check standardization & consistency of country codes
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;


------------------------------
-- Table ERP: Product Category
------------------------------

-- 19. Check spacing issues in product category IDs
SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id);

-- 20. Check referential integrity: category IDs must exist in CRM product info
SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

-- 21. Check standardization & consistency of maintenance field
SELECT DISTINCT maintance
FROM bronze.erp_px_cat_g1v2;
