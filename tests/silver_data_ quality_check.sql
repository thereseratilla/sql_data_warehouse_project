/* 
==================================================================================================
 QUALITY CHECKS OF THE SILVER LAYER 
==================================================================================================

Purpose:
  This script performs several quality checks for data consistency, accuracy and standardization 
  in the silver schema. We checked the following:
    - Null or duplicate primary keys
    - Unwanted spaces for string values
    - Data standardization and consistency 
    - Invalid dates (ranges and orders)
    - Data consistency in related fields
    - Casting into the correct data type

  Usage: 
     - Run these checks after loading the stored procedure for the silver layer.
     - Resolve issues during the checks.
==================================================================================================
*/

/* ==================================================================================================
 1. SILVER.CRM_CUST_INFO TABLE 
==================================================================================================*/

-- 1.1 Checking for nulls and duplicates in the primary key
-- Expectations: No results
  
SELECT 
	cst_id 
	, COUNT(*)
FROM silver.crm_cust_info
GROUP BY
	cst_id 
HAVING 
	COUNT(*) > 1 OR cst_id IS NULL;

-- 1.2 Check for unwanted spaces
-- Expectations: No results
SELECT 
	cst_firstname
FROM 
	silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
--
SELECT 
	cst_lastname
FROM 
	silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
--
SELECT 
	cst_gndr
FROM 
	silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- 1.3 Check Data Consistency and Standardization
SELECT 
	DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT 
	DISTINCT cst_gndr
FROM silver.crm_cust_info
  
-- 1.4 Check whole table
SELECT * 
FROM silver.crm_cust_info


/* ==================================================================================================
 2. SILVER.CRM_PRD_INFO TABLE 
==================================================================================================*/


-- 2.1. Checking for duplicatees/nulls on primary key
-- Expectations: No results
SELECT 
	prd_id 
	, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY	
	prd_id 
HAVING 
	COUNT(*) > 1 OR prd_id IS NULL

--2.2 Checking unwanted spaces
-- Expections: No results
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- 2.3 Check for NULLS or Negative Numbers
--Expections: No results

SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- 2.4 Check standardization
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info

-- 2.5 Check for invalid date orders
--Expections: No results
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- 2.6 Final look of the silver table
SELECT *
FROM silver.crm_prd_info

/* ==================================================================================================
 3. SILVER.CRM_SALES_DETAILS TABLE 
==================================================================================================*/


-- 3.1 Checking invalid date orders, if order date is higher than ship date, or order date is higher than due date
-- Expectations: No results
SELECT *
FROM silver.crm_sales_details
WHERE	sls_order_dt > sls_ship_dt 
		OR sls_order_dt > sls_due_dt

--3.2 Check calculations
-- Expectations: No results
SELECT DISTINCT
	sls_quantity
	,sls_sales
	, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
  OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price  <= 0
ORDER BY sls_sales,
	sls_quantity,
	sls_price

  -- 3.3 Check full table
SELECT *
FROM silver.crm_sales_details


/* ==================================================================================================
 4. SILVER.ERP_CUST_AZ12 TABLE 
==================================================================================================*/

-- 4.1 check for out of range dates
-- Note: We only removed those that are way past the current date
-- For those aged 100 and above, consult with other expert

SELECT DISTINCT 
	bdate
FROM silver.erp_cust_az12 
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- 4.2 check consistency of gender
-- Should only have Male, Female and n/a

SELECT 
	DISTINCT gen
FROM silver.erp_cust_az12 

-- 4.3 final look
SELECT *
FROM silver.erp_cust_az12

/* ==================================================================================================
 5. SILVER.ERP_CUST_AZ12 TABLE 
==================================================================================================*/

-- 5.2 check consistency of country variable
-- Should only have Male, Female and n/a
SELECT 
	DISTINCT cntry 
FROM silver.erp_loc_a101

-- 5.3 check if the minus in cid is removed
--Expectations -- no result
SELECT 
	cid
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%'

  -- 5.3 Chheck whole table
SELECT *
FROM silver.erp_loc_a101

/* ==================================================================================================
 6. SILVER.ERP_CUST_AZ12 TABLE 
==================================================================================================*/

-- 6.1 check unwanted spaces

SELECT cat
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT subcat
FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT maintenance
FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

-- 6.2 check consistency

SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2

-- 6.3 Check whole table
SELECT *
FROM silver.erp_px_cat_g1v2
