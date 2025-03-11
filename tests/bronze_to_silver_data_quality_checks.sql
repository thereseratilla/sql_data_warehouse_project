/* 
==================================================================================================
 QUALITY CHECKS AND DATA CLEANSING OF BRONZE TABLES TO INSERT TO THE SILVER LAYER
==================================================================================================

Purpose:
  This script performs several quality checks for data consistency, accuracy and standardization 
  in the bronze schema to be inserted into the silver schema. We checked the following:
    - Null or duplicate primary keys
    - Unwanted spaces for string values
    - Data standardization and consistency 
    - Invalid dates (ranges and orders)
    - Data consistency in related fields
    - Casting into the correct data type

==================================================================================================
*/

/* ==================================================================================================
 1. BRONZE.CRM_CUST_INFO TABLE 
==================================================================================================*/
-- 1.1 Checking for nulls and duplicates in the primary key
-- Expectations: No results
SELECT 
	cst_id 
	, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY
	cst_id 
HAVING 
	COUNT(*) > 1 OR cst_id IS NULL;
GO

-- 1.2. Check for unwanted spaces
-- Expectations: No results
SELECT 
	cst_firstname
FROM 
	bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
--
SELECT 
	cst_lastname
FROM 
	bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
--
SELECT 
	cst_gndr
FROM 
	bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- 1.3. Check Data Consistency and Standardization

SELECT 
	DISTINCT cst_material_status
FROM bronze.crm_cust_info

/* ==================================================================================================
 2. BRONZE.CRM_PRD_INFO TABLE 
==================================================================================================*/

-- 2.1. Checking for duplicatees/nulls
SELECT 
	prd_id 
	, COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY	
	prd_id 
HAVING 
	COUNT(*) > 1 OR prd_id IS NULL
  
--3.2. Checking if the split is correct by comparing it to other table: Split prd_key into two columns
SELECT 
	prd_id
	, prd_key
	, prd_nm
	, prd_cost 
	, prd_line 
	, prd_start_dt 
	, prd_end_dt 
FROM 
	bronze.crm_prd_info
WHERE 
	SUBSTRING(prd_key, 7, LEN(prd_key)) IN 
	(SELECT sls_prd_key FROM bronze.crm_sales_details )
 -- 
SELECT 
	prd_id
	, prd_key
	, prd_nm
	, prd_cost 
	, prd_line 
	, prd_start_dt 
	, prd_end_dt 
FROM 
	bronze.crm_prd_info
WHERE 
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
          (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
		
--2.3. Checking unwanted spaces
-- Expections: No results
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- 2.4. Check for NULLS or Negative Numbers
--Expections: No results

SELECT 
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- 2.5. Check standardization of prd_line
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info

--2.6 Check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- 2.7 slowly making a solution using window functions
SELECT 
	prd_id 
	, prd_key 
	, prd_nm 
	, prd_start_dt 
	, prd_end_dt 
	, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


/* ==================================================================================================
3. BRONZE.CRM_SALES_DETAILS TABLE 
==================================================================================================*/

-- 3.1. For strings: Check if there are trailing spaces
SELECT 
	  sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales  
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- 3.2. Check if it matches with tables in silver table

SELECT 
	  sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales  
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT 
	  sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales  
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- It can connect to other table, dont need more transformations.

-- 3.3. Change date variables in integer to date format

-- 3.3.1 Checking invalid dates
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0 
	OR LEN(sls_order_dt) != 8 
	OR sls_order_dt > 20500101 
	OR sls_order_dt < 19000101

-- 3.3.2. Checking invalid date orders, if order date is higher than ship date, or order date is higher than due date

SELECT *
FROM bronze.crm_sales_details
WHERE	sls_order_dt > sls_ship_dt 
		OR sls_order_dt > sls_due_dt

-- 3.4. Check data consistency for sales, quantity, price

SELECT DISTINCT
	sls_sales AS old_sls_sales
	,sls_quantity
	,sls_price AS old_sls_price
	,(CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) 
		THEN  sls_quantity * ABS(sls_price) 
		ELSE sls_sales 
		END) AS sls_sales
	,(CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales/ NULLIF(sls_quantity, 0)
		ELSE sls_price
		END) AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
  OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price  <= 0
ORDER BY sls_sales,
	sls_quantity,
	sls_price


/* ==================================================================================================
4. BRONZE.ERP_CUST_AZ12 TABLE 
==================================================================================================*/

-- 4.1 check the primary keys that connect to other tables

SELECT 
	cid 
	,bdate 
	,gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%'

SELECT *
FROM silver.crm_cust_info

-- 4.2 we should remove the NAS in cid

SELECT 
	cid 
	, (CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
			ELSE cid 
			END) AS cid
	,bdate 
	,gen
FROM bronze.erp_cust_az12

-- 4.3 Check with silver table

SELECT  
	(CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
			ELSE cid 
			END) AS cid
	,bdate 
	,gen
FROM bronze.erp_cust_az12
WHERE (CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
			ELSE cid 
			END) 
		NOT IN 
		(SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- 4.4 check for out of range dates

SELECT DISTINCT 
	bdate
FROM bronze.erp_cust_az12 
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- 4.5 check consistency of gender

SELECT 
	DISTINCT gen
	, (CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
			END) AS gen
FROM bronze.erp_cust_az12 


/* ==================================================================================================
5. BRONZE.ERP_LOC_A101 TABLE 
==================================================================================================*/
  
-- 5.1 replace - with NO_SPACE for cid
SELECT 
	REPLACE(cid, '-', '') AS cid
	,(CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END) AS cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- 5.2 check the consistency of the cntry variable

SELECT 
	DISTINCT cntry AS old_cntry
	,(CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END) AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


/* ==================================================================================================
6. BRONZE.ERP_PX_CAT_G1V2 TABLE 
==================================================================================================*/

-- 6.1  Check primary keys of both tables
SELECT *
FROM bronze.erp_px_cat_g1v2

SELECT * 
FROM silver.crm_prd_info

-- 6.2 check unwanted spaces

SELECT cat
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT subcat
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT maintenance
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

-- 6.3 check consistency

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

-- Note:  we do not have to clean up anything



