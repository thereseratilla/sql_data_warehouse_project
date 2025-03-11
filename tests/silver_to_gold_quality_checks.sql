/* 
==================================================================================================
 QUALITY CHECKS OF SILVER TABLES TO BE CREATED INTO A VIEW IN THE GOLD LAYER
==================================================================================================

Purpose:
  This script performs several quality checks for data consistency, accuracy and standardization 
  in the silver layer to created into views in the gold layer. We checked the following:
    - No duplicates upon joining tables
    - Ensuring consistency and standardization after joining the data

==================================================================================================
*/


/*==================================================================================================
1. Preparation in creating the gold.dim_customers
==================================================================================================*/

--1.1 make sure there are no duplicates after joining tables
SELECT cst_id 
	,COUNT(*)
FROM 
	(SELECT 
		 ci.cst_id
		,ci.cst_key 
		,ci.cst_firstname
		,ci.cst_lastname 
		,ci.cst_marital_status
		,ci.cst_gndr
		,ci.cst_create_date
		,ca.bdate 
		,ca.gen
		,la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid 
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid ) AS t
GROUP BY cst_id 
HAVING COUNT(*) > 1


-- 1.2. Integrating gender variable in two tables
SELECT 
		DISTINCT ci.cst_gndr
		, ca.gen
		, (CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
				ELSE COALESCE(ca.gen, 'n/a')
				END) AS new_gen 
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid 
ORDER BY 1,2

/*==================================================================================================
2. Preparation in creating the gold.dim_products
==================================================================================================*/

--2.1 make sure there are no duplicates after joining tables
SELECT	DISTINCT prd_key
		, COUNT(*)
FROM (
SELECT 
	 p.prd_id 
	,p.cat_id
	,p.prd_key 
	,p.prd_nm 
	,p.prd_cost 
	,p.prd_line
	,p.prd_start_dt 	
	,pc.cat 
	,pc.subcat 
	,pc.maintenance
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON p.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data, only current data
) AS t
GROUP BY prd_key
HAVING  COUNT(*) > 1
