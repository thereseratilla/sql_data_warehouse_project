/*
============================================================================
DDL SCRIPT: CREATING THE GOLD LAYER
===========================================================================

Purpose: 

  This script creates views for the GOLD layer.
  This layer is composed of the final fact and dimension tables in a STAR SCHEMA.

  Each view is derived from joining tables and transforming data from the
  Silver Layer to produce clean and business-ready dataset.

Usage: 
  These views can be queried directly for analytics and reporting.
===========================================================================
*/



/* ============================================================================
1. Creating the Dimension Table: CUSTOMERS 
=========================================================================== */

IF OBJECT_ID ('gold.dim_customers', 'U') IS NOT NULL
	DROP TABLE gold.dim_customers;

CREATE VIEW gold.dim_customers 
AS
SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key		-- surrogate key
		,ci.cst_id				AS customer_id
		,ci.cst_key				AS customer_number
		,ci.cst_firstname		AS first_name
		,ci.cst_lastname		AS last_name
		,la.cntry				AS country
		,ci.cst_marital_status	AS marital_status
		, (CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr		-- CRM is the master for gender info
				ELSE COALESCE(ca.gen, 'n/a')
				END)			AS gender
		,ca.bdate				AS birth_date
		,ci.cst_create_date		AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid 
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;
GO

/*============================================================================
2 Creating the Dimension Table: PRODUCTS
=========================================================================== */
	
IF OBJECT_ID ('gold.dim_products', 'U') IS NOT NULL
	DROP TABLE gold.dim_products;

CREATE VIEW gold.dim_products
AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY p.prd_start_dt, p.prd_key) AS product_key -- surrogate key
	,p.prd_id			AS product_id
	,p.prd_key			AS product_number
	,p.prd_nm			AS product_name
	,p.cat_id			AS category_id
	,pc.cat				AS category
	,pc.subcat 			AS subcategory
	,pc.maintenance		AS maintenance
	,p.prd_cost			AS cost
	,p.prd_line			AS product_line
	,p.prd_start_dt		AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON p.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- Filter out all historical data, only current data
GO


/* ============================================================================
3. Creating the FACT Table: SALES
=========================================================================== */
IF OBJECT_ID ('gold.fact_sales', 'U') IS NOT NULL
	DROP TABLE gold.fact_sales;

CREATE VIEW gold.fact_sales
AS
SELECT
	 sls_ord_num		AS order_number
	,pr.product_key  -- surrogate key
	,cu.customer_key -- surrogate key
	,sls_order_dt		AS order_date
	,sls_ship_dt		AS shipping_date
	,sls_due_dt			AS due_date
	,sls_sales			AS sales
	,sls_quantity		AS quantity
	,sls_price			AS price
FROM
	silver.crm_sales_details s
LEFT JOIN gold.dim_products pr 
ON s.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON s.sls_cust_id = cu.customer_id;
GO



-- In building the fact, we use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions -- like a LOOKUP
