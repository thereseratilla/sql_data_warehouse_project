/* 
==================================================================================================
 QUALITY CHECKS OF THE GOLD LAYER 
==================================================================================================

Purpose:
  This script performs several quality checks for data consistency, accuracy and integrity
  of the Gold layer. We checked the following:
    - Uniqueness of the surrogate keys in dimension tables
    - Referential integrity between fact and dimension
    - Validation of relationships in the data model for analytical purposes

  Usage: 
     - Run these checks after loading the the silver layer.
     - Resolve issues found during checks.
==================================================================================================
*/



/*==================================================================================================
1. Checking gold.dim_customers
==================================================================================================*/

-- 1.1 Check uniqueness of primary key
SELECT customer_key
      , COUNT(*) as duplicate_count
FROM gold.dim_customers
GROUP BY  customer_key
HAVING COUNT(*) > 1;

-- 1.2 Check gender variable
SELECT 
	DISTINCT gender
FROM gold.dim_customers

 
/*==================================================================================================
2. Checking gold.dim_customers
==================================================================================================*/

-- 2.1 Check uniqueness of primary key
SELECT product_key
      , COUNT(*) as duplicate_count
FROM gold.dim_products
GROUP BY  product_key
HAVING COUNT(*) > 1;


/*==================================================================================================
3. Checking gold.fact_sales
==================================================================================================*/

-- 3.1 check if all dimension tables was joined successfully to the fact table

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL

