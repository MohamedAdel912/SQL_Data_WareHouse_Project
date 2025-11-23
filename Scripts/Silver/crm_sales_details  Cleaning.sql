--------------------------------------------------------------
-- STEP 1: Explore raw data from crm_sales_details
--------------------------------------------------------------
SELECT [sls_ord_num],
       [sls_prd_key],
       [sls_cust_id],
       [sls_order_dt],
       [sls_ship_dt],
       [sls_due_dt],
       [sls_sales],
       [sls_quantity],
       [sls_price]
FROM [bronze].[crm_sales_details];


--------------------------------------------------------------
-- STEP 2: Check for whitespace issues in sls_ord_num
--------------------------------------------------------------
SELECT sls_ord_num
FROM [bronze].[crm_sales_details]
WHERE sls_ord_num != TRIM(sls_ord_num);
-- ✅ Great: no issues found


--------------------------------------------------------------
-- STEP 3: Validate customer IDs against silver.crm_cust_info
--------------------------------------------------------------
SELECT sls_cust_id
FROM [bronze].[crm_sales_details]
WHERE sls_cust_id NOT IN (SELECT cst_id FROM [silver].[crm_cust_info]);
-- ✅ Great: all customer IDs matched


--------------------------------------------------------------
-- STEP 4: Transform order, ship, and due dates
--------------------------------------------------------------
SELECT 
    CASE WHEN ISDATE(sls_order_dt) = 1 THEN CAST(sls_order_dt AS DATE) ELSE NULL END AS orderdt,
    CASE WHEN ISDATE(sls_ship_dt) = 1 THEN CAST(sls_ship_dt AS DATE) ELSE NULL END AS shipdt,
    CASE WHEN ISDATE(sls_due_dt) = 1 THEN CAST(sls_due_dt AS DATE) ELSE NULL END AS duedt
FROM bronze.crm_sales_details;


--------------------------------------------------------------
-- STEP 5: Clean sales, quantity, and enforce price rule
--------------------------------------------------------------
SELECT
    CASE 
        WHEN TRY_CAST(sls_sales AS INT) IS NULL OR TRY_CAST(sls_sales AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_sales AS INT)
    END AS sls_sales,

    CASE 
        WHEN TRY_CAST(sls_quantity AS INT) IS NULL OR TRY_CAST(sls_quantity AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_quantity AS INT)
    END AS sls_quantity,

    CASE 
        WHEN TRY_CAST(sls_sales AS INT) IS NULL OR TRY_CAST(sls_sales AS INT) < 0
          OR TRY_CAST(sls_quantity AS INT) IS NULL OR TRY_CAST(sls_quantity AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_sales AS INT) * TRY_CAST(sls_quantity AS INT)
    END AS sls_price
FROM bronze.crm_sales_details;
-- ✅ Great: business rule enforced


TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
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
    TRIM(sls_ord_num) AS sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Clean dates
    CASE WHEN ISDATE(sls_order_dt) = 1 THEN CAST(sls_order_dt AS DATE) ELSE NULL END AS sls_order_dt,
    CASE WHEN ISDATE(sls_ship_dt) = 1 THEN CAST(sls_ship_dt AS DATE) ELSE NULL END AS sls_ship_dt,
    CASE WHEN ISDATE(sls_due_dt) = 1 THEN CAST(sls_due_dt AS DATE) ELSE NULL END AS sls_due_dt,

    -- Clean sales
    CASE 
        WHEN TRY_CAST(sls_sales AS INT) IS NULL OR TRY_CAST(sls_sales AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_sales AS INT)
    END AS sls_sales,

    -- Clean quantity
    CASE 
        WHEN TRY_CAST(sls_quantity AS INT) IS NULL OR TRY_CAST(sls_quantity AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_quantity AS INT)
    END AS sls_quantity,

    -- Enforce business rule: sls_price = sls_sales * sls_quantity
    CASE 
        WHEN TRY_CAST(sls_sales AS INT) IS NULL OR TRY_CAST(sls_sales AS INT) < 0
          OR TRY_CAST(sls_quantity AS INT) IS NULL OR TRY_CAST(sls_quantity AS INT) < 0 THEN 0
        ELSE TRY_CAST(sls_sales AS INT) * TRY_CAST(sls_quantity AS INT)
    END AS sls_price
FROM bronze.crm_sales_details;
