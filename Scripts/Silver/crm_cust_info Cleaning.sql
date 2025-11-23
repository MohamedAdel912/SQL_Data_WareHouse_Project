--------------------------------------------------------------
-- STEP 1: Explore raw data from crm_cust_info
--------------------------------------------------------------
SELECT *
FROM bronze.crm_cust_info;


--------------------------------------------------------------
-- STEP 2: Check for duplicate and NULL customer IDs (cst_id)
--------------------------------------------------------------
WITH cte1 AS (
    SELECT 
        cst_id,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_id) AS checking_id_duplicates
    FROM bronze.crm_cust_info
)
SELECT *
FROM cte1
WHERE checking_id_duplicates != 1;   -- Duplicate or NULL IDs found


--------------------------------------------------------------
-- STEP 3: Fix duplicates and NULLs in cst_id
--------------------------------------------------------------
WITH cte2 AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS fix_duplicates
    FROM bronze.crm_cust_info
)
SELECT *
FROM cte2
WHERE fix_duplicates = 1 
  AND cst_id IS NOT NULL;   -- Keep newest record + remove NULL IDs


--------------------------------------------------------------
-- STEP 4: Check whitespace issues in first & last names
--------------------------------------------------------------
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);   -- Issues found

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);     -- Issues found


--------------------------------------------------------------
-- STEP 5: Fix whitespace issues in names
--------------------------------------------------------------
SELECT 
    TRIM(cst_firstname) AS cleaned_firstname,
    TRIM(cst_lastname)  AS cleaned_lastname
FROM bronze.crm_cust_info;


--------------------------------------------------------------
-- STEP 6: Validate gender values
--------------------------------------------------------------
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Standardize gender
SELECT
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS cst_gndr
FROM bronze.crm_cust_info;


--------------------------------------------------------------
-- STEP 7: Validate marital status values
--------------------------------------------------------------
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- Standardize marital status
SELECT 
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_marital_status
FROM bronze.crm_cust_info;


--------------------------------------------------------------
-- STEP 8: Validate date fields
--------------------------------------------------------------
WITH cte3 AS (
    SELECT *,
           ISDATE(CAST(cst_create_date AS NVARCHAR)) AS validationdate
    FROM bronze.crm_cust_info
)
SELECT *
FROM cte3
WHERE validationdate = 1;   -- Valid dates only


-- Reload silver with cleaned data from bronze
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    b.cst_id,
    b.cst_key,
    TRIM(b.cst_firstname) AS cst_firstname,
    TRIM(b.cst_lastname)  AS cst_lastname,

    CASE 
        WHEN UPPER(TRIM(b.cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(b.cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_marital_status,

    CASE 
        WHEN UPPER(TRIM(b.cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(b.cst_gndr)) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS cst_gndr,

    CAST(b.cst_create_date AS DATE) AS cst_create_date
FROM bronze.crm_cust_info b
WHERE b.cst_id IS NOT NULL
  AND ISDATE(CAST(b.cst_create_date AS NVARCHAR)) = 1
  AND b.cst_create_date = (
        SELECT MAX(b2.cst_create_date)
        FROM bronze.crm_cust_info b2
        WHERE b2.cst_id = b.cst_id
    );
