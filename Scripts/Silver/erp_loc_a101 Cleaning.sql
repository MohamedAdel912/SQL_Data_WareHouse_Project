--------------------------------------------------------------
-- STEP 1: Explore raw data from bronze.erp_loc_a101
--------------------------------------------------------------
SELECT [CID],
       [CNTRY]
FROM [bronze].[erp_loc_a101];


--------------------------------------------------------------
-- STEP 2: Check matching between CID and crm_cust_info.cst_key
--------------------------------------------------------------
SELECT * FROM [silver].[crm_cust_info];
SELECT * FROM [bronze].[erp_loc_a101];

-- Problem: CID contains '-' → fix by removing
SELECT REPLACE(CID,'-','') AS cid
FROM [bronze].[erp_loc_a101];

-- Verify cleaned CID against crm_cust_info
SELECT *
FROM (
    SELECT REPLACE(CID,'-','') AS cid
    FROM [bronze].[erp_loc_a101]
) t
WHERE cid NOT IN (SELECT cst_key FROM [silver].[crm_cust_info]);
-- Great: all matched after cleaning


--------------------------------------------------------------
-- STEP 3: Check for duplicates and trimming issues in CID
--------------------------------------------------------------
SELECT *
FROM (
    SELECT CID, ROW_NUMBER() OVER(PARTITION BY CID ORDER BY CID) AS checkingDuplicates
    FROM [bronze].[erp_loc_a101]
) t
WHERE checkingDuplicates != 1;

-- Check for leading/trailing spaces
SELECT CID
FROM [bronze].[erp_loc_a101]
WHERE CID != TRIM(CID);
-- Great: no issues found


--------------------------------------------------------------
-- STEP 4: Check distinct CNTRY values
--------------------------------------------------------------
SELECT DISTINCT CNTRY
FROM [bronze].[erp_loc_a101];

-- Found issues: [US, USA, United States, DE, United Kingdom, Canada, Australia, France, Germany, '']
-- Fix with CASE normalization
WITH CTE AS (
    SELECT 
        CASE 
            WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
            WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'Unknown'
            WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
            ELSE TRIM(CNTRY)
        END AS CNTRY
    FROM [bronze].[erp_loc_a101]
)
SELECT DISTINCT *
FROM CTE;
--  Great: normalized country values


TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT 
    -- Clean CID: remove '-' and trim
    TRIM(REPLACE(CID,'-','')) AS CID,

    -- Normalize CNTRY values
    CASE 
        WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
        WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'Unknown'
        WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
        ELSE TRIM(CNTRY)
    END AS CNTRY
FROM [bronze].[erp_loc_a101];
