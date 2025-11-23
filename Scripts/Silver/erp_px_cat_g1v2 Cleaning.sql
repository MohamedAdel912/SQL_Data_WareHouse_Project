--------------------------------------------------------------
-- STEP 1: Explore raw data from bronze.erp_px_cat_g1v2
--------------------------------------------------------------
SELECT [ID],
       [CAT],
       [SUBCAT],
       [MAINTENANCE]
FROM [bronze].[erp_px_cat_g1v2];


--------------------------------------------------------------
-- STEP 2: Check matching between ID and crm_prd_info.cat_id
--------------------------------------------------------------
SELECT ID FROM [bronze].[erp_px_cat_g1v2];
SELECT cat_id FROM [silver].[crm_prd_info];
-- ✅ Great: IDs matched


--------------------------------------------------------------
-- STEP 3: Check trimming issues in ID
--------------------------------------------------------------
SELECT ID
FROM [bronze].[erp_px_cat_g1v2]
WHERE ID != TRIM(ID);
-- ✅ Great: no issues found


--------------------------------------------------------------
-- STEP 4: Check distinct CAT values and trimming
--------------------------------------------------------------
SELECT DISTINCT CAT
FROM [bronze].[erp_px_cat_g1v2];

SELECT CAT
FROM [bronze].[erp_px_cat_g1v2]
WHERE CAT != TRIM(CAT);
-- ✅ Great: no issues found


--------------------------------------------------------------
-- STEP 5: Check distinct SUBCAT values and trimming
--------------------------------------------------------------
SELECT DISTINCT SUBCAT
FROM [bronze].[erp_px_cat_g1v2];

SELECT SUBCAT
FROM [bronze].[erp_px_cat_g1v2]
WHERE SUBCAT != TRIM(SUBCAT);
-- ✅ Great: no issues found


--------------------------------------------------------------
-- STEP 6: Check distinct MAINTENANCE values and trimming
--------------------------------------------------------------
SELECT DISTINCT MAINTENANCE
FROM [bronze].[erp_px_cat_g1v2];

SELECT MAINTENANCE
FROM [bronze].[erp_px_cat_g1v2]
WHERE MAINTENANCE != TRIM(MAINTENANCE);
-- ✅ Great: no issues found


TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT 
    TRIM(ID) AS ID,
    TRIM(CAT) AS CAT,
    TRIM(SUBCAT) AS SUBCAT,
    TRIM(MAINTENANCE) AS MAINTENANCE
FROM bronze.erp_px_cat_g1v2;