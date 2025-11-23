-- Clean and transform data from bronze.erp_cust_az12
SELECT 
    -- Fix CID: remove NAS prefix if present
    CASE 
        WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) 
        ELSE CID
    END AS cust_id,

    -- Fix BDATE: remove unlogical dates
    CASE 
        WHEN CAST(BDATE AS DATE) > GETDATE() THEN NULL           -- future date → NULL
        WHEN CAST(BDATE AS DATE) < '1900-01-01' THEN NULL        -- unrealistically old → NULL
        WHEN DATEDIFF(YEAR, CAST(BDATE AS DATE), GETDATE()) < 18 THEN NULL -- too young → NULL
        ELSE CAST(BDATE AS DATE)                                 -- keep valid logical date
    END AS BDATE,

    -- Normalize GEN values
    CASE 
        WHEN GEN IS NULL OR LTRIM(RTRIM(GEN)) = '' THEN 'Unknown'
        WHEN GEN = 'M' OR GEN = 'Male' THEN 'Male'
        WHEN GEN = 'F' OR GEN = 'Female' THEN 'Female'
        ELSE 'Unknown'
    END AS GEN_cleaned
FROM bronze.erp_cust_az12;


TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
    CID,
    BDATE,
    GEN
)
SELECT 
    -- CID cleanup
    CASE 
        WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) 
        ELSE CID
    END AS cust_id,

    -- BDATE cleanup
    CASE 
        WHEN CAST(BDATE AS DATE) > GETDATE() THEN NULL
        WHEN CAST(BDATE AS DATE) < '1900-01-01' THEN NULL
        WHEN DATEDIFF(YEAR, CAST(BDATE AS DATE), GETDATE()) < 18 THEN NULL
        ELSE CAST(BDATE AS DATE)
    END AS BDATE,

    -- GEN cleanup
    CASE 
        WHEN GEN IS NULL OR LTRIM(RTRIM(GEN)) = '' THEN 'Unknown'
        WHEN GEN = 'M' OR GEN = 'Male' THEN 'Male'
        WHEN GEN = 'F' OR GEN = 'Female' THEN 'Female'
        ELSE 'Unknown'
    END AS GEN
FROM bronze.erp_cust_az12;

