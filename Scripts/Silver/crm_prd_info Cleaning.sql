IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL 
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
      prd_id INT,
      cat_id NVARCHAR(50),
      prd_key VARCHAR(50),
      prd_nm VARCHAR(50),
      prd_cost INT,
      prd_line VARCHAR(50),
      prd_start_dt DATE,
      prd_end_dt DATE
);

INSERT INTO silver.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
)
SELECT
      prd_id,

      -------------------------------------------------------------------
      -- Extract category ID from prd_key and replace '-' with '_'
      -------------------------------------------------------------------
      REPLACE(SUBSTRING(LTRIM(RTRIM(prd_key)), 1, 5), '-', '_') AS cat_id,

      -------------------------------------------------------------------
      -- Extract clean product key portion after position 7
      -------------------------------------------------------------------
      SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

      -------------------------------------------------------------------
      -- Ensure product name has no leading/trailing spaces
      -------------------------------------------------------------------
      TRIM(prd_nm) AS prd_nm,

      -------------------------------------------------------------------
      -- Replace null or invalid (<=0) cost values with 0
      -------------------------------------------------------------------
      CASE 
          WHEN prd_cost IS NULL OR prd_cost <= 0 THEN 0
          ELSE prd_cost
      END AS prd_cost,

      -------------------------------------------------------------------
      -- Standardize prd_line
      -------------------------------------------------------------------
      CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'Unknown'
      END AS prd_line,

      -------------------------------------------------------------------
      -- Fix invalid start/end dates
      -------------------------------------------------------------------
      CASE 
            WHEN prd_start_dt > prd_end_dt 
                 THEN CAST(DATEADD(DAY, -1, DATEADD(YEAR, 1, prd_start_dt)) AS DATE)
            WHEN prd_start_dt IS NULL 
                 THEN CAST(DATEADD(DAY, 1, DATEADD(YEAR, -1, prd_end_dt)) AS DATE)
            ELSE CAST(prd_start_dt AS DATE)
      END AS prd_start_dt,

      CASE 
            WHEN prd_start_dt > prd_end_dt 
                 THEN CAST(DATEADD(DAY, -1, DATEADD(YEAR, 1, prd_start_dt)) AS DATE)
            ELSE CAST(prd_end_dt AS DATE)
      END AS prd_end_dt

FROM bronze.crm_prd_info;
