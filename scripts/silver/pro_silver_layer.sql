/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
===============================================================================
Overview:
    This stored procedure executes the ETL (Extract, Transform, Load) process 
    to migrate and refine data from the 'bronze' schema into the 'silver' schema.
    
Operations Performed:
    - Clears existing data in the Silver tables to ensure a fresh load.
    - Extracts raw data from the Bronze layer.
    - Applies necessary transformations and cleansing procedures.
    - Loads the refined data into the corresponding Silver tables.

Parameters:
    None. 
    This procedure operates without input parameters and does not return values.

Usage:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================================';
        PRINT '|                STARTING SILVER LAYER                 |';
        PRINT '========================================================';
        
		PRINT '--------------------------------------------------------';
		PRINT '|                   LOADING CRM TABLES                 |';
		PRINT '--------------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
		)
		SELECT
			cst_id, cst_key, 
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
        PRINT '   - Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm, ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '   - Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------';

        -- Loading silver.crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
		)
		SELECT 
			sls_ord_num, sls_prd_key, sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '   - Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------';

		PRINT '========================================================';
        PRINT '|              SILVER LAYER LOADING COMPLETED          |';
        PRINT '|   Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS NVARCHAR) + ' seconds' + SPACE(10) + '|';
        PRINT '========================================================';
    END TRY
    BEGIN CATCH
        PRINT '========================================================';
        PRINT '|                      ERROR DETECTED                  |';
        PRINT '| Message: ' + LEFT(ERROR_MESSAGE(), 47) + SPACE(47 - LEN(LEFT(ERROR_MESSAGE(), 47))) + '|';
        PRINT '| Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR) + SPACE(57 - LEN(CAST(ERROR_NUMBER() AS NVARCHAR))) + '|';
        PRINT '========================================================';
    END CATCH
END;
