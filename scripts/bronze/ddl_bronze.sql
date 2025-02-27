exec bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        
        -- Updated header format for better readability
        PRINT '=================================================================';
        PRINT '|                    BRONZE LAYER LOADING                        |';
        PRINT '=================================================================';
        
        -- CRM section with modified print statements
        PRINT '|                     CRM TABLES LOADING                         |';
        PRINT '-----------------------------------------------------------------';
        
        -- First CRM table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.crm_cust_info';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- Second CRM table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.crm_prd_info';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- Third CRM table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.crm_sales_details';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- ERP section with modified print statements
        PRINT '|                     ERP TABLES LOADING                         |';
        PRINT '-----------------------------------------------------------------';
        
        -- First ERP table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.erp_cust_az12';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- Second ERP table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.erp_loc_a101';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- Third ERP table
        SET @start_time = GETDATE();
        PRINT '>> Processing table: bronze.erp_px_cat_g1v2';
        PRINT '   - Performing truncate operation';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        PRINT '   - Loading data from CSV source';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\pilla\OneDrive\Documents\Sql Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '   - Operation completed in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------------------------';
        
        -- Updated completion message
        SET @batch_end_time = GETDATE();
        PRINT '=================================================================';
        PRINT '|              BRONZE LAYER LOADING COMPLETED                   |';
        PRINT '|   Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds' + SPACE(27-LEN(CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR))) + '|';
        PRINT '=================================================================';
    END TRY
    BEGIN CATCH
        PRINT '=================================================================';
        PRINT '|                      ERROR DETECTED                           |';
        PRINT '| Message: ' + LEFT(ERROR_MESSAGE(), 47) + SPACE(47-LEN(LEFT(ERROR_MESSAGE(), 47))) + '|';
        PRINT '| Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR) + SPACE(57-LEN(CAST(ERROR_NUMBER() AS NVARCHAR))) + '|';
        PRINT '=================================================================';
    END CATCH
END

