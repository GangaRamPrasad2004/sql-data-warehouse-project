/*
============================================================================
Stored Procedure: Load Bronze Layer (Source->Bronze)
============================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
   - Truncates the bronze tables before loading data.
   - Uses the ' BULK INSERT ' command to load data from CSV Files to bronze tables.

Parameters:
  None.
This Stored procedure does not accept any parameter or return any values.

Usage Example:
  EXEC bronze.load_bronze;
===========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME;
DECLARE @end_time DATETIME;
 BEGIN TRY
  PRINT '==============================================';
  PRINT 'Loading Bronze Later';
  PRINT '==============================================';

                   PRINT '----------------------------------------------';
                   PRINT 'Loading CRM Tables'
                   PRINT '----------------------------------------------';
  
  
  SET @start_time=GETDATE();
  Print'>>Truncating Table:bronze.crm_cust_info';
  TRUNCATE TABLE bronze.crm_cust_info;
  Print'>>Inserting data into Table:bronze.crm_prd_info';
  BULK INSERT bronze.crm_cust_info
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
  WITH (
      FIRSTROW = 2, 
      FIELDTERMINATOR = ',',
      TABLOCK
  );
  SET @end_time= GETDATE();
  PRINT'>>Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time)AS Nvarchar) + ' seconds';
  print'>> -----------';


  SET @start_time = GETDATE();
  Print'>>Truncating Table:bronze.crm_prd_info';
  TRUNCATE TABLE bronze.crm_prd_info;
  Print'>>Inserting data into Table:bronze.crm_prd_info';
  BULK INSERT bronze.crm_prd_info
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
  WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ','
    );
  SELECT COUNT(*) AS crm_prd_info_count FROM bronze.crm_prd_info;
  SET @end_time=GETDATE();
  PRINT'>>Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';
  print'>> -----------';


  SET @start_time = GETDATE();
  Print'>>Truncating Table:bronze.crm_sales_details';
  TRUNCATE TABLE bronze.crm_sales_details;
  Print'>>inserting data into Table:bronze.crm_sales_details';
  BULK INSERT bronze.crm_sales_details
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
  WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
       );
  SELECT COUNT(*) AS crm_sales_details_count FROM bronze.crm_sales_details;
  SET @end_time=GETDATE();
  PRINT'>>Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';
  Print'>>----------';

             PRINT '----------------------------------------------';
             PRINT 'Loading ERP Tables';
             PRINT '----------------------------------------------';


  SET @start_time = GETDATE();
  Print'>>Truncating Table:bronze.erp_loc_a101';
  TRUNCATE TABLE bronze.erp_loc_a101;
  Print'>>Inserting data into Table:bronze.erp_loc_a101';
  BULK INSERT bronze.erp_loc_a101
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
  WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
       );
  SELECT COUNT(*) AS erp_loc_a101_count FROM bronze.erp_loc_a101;
  SET @end_time=GETDATE();
  PRINT'>>Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';


  SET @start_time=GETDATE();
  Print'>>Truncating Table:bronze.erp_cust_az12';
  TRUNCATE TABLE bronze.erp_cust_az12;
  Print'>>Inserting data into Table:bronze.erp_cust_az12';
  BULK INSERT bronze.erp_cust_az12
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
  WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
       );
  SELECT COUNT(*) AS erp_cust_az12_count FROM bronze.erp_cust_az12;
  SET @end_time=GETDATE();
  PRINT'>>Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

  
  SET @start_time=GETDATE();
  Print'>>Truncating Table:bronze.erp_px_cat_g1v2';
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  Print'>>Inserting data into Table:bronze.erp_px_cat_g1v2';
  BULK INSERT bronze.erp_px_cat_g1v2
  FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
  WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
       );
 SELECT COUNT(*) AS erp_px_cat_g1v2_count FROM bronze.erp_px_cat_g1v2;
 SET @end_time=GETDATE();
 PRINT'>>Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

 END TRY
 BEGIN CATCH 
 PRINT 'Error: ' + ERROR_MESSAGE();
  PRINT 'Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
  PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');

 END CATCH
END
EXEC bronze.load_bronze;
