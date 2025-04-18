/*
=====================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=====================================================================================
Script Purpose:
	This stored procedure loads data into the 'silver' schema from bronze tables.
	It performs the following actions:
		- Truncates the silver tables before loading data.
		- Use 'INSERT' command to load data from bronze tables to silver tables.

Parameters:
	None
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '====================================';
		PRINT 'Loading Silver Layer';
		PRINT '====================================';

		PRINT '------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Loading silver.crm_cust_info'
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

			select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'						-- Normalize marital status values to readable format
			end as cst_marital_status,
			case when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'						-- Normalize gender values to readable format
			end as cst_gndr,
			cst_create_date
			from (
				select 
				*,
				row_number() over (Partition by cst_id order by cst_create_date desc) as flag_last
				from bronze.crm_cust_info
				where cst_id is not null
			) t
			where t.flag_last = 1 -- Select the most recent record per
		;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Loading silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,				
			cat_id,			
			prd_key,				
			prd_nm,			
			prd_cost,			
			prd_line,			
			prd_start_dt,		
			prd_end_dt	
			)
		select
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,		-- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
			END as prd_line,		-- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE),
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) as prd_end_dt    -- Calculate end date as one day before the next start date
		from bronze.crm_prd_info
		;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Loading silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
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

		select
			sls_ord_num,
			sls_prd_key, 
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		from bronze.crm_sales_details
		;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Loading silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		select
		CASE WHEN cid LIKE 'NAS%' THEN  SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END as cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		end as gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Loading silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		select 
		REPLACE(cid, '-', '') AS cid,
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		from bronze.erp_loc_a101
		;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Loading silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '----------------------';
		PRINT ' ';

		SET @batch_end_time = GETDATE()
		PRINT '===========================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================';

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
