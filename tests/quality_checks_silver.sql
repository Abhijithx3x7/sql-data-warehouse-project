/*
===============================================================================
Data Quality Checks – Silver Layer
===============================================================================
Purpose:
    This script is designed to validate the integrity, consistency, and 
    standardization of data within the 'silver' layer. The quality checks include:
    
    - Identifying NULL or duplicate values in primary key columns.
    - Detecting unnecessary whitespace in text fields.
    - Validating data standardization for categorical fields.
    - Ensuring date values fall within valid ranges and logical sequences.
    - Cross-checking data consistency between related fields.

Usage Guidelines:
    - Run this script after loading data into the Silver Layer.
    - Review the output to identify discrepancies and take corrective actions.
    - If any issues are found, investigate the root cause and update source data 
      or transformation logic accordingly.

===============================================================================
*/

-- ====================================================================
-- Validating 'silver.crm_cust_info' Table
-- ====================================================================
-- Check for NULLs or duplicate values in the primary key column
-- Expectation: No records should be returned.
