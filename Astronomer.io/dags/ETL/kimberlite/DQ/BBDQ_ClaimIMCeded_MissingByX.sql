/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimIMCeded_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLineIMCeded.TransactionPublicID
    ,ClaimFinancialTransactionLineIMCeded.PolicyNumber
    ,ClaimFinancialTransactionLineIMCeded.ClaimPublicId
    ,ClaimFinancialTransactionLineIMCeded.ClaimNumber
	, DATE('{date}') AS bq_load_date	
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineIMCeded` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLineIMCeded   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimIMCeded` WHERE bq_load_date = DATE({partition_date})) ClaimIMCeded
		ON ClaimIMCeded.FinancialTransactionKey = ClaimFinancialTransactionLineIMCeded.FinancialTransactionKey
WHERE 1=1
  AND ClaimFinancialTransactionLineIMCeded.IsTransactionSliceEffective = 1	
  AND ClaimIMCeded.FinancialTransactionKey IS NULL
