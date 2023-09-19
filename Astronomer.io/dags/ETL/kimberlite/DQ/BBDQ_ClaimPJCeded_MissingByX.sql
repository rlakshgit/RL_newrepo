/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimPJCeded_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLinePJCeded.TransactionPublicID
    ,ClaimFinancialTransactionLinePJCeded.PolicyNumber
    ,ClaimFinancialTransactionLinePJCeded.ClaimPublicId
    ,ClaimFinancialTransactionLinePJCeded.ClaimNumber
	, DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLinePJCeded   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimPJCeded` WHERE bq_load_date = DATE({partition_date})) ClaimPJCeded
		ON ClaimPJCeded.FinancialTransactionKey = ClaimFinancialTransactionLinePJCeded.FinancialTransactionKey
WHERE 1=1
  AND ClaimFinancialTransactionLinePJCeded.IsTransactionSliceEffective = 1	
  AND ClaimPJCeded.FinancialTransactionKey IS NULL
