/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimIMDirect_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLineIMDirect.TransactionPublicID
  	,ClaimFinancialTransactionLineIMDirect.TransactionLinePublicID
    ,ClaimFinancialTransactionLineIMDirect.PolicyNumber
    ,ClaimFinancialTransactionLineIMDirect.ClaimPublicId
    ,ClaimFinancialTransactionLineIMDirect.ClaimNumber
	, DATE('{date}') AS bq_load_date	
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineIMDirect` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLineIMDirect   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimIMDirect` WHERE bq_load_date = DATE({partition_date})) ClaimIMDirect
		ON ClaimIMDirect.FinancialTransactionKey = ClaimFinancialTransactionLineIMDirect.FinancialTransactionKey
		AND ClaimIMDirect.FinancialTransactionLineKey = ClaimFinancialTransactionLineIMDirect.FinancialTransactionLineKey
WHERE 1=1
  AND	ClaimFinancialTransactionLineIMDirect.IsTransactionSliceEffective = 1	
  AND (ClaimIMDirect.FinancialTransactionKey IS NULL
  OR ClaimIMDirect.FinancialTransactionLineKey IS NULL)
