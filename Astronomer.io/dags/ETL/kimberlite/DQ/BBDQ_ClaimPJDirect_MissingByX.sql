/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimPJDirect_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLinePJDirect.TransactionPublicID
  	,ClaimFinancialTransactionLinePJDirect.TransactionLinePublicID
    ,ClaimFinancialTransactionLinePJDirect.PolicyNumber
    ,ClaimFinancialTransactionLinePJDirect.ClaimPublicId
    ,ClaimFinancialTransactionLinePJDirect.ClaimNumber
	, DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLinePJDirect   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimPJDirect` WHERE bq_load_date = DATE({partition_date})) ClaimPJDirect
		ON ClaimPJDirect.FinancialTransactionKey = ClaimFinancialTransactionLinePJDirect.FinancialTransactionKey
		AND ClaimPJDirect.FinancialTransactionLineKey = ClaimFinancialTransactionLinePJDirect.FinancialTransactionLineKey
WHERE 1=1
  AND	ClaimFinancialTransactionLinePJDirect.IsTransactionSliceEffective = 1	
  AND (ClaimPJDirect.FinancialTransactionKey IS NULL
  OR ClaimPJDirect.FinancialTransactionLineKey IS NULL)
