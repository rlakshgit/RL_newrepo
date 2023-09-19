/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimPADirect_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLinePADirect.TransactionPublicID
  	,ClaimFinancialTransactionLinePADirect.TransactionLinePublicID
    ,ClaimFinancialTransactionLinePADirect.PolicyNumber
    ,ClaimFinancialTransactionLinePADirect.ClaimPublicId
    ,ClaimFinancialTransactionLinePADirect.ClaimNumber
	, DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePADirect` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLinePADirect   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimPADirect` WHERE bq_load_date = DATE({partition_date})) ClaimPADirect
		ON ClaimPADirect.FinancialTransactionKey = ClaimFinancialTransactionLinePADirect.FinancialTransactionKey
		AND ClaimPADirect.FinancialTransactionLineKey = ClaimFinancialTransactionLinePADirect.FinancialTransactionLineKey
WHERE 1=1
  AND	ClaimFinancialTransactionLinePADirect.IsTransactionSliceEffective = 1	
  AND (ClaimPADirect.FinancialTransactionKey IS NULL
  OR ClaimPADirect.FinancialTransactionLineKey IS NULL)
