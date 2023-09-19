/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimBOPDirect_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLineBOPDirect.TransactionPublicID
  	,ClaimFinancialTransactionLineBOPDirect.TransactionLinePublicID
    ,ClaimFinancialTransactionLineBOPDirect.PolicyNumber
    ,ClaimFinancialTransactionLineBOPDirect.ClaimPublicId
    ,ClaimFinancialTransactionLineBOPDirect.ClaimNumber
	, DATE('{date}') AS bq_load_date	
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLineBOPDirect   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimBOPDirect` WHERE bq_load_date = DATE({partition_date})) ClaimBOPDirect
		ON ClaimBOPDirect.FinancialTransactionKey = ClaimFinancialTransactionLineBOPDirect.FinancialTransactionKey
		AND ClaimBOPDirect.FinancialTransactionLineKey = ClaimFinancialTransactionLineBOPDirect.FinancialTransactionLineKey
WHERE 1=1
  AND	ClaimFinancialTransactionLineBOPDirect.IsTransactionSliceEffective = 1	
  AND (ClaimBOPDirect.FinancialTransactionKey IS NULL
  OR ClaimBOPDirect.FinancialTransactionLineKey IS NULL)
