/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimBOPCeded_Missing.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Missing' AS UnitTest
	,ClaimFinancialTransactionLineBOPCeded.TransactionPublicID
    ,ClaimFinancialTransactionLineBOPCeded.PolicyNumber
    ,ClaimFinancialTransactionLineBOPCeded.ClaimPublicId
    ,ClaimFinancialTransactionLineBOPCeded.ClaimNumber
	, DATE('{date}') AS bq_load_date	
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPCeded` WHERE bq_load_date = DATE({partition_date})) ClaimFinancialTransactionLineBOPCeded   
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.ClaimBOPCeded` WHERE bq_load_date = DATE({partition_date})) ClaimBOPCeded
		ON ClaimBOPCeded.FinancialTransactionKey = ClaimFinancialTransactionLineBOPCeded.FinancialTransactionKey
WHERE 1=1
  AND ClaimFinancialTransactionLineBOPCeded.IsTransactionSliceEffective = 1	
  AND ClaimBOPCeded.FinancialTransactionKey IS NULL
