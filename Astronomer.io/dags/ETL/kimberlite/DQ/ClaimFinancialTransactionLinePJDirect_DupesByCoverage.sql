-- tag: ClaimFinancialTransactionLinePJDirect_DupesByCoverage - tag ends/
/*** ClaimFinancialTransactionLinePJDirect_DupesByCoverage.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Change
-----------------------------------------------------------------------------
*/
--DUPES In Extract
		SELECT 'DUPES BY COVERAGE LEVEL & PUBLICID' AS UnitTest
				, FinancialTransactionKey
				, FinancialTransactionLineKey
				, TransactionPublicID
				, CoverageLevel
				, CoveragePublicID
				, COUNT(*)							AS NumRecords
				, DATE('{date}')					AS bq_load_date	 
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect` 
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, FinancialTransactionLineKey, TransactionPublicID, CoverageLevel, CoveragePublicID
		HAVING COUNT(*)>1 --dupe check

	UNION ALL
	
	--DUPES In Extract
		SELECT	'DUPES BY COVERAGE LEVEL'	AS UnitTest
				, FinancialTransactionKey
				, FinancialTransactionLineKey
				, TransactionPublicID
				, CoverageLevel
				, CAST(null AS STRING)		AS CoveragePublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	 
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect`  
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, FinancialTransactionLineKey, TransactionPublicID, CoverageLevel
		HAVING COUNT(*)>1 --dupe check