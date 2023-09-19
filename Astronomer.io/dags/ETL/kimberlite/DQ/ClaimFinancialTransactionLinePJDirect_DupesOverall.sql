-- tag: ClaimFinancialTransactionLinePJDirect_DupesOverall - tag ends/
/*** ClaimFinancialTransactionLinePJDirect_DupesOverall.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Change
-----------------------------------------------------------------------------
*/
	--DUPES In Extract
		SELECT	'DUPES OVERALL'				AS UnitTest
				, FinancialTransactionKey
				, FinancialTransactionLineKey
				, TransactionPublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect`
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, FinancialTransactionLineKey, TransactionPublicID
		HAVING count(*)>1 --dupe check