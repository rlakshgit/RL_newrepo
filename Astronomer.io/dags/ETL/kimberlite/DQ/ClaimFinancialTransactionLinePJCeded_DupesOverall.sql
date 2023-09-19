-- tag: ClaimFinancialTransactionLinePJCeded_DupesOverall - tag ends/
/*** ClaimFinancialTransactionLinePJCeded_DupesOverall.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Change
-----------------------------------------------------------------------------
*/
	--DUPES In Extract
		SELECT	'DUPES OVERALL'				AS UnitTest
				, FinancialTransactionKey
				, TransactionPublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded`
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, TransactionPublicID
		HAVING count(*)>1 --dupe check