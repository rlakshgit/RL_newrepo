-- tag: FinancialTransactionPJDirect_DupesOverall - tag ends/
/*** FinancialTransactionPJDirect_DupesOverall.sql ***

	*****  Change History  *****

	09/28/2021	DROBAK		Init
-----------------------------------------------------------------------------
*/
	--DUPES In Extract
		SELECT	'DUPES OVERALL'				AS UnitTest
				, FinancialTransactionKey
				, TransactionPublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	
		FROM `{project}.{dest_dataset}.FinancialTransactionPJDirect`
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, TransactionPublicID /*BOPCoverageKey,*/
		HAVING count(*)>1 --dupe check


