-- tag: ClaimFinancialTransactionLineBOPDirect_DupesOverall - tag ends/
/*** ClaimFinancialTransactionLineBOPDirect_DupesOverall.sql ***

	*****  Change History  *****

	01/13/2023	DROBAK		Init
-----------------------------------------------------------------------------
*/
	--DUPES In Extract
		SELECT	'DUPES OVERALL'				AS UnitTest
				, FinancialTransactionKey
				, FinancialTransactionLineKey
				, TransactionPublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect`
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, FinancialTransactionLineKey, TransactionPublicID
		HAVING count(*)>1 --dupe check