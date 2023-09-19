-- tag: ClaimFinancialTransactionLineBOPCeded_DupesOverall - tag ends/
/*** ClaimFinancialTransactionLineBOPCeded_DupesOverall.sql ***

	*****  Change History  *****

	01/23/2023	DROBAK		Init
-----------------------------------------------------------------------------
*/
	--DUPES In Extract
		SELECT	'DUPES OVERALL'				AS UnitTest
				, FinancialTransactionKey
				, TransactionPublicID
				, COUNT(*)					AS NumRecords
				, DATE('{date}')			AS bq_load_date	
		FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPCeded`
		WHERE bq_load_date = DATE({partition_date})
		GROUP BY FinancialTransactionKey, TransactionPublicID
		HAVING count(*)>1 --dupe check