-- tag: FinancialTransactionPJDirect_DupesByCoverage - tag ends/
/*** FinancialTransactionPJDirect_DupesByCoverage.sql ***

	*****  Change History  *****

	09/28/2021	DROBAK		Init
-----------------------------------------------------------------------------
*/
--DUPES In Extract
		SELECT 'DUPES BY COVERAGE LEVEL & PUBLICID' AS UnitTest, FinancialTransactionKey, TransactionPublicID, CoverageLevel, CoveragePublicID, count(*) as NumRecords, DATE('{date}') as bq_load_date	 
		FROM `{project}.{dest_dataset}.FinancialTransactionPJDirect` 
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, TransactionPublicID, /*BOPCoverageKey,*/ CoverageLevel, CoveragePublicID
		HAVING count(*)>1 --dupe check

	UNION ALL
	
	--DUPES In Extract
		SELECT 'DUPES BY COVERAGE LEVEL' AS UnitTest, FinancialTransactionKey, TransactionPublicID, CoverageLevel, CAST(null AS STRING), count(*) as NumRecords, DATE('{date}') as bq_load_date	 
		FROM `{project}.{dest_dataset}.FinancialTransactionPJDirect`  
		WHERE bq_load_date = DATE({partition_date})
		--and CoverageRank=1
		GROUP BY FinancialTransactionKey, TransactionPublicID, /*BOPCoverageKey,*/ CoverageLevel
		HAVING count(*)>1 --dupe check