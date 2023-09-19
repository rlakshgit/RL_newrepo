
	--DUPES In Extract
		SELECT 'DUPES BY COVERAGE LEVEL & PUBLICID' as UnitTest, FinancialTransactionKey, TransactionPublicID, CoverageLevel, CoveragePublicID, count(*) as NumRecords, DATE('{date}') as bq_load_date	 
		from `{project}.{dest_dataset}.FinancialTransactionPACeded` 
		WHERE bq_load_date = DATE({partition_date})
		--where Policynumber=@policynumber
		--and CoverageRank=1
		group by FinancialTransactionKey, TransactionPublicID, /*BOPCoverageKey,*/ CoverageLevel, CoveragePublicID
		having count(*)>1 --dupe check

	UNION ALL 
	
	--DUPES In Extract
		SELECT 'DUPES BY COVERAGE LEVEL' as UnitTest, FinancialTransactionKey, TransactionPublicID, CoverageLevel, CAST(null AS STRING), count(*) as NumRecords, DATE('{date}') as bq_load_date	 
		from `{project}.{dest_dataset}.FinancialTransactionPACeded` 
		WHERE bq_load_date = DATE({partition_date})
		--where Policynumber=@policynumber
		--and CoverageRank=1
		group by FinancialTransactionKey, TransactionPublicID, /*BOPCoverageKey,*/ CoverageLevel
		having count(*)>1 --dupe check



