

	--DUPES In Extract
		SELECT 'DUPES OVERALL' as UnitTest, FinancialTransactionKey, TransactionPublicID, count(*) as NumRecords, DATE('{date}') as bq_load_date	 
		from `{project}.{dest_dataset}.FinancialTransactionBOPDirect` 
		WHERE bq_load_date = DATE({partition_date})
		--where Policynumber=@policynumber
		--and CoverageRank=1
		group by FinancialTransactionKey, TransactionPublicID /*BOPCoverageKey,*/
		having count(*)>1 --dupe check


