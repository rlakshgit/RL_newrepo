

	--DUPES In Extract
		SELECT 'DUPES OVERALL' as UnitTest, PolicyTransactionProductKey, PolicyTransactionKey, PolicyPeriodPublicID, count(*) as NumRecords, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date}))
		--where Policynumber=ISNULL(@policynumber, PolicyNumber)
		--and CoverageRank=1
		group by PolicyTransactionProductKey, PolicyTransactionKey, PolicyPeriodPublicID
		having count(*)>1 --dupe check
