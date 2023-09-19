
	--Missing Trxn
		SELECT 'MISSING' as UnitTest
							, pcx_ilmcededpremiumtrans_jmic.PublicID
							, pcx_ilmcost_jmic.FixedID
							, pc_policyperiod.JobID
							, pcx_ilmcost_jmic.BranchID
							, pc_policyperiod.EditEffectiveDate
							, pcx_ilmcededpremiumtrans_jmic.DatePosted
							, pcx_ilmcededpremiumtrans_jmic.DateWritten
							, DATE('{date}') AS bq_load_date 
		--,* 
		from (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcededpremiumtrans_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcost_jmic on pcx_ilmcost_jmic.id=pcx_ilmcededpremiumjmic.ILMCovCost_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = pcx_ilmcost_jmic.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where pcx_ilmcededpremiumtrans_jmic.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionIMCeded` where bq_load_date = DATE({partition_date}) and CededCoverable = 'DefaultCov')
		--and PolicyNumber = ISNULL(@policynumber, PolicyNumber)

		UNION ALL

		SELECT 'MISSING' as UnitTest
							, trxn.PublicID
							, pcx_ilmcost_jmic.FixedID
							, pc_policyperiod.JobID
							, pcx_ilmcost_jmic.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.DatePosted
							, trxn.DateWritten
							, DATE('{date}') AS bq_load_date 
		--,* 
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmstkcededpremtrans_jmic` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmstockcededpremiumjmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmstockcededpremiumjmic
				ON pcx_ilmstockcededpremiumjmic.ID = trxn.ILMStockCededPremium_JMIC
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcost_jmic ON pcx_ilmstockcededpremiumjmic.ILMStockCovCost_JMIC = pcx_ilmcost_jmic.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionIMCeded` where bq_load_date = DATE({partition_date}) and CededCoverable = 'ILMStockCov')
		--and PolicyNumber = ISNULL(@policynumber, PolicyNumber)
		--AND pcx_ilmcost_jmic.JewelryStockCov IS NOT NULL

		UNION ALL

		SELECT 'MISSING' as UnitTest
							, trxn.PublicID
							, pcx_ilmcost_jmic.FixedID
							, pc_policyperiod.JobID
							, pcx_ilmcost_jmic.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.DatePosted
							, trxn.DateWritten
							, DATE('{date}') AS bq_load_date 
		--,* 
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstkcededpremtranjmic` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstkcededpremiumjmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmsubstkcededpremiumjmic
				ON pcx_ilmsubstkcededpremiumjmic.ID = trxn.ILMSubStkCededPremium_JMIC
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcost_jmic ON pcx_ilmsubstkcededpremiumjmic.ILMSubStockCovCost_JMIC = pcx_ilmcost_jmic.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionIMCeded` where bq_load_date = DATE({partition_date}) and CededCoverable = 'ILMSubStockCov')
		--and PolicyNumber = ISNULL(@policynumber, PolicyNumber)
		--AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL

	UNION ALL
		
		SELECT 'MISSING' as UnitTest
							, pcx_ilmsublncededpremtran_jmic.PublicID
							, pcx_ilmcost_jmic.FixedID
							, pc_policyperiod.JobID
							, pcx_ilmcost_jmic.BranchID
							, pc_policyperiod.EditEffectiveDate
							, pcx_ilmsublncededpremtran_jmic.DatePosted
							, pcx_ilmsublncededpremtran_jmic.DateWritten
							, DATE('{date}') AS bq_load_date 
		--,* 
		from (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublncededpremtran_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmsublncededpremtran_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecededpremiumjmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmsublinecededpremiumjmic
				ON pcx_ilmsublinecededpremiumjmic.ID = pcx_ilmsublncededpremtran_jmic.ILMSubLineCededPremium_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_ilmcost_jmic on pcx_ilmcost_jmic.id=pcx_ilmsublinecededpremiumjmic.ILMSubLineCovCost_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = pcx_ilmcost_jmic.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where pcx_ilmsublncededpremtran_jmic.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionIMCeded` where bq_load_date = DATE({partition_date}) and CededCoverable = 'ILMSubLineCov')
		--and PolicyNumber = ISNULL(@policynumber, PolicyNumber)


