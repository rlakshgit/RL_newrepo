/*** FinancialTransactionBOPCeded_MissingByX.sql ***

	*****  Change History  *****

	06/22/2021	DROBAK		Updated select output
-----------------------------------------------------
*/
	--Missing Trxn
		SELECT 'MISSING DefaultCov' as UnitTest
							, trxn.PublicID
							, pc_job.JobNumber
							, pc_policyperiod.PolicyNumber
							, cost.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.DatePosted as PostedDate
							, trxn.DateWritten as WrittenDate
							, DATE('{date}') AS bq_load_date 
							
		--,* 
		from (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME={partition_date}) pc_bopcededpremium
				ON pc_bopcededpremium.ID >= trxn.BOPCededPremium
				AND pc_bopcededpremium.ID <= trxn.BOPCededPremium
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME={partition_date}) cost on cost.id=pc_bopcededpremium.BOPCost
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cost.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionBOPCeded` 
									WHERE bq_load_date = DATE({partition_date}) and CededCoverable = 'DefaultCov')

		UNION ALL

		SELECT 'MISSING BOPCov' as UnitTest
							, trxn.PublicID
							, pc_job.JobNumber
							, pc_policyperiod.PolicyNumber
							, pc_bopcost.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.DatePosted	as PostedDate
							, trxn.DateWritten	as WrittenDate
							, DATE('{date}') AS bq_load_date 							
		--,* 
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremtransaction` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremium` WHERE _PARTITIONTIME={partition_date}) pcx_bopcovcededpremium
				ON pcx_bopcovcededpremium.ID = trxn.BOPCovCededPremium
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME={partition_date}) pc_bopcost ON pcx_bopcovcededpremium.BOPCovCost = pc_bopcost.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod ON pc_bopcost.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionBOPCeded` 
									WHERE bq_load_date = DATE({partition_date}) and CededCoverable = 'BOPCov')

		UNION ALL

		SELECT 'MISSING BOPBuildingCov' as UnitTest
							, trxn.PublicID
							, pc_job.JobNumber
							, pc_policyperiod.PolicyNumber
							, pc_bopcost.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.DatePosted
							, trxn.DateWritten
							, DATE('{date}') AS bq_load_date 
		--,* 
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopbuildingcededpremtrans` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopbuildingcededpremium` WHERE _PARTITIONTIME={partition_date}) pcx_bopbuildingcededpremium
				ON pcx_bopbuildingcededpremium.ID = trxn.BOPBuildingCededPremium
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME={partition_date}) pc_bopcost ON pcx_bopbuildingcededpremium.BOPBuildingCovCost = pc_bopcost.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod ON pc_bopcost.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionBOPCeded` 
									WHERE bq_load_date = DATE({partition_date}) and CededCoverable = 'BOPBuildingCov')

