/*** FinancialTransactionUMBCeded_MissingByX.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		Updated fields for consistent output
---------------------------------------------------------------------------------------------------
*/
	--Missing Trxn
		SELECT 'MISSING' as UnitTest
				, trxn.PublicID
				, pc_job.JobNumber
				, pc_policyperiod.PolicyNumber
				, cost.BranchID
				, pc_policyperiod.EditEffectiveDate
				, trxn.DatePosted as PostedDate
				, trxn.DateWritten as WrittenDate
				, DATE('{date}') AS bq_load_date 	

		from (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcededpremiumtrans_jmic` WHERE _PARTITIONTIME={partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcededpremiumjmic` WHERE _PARTITIONTIME={partition_date}) pcx_umbcededpremiumjmic
				ON pcx_umbcededpremiumjmic.ID = trxn.UMBCededPremium_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME={partition_date}) cost on cost.id=pcx_umbcededpremiumjmic.UMBCovCost_JMIC
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cost.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionUMBCeded` where bq_load_date = DATE({partition_date}) and CededCoverable = 'DefaultCov')
		--and trxn.PostedDate IS NOT NULL