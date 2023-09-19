/*** FinancialTransactionPACeded_MissingByX.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		add CoveragePublicID, CreateTime/PostedDate logic
---------------------------------------------------------------------------------------------------
*/
	--Missing Trxn  
		SELECT 'MISSING' as UnitTest
				, trxn.PublicID AS CoveragePublicID
				, pc_job.JobNumber
				, pc_policyperiod.PolicyNumber
				, cost.BranchID
				, pc_policyperiod.EditEffectiveDate
				, trxn.CreateTime AS TransactionPostedDate
				, trxn.DateWritten as WrittenDate
				, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremiumtrans_jm` WHERE _PARTITIONTIME = {partition_date}) trxn
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremium_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpacededpremium_jm
				ON pcx_jpacededpremium_jm.ID = trxn.JPACededPremium_JM
		left join (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) cost on cost.id = pcx_jpacededpremium_jm.JPACost_JM
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod on pc_policyperiod.id = cost.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionPACeded` WHERE bq_load_date = DATE({partition_date}))
		and trxn.CreateTime IS NOT NULL