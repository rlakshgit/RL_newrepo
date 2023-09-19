/*** FinancialTransactionPADirect_MissingByX.sql ***

	*****  Change History  *****

	06/25/2021	DROBAK		Updated select output
-----------------------------------------------------
*/
	--Missing Trxn
		SELECT 'MISSING' as UnitTest
							, trxn.PublicID
							, pc_policyperiod.PolicyNumber
							, pc_job.JobNumber
							, trxn.BranchID
							, pc_policyperiod.EditEffectiveDate
							, trxn.PostedDate
							, trxn.WrittenDate
							, DATE('{date}') AS bq_load_date  
		from (SELECT * FROM `{project}.{pc_dataset}.pcx_jpatransaction_jm` WHERE _PARTITIONTIME = {partition_date}) trxn
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod on pc_policyperiod.id = trxn.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		left join (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) cost on cost.id = trxn.JPACost_JM
		where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionPADirect` WHERE bq_load_date = DATE({partition_date}))
		and trxn.PostedDate iS NOT NULL