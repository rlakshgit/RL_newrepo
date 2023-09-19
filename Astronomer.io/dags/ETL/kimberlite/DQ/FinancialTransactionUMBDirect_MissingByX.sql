/*** FinancialTransactionUMBDirect_MissingByX.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		Updated to work in BQ; updated fields for consistent output
---------------------------------------------------------------------------------------------------
*/
--Missing Trxn
	SELECT 'MISSING' as UnitTest, trxn.PublicID, pc_job.JobNumber, pc_policyperiod.PolicyNumber
		, trxn.BranchID, pc_policyperiod.EditEffectiveDate
		, trxn.PostedDate, trxn.WrittenDate
		, DATE('{date}') AS bq_load_date
	from (SELECT * FROM `{project}.{pc_dataset}.pcx_umbtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) trxn
	left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod on pc_policyperiod.id = trxn.BranchID
	left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
	left join (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) cost on cost.id = trxn.UMBCost_JMIC
	where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionUMBDirect` WHERE bq_load_date = DATE({partition_date}))
	and trxn.PostedDate IS NOT NULL
