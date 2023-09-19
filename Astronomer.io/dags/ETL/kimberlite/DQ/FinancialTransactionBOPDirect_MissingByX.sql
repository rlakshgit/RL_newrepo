/**** FinancialTransactionBOPDirect ********
 
 *****  Change History  *****

	06/25/2021	DROBAK		Add PolicyNumber & JobNumber; PostedDate IS NOT NULL
*/
	--Missing Trxn
		SELECT 'MISSING' as UnitTest
							, pc_boptransaction.PublicID
							, pc_policyperiod.JobID
							, pc_job.JobNumber
							, pc_policyperiod.PolicyNumber
							, pc_boptransaction.BranchID
							, pc_policyperiod.EditEffectiveDate
							, pc_boptransaction.PostedDate as PostedDate
							, pc_boptransaction.WrittenDate as WrittenDate
							, DATE('{date}') AS bq_load_date 
							
		from (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME={partition_date}) pc_boptransaction
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME={partition_date}) cost on cost.id=pc_boptransaction.BOPCost AND cost.BusinessOwnersCov IS NOT NULL
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = pc_boptransaction.BranchID
		left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		where pc_boptransaction.PostedDate IS NOT NULL
		and pc_boptransaction.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionBOPDirect` 
									WHERE bq_load_date = DATE({partition_date}))
							