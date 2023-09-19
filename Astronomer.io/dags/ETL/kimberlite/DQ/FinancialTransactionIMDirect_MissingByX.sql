/*** FinancialTransactionIMDirect_MissingByX.sql ***

	*****  Change History  *****

	06/28/2021	SLJ			INIT
---------------------------------------------------------------------------------------------------
*/
--Missing Trxn
SELECT 'MISSING' as UnitTest
		, JobNumber
		, trxn.PublicID
		, trxn.FixedID
		, trxn.BranchID
		, pc_policyperiod.EditEffectiveDate
		, PostedDate
		, trxn.WrittenDate
		, cost.ILMLineCov
		, cost.ILMSubLineCov
		, cost.ILMLocationCov
		, cost.ILMSubLocCov
		, cost.JewelryStockCov
		, cost.ILMSubStockCov
		, cost.ILMOneTimeCredit
		, cost.ILMMinPremPolicyLocation
		, cost.ILMTaxLocation_JMIC
		, DATE('{date}') AS bq_load_date
from (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME={partition_date}) trxn
left join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = trxn.BranchID
left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
left join (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME={partition_date}) cost on cost.id=trxn.ILMCost_JMIC
where trxn.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date}))
--and Jobnumber=ISNULL(@jobnumber,JobNumber)
--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
and trxn.PostedDate is not null
