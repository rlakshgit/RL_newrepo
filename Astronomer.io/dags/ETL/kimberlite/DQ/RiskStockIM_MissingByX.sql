-- tag: RiskStockIM_MissingByX - tag ends/
/**** RiskStockIM_MissingByX.sql ********
 
 *****  Change History  *****
 --------------------------------------------------------------------------------------------------------------------
	06/01/2021	SLJ		Init
	12/22/2021	DROBAK	Added filter: status <> withdrawn
--------------------------------------------------------------------------------------------------------------------
*/

SELECT 
	'MISSING' as UnitTest
	, pcx_jewelrystock_jmic.PublicID
	, pc_policyperiod.PolicyNumber
	, pc_job.JobNumber 
	, DATE('{date}') AS bq_load_date 	
FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date})) RiskStockIM
		ON RiskStockIM.StockPublicID=pcx_jewelrystock_jmic.PublicID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
		ON pc_policyperiod.id = pcx_jewelrystock_jmic.BranchID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
		ON pc_job.ID = pc_policyperiod.JobID
/* -- will not implement yet; use status <> withdrawn first
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
		ON  pc_policylocation.BranchID = pcx_jewelrystock_jmic.BranchID
		AND pc_policylocation.FixedID = pcx_jewelrystock_jmic.Location
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
*/
WHERE	1 = 1
		AND RiskStockIM.StockPublicID IS NULL
		AND pc_job.JobNumber IS NOT NULL		
		AND(    pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd))
		AND pc_policyperiod.status <> 3 --Withdrawn
