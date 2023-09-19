-- tag: RiskLocationIM_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Checks
	RiskLocationIM_MissingByX.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/01/2021	SLJ			INIT
	
	-------------------------------------------------------------------------------------------
*/
--Missing Values[Locations]
SELECT 'MISSING' AS UnitTest
	, pcx_ilmlocation_jmic.PublicID AS LocationPublicID
	, pc_policyperiod.PolicyNumber
	, pc_job.JobNumber 	
	,'Location' AS RiskLevel
	, DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
	ON pc_policyperiod.id = pcx_ilmlocation_jmic.BranchID
LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date})) d	--#Data 
	ON d.LocationPublicID = pcx_ilmlocation_jmic.PublicID
LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date} ) pc_job
	ON pc_job.ID = pc_policyperiod.JobID
WHERE	1 = 1
AND	d.LocationPublicID IS NULL
AND pc_policyperiod.PolicyNumber IS NOT NULL
AND pc_job.JobNumber IS NOT NULL
AND(	pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
	AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
)