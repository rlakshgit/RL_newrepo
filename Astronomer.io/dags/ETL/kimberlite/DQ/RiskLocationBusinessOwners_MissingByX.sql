
--Missing Values[Locations]
SELECT 'MISSING' AS UnitTest
	   , pc_boplocation.PublicID 
	   , pc_policyperiod.PolicyNumber	   
	   , pc_job.JobNumber
	   , TYPECODE
	   ,'Location' AS RiskLevel
	   , DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
		ON pc_policyperiod.id = pc_boplocation.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
	INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'BusinessOwnersLine' 
	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date})) AS d	--#RiskLocBOPTemp
		ON d.LocationPublicID = pc_boplocation.PublicID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
		ON pc_job.ID = pc_policyperiod.JobID
WHERE	1 = 1
	AND d.LocationPublicID IS NULL
	AND pc_job.JobNumber IS NOT NULL		
	AND(    pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		)
