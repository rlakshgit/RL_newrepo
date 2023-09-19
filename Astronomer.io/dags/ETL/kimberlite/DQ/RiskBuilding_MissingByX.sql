-- tag: RiskBuilding_MissingByX - tag ends/
/*** RiskBuilding_MissingByX.sql ***
---------------------------------------------------------------------------------------------------------------------

	*****  Change History  *****

	09/28/2021	SLJ			Initial
	01/04/2023	DROBAK		Add join criteria to pc_boplocation (AND pc_boplocation.BOPLine = pc_policyline.FixedID)
---------------------------------------------------------------------------------------------------------------------
*/
--Missing Values[Building]
	SELECT DISTINCT 'MISSING' AS TestCase
	, 'Building' AS RiskLevel
	, pc_bopbuilding.PublicID AS BuildingPublicID
	, pc_boplocation.PublicID AS LocationPublicID
	, pc_policyperiod.PolicyNumber
	, pc_job.JobNumber 
	, DATE('{date}') AS bq_load_date 

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopbuilding
		ON pc_bopbuilding.BranchID = pc_policyperiod.ID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
		ON pc_boplocation.BranchID = pc_bopbuilding.BranchID
		AND pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
		AND pc_boplocation.BOPLine = pc_policyline.FixedID
		--AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
		--AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
	INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	LEFT JOIN `{project}.{dest_dataset}.RiskBuilding` AS e	--#Data e
		ON e.BuildingPublicID= pc_bopbuilding.PublicID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID
	WHERE 1=1
	AND e.BuildingPublicID IS NULL
	AND pc_job.JobNumber IS NOT NULL
	--AND pc_policyperiod.PolicyNumber = ISNULL(@policynumber, pc_policyperiod.PolicyNumber) 
	ORDER BY pc_bopbuilding.PublicID

/*
	SELECT 'MISSING' AS UnitTest, pc_bopbuilding.PublicID AS BuildingPublicID, pc_boplocation.PublicID AS LocationPublicID, 'Building' AS RiskLevel, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod ON pc_policyperiod.id = pc_boplocation.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuilding ON pc_bopbuilding.BranchID = pc_boplocation.BranchID
																															  AND pc_bopbuilding.BOPLocation = pc_boplocation.FixedID
	WHERE pc_bopbuilding.PublicID NOT IN (SELECT BuildingPublicID FROM `{project}.{dest_dataset}.RiskBuilding` WHERE bq_load_date = DATE({partition_date}))
	--AND pc_job.jobNumber=@jobNumber
	AND pc_policyperiod.EditEffectiveDate < pc_bopbuilding.ExpirationDate
*/