--Missing Article records
SELECT DISTINCT
	'Missing Article' AS UnitTest
	,pcx_personalarticle_jm.PublicID
	,pc_policyperiod.PolicyNumber
	,pc_job.JobNumber
	, DATE('{date}') AS bq_load_date 	

FROM  (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_personalarticle_jm 

	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
	ON pc_policyperiod.ID = pcx_personalarticle_jm.BranchID

	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job 
	ON pc_job.ID = pc_policyperiod.JobID

	LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelry` WHERE bq_load_date = DATE({partition_date})) ArticleJewelryRisk
	ON ArticleJewelryRisk.PAJewelryPublicID = pcx_personalarticle_jm.PublicID

WHERE ArticleJewelryRisk.PAJewelryPublicID IS NULL
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd))
--AND pc_policyperiod.EditEffectiveDate < jpa.ExpirationDate


