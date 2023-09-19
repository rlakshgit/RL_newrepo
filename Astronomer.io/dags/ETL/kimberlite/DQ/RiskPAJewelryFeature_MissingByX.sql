/**** RiskPAJewelryFeature_MissingByX.sql ********
 
 *****  Change History  *****

	06/28/2021	SLJ		Init

******************************/
--Missing Article Feature records
	SELECT 
		'Missing Article Feature' AS UnitTest
		,pcx_JwlryFeature_JMIC_PL.PublicID
		,pc_policyperiod.PolicyNumber
		,pc_job.JobNumber
		,pcx_JwlryFeature_JMIC_PL.FeatureType
		,DATE('{date}') AS bq_load_date 

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job 
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeature_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) AS pcx_JwlryFeature_JMIC_PL
			ON pc_policyperiod.ID = pcx_JwlryFeature_JMIC_PL.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.SubType
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
		LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelryFeature` WHERE bq_load_date = DATE({partition_date})) RiskPAJewelryFeature	--#PAJwlryFeatureData
			ON RiskPAJewelryFeature.PAJewelryFeaturePublicID = pcx_JwlryFeature_JMIC_PL.PublicID

	WHERE RiskPAJewelryFeature.PAJewelryFeaturePublicID IS NULL
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd))