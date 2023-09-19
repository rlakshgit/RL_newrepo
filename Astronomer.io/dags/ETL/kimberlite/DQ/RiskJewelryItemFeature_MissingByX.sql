/**** RiskJewelryItemFeature_MissingByX.sql ********
 
 *****  Change History  *****

	06/01/2021	SLJ		Init
	01/17/2022	DROBAK	Added date logic, Left Join to RiskJewelryItemFeature (modified Kimberlite layer to use left joins in some table like feature type)
	02/01/2022	DROBAK	Typo correction (had qa-edl hardcoded)

****************************************************/
--Missing Values[Item]
	SELECT
		'Missing Item Feature'					AS UnitTest
		, pcx_JwlryFeature_JMIC_PL.PublicID		AS ItemFeaturePublicID
		, pc_job.JobNumber
		, pcx_JwlryFeature_JMIC_PL.FeatureType	AS FeatureType
		, 'Detail'								AS RiskLevel
		, DATE('{date}')						AS bq_load_date

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job 
			ON pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeature_JMIC_PL` WHERE _PARTITIONTIME = {partition_date})  pcx_JwlryFeature_JMIC_PL
			ON pc_policyperiod.ID = pcx_JwlryFeature_JMIC_PL.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.SubType
			AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'
		LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskJewelryItemFeature` WHERE bq_load_date = DATE({partition_date})) RiskJewelryItemFeature
			ON RiskJewelryItemFeature.ItemFeaturePublicID = pcx_JwlryFeature_JMIC_PL.PublicID
	WHERE RiskJewelryItemFeature.ItemFeaturePublicID IS NULL
		AND(	pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd))
	--AND JobNUmber = '6184921'


/* Original Code
SELECT 'MISSING' AS UnitTest
       , ItemFeaturePublicID
	   , ItemFeatureDetailPublicID
	   , JobNumber
	   , 'Detail' AS RiskLevel
	   , DATE('{date}') AS bq_load_date 

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) jfd
INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeature_JMIC_PL` WHERE _PARTITIONTIME = {partition_date})  pcx_JwlryFeature_JMIC_PL
		ON pcx_JwlryFeature_JMIC_PL.PersonalItem = jfd.FixedID
		AND pcx_JwlryFeature_JMIC_PL.BranchID = jfd.BranchID	
INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeatDetail_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) pcx_JwlryFeatDetail_JMIC_PL
		ON pcx_JwlryFeatDetail_JMIC_PL.Feature = pcx_JwlryFeature_JMIC_PL.FixedID
		AND pcx_JwlryFeatDetail_JMIC_PL.BranchID = pcx_JwlryFeature_JMIC_PL.BranchID		
INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod ON pc_policyperiod.id = jfd.BranchID
INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job ON pc_job.id = pc_policyperiod.JobID
WHERE pcx_JwlryFeature_JMIC_PL.PublicID NOT IN (SELECT ItemFeaturePublicID FROM `{project}.{dest_dataset}.RiskJewelryItemFeature` WHERE bq_load_date = DATE({partition_date}))
--AND pc_job.jobNumber=@jobNumber
AND pc_policyperiod.EditEffectiveDate < jfd.ExpirationDate
*/

