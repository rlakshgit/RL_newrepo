/*************  RiskPAJewelry   *********************
*****************************************************
	--Converted to GCP-BQ
-------------------------------------------------------------------------------------------
 *****  Change History  *****

	07/2020		SLJ			Init create
	11/2020		DROBAK		updated for better compatibility for BQ conversion
	02/26/2021	DROBAK		Cost Join fix applied (now a left join)
	04/05/2021	DROBAK		Changed 'ArticleRisk' Value from 'JPAJ' to 'PersonalArticleJewelry'
	06/07/2021	DROBAK		Changed PAJewelryPublicID to PAJewelryPublicID
	06/07/2021	DROBAK		GW replaced acquisitionyear with YearOfManufacture
	06/08/2021	DROBAK		Fix / Add join ConfigLineCode to pctl table for subtype
	06/09/2021  SLJ			FixedRank fix applied; IsTransactionSliceEffective
	06/09/2021  SLJ			Field Cleanup
	06/09/2021  SLJ			Add Columns
	08/27/2021	DROBAK		Added CASE Logic to Keys

--------------------------------------------------------------------------------------------
********************************************************************************************/
--DECLARE vjobNumber STRING; DECLARE {partition_date} Timestamp;
--SET vjobNumber = '7149695';

/* Set universal latest partitiontime -	In future will come from Airflow DAG (Today - 1) 
--SET {partition_date} = '2020-11-30'; 
SET {partition_date} = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pcx_personalarticle_jm`);

CREATE OR REPLACE TABLE `{project}.B_QA_ref_kimberlite_config.RiskPAConfig`
	(
		key STRING,
		value STRING
	);
	INSERT INTO 
			`{project}.B_QA_ref_kimberlite_config.RiskPAConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm','SHA2_256')
			,('LineCode','PersonalArtclLine_JM') 
			,('LineLevel','JewelryItem_JM')	 
			,('ArticleRisk','PersonalArticleJewelry');
			
*/

WITH RiskJPAConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'PAType','JewelryItem_JM' UNION ALL	 
  SELECT 'PJALevelRisk','PersonalArticleJewelry'	
)

SELECT 
	sourceConfig.Value AS SourceSystem
	,CASE WHEN PAJewelryPublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PAJewelryPublicID,hashKeySeparator.Value,ArticleRisk.RiskLevel))
	END AS RiskPAJewelryKey
	,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID))
	END AS PolicyTransactionKey
	,DENSE_RANK() OVER(PARTITION BY	ArticleRisk.JewelryArticleFixedID
									,ArticleRisk.PolicyPeriodPublicID
									,ArticleRisk.RiskLevel
					   ORDER BY		ArticleRisk.IsTransactionSliceEffective DESC, ArticleRisk.FixedArticleRank ASC) AS FixedArticleRank

	,ArticleRisk.PolicyPeriodPublicID						AS PolicyPeriodPublicID
	,ArticleRisk.PolicyLinePublicID							AS PolicyLinePublicID
	,ArticleRisk.PAJewelryPublicID							AS PAJewelryPublicID
	,ArticleRisk.JewelryArticleFixedID						AS JewelryArticleFixedID
	,ArticleRisk.JobNumber									AS JobNumber
	,ArticleRisk.RiskLevel									AS RiskLevel
	,ArticleRisk.JewelryArticleNumber						AS JewelryArticleNumber
	,ArticleRisk.EffectiveDate								AS EffectiveDate
	,ArticleRisk.ExpirationDate								AS ExpirationDate
	,ArticleRisk.IsTransactionSliceEffective				AS IsTransactionSliceEffective
	,ArticleRisk.ArticleType								AS ArticleType
	,ArticleRisk.ArticleSubType								AS ArticleSubType
	,ArticleRisk.ArticleGender								AS ArticleGender
	,CAST(ArticleRisk.IsWearableTech AS INT64)				AS IsWearableTech
	,ArticleRisk.ArticleBrand								AS ArticleBrand
	,ArticleRisk.ArticleStyle								AS ArticleStyle
	,ArticleRisk.InitialValue								AS InitialValue
	,CAST(ArticleRisk.IsFullDescOverridden AS INT64)		AS IsFullDescOverridden
	,ArticleRisk.FullDescription							AS FullDescription
	,CAST(ArticleRisk.IsAppraisalRequested AS INT64)		AS IsAppraisalRequested
	,CAST(ArticleRisk.IsAppraisalReceived AS INT64)			AS IsAppraisalReceived
	,ArticleRisk.AppraisalDate 								AS AppraisalDate 
	,ArticleRisk.InspectionDate								AS InspectionDate
	,CAST(ArticleRisk.IsIVADeclined AS INT64)				AS IsIVADeclined
	,ArticleRisk.IVADate									AS IVADate
	,CAST(ArticleRisk.IsIVAApplied AS INT64)				AS IsIVAApplied
	,ArticleRisk.IVAPercentage								AS IVAPercentage
	,ArticleRisk.ValuationType								AS ValuationType
	,CAST(ArticleRisk.IsDamaged AS INT64)					AS IsDamaged
	,ArticleRisk.DamageType									AS DamageType
	,CAST(ArticleRisk.IsInactive AS INT64)					AS IsInactive
	,ArticleRisk.InactiveReason								AS InactiveReason
	,ArticleRisk.ArticleStored								AS ArticleStored
	,ArticleRisk.SafeDetails								AS SafeDetails
	,ArticleRisk.TimeOutOfVault								AS TimeOutOfVault
	,CAST(ArticleRisk.HasCarePlan AS INT64)					AS HasCarePlan
	,ArticleRisk.CarePlanExpirationDate						AS CarePlanExpirationDate		
	,ArticleRisk.CarePlanID									AS CarePlanID					
	,ArticleRisk.ArticleHowAcquired 						AS ArticleHowAcquired 		
	,ArticleRisk.DurationWithOtherInsurer 					AS DurationWithOtherInsurer 	
	,ArticleRisk.ArticleYearAcquired						AS ArticleYearAcquired		
    ,DATE('{date}') as bq_load_date		

--INTO #ArticleJewelryRisk  -- COMMENT OUT

FROM 
(
	SELECT
	 -- Relevant Risk Segment natural SS keys
		pc_policyperiod.PublicID								AS PolicyPeriodPublicID
		,pc_policyline.PublicID									AS PolicyLinePublicID
		,pcx_personalarticle_jm.PublicID						AS PAJewelryPublicID
		,pcx_personalarticle_jm.FixedID							AS JewelryArticleFixedID
		,pc_job.JobNumber										AS JobNumber
		,ConfigRisk.Value										AS RiskLevel
		,COALESCE(pcx_personalarticle_jm.ItemNumber,0)			AS JewelryArticleNumber
		,pcx_personalarticle_jm.EffectiveDate					AS EffectiveDate
		,pcx_personalarticle_jm.ExpirationDate					AS ExpirationDate
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 END								AS IsTransactionSliceEffective

		--JewelryArticle Specific Fields
		,pctl_jpaitemtype_jm.Name								AS ArticleType
		,pctl_jpaitemsubtype_jm.Name							AS ArticleSubType
		,pctl_jpaitemgendertype_jm.Name							AS ArticleGender
		,pcx_personalarticle_jm.IsWearableTech					AS IsWearableTech
		,pctl_jpaitembrand_jm.Name								AS ArticleBrand
		,pctl_jpaitemstyle_jm.Name								AS ArticleStyle
		,pcx_personalarticle_jm.InitialLimit					AS InitialValue
		,pcx_personalarticle_jm.IsFullDescOverridden			AS IsFullDescOverridden
		,pcx_personalarticle_jm.FullDescription					AS FullDescription
		,pcx_personalarticle_jm.IsAppraisalRequested			AS IsAppraisalRequested
		,pcx_personalarticle_jm.IsAppraisalReceived				AS IsAppraisalReceived
		,pcx_personalarticle_jm.AppraisalDate					AS AppraisalDate 
		,pcx_personalarticle_jm.InspectionDate					AS InspectionDate
		,pcx_personalarticle_jm.IsIVADeclined					AS IsIVADeclined
		,pcx_personalarticle_jm.IVADate							AS IVADate
		,pcx_personalarticle_jm.IsIVAApplied					AS IsIVAApplied
		,pcx_personalarticle_jm.IVAPercentage					AS IVAPercentage
		--Retailer or Appraiser
		,pctl_jpavaluationtype_jm.Name							AS ValuationType
		--affinity group
		,pcx_personalarticle_jm.IsDamaged						AS IsDamaged
		,pctl_jpadamagetype_jm.Name								AS DamageType
		,pcx_personalarticle_jm.IsItemInactive					AS IsInactive
		,pcx_personalarticle_jm.InactiveReason					AS InactiveReason
		,pctl_jpastoragetype_jm.Name							AS ArticleStored
		,pcx_personalarticle_jm.SafeDetails						AS SafeDetails -- populated when articlestored = safe
		--,pcx_personalarticle_jm.VaultContact
		--,pc_contact_ar.Name								AS Appraiser 
		--,pc_contact_bv.Name AS Bankvault
		,pctl_jpaarticlesafestatus_jm.Name						AS TimeOutOfVault
		,pcx_personalarticle_jm.HasCarePlan						AS HasCarePlan
		,pcx_personalarticle_jm.CarePlanDate					AS CarePlanExpirationDate
		,pcx_personalarticle_jm.CarePlanID						AS CarePlanID	
		,pctl_jpaartacquisition_jm.Name							AS ArticleHowAcquired 
		,pctl_jpainsuredbyothercomp_jm.Name						AS DurationWithOtherInsurer 
		--how old article
				
		, pcx_personalarticle_jm.YearOfManufacture				AS ArticleYearAcquired

		
		--, 'Located With' AS JewelryArticleLocatedWithContactType 
		--, LEFT(pc_affinitygroup.Name,256) AS JewelryArticleAffinityGroup 
		--, pctl_affinitygrouplevel_jm.Name AS JewelryArticleAffinityGroupLevel 

		--, pctl_Personalarticle_jm.TYPECODE AS PersonalArticleSubType
		
			
	-- foreign natural keys  ????	--Not all tables in BQ so hide
	/*
		, pctl_distrchannel_jm.DESCRIPTION AS DistributionChannel
		, pctl_distrsubchannel_jm.DESCRIPTION AS DistributionSubChannel
		, pctl_distrmethod_jm.DESCRIPTION AS Method
		, pctl_distrSubMethod_jm.DESCRIPTION AS SubMethod
	*/	
						
		,DENSE_RANK() OVER(PARTITION BY pcx_personalarticle_jm.ID
			ORDER BY IFNULL(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
		) AS ArticleRank
		,DENSE_RANK()  OVER(PARTITION BY pc_policyperiod.ID, pcx_personalarticle_jm.FixedID 
			ORDER BY IFNULL(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
		) AS FixedArticleRank										
		,DENSE_RANK() OVER(PARTITION BY pcx_personalarticle_jm.ID
			ORDER BY IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				,pcx_jpacost_jm.ID DESC
		) AS CostRank
			
		,CURRENT_DATE() AS CreateDate
				
	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod

		INNER JOIN `{project}.{pc_dataset}.pcx_personalarticle_jm` AS pcx_personalarticle_jm
		ON pcx_personalarticle_jm.BranchID = pc_policyperiod.ID

		INNER JOIN `{project}.{pc_dataset}.pc_job`  AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID
			
		INNER JOIN `{project}.{pc_dataset}.pc_policyline` AS pc_policyline
		ON pc_policyperiod.ID = pc_policyline.BranchID
		AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		--AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
	
		INNER JOIN RiskJPAConfig AS ConfigPJALine
		ON ConfigPJALine.Value = pctl_policyline.TYPECODE
		AND ConfigPJALine.Key = 'PJALineCode' 
	
		INNER JOIN RiskJPAConfig AS ConfigRisk
		ON ConfigRisk.Key = 'PJALevelRisk'

		INNER JOIN `{project}.{pc_dataset}.pctl_personalarticle_jm` AS pctl_personalarticle_jm
		ON pcx_personalarticle_jm.Subtype = pctl_personalarticle_jm.ID	
		--AND pctl_Personalarticle_jm.TYPECODE = 'JewelryItem_JM'

		INNER JOIN RiskJPAConfig AS ConfigLineLevel
		ON ConfigLineLevel.Value = pctl_Personalarticle_jm.TYPECODE
		AND ConfigLineLevel.Key = 'PAType'
			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
		ON pcx_jpacost_jm.BranchID = pc_policyperiod.ID
		AND pcx_jpacost_jm.PersonalArticle = pcx_personalarticle_jm.FixedID
		AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaitemtype_jm`  AS pctl_jpaitemtype_jm
		ON pcx_personalarticle_jm.ItemType = pctl_jpaitemtype_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaitemsubtype_jm` AS pctl_jpaitemsubtype_jm
		ON pcx_personalarticle_jm.ItemSubType = pctl_jpaitemsubtype_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaitemgendertype_jm` AS  pctl_jpaitemgendertype_jm
		ON pcx_personalarticle_jm.ItemGenderType= pctl_jpaitemgendertype_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaarticlesafestatus_jm` AS pctl_jpaarticlesafestatus_jm
		ON pcx_personalarticle_jm.ArticleSafeStatus = pctl_jpaarticlesafestatus_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpastoragetype_jm` AS pctl_jpastoragetype_jm
		ON pcx_personalarticle_jm.StorageType = pctl_jpastoragetype_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaitembrand_jm` AS pctl_jpaitembrand_jm
		ON pcx_personalArticle_jm.Brand = pctl_jpaitembrand_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaitemstyle_jm` AS  pctl_jpaitemstyle_jm
		ON pcx_personalarticle_jm.Style = pctl_jpaitemstyle_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpavaluationtype_jm` AS pctl_jpavaluationtype_jm
		ON pcx_personalarticle_jm.ValuationType = pctl_jpavaluationtype_jm.ID

		--No data in Affinity Groups as of Jan 2021
		/*		LEFT OUTER JOIN `{project}.{pc_dataset}.pc_affinitygroup` AS pc_affinitygroup
		ON pcx_personalarticle_jm.AffinityGroupCode_JM = pc_affinitygroup.Code
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_affinitygrouplevel_jm` AS pctl_affinitygrouplevel_jm
		ON pcx_personalarticle_jm.AffinityGroupLevel_JM = pctl_affinitygrouplevel_jm.ID
		*/
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpadamagetype_jm` AS pctl_jpadamagetype_jm
		ON pcx_personalarticle_jm.DamageType = pctl_jpadamagetype_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpaartacquisition_jm` AS pctl_jpaartacquisition_jm
		ON pcx_personalarticle_jm.ArticlesAcquisition = pctl_jpaartacquisition_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jpainsuredbyothercomp_jm` AS pctl_jpainsuredbyothercomp_jm
		ON pcx_personalarticle_jm.DurationInsuredByOtherCompany = pctl_jpainsuredbyothercomp_jm.ID
		

		/***  not sure if we want these fields in this table  ****/
		--Distribution channels and methods...
		/*		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_distrchannel_jm` AS pctl_distrchannel_jm
		ON pcx_personalarticle_jm.Channel = pctl_distrchannel_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_distrsubchannel_jm` AS pctl_distrsubchannel_jm
		ON pcx_personalarticle_jm.SubChannel = pctl_distrsubchannel_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_distrmethod_jm` AS pctl_distrmethod_jm
		ON pcx_personalarticle_jm.Method = pctl_distrmethod_jm.ID

		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_distrSubMethod_jm` AS pctl_distrSubMethod_jm
		ON pcx_personalarticle_jm.SubMethod = pctl_distrSubMethod_jm.ID
		*/

		WHERE	1 = 1
				AND pc_policyperiod._PARTITIONTIME = {partition_date}
				AND pc_job._PARTITIONTIME = {partition_date}
				AND pcx_personalarticle_jm._PARTITIONTIME = {partition_date}
				AND pc_policyline._PARTITIONTIME = {partition_date}

) AS ArticleRisk
					
		INNER JOIN RiskJPAConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN RiskJPAConfig HashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN RiskJPAConfig HashAlgorithm
			ON HashAlgorithm.Key='HashAlgorithm'

WHERE 1=1
	AND ArticleRisk.ArticleRank = 1
	AND ArticleRisk.CostRank = 1