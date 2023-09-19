/********************************************************
************  Coverage Jewelry Item   *******************
				Converted to Big Query
*********************************************************
---------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****-

	07/22/2020	DROBAK		Init create
	03/04/2021	DROBAK		Cost Join fix applied (now a left join)
	03/15/2021	DROBAK		Added CoverageNumber, ItemValue, CostPublicID to be similar to PA Covg
							And renamed Limit and Deductible to ItemLimit, ItemDeductible for BQ
	05/13/2021	DROBAK		Added CASE for CoverageTypeCode to display as “MPA” for Min_Premium_Adj vs. "UNS" (JobNumber 1755327)
	06/08/2021	DROBAK		Added PolicyTransactionKey, PolicyNumber, ItemAnnualPremium; Updated DQ Checks
	07/01/2021	DROBAK		Added IsTransactionSliceEffective, CASE Logic for Hash Key fields; updated unit checks
	10/14/2021	DROBAK		Changed field name from JewelryItemPublicID to ItemPublicID (consistent with other tables)
	11/08/2021	DROBAK		Make CoverageLevel Values = Financial PJ Direct; align Key for Risk Key Values = RiskJewelryItem SQL
	11/09/2021	DROBAK		(BQ Only) Make same as PJDirect: replace 'None' with CAST(NULL AS STRING) AS ItemPublicId
	12/22/2021	SLJ			Unit tests modified, fixed id added to final select

-----------------------------------------------------------------------------------------------------------------------------------
*/
/*
CREATE OR REPLACE TABLE `{project}.B_QA_ref_kimberlite_config.JewelryItemCoverageConfig` 
 (
		 Key STRING,
		 Value STRING
	);
  
INSERT INTO `{project}.B_QA_ref_kimberlite_config.JewelryItemCoverageConfig` 
  VALUES	
	('SourceSystem','GW')
	,('HashKeySeparator','_')
	,('HashAlgorithm','SHA2_256')
	,('LineCode','PersonalJewelryLine_JMIC_PL')
	--- CoverageLevel Values
	,('ScheduledCoverage','ScheduledCov')
	,('UnScheduledCoverage','UnScheduledCov')
	--- Risk Key Values
	,('PJILevelRisk','PersonalJewelryItem');
*/

--INSERT INTO `{project}.{dest_dataset}.ItemJewelryCoverages

WITH JewelryItemCoverageConfig AS
(
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem'
)

SELECT 
	CASE WHEN CoveragePublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel))
	END AS ItemCoverageKey
	--SK For FK <Source>_<PolicyPeriodPublicID>
	,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
	END AS PolicyTransactionKey
	,CASE WHEN ItemPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ItemPublicID,hashKeySeparator.Value,ConfigRisk.Value)) 
	END AS RiskJewelryItemKey
	,sourceConfig.Value AS SourceSystem
	,ItemCoverages.PolicyPeriodPublicID
	,ItemCoverages.JobNumber
	,ItemCoverages.PolicyNumber
	,ItemCoverages.PolicyLinePublicID
	,ItemCoverages.ItemPublicID
	,ItemCoverages.CoveragePublicID
	,ItemCoverages.CoverageTypeCode
	,ItemCoverages.CoverageLevel
	,ItemCoverages.CoverageNumber
	,ItemCoverages.EffectiveDate
	,ItemCoverages.ExpirationDate
	,ItemCoverages.IsTempCoverage
	,ItemCoverages.ItemLimit
	,ItemCoverages.ItemDeductible
	,ItemCoverages.ItemValue
	,ItemCoverages.ItemAnnualPremium
	,ItemCoverages.CoverageCode
	,ItemCoverages.CostPublicID
	,ItemCoverages.IsTransactionSliceEffective
	,DENSE_RANK() OVER(PARTITION BY CoverageFixedID, PolicyPeriodPublicID, CoverageLevel
						ORDER BY	ItemCoverages.IsTransactionSliceEffective DESC
									,ItemCoverages.FixedCoverageInBranchRank
						) AS FixedCoverageInBranchRank 
	,ItemCoverages.CoverageFixedID
    ,DATE('{date}') as bq_load_date	

FROM (

	/***********************************************************
				Personal Jewelry Line (UnScheduled)
	************************************************************/
	SELECT 
		pcx_jmpersonallinecov.PublicID											AS CoveragePublicID 
		,CAST(NULL AS STRING)													AS ItemPublicID
		,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
		,pcx_cost_jmic.PublicID													AS CostPublicID
		,pc_policyline.PublicID													AS PolicyLinePublicID
		,pcx_jmpersonallinecov.FixedID											AS CoverageFixedID
		,pc_policyperiod.PolicyNumber											AS PolicyNumber
		,pc_job.JobNumber														AS JobNumber
		,COALESCE(pc_policyline.PersonalJewelryAutoNumberSeq,0)					AS CoverageNumber
		,ConfigCoverageLineLevel.Value										AS CoverageLevel
		,pcx_jmpersonallinecov.PatternCode										AS CoverageCode
		,CASE	WHEN pcx_cost_jmic.ChargeSubGroup = 'MIN_PREMIUM_ADJ' THEN 'MPA' 
				WHEN pcx_cost_jmic.ChargeGroup = 'UnScheduledPremium' AND pcx_cost_jmic.ChargeSubGroup IS NULL THEN 'UNS'
				WHEN pcx_jmpersonallinecov.FinalPersistedLimit_JMIC IS NULL THEN 'MPA' 
				ELSE 'UNS' 
		 END																	AS CoverageTypeCode--check
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 
		 END																	AS IsTransactionSliceEffective
		,CASE	WHEN pcx_jmpersonallinecov.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempFromDt_JMIC 
				ELSE COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.PeriodStart) 
		 END																	AS EffectiveDate
		,CASE	WHEN pcx_jmpersonallinecov.FinalPersistedTempToDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempToDt_JMIC 
				ELSE COALESCE(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd) 
		 END																	AS ExpirationDate
		,CASE	WHEN pcx_jmpersonallinecov.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN 1 ELSE 0 
		 END																	AS IsTempCoverage
		,pcx_jmpersonallinecov.FinalPersistedLimit_JMIC							AS ItemLimit
		,pcx_jmpersonallinecov.FinalPersistedDeductible_JMIC					AS ItemDeductible
		,NULL																	AS ItemValue
		,CASE	WHEN pcx_cost_jmic.ChargeGroup = 'UnscheduledPremium' 
				THEN pcx_cost_jmic.ActualTermAmount 
				ELSE 0
		 END																	AS	ItemAnnualPremium	/* or ActualTermAmount or BaseRateActualTermAmount */			
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.ID
							ORDER BY	IFNULL(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							)													AS CoverageRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							)													AS FixedCoverageInBranchRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.ID
							ORDER BY	IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pcx_cost_jmic.ID DESC
							)													AS CostRank

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod

		-- Personal Line Coverage (unscheduled)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jmpersonallinecov` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmpersonallinecov
		ON pcx_jmpersonallinecov.BranchID = pc_policyperiod.ID	  

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID	
				
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType

		INNER JOIN JewelryItemCoverageConfig AS ConfigLineCode
		ON ConfigLineCode.Key = 'LineCode' 	
		AND ConfigLineCode.Value = pctl_policyline.TYPECODE	

		INNER JOIN JewelryItemCoverageConfig AS ConfigCoverageLineLevel
		ON ConfigCoverageLineLevel.Key = 'UnScheduledCoverage'
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
		ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		AND pcx_cost_jmic.JewelryLineCov_JMIC_PL = pcx_jmpersonallinecov.FixedID
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

	UNION ALL

	/***********************************************************
				Personal Jewelry Line (Scheduled)
	************************************************************/
	SELECT  
		pcx_jwryitemcov_jmic_pl.PublicID										AS CoveragePublicID
		,pcx_jewelryitem_jmic_pl.PublicID										AS ItemPublicID
		,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
		,pcx_cost_jmic.PublicID													AS CostPublicID
		,pc_policyline.PublicID													AS PolicyLinePublicID
		,pcx_jwryitemcov_jmic_pl.FixedID										AS CoverageFixedID
		,pc_policyperiod.PolicyNumber											AS PolicyNumber
		,pc_job.JobNumber														AS JobNumber
		,pcx_jewelryitem_jmic_pl.ItemNumber										AS CoverageNumber	
		,ConfigCoverageLineLevel.Value										AS CoverageLevel	
		,pcx_jwryitemcov_jmic_pl.PatternCode									AS CoverageCode		
		,'SCH'																	AS CoverageTypeCode -- JewelryItemCov_JMIC_PL --> SCH	
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) THEN 1 ELSE 0 
		 END																	AS IsTransactionSliceEffective
		,CASE	WHEN pcx_jwryitemcov_jmic_pl.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempFromDt_JMIC 
				ELSE COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart) 
		 END																	AS EffectiveDate
		,CASE	WHEN pcx_jwryitemcov_jmic_pl.FinalPersistedTempToDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempToDt_JMIC 
				ELSE COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) 
		 END																	AS ExpirationDate
		,CASE	WHEN pcx_jwryitemcov_jmic_pl.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN 1 ELSE 0 
		 END																	AS IsTempCoverage
		,pcx_jwryitemcov_jmic_pl.FinalPersistedLimit_JMIC						AS ItemLimit
		,pcx_jwryitemcov_jmic_pl.FinalPersistedDeductible_JMIC					AS ItemDeductible
		,pcx_jwryitemcov_jmic_pl.DirectTerm1									AS ItemValue
		,CASE	WHEN pcx_cost_jmic.ChargeGroup = 'PREMIUM' 
				THEN pcx_cost_jmic.ActualTermAmount 
				ELSE 0
		 END																	AS	ItemAnnualPremium	/* or ActualTermAmount or BaseRateActualTermAmount */
		,DENSE_RANK() OVER(PARTITION BY pcx_jwryitemcov_jmic_pl.ID
							ORDER BY	IFNULL(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							)													AS CoverageRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jwryitemcov_jmic_pl.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							)													AS FixedCoverageInBranchRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jwryitemcov_jmic_pl.ID
							ORDER BY	IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pcx_cost_jmic.ID DESC
							)													AS CostRank
		
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	 
		-- Jewelry Item Coverage
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jwryitemcov_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jwryitemcov_jmic_pl
		ON pcx_jwryitemcov_jmic_pl.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType

		INNER JOIN JewelryItemCoverageConfig AS ConfigLineCode
		ON ConfigLineCode.Key = 'LineCode' 
		AND ConfigLineCode.Value = pctl_policyline.TYPECODE

		INNER JOIN JewelryItemCoverageConfig AS ConfigCoverageLineLevel
		ON ConfigCoverageLineLevel.Key = 'ScheduledCoverage'

		-- Jewelry Item -- join to fixedid of the item coverage, not the slice (i.e., policyline)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelryitem_jmic_pl
		ON pcx_jewelryitem_jmic_pl.FixedID = pcx_jwryitemcov_jmic_pl.JewelryItem_JMIC_PL
		AND pcx_jewelryitem_jmic_pl.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
		ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		AND pcx_cost_jmic.JewelryItemCov_JMIC_PL = pcx_jwryitemcov_jmic_pl.FixedID
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
				
) ItemCoverages
	
	INNER JOIN JewelryItemCoverageConfig sourceConfig
	ON sourceConfig.Key = 'SourceSystem'

	INNER JOIN JewelryItemCoverageConfig hashKeySeparator
	ON hashKeySeparator.Key = 'HashKeySeparator'

	INNER JOIN JewelryItemCoverageConfig HashAlgorithm
	ON HashAlgorithm.Key = 'HashAlgorithm'

	INNER JOIN JewelryItemCoverageConfig ConfigRisk
	ON ConfigRisk.Key = 'PJILevelRisk'

WHERE	1 = 1
		AND CoveragePublicID IS NOT NULL
		AND CoverageRank = 1
		AND CostRank = 1

