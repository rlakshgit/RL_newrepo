-- tag: CoverageIM - tag ends/
/**************************************************************************************************************************************************************************
CoverageIM.sql
	-- This query will bring in all Instances of all types of coverages provided under the ILM product. 
		--BQ Converted

	IMPORTANT UPDATES
		DW STAGE tables, GW Product Model XML, and Ratabase XML related feeds were commented until ready in Data Lake as data source for Kimberlite
		BigQuery does not contain needed table	gw_pm.CoveragePatterns_CoveragePattern

**************************************************************************************************************************************************************************/
/*
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/26/2021	DROBAK		Cost Join fix applied (now a left join)
	04/05/2021	DROBAK		Changed ILMCoverageKey to IMCoverageKey
	04/05/2021	DROBAK		Changed ILMLimit and Dedeuctible to IMLimit and IMDeductible
	05/20/2021	DROBAK		Fix CoverageLevel = Line and SubLine (Use PrimaryLocation in place of pcx_ilmlocation_jmic)
	06/02/2021	DROBAK		Added IsTransactionSliceEffective field for use in BB layer to select Policy Slice Eff records
	08/25/2021	DROBAK		Added 2 new fields: CoverageNumber (based on DW logic) and IMLocationPublicID; fixed unit tests where clause
	12/28/2021	SLJ			ILMStockPublicID renamed to IMStockPublicID
	12/28/2021	SLJ			Added CostPublicID, CoverageFixedID
	01/07/2021	SLJ			Added temp effective date to coalesce in joins to incorporate dates for temp coverages
	08/05/2022	DROBAK		For IMLocationPublicID: changed from pc_policylocation table to pcx_ilmlocation_jmic for Line and SubLine (added logic to get min rating location number)
							Added PolicyVersionLOB_PrimaryRatingLocation temp table
	12/21/2022	DROBAK		Add PolicyPeriodStatus
	04/13/2023	DROBAK		Though not used herein, for consistency, added IsPrimaryLocation
	04/27/2023	SLJ			Remove RiskLocationLevel from inner to outer query join statement as every level need to be location
							Change cte for primary location to t_PrimaryRatingLocationIM. Rating Location logic modified.
							IsTransactionSliceEffective modified for Location, SubLoc, Stock, SubStock
							date logic modified for joins to pcx_ilmlocation_jmic and pcx_jewelrystock_jmic for Location, SubLoc, Stock, SubStock
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/

CASE -- 04/27/2023
  WHEN
    NOT EXISTS
      ( 
		SELECT *
        FROM `{project}.{dest_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_schema = '{dest_dataset}'
        AND table_name = 't_PrimaryRatingLocationIM'
        AND CAST(last_modified_time AS DATE) = CURRENT_DATE()
      ) 
  THEN 
	CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_PrimaryRatingLocationIM`
	AS SELECT *
	FROM (
		SELECT 
			pc_policyperiod.ID	AS PolicyPeriodID
			,pc_policyperiod.EditEffectiveDate AS EditEffectiveDate
			,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
			,COALESCE(MIN(CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END) -- Primary matches LOB location
								,MIN(CASE WHEN pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID THEN pc_policylocation.LocationNum ELSE NULL END) -- Locations available for IM product
								,MIN(pc_policylocation.LocationNum)) AS RatingLocationNum
										
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			--Blow out to include all policy locations for policy version / date segment
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON pc_policyperiod.ID = pc_policylocation.BranchID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID			
				AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'
			--Inland Marine Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
				AND pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
				AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date} ) AS pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) AS PrimaryPolicyLocation
				ON PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation 
				AND PrimaryPolicyLocation.BranchID = pc_effectivedatedfields.BranchID 
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		GROUP BY
			pc_policyperiod.ID
			,pc_policyperiod.EditEffectiveDate
			,pctl_policyline.TYPECODE

		) AS PrimaryRatingLocations;
END CASE;

--DELETE `{project}.{dest_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date});
/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.DAR_CoverageIM`
AS SELECT outerquery.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.CoverageIM`
( 
	SourceSystem
	,IMCoverageKey
	,PolicyTransactionKey
	,RiskLocationKey
	,RiskStockKey
	,PolicyPeriodPublicID
	,JobNumber
	,PolicyNumber
	,PolicyPeriodStatus
	,CoveragePublicID
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,IsTempCoverage
	,IMLimit
	,IMDeductible
	,CoverageNumber
	,CoverageCode
	,IMStockPublicID
	,PolicyLocationPublicID
	,IMLocationPublicID
	,SpecifiedCarrier
	,SpecifiedCarrierExposure
	,IsTransactionSliceEffective
	,FixedCoverageInBranchRank 
	,CostPublicID
	,CoverageFixedID
    ,bq_load_date		

)

WITH ILMCoverageConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'StockLevelCoverage','Stock' UNION ALL
  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
  SELECT 'LOBLevelRisk','Line' UNION ALL
  SELECT 'LocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'StockLevelRisk','ILMStock' UNION ALL
  SELECT 'SpecifiedCarriers','ILMSpecifiedCarriers_JMIC'
)

SELECT 
	SourceSystem
	,IMCoverageKey
	,PolicyTransactionKey
	,RiskLocationKey
	,RiskStockKey
	,PolicyPeriodPublicID
	,JobNumber
	,PolicyNumber
	,PolicyPeriodStatus
	,CoveragePublicID
	,CoverageLevel
	--,CoverageReferenceCode
	,EffectiveDate
	,ExpirationDate
	,IsTempCoverage
	,IMLimit
	,IMDeductible
	,CoverageNumber
	,CoverageCode
	,IMStockPublicID
	,PolicyLocationPublicID
	,IMLocationPublicID
	,SpecifiedCarrier
	,SpecifiedCarrierExposure
	,IsTransactionSliceEffective
	,DENSE_RANK() OVER(PARTITION BY CoverageFixedID, PolicyPeriodPublicID, CoverageLevel
					ORDER BY IsTransactionSliceEffective DESC
							,FixedCoverageInBranchRank
				) AS FixedCoverageInBranchRank 
	,CostPublicID
	,CoverageFixedID
    ,DATE('{date}') as bq_load_date		
	
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		--SK For PK <Source>_<CoveragePublicID>_<Level>
		,CASE WHEN CoveragePublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) END AS IMCoverageKey
		--SK For FK <Source>_<PolicyPeriodPublicID>
		,CASE WHEN PolicyPeriodPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) END AS PolicyTransactionKey
		--SK For FK <Source>_<IMLocationPublicID>_<Level>
		,CASE WHEN IMLocationPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) END AS RiskLocationKey -- 04/27/2023
		--SK For FK <Source>_<ILMStockPublicID>_<Level>
		,CASE WHEN IMStockPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMStockPublicID,hashKeySeparator.Value,stockRisk.Value)) END AS RiskStockKey
		,ilmCoverages.*		
	FROM (
		SELECT 
			pcx_ilmlinecov_jmic.PublicID											AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,CAST(NULL AS STRING)													AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_ilmlinecov_jmic.FixedID											AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,pc_policyline.BOPBlanketAutoNumberSeq_JMIC								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_ilmlinecov_jmic.PatternCode										AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	WHEN pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC 
					ELSE COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC 
					ELSE COALESCE(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_ilmlinecov_jmic.FinalPersistedLimit_JMIC							AS IMLimit
			,pcx_ilmlinecov_jmic.FinalPersistedDeductible_JMIC						AS IMDeductible
			,CAST(NULL AS STRING)													AS SpecifiedCarrier
			,NULL																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlinecov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlinecov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlinecov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod

			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlinecov_jmic
			ON  pcx_ilmlinecov_jmic.BranchID = pc_policyperiod.ID

			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE

			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'LineLevelCoverage'
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LOBLevelRisk'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.ILMLineCov = pcx_ilmlinecov_jmic.FixedID
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)

			--Join in the PolicyVersionLOB_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
			LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationIM` AS t_PrimaryRatingLocationIM
			ON t_PrimaryRatingLocationIM.PolicyPeriodID = pc_policyperiod.ID
			AND t_PrimaryRatingLocationIM.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((t_PrimaryRatingLocationIM.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				OR 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationIM.PolicyLineOfBusiness = 'BusinessOwnersLine')) 
	
			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = t_PrimaryRatingLocationIM.PolicyPeriodID
			AND pc_policylocation.LocationNum = t_PrimaryRatingLocationIM.RatingLocationNum 
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			--AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
			AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
			--AND COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			--AND COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_ilmlinecov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id

		UNION ALL

		SELECT 
			pcx_ilmsublinecov_jmic.PublicID											AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,CAST(NULL AS STRING)													AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_ilmsublinecov_jmic.FixedID											AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,pc_policyline.BOPBlanketAutoNumberSeq_JMIC								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_ilmsublinecov_jmic.PatternCode										AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	WHEN pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC 
					ELSE COALESCE(pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC 
					ELSE COALESCE(pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_ilmsublinecov_jmic.FinalPersistedLimit_JMIC						AS IMLimit
			,pcx_ilmsublinecov_jmic.FinalPersistedDeductible_JMIC					AS IMDeductible
			,CASE	WHEN pctl_specifiedcarrier_jmic.ID IS NOT NULL 
					THEN pctl_specifiedcarrier_jmic.NAME 
					ELSE NULL 
			 END																	AS SpecifiedCarrier
			,CASE	WHEN pctl_specifiedcarrier_jmic.ID IS NOT NULL 
					THEN pcx_ilmsublinecov_jmic.DirectTerm2 
					ELSE NULL 
			 END																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsublinecov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsublinecov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC 				
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsublinecov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublinecov_jmic
			ON  pcx_ilmsublinecov_jmic.BranchID = pc_policyperiod.ID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubline_jmic
			ON pcx_ilmsubline_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmsubline_jmic.FixedID = pcx_ilmsublinecov_jmic.ILMSubLine
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmsubline_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE	
			INNER JOIN ILMCoverageConfig AS specifiedCarrierConfig
				ON specifiedCarrierConfig.Key = 'SpecifiedCarriers' 
			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LOBLevelRisk'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.ILMSubLineCov = pcx_ilmsublinecov_jmic.FixedID
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)

			--Join in the PolicyVersionLOB_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
			LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationIM` AS t_PrimaryRatingLocationIM
			ON t_PrimaryRatingLocationIM.PolicyPeriodID = pc_policyperiod.ID
			AND t_PrimaryRatingLocationIM.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((t_PrimaryRatingLocationIM.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				OR 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationIM.PolicyLineOfBusiness = 'BusinessOwnersLine')) 
	
			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = t_PrimaryRatingLocationIM.PolicyPeriodID
			AND pc_policylocation.LocationNum = t_PrimaryRatingLocationIM.RatingLocationNum 
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			--AND COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
			AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
			--AND COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			--AND COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			/*LEFT OUTER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			*/	
	
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_specifiedcarrier_jmic`) AS pctl_specifiedcarrier_jmic
			ON pcx_ilmsublinecov_jmic.ChoiceTerm2 = pctl_specifiedcarrier_jmic.TYPECODE
			AND pcx_ilmsublinecov_jmic.PatternCode = specifiedCarrierConfig.Value
	
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_ilmsublinecov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id


		UNION ALL 
		--LOCATION--
		SELECT 
			pcx_ilmlocationcov_jmic.PublicID										AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,CAST(NULL AS STRING)													AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_ilmlocationcov_jmic.FixedID										AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,COALESCE(pc_policyline.BOPLocationSeq,0)								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_ilmlocationcov_jmic.PatternCode									AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID -- 04/27/2023 sj
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	 WHEN pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_ilmlocationcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_ilmlocationcov_jmic.FinalPersistedLimit_JMIC						AS IMLimit
			,pcx_ilmlocationcov_jmic.FinalPersistedDeductible_JMIC					AS IMDeductible
			,CAST(NULL AS STRING)													AS SpecifiedCarrier
			,NULL																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocationcov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocationcov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocationcov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod 
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocationcov_jmic
			ON  pcx_ilmlocationcov_jmic.BranchID = pc_policyperiod.ID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline 
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE
			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'LocationLevelCoverage'
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LocationLevelRisk'
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.FixedID = pcx_ilmlocationcov_jmic.ILMLocation
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
			--AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.ILMLocationCov = pcx_ilmlocationcov_jmic.FixedID
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_ilmlocationcov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id


		UNION ALL 

		SELECT 
			pcx_ilmsubloccov_jmic.PublicID											AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,CAST(NULL AS STRING)													AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_ilmsubloccov_jmic.FixedID											AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,COALESCE(pc_policyline.BOPLocationSeq,0)								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_ilmsubloccov_jmic.PatternCode										AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID -- 04/27/2023 sj
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	WHEN pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC 
					ELSE COALESCE(pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_ilmsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC 
					ELSE COALESCE(pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_ilmsubloccov_jmic.FinalPersistedLimit_JMIC							AS IMLimit
			,pcx_ilmsubloccov_jmic.FinalPersistedDeductible_JMIC					AS IMDeductible
			,CAST(NULL AS STRING)													AS SpecifiedCarrier
			,NULL																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsubloccov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pcx_ilmsubloccov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsubloccov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod 
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloccov_jmic
			ON  pcx_ilmsubloccov_jmic.BranchID = pc_policyperiod.ID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloc_jmic
			ON pcx_ilmsubloc_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmsubloc_jmic.FixedID = pcx_ilmsubloccov_jmic.ILMSubLoc
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmsubloc_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline 
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE
			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LocationLevelRisk'
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.FixedID = pcx_ilmsubloc_jmic.ILMLocation
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
			--AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.ILMSubLocCov = pcx_ilmsubloccov_jmic.FixedID
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_ilmsubloccov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id


		UNION ALL

		SELECT 
			pcx_jewelrystockcov_jmic.PublicID										AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,pcx_jewelrystock_jmic.PublicID											AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_jewelrystockcov_jmic.FixedID										AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,COALESCE(pc_policyline.BOPLocationSeq,0)								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_jewelrystockcov_jmic.PatternCode									AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID -- 04/27/2023 sj
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	WHEN pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_jewelrystockcov_jmic.FinalPersistedLimit_JMIC						AS IMLimit
			,pcx_jewelrystockcov_jmic.FinalPersistedDeductible_JMIC					AS IMDeductible
			,CAST(NULL AS STRING)													AS SpecifiedCarrier
			,NULL																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_jewelrystockcov_jmic.ID
								ORDER BY	IFNULL(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pcx_jewelrystockcov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_jewelrystockcov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockcov_jmic
			ON pcx_jewelrystockcov_jmic.BranchID = pc_policyperiod.ID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID
			AND pcx_jewelrystock_jmic.FixedID = pcx_jewelrystockcov_jmic.JewelryStock
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline 
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE
			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'StockLevelCoverage' 
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LocationLevelRisk'
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
			--AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.JewelryStockCov = pcx_jewelrystockcov_jmic.FixedID
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_jewelrystockcov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id	


		UNION ALL

		SELECT 
			pcx_ilmsubstockcov_jmic.PublicID										AS CoveragePublicID
			,pcx_ilmlocation_jmic.PublicID											AS IMLocationPublicID
			,pcx_jewelrystock_jmic.PublicID											AS IMStockPublicID
			,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
			,pc_policylocation.PublicID												AS PolicyLocationPublicID
			,pcx_ilmcost_jmic.PublicID												AS CostPublicID
			,pcx_ilmsubstockcov_jmic.FixedID										AS CoverageFixedID
			,pc_job.JobNumber														AS JobNumber
			,pc_policyperiod.PolicyNumber											AS PolicyNumber
			,pc_policyperiod.status													AS PolicyPeriodStatus
			,COALESCE(pc_policyline.BOPLocationSeq,0)								AS CoverageNumber
			,coverageLevelConfig.Value												AS CoverageLevel
			--,locationRisk.Value														AS RiskLocationLevel -- 04/27/2023
			,pcx_ilmsubstockcov_jmic.PatternCode									AS CoverageCode
			--,CoveragePatterns_CoveragePattern.refCode								AS CoverageReferenceCode
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID -- 04/27/2023 sj
					THEN 1 ELSE 0 
			 END																	AS IsTransactionSliceEffective
			,CASE	WHEN pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
			 END																	AS EffectiveDate
			,CASE	WHEN pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
			 END																	AS ExpirationDate
			,CASE	WHEN pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN 1 ELSE 0 
			 END																	AS IsTempCoverage
			,pcx_ilmsubstockcov_jmic.FinalPersistedLimit_JMIC						AS IMLimit
			,pcx_ilmsubstockcov_jmic.FinalPersistedDeductible_JMIC					AS IMDeductible
			,CAST(NULL AS STRING)													AS SpecifiedCarrier
			,NULL																	AS SpecifiedCarrierExposure
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsubstockcov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsubstockcov_jmic.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_ilmLocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
								)													AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_ilmsubstockcov_jmic.ID
								ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_ilmcost_jmic.ID DESC
								)													AS CostRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod 
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstockcov_jmic
			ON  pcx_ilmsubstockcov_jmic.BranchID = pc_policyperiod.ID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline 
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
	
			INNER JOIN ILMCoverageConfig AS lineConfig 
				ON lineConfig.Key = 'LineCode' 
				AND lineConfig.Value = pctl_policyline.TYPECODE
			INNER JOIN ILMCoverageConfig AS coverageLevelConfig
				ON coverageLevelConfig.Key = 'SubStockLevelCoverage'
			INNER JOIN ILMCoverageConfig AS locationRisk
				ON locationRisk.Key = 'LocationLevelRisk' 
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstock_jmic
			ON pcx_ilmsubstock_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmsubstock_jmic.FixedID = pcx_ilmsubstockcov_jmic.ILMSubStock
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmsubstock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID
			AND pcx_jewelrystock_jmic.FixedID = pcx_ilmsubstock_jmic.JewelryStock
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 04/27/2023 sj
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)	

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmcost_jmic.ILMSubStockCov = pcx_ilmsubstockcov_jmic.FixedID
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			--LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
			--ON pcx_ilmsubstockcov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id	

	) ilmCoverages
	
	INNER JOIN ILMCoverageConfig AS sourceConfig
	ON sourceConfig.Key = 'SourceSystem'
	
	INNER JOIN ILMCoverageConfig AS hashKeySeparator
	ON hashKeySeparator.Key = 'HashKeySeparator'
	
	INNER JOIN ILMCoverageConfig AS locationRisk -- 04/27/2023
	ON locationRisk.Key = 'LocationLevelRisk'
	
	INNER JOIN ILMCoverageConfig AS stockRisk
	ON stockRisk.Key = 'StockLevelRisk'
	
	INNER JOIN ILMCoverageConfig AS hashingAlgo
	ON hashingAlgo.Key = 'HashingAlgorithm'

	WHERE	1 = 1
			AND CoveragePublicID IS NOT NULL
			AND CoverageRank = 1
			AND CostRank = 1
) extractData 
--) outerquery
