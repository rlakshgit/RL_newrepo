-- tag: CoverageUMB - tag ends/
/*******************************************************************************************************
		Kimberlite
			CoverageUMB.sql
				Converted to BigQuery
********************************************************************************************************
	This query will bring in all Instances of all types of coverages provided under the UMB product. 
********************************************************************************************************

	IMPORTANT FUTURE UPDATES
		--DW STAGE tables, GW Product Model XML, and Ratabase XML related feeds were commented until ready in Data Lake as data source for Kimberlite
		--BigQuery does not contain needed table	gw_pm.CoveragePatterns_CoveragePattern
		--Need to add to Kimberlite
			--Summarize Ceded Premium from Excess of Loss Facultative Agreements
			--Get the original Effective date for EPLI Coverages per term
			--Calculate UmbrellaExcessOfLossFacultativePremium
			--Extended Ratabase measures
				--Calculate UmbrellaLiability4xs1MPremium, UmbrellaLiability5xs5MPremium, UmbrellaLiabilityAnnualPremium, UmbrellaLiability1MPremium
**************************************************************************************************************************************************************************/
/*
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/26/2021	DROBAK		Cost Join fix applied (now a left join)
	04/05/2021	DROBAK		Changed Deductiuble to UMBDeductible
	06/02/2021	DROBAK		Added IsTransactionSliceEffective field for use in BB layer to select Policy Slice Eff records
	06/04/2021	DROBAK		Replace pc_boplocation with pc_policylocation, use Primary Location for RiskLocationKey; drop PolicyLocationPublicID (redundant now)
	06/04/2021	DROBAK		DQ Check 'MISSING RISKS' was modified for an edge case
	08/26/2021	DROBAK		Added field CoverageNumber
	10/27/2021	DROBAK		Change LocationLevelRisk value from BOPLocation to BusinessOwnersLocation, to match other queries
	10/28/2021	DROBAK		To prevent dupes no need to join to pc_boplocation
	08/19/2022	DROBAK		Remove: PrimaryLocationNumber (keep only in Risk Tables)
							For BOPLocationPublicID: changed from pc_policylocation table to pc_boplocation for Line and SubLine (added logic to get min rating location number)
	04/13/2023	DROBAK		Though not used herein, for consistency, added IsPrimaryLocation

--------------------------------------------------------------------------------------------
*/
--This code is also used in RiskLocationBusinessOwners, so the two tables use SAME PublicID from SAME Table (pc_boplocation)
CREATE OR REPLACE TEMP TABLE PolicyVersionLOB_PrimaryRatingLocation
AS SELECT *
FROM (
	SELECT 
		pc_policyperiod.ID AS PolicyPeriodID
		,pc_policyperiod.EditEffectiveDate AS SubEffectiveDate
		,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
		--This flag displays whether or not the LOB Location matches the PrimaryLocation
		,MAX(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
		--If the Primary loc matches the LOB loc, use it, otherwise use the MIN location num's corresponding LOB Location
		,COALESCE(MIN(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
				,MIN(pc_policylocation.LocationNum)) AS RatingLocationNum  

	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
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
		    AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'  
		--BOP Location  
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
			ON pc_boplocation.Location = pc_policylocation.FixedID
			AND pc_boplocation.BranchID = pc_policylocation.BranchID  
			AND pc_boplocation.BOPLine = pc_policyline.FixedID  
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			--AND (pc_boplocation.EffectiveDate <= tempPolicyPeriodEffectiveDate.SubEffectiveDate OR pc_boplocation.EffectiveDate IS NULL)  
			--AND (pc_boplocation.ExpirationDate > tempPolicyPeriodEffectiveDate.SubEffectiveDate OR pc_boplocation.ExpirationDate IS NULL)      
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
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS PrimaryPolicyLocation_state
			ON PrimaryPolicyLocation_state.ID = PrimaryPolicyLocation.StateInternal  
	WHERE 1 = 1
		AND pc_policyperiod._PARTITIONTIME = {partition_date}
	GROUP BY
		pc_policyperiod.ID
		,pc_policyperiod.EditEffectiveDate
		,pctl_policyline.TYPECODE

	) AS PrimaryRatingLocations;

/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.DAR_CoverageUMB`
AS SELECT outerquery.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.CoverageUMB` (
		SourceSystem,
		UMBCoverageKey,
		PolicyTransactionKey,
		RiskLocationKey,
		CoveragePublicID,
		CoverageLevel,		
		JobNumber,
		PolicyNumber,
		EffectiveDate,
		ExpirationDate,
		IsTempCoverage,
		UMBLimit,
		UMBDeductible,
		CoverageNumber,
		CoverageCode,		
		PolicyPeriodPublicID,
		BOPLocationPublicID,
		IsTransactionSliceEffective,
	    PolicyPeriodFixedID,
		FixedCoverageInBranchRank,
		bq_load_date
)

WITH ilmCoverages AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','UmbrellaLine_JMIC' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'LocationLevelRisk','BusinessOwnersLocation'
)

SELECT 
	SourceSystem
	,UMBCoverageKey
	,PolicyTransactionKey
	,RiskLocationKey
	,CoveragePublicID
	,CoverageLevel
	--,PrimaryLocationNumber
	,JobNumber
	,PolicyNumber
--	,CoverageReferenceCode
	,EffectiveDate
	,ExpirationDate
	,IsTempCoverage
	,UMBLimit
	,UMBDeductible
	,CoverageNumber
	,CoverageCode
	,PolicyPeriodPublicID
	,BOPLocationPublicID
	,IsTransactionSliceEffective
	,PolicyPeriodFixedID
	,DENSE_RANK() OVER(PARTITION BY CoverageFixedID, PolicyPeriodPublicID, CoverageLevel
					ORDER BY	IsTransactionSliceEffective DESC
								,FixedCoverageInBranchRank
				) AS FixedCoverageInBranchRank 
    ,DATE('{date}') as bq_load_date	
	
FROM
(
	SELECT 
		sourceConfig.Value AS SourceSystem
		--SK For PK <Source>_<CoveragePublicID>_<Level>
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS UMBCoverageKey
		--SK For FK <Source>_<PolicyPeriodPublicID>
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		--SK For FK <Source>_<BOPLocationPublicID>_<Level>
		,CASE WHEN BOPLocationPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
		END AS RiskLocationKey
		,umbCoverages.*		
	FROM (
		SELECT 
			pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,pcx_umbrellalinecov_jmic.PublicID AS CoveragePublicID
			,pcx_umbrellalinecov_jmic.FixedID AS CoverageFixedID
			,coverageLevelConfig.Value AS CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_umbrellalinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
-- 			,CoveragePatterns_CoveragePattern.refCode AS CoverageReferenceCode
			,CASE WHEN pcx_umbrellalinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pcx_umbrellalinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL THEN FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_umbrellalinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN pcx_umbrellalinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN 1 ELSE 0 END AS IsTempCoverage
			,pcx_umbrellalinecov_jmic.FinalPersistedLimit_JMIC AS UMBLimit
			,pcx_umbrellalinecov_jmic.FinalPersistedDeductible_JMIC AS UMBDeductible
			,pcx_umbrellalinecov_jmic.PatternCode AS CoverageCode
			,COALESCE(pc_policyline.BOPBlanketAutoNumberSeq_JMIC,0) AS CoverageNumber	--No direct equivalent in GW
			,pc_boplocation.PublicID AS BOPLocationPublicID
			--,pc_policylocation.PublicID AS PolicyLocationPublicID
			--,pc_policylocation.LocationNum AS PrimaryLocationNumber
			,pc_policylocation.FixedID AS PolicyPeriodFixedID
			,DENSE_RANK() OVER(PARTITION BY pcx_umbrellalinecov_jmic.ID
					ORDER BY IFNULL(pcx_umbrellalinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pcx_umbrellalinecov_jmic.FixedID, pc_policyperiod.ID
					ORDER BY IFNULL(pcx_umbrellalinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_umbrellalinecov_jmic.ID
					ORDER BY IFNULL(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,pcx_umbcost_jmic.ID DESC
				) AS CostRank
		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
    			ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbrellalinecov_jmic
     			ON  pcx_umbrellalinecov_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
    			ON pc_policyline.BranchID = pc_policyperiod.ID
					AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
    			ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN ilmCoverages lineConfig
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ilmCoverages coverageLevelConfig
				ON coverageLevelConfig.Key = 'LineLevelCoverage' 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcost_jmic
    			ON pcx_umbcost_jmic.BranchID = pc_policyperiod.ID
				AND pcx_umbcost_jmic.UmbrellaLineCov = pcx_umbrellalinecov_jmic.FixedID
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_umbcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
    			ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
	/*		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
    			ON  pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
	*/
			--Join in the PolicyVersionLOB_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
			LEFT OUTER JOIN PolicyVersionLOB_PrimaryRatingLocation PolicyVersionLOB_PrimaryRatingLocation
			ON PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
			AND PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				or 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID  
			AND pc_policylocation.LocationNum = PolicyVersionLOB_PrimaryRatingLocation.RatingLocationNum  
			AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
			ON pc_boplocation.BranchID = pc_policylocation.BranchID
			AND pc_boplocation.Location = pc_policylocation.FixedID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	

	-- 		LEFT OUTER JOIN DW_STAGE.gw_pm.CoveragePatterns_CoveragePattern
	-- 			ON pcx_umbrellalinecov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id

		--WHERE 1=1

	) umbCoverages
	INNER JOIN ilmCoverages sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN ilmCoverages hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN ilmCoverages locationRisk
		ON locationRisk.Key='LocationLevelRisk'
	INNER JOIN ilmCoverages hashingAlgo
		ON hashingAlgo.Key='HashingAlgorithm'

	WHERE 1=1
		AND CoveragePublicID IS NOT NULL
		AND CoverageRank = 1
		AND CostRank = 1
    
) extractData
--) outerquery
