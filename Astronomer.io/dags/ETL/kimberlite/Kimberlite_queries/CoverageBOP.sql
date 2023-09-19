-- tag: CoverageBOP - tag ends/
/***************************************************
		Kimberlite
			CoverageBOP.sql
				Converted to BigQuery
****************************************************

	COVERAGE INFO:
		pc_businessownerscov
		pcx_bopsublinecov_jmic
		pc_boplocationcov
		pcx_bopsubloccov_jmic
		pc_bopbuildingcov
		pcx_bopblanketcov_jmic
		
	IMPORTANT UPDATES
		DW STAGE tables, GW Product Model XML, and Ratabase XML related feeds were commented until ready in Data Lake as data source for Kimberlite
		BigQuery does not contain needed table	gw_pm.CoveragePatterns_CoveragePattern

		RateBookUsed -- in DW only used with PJ PolicyType; can be removed from this table in future

****************************************************************************************************************************************************/
/*--------------------------------------------------------------------------------------------
 *****  Change History  *****

	07/22/2020	DROBAK		Init create
	03/04/2021	DROBAK		Cost Join fix applied (now a left join)
	04/05/2021	DROBAK		'NONE' to 'None'
	04/05/2021	DROBAK		Changed CoverageLimit & CoverageDeductible to BOPLimit & BOPDeductible
	04/05/2021	DROBAK		Dropped unused column CoverageReferenceCode
	05/20/2021	DROBAK		Fix CoverageLevel = Line and SubLine (Use PrimaryLocation in place of pc_BOPLocation)
	06/03/2021	DROBAK		Add pc_policylocation.LocationNum AS PrimaryLocationNumber
	06/17/2021	DROBAK		Added IsTransactionSliceEffective field (BB layer use); Locn tables to left joins; updated unit tests
	08/25/2021	DROBAK		Fixed logic for CoverageNumber (= to DW logic)
	12/15/2021	DROBAK		Corrected some join syntax specifically for BQ (shift _PARTITIONTIME from where clause to join)
	01/25/2022	DROBAK		BQ Syntax correction for a pc_job join line
	08/19/2022	DROBAK		Remove: PrimaryLocationNumber  (keep only in Risk Tables)
							For BOPLocationPublicID: changed from pc_policylocation table to pc_boplocation for Line and SubLine (added logic to get min rating location number)
	10/03/2022	DROBAK		Added Insert stmnt; corrected fieldname typo: Is BOPBuildingPublicID, was BOPBuildingPublicId
	01/01/2023	DROBAK		Added Unioned Section for BOP BlanketLevelCoverage
	02/09/2023	DROBAK		Wrap PolicyTransactionKey in CASE Stmnt; fix Dupes issue by joining pc_boplocation to pc_policyline.FixedID
							Not using pcx_commonlocation_jmic, so removed left join
	03/07/2023	DROBAK		Made CoverageFixedID visible in output table	
	04/27/2023	DROBAK		Common table used for BOP: t_PrimaryRatingLocationBOP
							Changed date logic for joins to pc_boplocation and pc_bopbuilding for Location, SubLoc, Building, Blanket (incl Temp dates)

-------------------------------------------------------------------------------------------------------------------------------*/

CASE
  WHEN
    NOT EXISTS
      ( SELECT *
        FROM `{project}.{dest_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_schema = '{dest_dataset}'
        AND table_name = 't_PrimaryRatingLocationBOP'
        AND CAST(last_modified_time AS DATE) = CURRENT_DATE()
      ) 
  THEN 
	--This code is also used in RiskLocationBusinessOwners, CoverageBOP, CoverageUMB, ClaimFinancialTransactionLineBOPCeded, ClaimFinancialTransactionLineBOPDirect
	--So the two tables use SAME PublicID from SAME Table (pc_boplocation)
	CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP`
	AS SELECT LOBPrimary.* FROM
    (
		SELECT
		  pc_policyperiod.ID AS PolicyPeriodID
		  ,pc_policyperiod.EditEffectiveDate AS EditEffectiveDate
		  ,pctl_policyline.TYPECODE AS PolicyLineOfBusiness
		  --,MAX(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
		  --1. If the Primary loc matches the LOB loc, use it
		  --2. Else use Coverage Effective Location (Same as RatingLocation and may be different than the min or primary)
		  --3. Otherwise use the MIN location num's corresponding LOB Location
		  ,COALESCE(MIN(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
			  ,MIN(CASE WHEN pc_boplocation.BOPLine = pc_policyline.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
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
				AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'  
			--BOP Location  
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BranchID = pc_policylocation.BranchID  
				AND pc_boplocation.BOPLine = pc_policyline.FixedID  
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
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

	) LOBPrimary;
END CASE;

--DELETE `{project}.{dest_dataset}.CoverageBOP` WHERE bq_load_date = DATE({partition_date});
/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite_core.CoverageBOP`
AS SELECT outerquery.*
FROM (
*/

INSERT INTO `{project}.{dest_dataset}.CoverageBOP`
( 
	SourceSystem
	,BOPCoverageKey
	,PolicyTransactionKey
	,RiskLocationKey
	,RiskBuildingKey
	,PolicyPeriodPublicID
	,JobNumber
	,PolicyNumber
	,CoveragePublicID
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,IsTempCoverage
	,BOPLimit
	,BOPDeductible
	,CoverageCode
	,CoverageNumber
	,BOPLocationPublicID
	,BOPBuildingPublicID
	,PolicyLocationPublicID
	,EPLICode
	,PropertyRateNum
	,TheftAdjPropertyRateNum
	,RetroactiveDate
	,RateBookUsed
	,IsTransactionSliceEffective
	,FixedCoverageInBranchRank
	,CoverageFixedID
    ,bq_load_date
)

WITH BOPCoverageConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL 

  SELECT 'LineCode','BusinessOwnersLine' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'RatabaseLineCovgEntity','BusinessOwnersCov' UNION ALL

  SELECT 'epliLineLevelCoverage', 'BOPLine' UNION ALL
  SELECT 'epliSubLineLevelCoverage', 'BOPSubLine' UNION ALL
  SELECT 'RatabaseSubLineCovgEntity', 'BOPSubLineCov_JMIC' UNION ALL

  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'epliLocationLevelCoverage', 'BOPLoc' UNION ALL
  SELECT 'RatabaseLocCovgEntity', 'BOPLocationCov' UNION ALL

  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'epliSubLocLevelCoverage', 'BOPSubLoc' UNION ALL
  SELECT 'RatabaseSubLocCovgEntity', 'BOPSubLocCov_JMIC' UNION ALL

  SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
  SELECT 'epliBuildingLevelCoverage', 'BOPBldg' UNION ALL
  SELECT 'RatabaseBuildingCovgEntity', 'BOPSubLocCov_JMIC' UNION ALL
  
  SELECT 'BlanketLevelCoverage', 'Blanket' UNION ALL

  SELECT 'LocationLevelRisk','BusinessOwnersLocation' UNION ALL
  SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
)

SELECT 
	SourceSystem
	,BOPCoverageKey
	,PolicyTransactionKey
	,RiskLocationKey
	,RiskBuildingKey
	,PolicyPeriodPublicID
	,JobNumber
	,PolicyNumber
	,CoveragePublicID
	--,PrimaryLocationNumber
	,CoverageLevel
	--,CoverageReferenceCode
	,EffectiveDate
	,ExpirationDate
	,IsTempCoverage
	,BOPLimit
	,BOPDeductible
	,CoverageCode
	,CoverageNumber
	,BOPLocationPublicID
	,BOPBuildingPublicID
	,PolicyLocationPublicID
	,EPLICode
	,PropertyRateNum
	,TheftAdjPropertyRateNum
	,RetroactiveDate
	,RateBookUsed
	,IsTransactionSliceEffective
	,DENSE_RANK()  OVER(PARTITION BY CoverageFixedID, PolicyPeriodPublicID, CoverageLevel 
					ORDER BY IsTransactionSliceEffective DESC, FixedCoverageInBranchRank ) 
		AS FixedCoverageInBranchRank
	,CoverageFixedID
    ,DATE('{date}') as bq_load_date
    --,CURRENT_DATE() as bq_load_date
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		--SK For PK <Source>_<CoveragePublicID>_<Level>
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS BOPCoverageKey
		--SK For FK <Source>_<PolicyPeriodPublicID>
		,CASE WHEN PolicyPeriodPublicID IS NOT NULL
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
			END AS PolicyTransactionKey
		--SK For FK <Source>_<BOPLocationPublicID>_<Level>
		,CASE WHEN BOPLocationPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
			END AS RiskLocationKey
		,CASE WHEN BOPBuildingPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPBuildingPublicID,hashKeySeparator.Value,buildingRisk.Value)) 
			END AS RiskBuildingKey
		,BOPCoverages.*
	FROM (
		--- LINE COVERAGE ---
		SELECT 
			pc_businessownerscov.PublicID											AS	CoveragePublicID
			,coverageLevelConfig.Value												AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_businessownerscov.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pc_businessownerscov.FixedID											AS	CoverageFixedID
			,pc_job.JobNumber
			,CAST(NULL AS STRING)													AS	BOPBuildingPublicID
			,pc_boplocation.PublicID												AS	BOPLocationPublicID
			,pc_policylocation.PublicID												AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID												AS	PolicyPeriodPublicID
			,pc_policyline.PublicID													AS	PolicyLinePublicID
			,pc_policyterm.PublicID													AS	PolicyTermPublicID
			,pc_policyline.BOPBlanketAutoNumberSeq_JMIC								AS	CoverageNumber
			,pc_businessownerscov.PatternCode										AS	CoverageCode
			,pc_businessownerscov.FinalPersistedLimit_JMIC							AS	BOPLimit
			,pc_businessownerscov.FinalPersistedDeductible_JMIC						AS	BOPDeductible
			,pc_policyperiod.PolicyNumber
			--,'None' /* CoveragePatterns_CoveragePattern.RefCode	*/				AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			,CAST(NULL AS STRING) /* CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
				THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END */					AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum
			/* New method on how to ID Temp Coverage */
			,CASE WHEN pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN FinalPersistedTempFromDt_JMIC ELSE COALESCE(pc_businessownerscov.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL THEN FinalPersistedTempToDt_JMIC ELSE COALESCE(pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN (pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL) THEN 1 ELSE 0	END AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pc_businessownerscov.DateTerm1
				WHEN 'DateTerm2' THEN pc_businessownerscov.DateTerm2
				WHEN 'DateTerm3' THEN pc_businessownerscov.DateTerm3
				WHEN 'DateTerm4' THEN pc_businessownerscov.DateTerm4
				WHEN 'DateTerm5' THEN pc_businessownerscov.DateTerm5
				WHEN 'DateTerm6' THEN pc_businessownerscov.DateTerm6
				ELSE NULL
			END) */																	AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pc_businessownerscov.ID
				ORDER BY IFNULL(pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK() OVER(PARTITION BY pc_businessownerscov.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pc_businessownerscov.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod`  WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date})  pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) pc_businessownerscov
				ON pc_businessownerscov.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'LineLevelCoverage' 

			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND pc_businessownerscov.FixedID = pc_bopcost.BusinessOwnersCov
				AND COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)

			--Join in the t_PrimaryRatingLocationBOP Table to map the Natural Key for RatingLocationKey		
			LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP` AS t_PrimaryRatingLocationBOP
				ON t_PrimaryRatingLocationBOP.PolicyPeriodID = pc_policyperiod.ID
				AND t_PrimaryRatingLocationBOP.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
				--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
				AND ((t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
					or 
					(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON pc_policylocation.BranchID = t_PrimaryRatingLocationBOP.PolicyPeriodID  
				AND pc_policylocation.LocationNum = t_PrimaryRatingLocationBOP.RatingLocationNum  
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON pc_boplocation.BranchID = pc_policylocation.BranchID
				AND pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BOPLine = pc_policyline.FixedID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	

		/* 11/09/2020 -- CoveragePattern is from GW XML feeds and CoverageType is from  stage (previous user-entered table); neither is loaded into the Data Lake yet
	
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pc_businessownerscov.PatternCode = CoveragePatterns_CoveragePattern.public-id
			-- This is a user-controlled table in SharePoint --
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode
			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.gw_pm_CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				LEFT OUTER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_CovTerms CoveragePatterns_CovTerms
				ON CoveragePatterns_CoveragePattern.CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_GenericCovTermPattern CoveragePatterns_GenericCovTermPattern
				ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = CoveragePatterns_CovTerms.CovTerms_Id
				WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
				AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
					OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
					)
				GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
				) RetroDateCovTerms
				ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id

		*/
		/* 11/09/2020 -- Will use Ratabase XML data loaded to data lake; however, we are commenting this section until RB data is refined & mapped

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail RB_XML_Detail
				ON RB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND RB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND RB_XML_Detail.CoveragePublicID = pc_businessownerscov.PublicID
				--AND RB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND (
				RB_XML_Detail.ChangeTrackingStatus IS NULL
				OR RB_XML_Detail.ChangeTrackingStatus=1
				)
			LEFT JOIN @BOPCoverageConfig ratabaseConfig 
				ON ratabaseConfig.Key = 'RatabaseLineCovgEntity' AND ratabaseConfig.Value=RB_XML_Detail.CoverageEntityType

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail DeletedRB_XML_Detail
				ON DeletedRB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND DeletedRB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND DeletedRB_XML_Detail.CoveragePublicID = pc_businessownerscov.PublicID
			--	AND DeletedRB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND DeletedRB_XML_Detail.ChangeTrackingStatus = 2
			LEFT JOIN @BOPCoverageConfig deletedRatabaseConfig 
				ON deletedRatabaseConfig.Key = 'RatabaseLineCovgEntity' AND deletedRatabaseConfig.Value=DeletedRB_XML_Detail.CoverageEntityType

		*/

			WHERE 1 = 1
			--AND pc_policyperiod.PolicyNumber = vpolicynumber	/**** TEST *****/
			--AND JobNumber = vjobnumber		/**** TEST *****/

		UNION ALL

		--- SUB LINE COVERAGE ---
		SELECT 
			pcx_bopsublinecov_jmic.PublicID											AS	CoveragePublicID
			,coverageLevelConfig.Value												AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pcx_bopsublinecov_jmic.FixedID											AS	CoverageFixedID
			,pc_job.JobNumber
			,CAST(NULL AS STRING)													AS	BOPBuildingPublicID
			,pc_boplocation.PublicID												AS	BOPLocationPublicID
			,pc_policylocation.PublicID												AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID												AS	PolicyPeriodPublicID
			,pc_policyline.PublicID													AS	PolicyLinePublicID
			,pc_policyterm.PublicID													AS	PolicyTermPublicID
			--TBD																	AS	IsLineLevel
			,pc_policyline.BOPBlanketAutoNumberSeq_JMIC								AS	CoverageNumber
			,pcx_bopsublinecov_jmic.PatternCode										AS	CoverageCode
			,pcx_bopsublinecov_jmic.FinalPersistedLimit_JMIC						AS	BOPLimit
			,pcx_bopsublinecov_jmic.FinalPersistedDeductible_JMIC					AS	BOPDeductible		
			,pc_policyperiod.PolicyNumber
			--,'None' /* CoveragePatterns_CoveragePattern.RefCode	*/				AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			,CAST(NULL AS STRING) /* CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
				THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END */					AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum
			/* New method on how to ID Temp Coverage */
			,CASE WHEN pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL THEN pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN (pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) THEN 1 ELSE 0 END AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pcx_bopsublinecov_jmic.DateTerm1
				WHEN 'DateTerm2' THEN pcx_bopsublinecov_jmic.DateTerm2
				ELSE NULL
			END)		*/															AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pcx_bopsublinecov_jmic.ID
				ORDER BY IFNULL(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pcx_bopsublinecov_jmic.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_bopsublinecov_jmic.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank

			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date})  pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date})  pcx_bopsublinecov_jmic
				ON	pcx_bopsublinecov_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date})  pc_policyline
				ON	pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			-- BOP Sub-Line
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubline_jmic` WHERE _PARTITIONTIME = {partition_date})  pcx_bopsubline_jmic
				ON	pcx_bopsubline_jmic.BranchID = pc_policyperiod.ID
				AND	pcx_bopsubline_jmic.FixedID = pcx_bopsublinecov_jmic.BOPSubLine
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_bopsubline_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_bopsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'SubLineLevelCoverage'

			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND pcx_bopsublinecov_jmic.FixedID = pc_bopcost.BOPSubLineCov
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
/*			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND COALESCE(pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
*/
			--Join in the t_PrimaryRatingLocationBOP Table to map the Natural Key for RatingLocationKey		
			LEFT JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP` AS t_PrimaryRatingLocationBOP
				ON t_PrimaryRatingLocationBOP.PolicyPeriodID = pc_policyperiod.ID
				AND t_PrimaryRatingLocationBOP.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
				--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
				AND ((t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
					or 
					(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON pc_policylocation.BranchID = t_PrimaryRatingLocationBOP.PolicyPeriodID  
				AND pc_policylocation.LocationNum = t_PrimaryRatingLocationBOP.RatingLocationNum  
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON pc_boplocation.BranchID = pc_policylocation.BranchID
				AND pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BOPLine = pc_policyline.FixedID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	

		/* 11/09/2020 -- CoveragePattern is from GW XML feeds and CoverageType is from  stage (previous user-entered table); neither is loaded into the Data Lake yet
	
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pcx_bopsublinecov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id
			-- This is a user-controlled table in SharePoint --
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode
	
			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.gw_pm_CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				LEFT JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_CovTerms CoveragePatterns_CovTerms
				ON CoveragePatterns_CoveragePattern.CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_GenericCovTermPattern CoveragePatterns_GenericCovTermPattern
				ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = CoveragePatterns_CovTerms.CovTerms_Id
				WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
				AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
					OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
					)
				GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
				) RetroDateCovTerms
				ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id
		*/

		/* 11/09/2020 -- Will use Ratabase XML data loaded to data lake; however, we are commenting this section until RB data is refined & mapped

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail RB_XML_Detail
				ON RB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND RB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND RB_XML_Detail.CoveragePublicID = pcx_bopsublinecov_jmic.PublicID
				--AND RB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND (RB_XML_Detail.ChangeTrackingStatus IS NULL
					OR RB_XML_Detail.ChangeTrackingStatus=1
					)
			LEFT JOIN BOPCoverageConfig ratabaseConfig 
				ON ratabaseConfig.Key = 'RatabaseSubLineCovgEntity' AND ratabaseConfig.Value=RB_XML_Detail.CoverageEntityType

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail DeletedRB_XML_Detail
				ON DeletedRB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND DeletedRB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND DeletedRB_XML_Detail.CoveragePublicID = pcx_bopsublinecov_jmic.PublicID
			--	AND DeletedRB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND DeletedRB_XML_Detail.ChangeTrackingStatus = 2
			LEFT JOIN BOPCoverageConfig deletedRatabaseConfig 
				ON deletedRatabaseConfig.Key = 'RatabaseSubLineCovgEntity' AND deletedRatabaseConfig.Value=DeletedRB_XML_Detail.CoverageEntityType
		*/
		
			WHERE 1 = 1
			--AND pc_policyperiod.PolicyNumber = vpolicynumber	/**** TEST *****/
			--AND JobNumber = vjobnumber		/**** TEST *****/

		UNION ALL
	
		-- Location Level Coverage --
		SELECT 
			pc_boplocationcov.PublicID												AS	CoveragePublicID
			,coverageLevelConfig.Value												AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_boplocationcov.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pc_boplocation.BOPLine = pc_policyline.FixedID
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pc_boplocationcov.FixedID												AS	CoverageFixedID
			,pc_job.JobNumber
			,CAST(NULL AS STRING)													AS	BOPBuildingPublicID
			,pc_boplocation.PublicID												AS	BOPLocationPublicID
			,pc_policylocation.PublicID												AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID												AS	PolicyPeriodPublicID
			,pc_policyline.PublicID													AS	PolicyLinePublicID
			,pc_policyterm.PublicID													AS	PolicyTermPublicID
			,COALESCE(pc_policyline.BOPLocationSeq, 0)								AS	CoverageNumber
			,pc_boplocationcov.PatternCode											AS	CoverageCode
			,pc_boplocationcov.FinalPersistedLimit_JMIC								AS	BOPLimit
			,pc_boplocationcov.FinalPersistedDeductible_JMIC						AS	BOPDeductible
			,pc_policyperiod.PolicyNumber
			--,'None' /* CoveragePatterns_CoveragePattern.RefCode	*/				AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			,CAST(NULL AS STRING) /* CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
				THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END */	AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum
			/* New method on how to ID Temp Coverage */
			,CASE WHEN pc_boplocationcov.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN pc_boplocationcov.FinalPersistedTempFromDt_JMIC ELSE COALESCE(pc_boplocationcov.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pc_boplocationcov.FinalPersistedTempToDt_JMIC IS NOT NULL THEN pc_boplocationcov.FinalPersistedTempToDt_JMIC ELSE COALESCE(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN (pc_boplocationcov.FinalPersistedTempFromDt_JMIC IS NOT NULL	AND pc_boplocationcov.FinalPersistedTempToDt_JMIC IS NOT NULL) THEN 1 ELSE 0 END AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pc_boplocationcov.DateTerm1
				WHEN 'DateTerm2' THEN pc_boplocationcov.DateTerm2
				ELSE NULL
			END)		*/															AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pc_boplocationcov.ID
			ORDER BY IFNULL(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pc_boplocationcov.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pc_boplocationcov.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank

			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date}) pc_boplocationcov
				ON pc_boplocationcov.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'LocationLevelCoverage'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
				ON pc_boplocation.BranchID = pc_policyperiod.ID
				AND pc_boplocation.FixedID = pc_boplocationcov.BOPLocation
				--4/27/2023--
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	
				--AND COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
				ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_boplocation.Location
				--4/27/2023--
				AND COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME  = {partition_date}) pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND pc_bopcost.BOPLocationCov = pc_boplocationcov.FixedID
				--4/27/2023--
				AND COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
/* 2023-02-09
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME  = {partition_date}) pcx_commonlocation_jmic
				ON pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
				AND COALESCE(pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_boplocationcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)		
*/
		/* 11/09/2020 -- CoveragePattern is from GW XML feeds and CoverageType is from  stage (previous user-entered table); neither is loaded into the Data Lake yet
	
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pc_boplocationcov.PatternCode = CoveragePatterns_CoveragePattern.public-id
			-- This is a user-controlled table in SharePoint --
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode
	
			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.gw_pm_CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				LEFT JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_CovTerms CoveragePatterns_CovTerms
				ON CoveragePatterns_CoveragePattern.CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_GenericCovTermPattern CoveragePatterns_GenericCovTermPattern
				ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = CoveragePatterns_CovTerms.CovTerms_Id
				WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
				AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
					OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
					)
				AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
					OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
					)
				GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
				) RetroDateCovTerms
				ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id

		*/
		/* 11/09/2020 -- Will use Ratabase XML data loaded to data lake; however, we are commenting this section until RB data is refined & mapped

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail RB_XML_Detail
				ON RB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND RB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND RB_XML_Detail.CoveragePublicID = pc_boplocationcov.PublicID
				--AND RB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND (RB_XML_Detail.ChangeTrackingStatus IS NULL
					OR RB_XML_Detail.ChangeTrackingStatus=1
					)
			LEFT JOIN BOPCoverageConfig ratabaseConfig 
				ON ratabaseConfig.Key = 'RatabaseLocCovgEntity' AND ratabaseConfig.Value=RB_XML_Detail.CoverageEntityType

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail DeletedRB_XML_Detail
				ON DeletedRB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND DeletedRB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND DeletedRB_XML_Detail.CoveragePublicID = pc_boplocationcov.PublicID
			--	AND DeletedRB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND DeletedRB_XML_Detail.ChangeTrackingStatus = 2
			LEFT JOIN BOPCoverageConfig deletedRatabaseConfig 
				ON deletedRatabaseConfig.Key = 'RatabaseLocCovgEntity' AND deletedRatabaseConfig.Value=DeletedRB_XML_Detail.CoverageEntityType

		*/

			WHERE 1 = 1
			--AND pc_policyperiod.PolicyNumber = vpolicynumber	/**** TEST *****/
			--AND JobNumber = vjobnumber		/**** TEST *****/

		UNION ALL
	
		-- SUB-LOCATION LEVEL --
		SELECT
			pcx_bopsubloccov_jmic.PublicID											AS	CoveragePublicID
			,coverageLevelConfig.Value												AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pc_boplocation.BOPLine = pc_policyline.FixedID
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pcx_bopsubloccov_jmic.FixedID											AS	CoverageFixedID
			,pc_job.JobNumber
			,CAST(NULL AS STRING)													AS	BOPBuildingPublicID
			,pc_boplocation.PublicID												AS	BOPLocationPublicID
			,pc_policylocation.PublicID												AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID												AS	PolicyPeriodPublicID
			,pc_policyline.PublicID													AS	PolicyLinePublicID
			,pc_policyterm.PublicID													AS	PolicyTermPublicID
			,COALESCE(pc_policyline.BOPLocationSeq, 0)								AS	CoverageNumber
			,pcx_bopsubloccov_jmic.PatternCode										AS	CoverageCode
			,pcx_bopsubloccov_jmic.FinalPersistedLimit_JMIC							AS	BOPLimit
			,pcx_bopsubloccov_jmic.FinalPersistedDeductible_JMIC					AS	BOPDeductible
			,pc_policyperiod.PolicyNumber
			--,'None' /* CoveragePatterns_CoveragePattern.RefCode	*/				AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			,CAST(NULL AS STRING) /* CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
				THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END */	AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum
			/* New method on how to ID Temp Coverage */
			,CASE WHEN pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC ELSE COALESCE(pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL THEN pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC ELSE COALESCE(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN (pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL	AND pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) THEN 1 ELSE 0 END AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pcx_bopsubloccov_jmic.DateTerm1
				WHEN 'DateTerm2' THEN pcx_bopsubloccov_jmic.DateTerm2
				ELSE NULL
			END)		*/															AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pcx_bopsubloccov_jmic.ID
				ORDER BY IFNULL(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pcx_bopsubloccov_jmic.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_bopsubloccov_jmic.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank

			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_bopsubloccov_jmic
				ON pcx_bopsubloccov_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_bopsubloc_jmic
				ON	pcx_bopsubloc_jmic.BranchID = pc_policyperiod.ID
				AND pcx_bopsubloc_jmic.FixedID = pcx_bopsubloccov_jmic.BOPSubLoc
				--4/27/2023--
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_bopsubloc_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_bopsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				--4/27/2023--
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'SubLocLevelCoverage'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
				ON pc_boplocation.BranchID = pc_policyperiod.ID
				AND pc_boplocation.FixedID = pcx_bopsubloc_jmic.BOPLocation
				--4/27/2023--
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	
				--AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
				ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_boplocation.Location
				--4/27/2023--
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME  = {partition_date}) AS pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND pcx_bopsubloccov_jmic.FixedID = pc_bopcost.BOPSubLocCov
				--4/27/2023--
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
/* 2023-02-09
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME  = {partition_date}) pcx_commonlocation_jmic
				ON pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
				AND COALESCE(pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)		
*/		
		/* 11/09/2020 -- CoveragePattern is from GW XML feeds and CoverageType is from  stage (previous user-entered table); neither is loaded into the Data Lake yet
	
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pcx_bopsubloccov_jmic.PatternCode = CoveragePatterns_CoveragePattern.public-id
			-- This is a user-controlled table in SharePoint --
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode

			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.gw_pm_CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				LEFT JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_CovTerms CoveragePatterns_CovTerms
					ON CoveragePatterns_CoveragePattern.CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_GenericCovTermPattern CoveragePatterns_GenericCovTermPattern
					ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = CoveragePatterns_CovTerms.CovTerms_Id
					WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
					AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
						OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
						)
					GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
					) RetroDateCovTerms
					ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id

		*/
		/* 11/09/2020 -- Will use Ratabase XML data loaded to data lake; however, we are commenting this section until RB data is refined & mapped
			
			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail RB_XML_Detail
				ON RB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND RB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND RB_XML_Detail.CoveragePublicID = pcx_bopsubloccov_jmic.PublicID
				--AND RB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND (RB_XML_Detail.ChangeTrackingStatus IS NULL
					OR RB_XML_Detail.ChangeTrackingStatus=1
					)
			LEFT JOIN BOPCoverageConfig ratabaseConfig 
				ON ratabaseConfig.Key = 'RatabaseSubLocCovgEntity' AND ratabaseConfig.Value=RB_XML_Detail.CoverageEntityType

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail DeletedRB_XML_Detail
				ON DeletedRB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND DeletedRB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND DeletedRB_XML_Detail.CoveragePublicID = pcx_bopsubloccov_jmic.PublicID
			--	AND DeletedRB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND DeletedRB_XML_Detail.ChangeTrackingStatus = 2
			LEFT JOIN BOPCoverageConfig deletedRatabaseConfig 
				ON deletedRatabaseConfig.Key = 'RatabaseSubLocCovgEntity' AND deletedRatabaseConfig.Value=DeletedRB_XML_Detail.CoverageEntityType

		*/

			WHERE 1 = 1
			--AND pc_policyperiod.PolicyNumber = vpolicynumber	/**** TEST *****/
			--AND JobNumber = vjobnumber		/**** TEST *****/

		UNION ALL
	
		--Building Level Coverage
		SELECT
			pc_bopbuildingcov.PublicID												AS	CoveragePublicID
			,coverageLevelConfig.Value												AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pc_boplocation.BOPLine = pc_policyline.FixedID
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pc_bopbuildingcov.FixedID												AS	CoverageFixedID
			,pc_job.JobNumber
			,pc_bopbuilding.PublicID												AS	BOPBuildingPublicID
			,pc_boplocation.PublicID												AS	BOPLocationPublicID
			,pc_policylocation.PublicID												AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID												AS	PolicyPeriodPublicID
			,pc_policyline.PublicID													AS	PolicyLinePublicID
			,pc_policyterm.PublicID													AS	PolicyTermPublicID
			,COALESCE(pc_policyline.BOPLocationSeq, 0)								AS	CoverageNumber
			,pc_bopbuildingcov.PatternCode											AS	CoverageCode
			,pc_bopbuildingcov.FinalPersistedLimit_JMIC								AS	BOPLimit
			,pc_bopbuildingcov.FinalPersistedDeductible_JMIC						AS	BOPDeductible
			,pc_policyperiod.PolicyNumber
			--,'None' /* CoveragePatterns_CoveragePattern.RefCode	*/				AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			,CAST(NULL AS STRING) /* CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
				THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END */	AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum
			/* New method on how to ID Temp Coverage */
			,CASE WHEN pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC IS NOT NULL THEN pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC ELSE COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.PeriodStart) END AS EffectiveDate
			,CASE WHEN pc_bopbuildingcov.FinalPersistedTempToDt_JMIC IS NOT NULL THEN pc_bopbuildingcov.FinalPersistedTempToDt_JMIC ELSE COALESCE(pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) END AS ExpirationDate
			,CASE WHEN (pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC IS NOT NULL	AND pc_bopbuildingcov.FinalPersistedTempToDt_JMIC IS NOT NULL) THEN 1 ELSE 0 END AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pc_bopbuildingcov.DateTerm1
				WHEN 'DateTerm2' THEN pc_bopbuildingcov.DateTerm2
				ELSE NULL
			END)		*/															AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pc_bopbuildingcov.ID
				ORDER BY IFNULL(pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pc_bopbuildingcov.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pc_bopbuildingcov.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank
						
			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			-- BOP Building Coverages
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuildingcov
				ON pc_bopbuildingcov.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'BuildingLevelCoverage'

			-- BOP Building
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuilding
				ON pc_bopbuilding.BranchID = pc_policyperiod.ID
				AND pc_bopbuilding.FixedID = pc_bopbuildingcov.BOPBuilding
				--AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart)
				--AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd)
				--4/27/2023--
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd)	
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
				ON pc_boplocation.BranchID = pc_policyperiod.ID
				AND pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
				AND pc_boplocation.BOPLine = pc_policyline.FixedID		--2023-02-09
				--AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				--AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				--4/27/2023--
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
				ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_boplocation.Location
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND pc_bopcost.BOPBuildingCov = pc_bopbuildingcov.FixedID
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
/* 2023-02-09
			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_commonlocation_jmic
				ON pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
				AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_bopbuildingcov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)		
*/	
		/* 11/09/2020 -- CoveragePattern is from GW XML feeds and CoverageType is from  stage (previous user-entered table); neither is loaded into the Data Lake yet
		
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pc_bopbuildingcov.PatternCode = CoveragePatterns_CoveragePattern.public-id
			-- This is a user-controlled table in SharePoint --
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode

			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.gw_pm_CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				LEFT JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_CovTerms CoveragePatterns_CovTerms
					ON CoveragePatterns_CoveragePattern.CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.gw_pm_CoveragePatterns_GenericCovTermPattern CoveragePatterns_GenericCovTermPattern
					ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = CoveragePatterns_CovTerms.CovTerms_Id
					WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
					AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
						OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
						)
					GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
					) RetroDateCovTerms
					ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id

		*/
		/* 11/09/2020 -- Will use Ratabase XML data loaded to data lake; however, we are commenting this section until RB data is refined & mapped

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail RB_XML_Detail
				ON RB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND RB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND RB_XML_Detail.CoveragePublicID = pc_bopbuildingcov.PublicID
				--AND RB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND (RB_XML_Detail.ChangeTrackingStatus IS NULL
					OR RB_XML_Detail.ChangeTrackingStatus=1
					)
			LEFT JOIN BOPCoverageConfig ratabaseConfig 
				ON ratabaseConfig.Key = 'RatabaseBuildingCovgEntity' AND ratabaseConfig.Value=RB_XML_Detail.CoverageEntityType

			LEFT JOIN dw_stage.bi_stage.tempDimCoverageXMLRatingDetail DeletedRB_XML_Detail
				ON DeletedRB_XML_Detail.PC_Policy_Period_publicID = pc_policyperiod.PublicID
				AND DeletedRB_XML_Detail.FileName like '%' + pc_policyperiod.QuoteServiceID + '%'
				AND DeletedRB_XML_Detail.CoveragePublicID = pc_bopbuildingcov.PublicID
			--	AND DeletedRB_XML_Detail.CoverageEntityType = 'BusinessOwnersCov'
				AND DeletedRB_XML_Detail.ChangeTrackingStatus = 2
			LEFT JOIN BOPCoverageConfig deletedRatabaseConfig 
				ON deletedRatabaseConfig.Key = 'RatabaseBuildingCovgEntity' AND deletedRatabaseConfig.Value=DeletedRB_XML_Detail.CoverageEntityType

		*/

			WHERE 1 = 1
			--AND pc_policyperiod.PolicyNumber = vpolicynumber	/**** TEST *****/
			--AND JobNumber = vjobnumber		/**** TEST *****/

		UNION ALL

		--- BOP BLANKET COVERAGE ---
		SELECT
			pcx_bopblanketcov_jmic.PublicID										AS	CoveragePublicID
			,coverageLevelConfig.Value											AS	CoverageLevel
			,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_bopblanketcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
						AND pc_boplocation.BOPLine = pc_policyline.FixedID
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pcx_bopblanketcov_jmic.FixedID										AS	CoverageFixedID
			,pc_job.JobNumber
			,CAST(NULL AS STRING)												AS	BOPBuildingPublicID
			,pc_boplocation.PublicID											AS	BOPLocationPublicID
			,pc_policylocation.PublicID											AS	PolicyLocationPublicID
			,pc_policyperiod.PublicID											AS	PolicyPeriodPublicID
			,pc_policyline.PublicID												AS	PolicyLinePublicID
			,pc_policyterm.PublicID												AS	PolicyTermPublicID
			,COALESCE(pc_policyline.BOPLocationSeq, 0)							AS	CoverageNumber
			,pcx_bopblanketcov_jmic.PatternCode										AS	CoverageCode
			,pcx_bopblanketcov_jmic.FinalPersistedLimit_JMIC							AS	BOPLimit
			,pcx_bopblanketcov_jmic.FinalPersistedDeductible_JMIC					AS	BOPDeductible
			,pc_policyperiod.PolicyNumber
			--,CoveragePatterns_CoveragePattern.RefCode							AS	CoverageReferenceCode
			,pc_policyperiod.ModelNumber
			--,CASE WHEN DimCoverageType.ReinsuranceCoverageGroup = 'EPLI' 
			--	THEN  CoveragePatterns_CoveragePattern.refCode ELSE NULL END	AS	EPLICode
			,CAST(NULL AS STRING)               AS	EPLICode
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_PROP_RATE_NBR	*/						AS	PropertyRateNum
			,CAST(NULL AS STRING) /* RB_XML_Detail.BOP_THEFT_ADJ_BPP_PROP_RATE_NBR */			AS	TheftAdjPropertyRateNum

			/* New method on how to ID Temp Coverage */
			,CASE WHEN pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					THEN pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC 
					ELSE COALESCE(pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) 
				END																AS EffectiveDate
			,CASE WHEN pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL 
					THEN pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC 
					ELSE COALESCE(pcx_bopblanketcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
				END																AS ExpirationDate
			,CASE WHEN (pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL 
					AND pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
				THEN 1 ELSE 0 END												AS isTempCoverage
			,CAST(NULL AS STRING) /* (CASE RetroDateCovTerms.coverageColumn
				WHEN 'DateTerm1' THEN pc_bopbuildingcov.DateTerm1
				WHEN 'DateTerm2' THEN pc_bopbuildingcov.DateTerm2
				ELSE NULL
			END)		*/															AS	RetroactiveDate
			,pc_bopcost.RateBookUsed
			,DENSE_RANK() OVER(PARTITION BY pcx_bopblanketcov_jmic.ID
				ORDER BY IFNULL(pcx_bopblanketcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopblanket_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS CoverageRank
			,DENSE_RANK()  OVER(PARTITION BY pcx_bopblanketcov_jmic.FixedID, pc_policyperiod.ID
				ORDER BY IFNULL(pcx_bopblanketcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopblanket_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS FixedCoverageInBranchRank
			,DENSE_RANK() OVER(PARTITION BY pcx_bopblanketcov_jmic.ID
				ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC, pc_bopcost.ID DESC
			) AS CostRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyterm.ID = pc_policyperiod.PolicyTermID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			-- BOP Blanket Coverage
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopblanketcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_bopblanketcov_jmic
				ON pcx_bopblanketcov_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyperiod.ID = pc_policyline.BranchID
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
				ON pc_policyline.SubType = pctl_policyline.ID
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
			INNER JOIN BOPCoverageConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN BOPCoverageConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'BlanketLevelCoverage'
			
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopblanket_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopblanket_jmic
				ON pcx_bopblanket_jmic.BranchID = pcx_bopblanketcov_jmic.BranchID
				AND pcx_bopblanket_jmic.FixedID = pcx_bopblanketcov_jmic.FixedID
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_bopblanket_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_bopblanket_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

			--Join in the t_PrimaryRatingLocationBOP Table to map the Natural Key for RatingLocationKey		
			LEFT JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP` AS t_PrimaryRatingLocationBOP
				ON t_PrimaryRatingLocationBOP.PolicyPeriodID = pc_policyperiod.ID
				AND t_PrimaryRatingLocationBOP.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
				--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
				AND ((t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
					or 
					(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationBOP.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON pc_policylocation.BranchID = t_PrimaryRatingLocationBOP.PolicyPeriodID  
				AND pc_policylocation.LocationNum = t_PrimaryRatingLocationBOP.RatingLocationNum  
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(t_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON pc_boplocation.BranchID = pc_policylocation.BranchID
				AND pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BOPLine = pc_policyline.FixedID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				--AND pcx_bopblanketcov_jmic.FixedID = pc_bopcost.???????
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopblanketcov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
/* 2023-02-09

			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_commonlocation_jmic
				ON pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
				AND COALESCE(pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_bopblanketcov_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)		
*/
		/* 1/01/2023 -- CoveragePattern is from GW XML feeds and is not loaded into the Data Lake yet
					 -- CoverageType is a merge-script table and is avail in BQ

			----Coverage Type Code	
			LEFT JOIN dw_stage.gw_pm.CoveragePatterns_CoveragePattern CoveragePatterns_CoveragePattern
				ON pcx_bopblanketcov_jmic.PatternCode = CoveragePatterns_CoveragePattern.[public-id]
			-- This is a user-controlled table in SharePoint 
			LEFT JOIN DW_STAGE.bi_stage.syn_DDS_DimCoverageType DimCoverageType 
				ON DimCoverageType.CoverageTypeCode = CoveragePatterns_CoveragePattern.refCode

			LEFT JOIN 
			(
				SELECT CoveragePatterns_CoveragePattern.CoveragePattern_Id, 
				MIN(CoveragePatterns_GenericCovTermPattern.coverageColumn) coverageColumn,
				MAX(	CASE 
						WHEN CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_CovTerms._ChangeTrackingStatus IS NOT NULL THEN 1
						WHEN CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus= 1 THEN 1  ELSE NULL
						END
					) ChangeTrackingStatus
				FROM dw_stage.changetracking.[gw_pm_CoveragePatterns_CoveragePattern] CoveragePatterns_CoveragePattern
				LEFT JOIN dw_stage.changetracking.[gw_pm_CoveragePatterns_CovTerms] CoveragePatterns_CovTerms
					ON [CoveragePatterns_CoveragePattern].CoveragePattern_Id = CoveragePatterns_CovTerms.CoveragePattern_Id
				INNER JOIN dw_stage.changetracking.[gw_pm_CoveragePatterns_GenericCovTermPattern] CoveragePatterns_GenericCovTermPattern
					ON CoveragePatterns_GenericCovTermPattern.CovTerms_Id = [CoveragePatterns_CovTerms].CovTerms_Id
					WHERE CoveragePatterns_GenericCovTermPattern.modelType = 'RetroDate_JMIC'
					AND ( CoveragePatterns_CoveragePattern._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CoveragePattern._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_CovTerms._ChangeTrackingStatus IS NULL 
						OR CoveragePatterns_CovTerms._ChangeTrackingStatus=1--don't include 2's
						)
					AND (CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus IS NULL
						OR CoveragePatterns_GenericCovTermPattern._ChangeTrackingStatus = 1 --don't include 2's
						)
					GROUP BY CoveragePatterns_CoveragePattern.CoveragePattern_Id
					) RetroDateCovTerms
					ON RetroDateCovTerms.CoveragePattern_Id = CoveragePatterns_CoveragePattern.CoveragePattern_Id


	-- Blanket Coverage is not available in RB_XML_Detail table in DWH, so this section not included

*/
			/**** TEST *****/
			WHERE 1=1 
			--AND pc_policyperiod.PolicyNumber = ISNULL(@policynumber, pc_policyperiod.PolicyNumber)

	) BOPCoverages
	INNER JOIN BOPCoverageConfig sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN BOPCoverageConfig hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN BOPCoverageConfig locationRisk
		ON locationRisk.Key='LocationLevelRisk'
	INNER JOIN BOPCoverageConfig hashAlgorithm
		ON hashAlgorithm.Key = 'HashAlgorithm'
	INNER JOIN BOPCoverageConfig buildingRisk
		ON buildingRisk.Key='BuildingLevelRisk'

	WHERE 1=1
		AND	CoveragePublicID IS NOT NULL
		AND CoverageRank = 1
		AND CostRank = 1

) extractData 
--) outerquery
