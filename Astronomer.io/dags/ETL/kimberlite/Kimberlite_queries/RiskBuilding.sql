-- tag: RiskBuilding - tag ends/
/********************************************************************************************
	Kimberlite Extract Query
		RiskBuilding.sql
				GCP_TESTED

	NOTES:
		--WindHail Exclusions and Theft Exclusions multiple records (may be as features)
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	11/2020		SLJ			Init create
	01/07/2021	DROBAK		Replaced PIVOT code with CASE Stmnts
	03/06/2021	DROBAK		Cost Join fix applied (now a left join)
	04/15/2021	DROBAK		Add pc_building Effective and Expiration Dates; hashAlgorithm not used in BQ
	04/20/2021	DROBAK		Fixed dupes in RiskBuildingImpr subquery; field name clean-up
	05/20/2021	DROBAK		Anchor table is now pc_BOPBuilding instead of pc_BOLocation
	05/20/2021	DROBAK		Added Temp table for BldgImprBreakdown - to remove dupes in this area
	05/24/2021	DROBAK		Updated Data Health Check code:  added "pc_job.JobNumber IS NOT NULL" to filter unwanted records
	05/27/2021	DROBAK		Added Primary Building & Location; Added LocationEffectiveDate & LocationExpirationDate
	09/13/2022	DROBAK/SLJ	Add fields: BuildingProtectionClassCode, JobNumber, PrimaryRatingBuildingFixedID 
							& IsPrimaryBuildingLocation (via CTE_PolicyVerLOB_PrimaryRatingLocn)
	02/07/2023	DROBAK/SLJ	Change INNER JOIN to LEFT JOIN and date logic for pc_boplocation (JobNumber 5743791; 8074208; 11486515; 11486502)
							IsTransactionSliceEffective Modified
							BuildingRank modified (JobNumber 8186336)
	03/29/2023	SLJ			IsTransactionSliceEffective modified (Reason: IsTransactionSliceEFfective = 1 when the Location key is NULL Job Number: 4713498 and Building public id: 'pc:559501')
							Change date logic for pc_boplocation (Job Number: 11209740 and Location Number: 5)
							BuildingFixedID added to select list
							
--------------------------------------------------------------------------------------------
**********************************************************************************************
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_RiskBuilding`
AS SELECT extractData.*
FROM (
*/
----------------------------------------------------------------------------
-- Determine Primary Building (Min BOP BuildingNum per Location)
----------------------------------------------------------------------------
WITH BOPLocationMinBuilding AS (
SELECT 
	pc_policyperiod.ID AS PolicyPeriodID
	,pc_policyperiod.EditEffectiveDate
	,pc_policylocation.FixedID AS PolicyLocationFixedID
	,MIN(pc_building.BuildingNum) as MinBuildingNum						
FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
	INNER JOIN `{project}.{pc_dataset}.pc_bopbuilding` AS pc_bopbuilding
		ON pc_bopbuilding.BranchID = pc_policyperiod.ID
	--Blow out to include all buildings for policy version / date segment / policy location
	INNER JOIN `{project}.{pc_dataset}.pc_building` AS pc_building
		ON pc_building.FixedID = pc_bopbuilding.Building
		AND pc_building.BranchID = pc_bopbuilding.BranchID
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_building.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_building.ExpirationDate,pc_policyperiod.PeriodEnd)
	--Blow out to include all policy locations for policy version / date segment
	INNER JOIN `{project}.{pc_dataset}.pc_policylocation` AS pc_policylocation
		ON pc_policylocation.FixedID = pc_building.PolicyLocation
		AND pc_policylocation.BranchID = pc_building.BranchID
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
WHERE 1 = 1
	AND pc_policyperiod._PARTITIONTIME = {partition_date}
	AND pc_bopbuilding._PARTITIONTIME = {partition_date}
	AND pc_building._PARTITIONTIME = {partition_date}
	AND pc_policylocation._PARTITIONTIME = {partition_date}
	--AND pc_policyperiod.PolicyNumber = @policynumber	--testing
GROUP BY
	pc_policyperiod.ID
	,pc_policyperiod.EditEffectiveDate
	,pc_policylocation.FixedID
),

BldgImprBreakdown AS (
		SELECT DISTINCT
			pc_buildingimpr.BranchID
			,pc_buildingimpr.Building
			,pc_buildingimpr.EffectiveDate
			,pc_buildingimpr.ExpirationDate
			,pctl_buildingimprtype.NAME||'Year' AS BldgImprName
			,pc_buildingimpr.YearAdded
		
		FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
			INNER JOIN `{project}.{pc_dataset}.pc_boplocation` AS pc_boplocation
				ON pc_boplocation.BranchID = pc_policyperiod.ID
			INNER JOIN `{project}.{pc_dataset}.pc_bopbuilding` AS pc_bopbuilding
				ON pc_bopbuilding.BranchID = pc_boplocation.BranchID
				AND pc_bopbuilding.BOPLocation = pc_boplocation.FixedID
			INNER JOIN `{project}.{pc_dataset}.pc_buildingimpr` AS pc_buildingimpr
				ON pc_buildingimpr.BranchID = pc_bopbuilding.BranchID
				AND pc_buildingimpr.Building = pc_bopbuilding.Building
				AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_buildingimpr.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_buildingimpr.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN `{project}.{pc_dataset}.pctl_buildingimprtype` AS pctl_buildingimprtype
				ON pctl_buildingimprtype.ID = pc_buildingimpr.BuildingImprType
				AND pc_buildingimpr.BuildingImprType <> 10002	--Other type
				AND pc_buildingimpr.YearAdded IS NOT NULL
		WHERE 1 = 1
			AND pc_policyperiod._PARTITIONTIME = {partition_date}
			AND pc_boplocation._PARTITIONTIME = {partition_date}
			AND pc_bopbuilding._PARTITIONTIME = {partition_date}
			AND pc_buildingimpr._PARTITIONTIME = {partition_date}

	) --AS RankedBldgImprResults;

	--This code is also used in RiskLocationBusinessOwners, so the two tables use SAME PublicID from SAME Table (pc_boplocation)
	--NOTE: If this CTE is referenced more than once you must convert to a Temp or Perm Temp table for BQ
,CTE_PolicyVerLOB_PrimaryRatingLocn AS
(
	SELECT 
		pc_policyperiod.ID AS PolicyPeriodID
		,pc_policyperiod.EditEffectiveDate AS SubEffectiveDate
		,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
		,MinimumBuilding.FixedID AS MinimumBuildingFixedID
		,MAX(CASE WHEN MinimumBuilding.BuildingNum = BOPLocationMinBuilding.MinBuildingNum AND pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryBuildingLocation
		,MAX(CASE WHEN MinimumBuilding.BuildingNum = BOPLocationMinBuilding.MinBuildingNum AND pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN MinimumBuilding.BuildingNum ELSE 0 END)	AS PrimaryRatingBuildingNumber

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
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID  
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS PrimaryPolicyLocation
			ON PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation 
			AND PrimaryPolicyLocation.BranchID = pc_effectivedatedfields.BranchID 
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		--Get Primary / Min BuildingNumber based on Primary Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopbuilding
			ON pc_bopbuilding.BranchID = pc_policyperiod.ID
		LEFT JOIN BOPLocationMinBuilding AS BOPLocationMinBuilding
			ON BOPLocationMinBuilding.PolicyPeriodID = pc_policyperiod.ID
			AND BOPLocationMinBuilding.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			AND BOPLocationMinBuilding.PolicyLocationFixedID = PrimaryPolicyLocation.FixedID
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_building` WHERE _PARTITIONTIME = {partition_date}) AS MinimumBuilding
			ON PrimaryPolicyLocation.FixedID = MinimumBuilding.PolicyLocation
			AND PrimaryPolicyLocation.BranchID = MinimumBuilding.BranchID
			AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(MinimumBuilding.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(MinimumBuilding.ExpirationDate,pc_policyperiod.PeriodEnd)
			AND MinimumBuilding.BuildingNum = BOPLocationMinBuilding.MinBuildingNum --Only want Min / Primary Building at loc

	--WHERE pc_policyperiod.PolicyNumber = ISNULL(@policynumber, pc_policyperiod.PolicyNumber)	--Testing
	GROUP BY
		pc_policyperiod.ID
		,pc_policyperiod.EditEffectiveDate
		,pctl_policyline.TYPECODE
		,MinimumBuilding.FixedID
)

, RiskBuildingConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  --SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','BusinessOwnersLine' UNION ALL
  SELECT 'LocationLevelRisk','BusinessOwnersLocation' UNION ALL
  SELECT 'BuildingLevelRisk','BusinessOwnersBuilding'
)
SELECT 
	SourceSystem
	,RiskBuildingKey
	,RiskLocationKey
	,PolicyTransactionKey
	,BuildingFixedID
	,BuildingPublicID
	,LocationPublicID
	,LocationLevelRisk
	,BuildingLevelRisk
	,PolicyPeriodPublicID
	--,EditEffectiveDate
	--,PolicyNumber
	,JobNumber
	,EffectiveDate
	,ExpirationDate
	--,PrimaryRatingBuildingNumber
	,IsPrimaryBuildingLocation
	,BuildingNumber
	,BuildingDescription
	,BuildingProtectionClassCode
	,LocationClassPredominantBldgOccupancyCode
	,LocationClassPredominantBldgOccupancyClass
	,LocationClassInsuredBusinessCode
	,LocationClassInsuredBusinessClass
	,LocationEffectiveDate
	,LocationExpirationDate
	,BldgCodeEffectivenessGrade
	,BldgLimitToValuePercent
	,BPPLimitToValuePercent
	,IsTheftExclusion
	,IsBrandsAndLabels
	,IsWindOrHail
	,PublicUtilities
	,BuildingClass
	,EQTerritory
	,PremiumBasisAmount
	,ConstructionCode
	,ConstructionType
	,ConstructionYearBuilt
	,NumberOfFloors
	,TotalBuildngAreaSQFT
	,AreaOccupiedSQFT
	,Basement
	,IsBasementFinished
	,RoofMaterial
	,SmokeDetectors
	,PercentSprinklered
	,HeatingYear
	,PlumbingYear
	,RoofingYear
	,WiringYear
	,LastBldgInspectionDate
	,LastBldgValidationDate
	,PercentOccupied
	,AdjacentOccupancies
	,SharedPremises
	,PremisesSharedWith
	,BldgHandlePawnPropOtherJwlry
	,BldgNatlPawnAssociation
	,BldgMemberOtherPawnAssociations
	,BldgHavePawnLocalLicense
	,BldgHavePawnStateLicense
	,BldgHavePawnFederalLicense
	,BldgPawnLicenseAreYouBounded
	,BldgTotalAnnualSales
	,BldgSalesPawnPercent
	,BldgSalesRetailPercent
	,BldgSalesCheckCashingPercent
	,BldgSalesGunsAndAmmunitionPercent
	,BldgSalesAutoPawnPercent
	,BldgSalesTitlePawnPercent
	,BldgSalesOtherPercent
	,BldgSalesOtherPercentDescription
	,BldgAvgDailyAmtOfNonJewelryPawnInventory
	,BldgReportTransactionsRealTime
	,BldgReportTransactionsDaily
	,BldgReportTransactionsWeekly
	,BldgReportTransactionsOther
	,BldgReportTransactionsOtherDesc
	,BldgLoanPossessOrSellFireArms
	,BldgFedFirearmsDealerLicense
	,BldgTypesFedFirearmsLicenses
	,BldgFirearmsDisplayLockAndKey
	,BldgHowSecureLongGuns
	,BldgHowSecureOtherFirearms
	,BldgShootingRangeOnPrem
	,BldgHowSafeguardAmmoGunPowder
	,BuildingAddlCommentsAboutBusiness
	,PrimaryRatingBuildingFixedID
	,FixedBuildingRank
	,IsTransactionSliceEffective
    ,DATE('{date}') as bq_load_date		
	
FROM (
		SELECT 
			sourceConfig.Value AS SourceSystem
			--SK For PK [<Source>_<BuildingPublicID>]
			,CASE WHEN BuildingPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BuildingPublicID, hashKeySeparator.Value, BuildingLevelRisk)) END AS RiskBuildingKey
			--SK For FK [<Source>_<LocationPublicID>_<Level>]
			,CASE WHEN LocationPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,LocationPublicID, hashKeySeparator.Value, LocationLevelRisk)) END AS RiskLocationKey
			--SK For FK [<Source>_<PolicyPeriodPublicID>]
			,CASE WHEN PolicyPeriodPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) END AS PolicyTransactionKey
			,DENSE_RANK() OVER(PARTITION BY	RiskBuilding.BuildingFixedID
									,RiskBuilding.PolicyPeriodPublicID
									,RiskBuilding.BuildingLevelRisk
						ORDER BY	IsTransactionSliceEffective DESC
									,RiskBuilding.FixedBuildingRank
					) AS FixedBuildingRank
			
			,RiskBuilding.BuildingFixedID
			,RiskBuilding.BuildingRank
			,RiskBuilding.CostRank
			,RiskBuilding.BuildingPublicID
			,RiskBuilding.LocationPublicID
			,RiskBuilding.LocationLevelRisk
			,RiskBuilding.BuildingLevelRisk
			,RiskBuilding.PolicyPeriodPublicID
			--,RiskBuilding.EditEffectiveDate
			--,RiskBuilding.PolicyNumber
			,RiskBuilding.JobNumber
			,RiskBuilding.EffectiveDate
			,RiskBuilding.ExpirationDate
			,RiskBuilding.LocationEffectiveDate
			,RiskBuilding.LocationExpirationDate
			,RiskBuilding.IsTransactionSliceEffective

			-- Building Information
			,RiskBuilding.BuildingNumber
			,RiskBuilding.BuildingDescription
			,RiskBuilding.BuildingProtectionClassCode
			--,RiskBuilding.PrimaryRatingBuildingNumber
			,RiskBuilding.IsPrimaryBuildingLocation
			,RiskBuilding.PrimaryRatingBuildingFixedID
			,RiskBuilding.LocationClassPredominantBldgOccupancyCode
			,RiskBuilding.LocationClassPredominantBldgOccupancyClass
			,RiskBuilding.LocationClassInsuredBusinessCode
			,RiskBuilding.LocationClassInsuredBusinessClass
			,RiskBuilding.BldgCodeEffectivenessGrade
			,RiskBuilding.BldgLimitToValuePercent
			,RiskBuilding.BPPLimitToValuePercent
			,RiskBuilding.IsTheftExclusion
			,RiskBuilding.IsBrandsAndLabels
			,RiskBuilding.IsWindOrHail
			,RiskBuilding.PublicUtilities
			,RiskBuilding.BuildingClass
			,RiskBuilding.EQTerritory
			--,PremiumBasisType
			,RiskBuilding.PremiumBasisAmount

			-- Building Construction
			,RiskBuilding.ConstructionCode
			,RiskBuilding.ConstructionType
			,RiskBuilding.ConstructionYearBuilt
			,RiskBuilding.NumberOfFloors
			,RiskBuilding.TotalBuildngAreaSQFT
			,RiskBuilding.AreaOccupiedSQFT
			,RiskBuilding.Basement
			,RiskBuilding.IsBasementFinished
			,RiskBuilding.RoofMaterial
			,RiskBuilding.SmokeDetectors
			,RiskBuilding.PercentSprinklered
		
			-- Building Improvement
			,RiskBuilding.HeatingYear
			,RiskBuilding.PlumbingYear
			,RiskBuilding.RoofingYear
			,RiskBuilding.WiringYear	
			,RiskBuilding.LastBldgInspectionDate
			,RiskBuilding.LastBldgValidationDate

			-- Occupancy Information
			,RiskBuilding.PercentOccupied
			--,occupancy
			,RiskBuilding.AdjacentOccupancies
			,RiskBuilding.SharedPremises
			,RiskBuilding.PremisesSharedWith

			-- Pawn Questions
			,RiskBuilding.BldgHandlePawnPropOtherJwlry
			,RiskBuilding.BldgNatlPawnAssociation
			,RiskBuilding.BldgMemberOtherPawnAssociations
			,RiskBuilding.BldgHavePawnLocalLicense 
			,RiskBuilding.BldgHavePawnStateLicense
			,RiskBuilding.BldgHavePawnFederalLicense
			,RiskBuilding.BldgPawnLicenseAreYouBounded
			,RiskBuilding.BldgTotalAnnualSales
			,RiskBuilding.BldgSalesPawnPercent
			,RiskBuilding.BldgSalesRetailPercent
			,RiskBuilding.BldgSalesCheckCashingPercent
			,RiskBuilding.BldgSalesGunsAndAmmunitionPercent
			,RiskBuilding.BldgSalesAutoPawnPercent
			,RiskBuilding.BldgSalesTitlePawnPercent
			,RiskBuilding.BldgSalesOtherPercent
			,RiskBuilding.BldgSalesOtherPercentDescription
			,RiskBuilding.BldgAvgDailyAmtOfNonJewelryPawnInventory
			,RiskBuilding.BldgReportTransactionsRealTime
			,RiskBuilding.BldgReportTransactionsDaily
			,RiskBuilding.BldgReportTransactionsWeekly
			,RiskBuilding.BldgReportTransactionsOther
			,RiskBuilding.BldgReportTransactionsOtherDesc
			,RiskBuilding.BldgLoanPossessOrSellFireArms
			,RiskBuilding.BldgFedFirearmsDealerLicense
			,RiskBuilding.BldgTypesFedFirearmsLicenses
			,RiskBuilding.BldgFirearmsDisplayLockAndKey
			,RiskBuilding.BldgHowSecureLongGuns
			,RiskBuilding.BldgHowSecureOtherFirearms
			,RiskBuilding.BldgShootingRangeOnPrem
			,RiskBuilding.BldgHowSafeguardAmmoGunPowder
			,RiskBuilding.BuildingAddlCommentsAboutBusiness 

			---added for testing
			--,RiskBuilding.PeriodStart
			--,RiskBuilding.PeriodEnd

		FROM (
			SELECT 
				DENSE_RANK() OVER(PARTITION BY	pc_bopbuilding.ID
								ORDER BY IFNULL(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC -- 02/07/2023
							) AS BuildingRank
				,DENSE_RANK() OVER(PARTITION BY pc_bopbuilding.ID
								ORDER BY IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
												,pc_bopcost.ID DESC
							) AS CostRank
				,DENSE_RANK() OVER(PARTITION BY pc_bopbuilding.FixedID, pc_policyperiod.ID
								ORDER BY IFNULL(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS FixedBuildingRank

				,pc_bopbuilding.PublicID								AS BuildingPublicID
				,pc_boplocation.PublicID								AS LocationPublicID
				,ConfigBOPLocationRisk.Value							AS LocationLevelRisk
				,ConfigBOPBuildingRisk.Value							AS BuildingLevelRisk
				,pc_policyperiod.PublicID								AS PolicyPeriodPublicID		
				--,pc_policyperiod.EditEffectiveDate
				--,pc_policyperiod.PolicyNumber							AS PolicyNumber
				,pc_job.JobNumber										AS JobNumber	
				,pc_bopbuilding.EffectiveDate							AS EffectiveDate
				,pc_bopbuilding.ExpirationDate							AS ExpirationDate
				,pc_boplocation.EffectiveDate							AS LocationEffectiveDate
				,pc_boplocation.ExpirationDate							AS LocationExpirationDate
				--,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart)
				--		AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) 
				--		THEN 1 ELSE 0 
				--	END AS IsTransactionSliceEffective
				,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart)
							AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_bopbuilding.ExpirationDate,pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)  
							 AND pc_boplocation.BOPLine = pc_policyline.FixedID -- 03/29/2023
						THEN 1 ELSE 0 
					END AS IsTransactionSliceEffective -- 02/07/2023

				-- Building Information
				,pc_building.BuildingNum								AS BuildingNumber
				,pc_building.Description								AS BuildingDescription
				,pctl_fireprotectclass.TYPECODE							AS BuildingProtectionClassCode
				,pc_bopbuilding.FixedID									AS BuildingFixedID

				--For BOP LOB, set building attributes based on building associated WITH Primary Location (choose min Building Number)
				--Based on Minimum Building at Primary Location for BOP; otherwise N/A
				--,CTE_PolicyVerLOB_PrimaryRatingLocn.PrimaryRatingBuildingNumber				AS PrimaryRatingBuildingNumber
				,COALESCE(CTE_PolicyVerLOB_PrimaryRatingLocn.IsPrimaryBuildingLocation,'N') AS IsPrimaryBuildingLocation
				,CTE_PolicyVerLOB_PrimaryRatingLocn.MinimumBuildingFixedID					AS PrimaryRatingBuildingFixedID
				--,PrimaryRatingPolicyLocation.FixedID					AS PrimaryLocationFixedID
				,pc_bopclasscode.Code									AS LocationClassPredominantBldgOccupancyCode
				,pc_bopclasscode.Classification							AS LocationClassPredominantBldgOccupancyClass
				,pcBOPInsClassCode.Code									AS LocationClassInsuredBusinessCode
				,pcBOPInsClassCode.Classification						AS LocationClassInsuredBusinessClass
				,pctl_bopbuildingeffgrade_jmic.NAME 					AS BldgCodeEffectivenessGrade
				,pctlBldgLimitToValue.NAME								AS BldgLimitToValuePercent
				,pctlBPPLimitToValue.NAME								AS BPPLimitToValuePercent
				,CAST(pc_bopbuilding.TheftExclIndicator_JMIC AS INT64)	AS IsTheftExclusion
				,CAST(pc_bopbuilding.BrandsLabelsInd_JMIC AS INT64)		AS IsBrandsAndLabels
				,CAST(pc_bopbuilding.WindHailIndicator_JMIC AS INT64)	AS IsWindOrHail
				,pctl_publicutilities_jmic.NAME							AS PublicUtilities
				,pctl_buildingclass_jmic.NAME							AS BuildingClass
				,EQTerritoryCode_JMIC									AS EQTerritory
				--,PremiumBasisType
				,pc_bopbuilding.BasisAmount								AS PremiumBasisAmount

				-- Building Construction
				,pctl_bopconstructiontype.TYPECODE							AS ConstructionCode
				,pctl_bopconstructiontype.DESCRIPTION						AS ConstructionType
				,pc_building.YearBuilt										AS ConstructionYearBuilt
				,pc_building.NumStories										AS NumberOfFloors
				,pc_building.TotalArea										AS TotalBuildngAreaSQFT
				,pcx_commonbuilding_jmic.BOPAreaOccupied_JMIC				AS AreaOccupiedSQFT
				,CAST(pcx_commonbuilding_jmic.Basement_JMIC AS INT64)		AS Basement
				,CAST(pc_bopbuilding.IsBasementFinished_JMIC AS INT64)		AS IsBasementFinished
				,pc_bopbuilding.RoofMaterial_JMIC							AS RoofMaterial
				,CAST(pcx_commonbuilding_jmic.SmokeDetectors_JMIC AS INT64)	AS SmokeDetectors
				,pctl_sprinklered.NAME										AS PercentSprinklered
		
				-- Building Improvement
				,cBldgImpr.HeatingYear									AS HeatingYear
				,cBldgImpr.PlumbingYear									AS PlumbingYear
				,cBldgImpr.RoofingYear									AS RoofingYear
				,cBldgImpr.WiringYear									AS WiringYear	
				,pc_bopbuilding.LastBuiInsDate_JMIC						AS LastBldgInspectionDate
				,pc_bopbuilding.LastBuiValDate_JMIC						AS LastBldgValidationDate

				-- Occupancy Information
				,pc_building.PercentOccupied							AS PercentOccupied
				--,occupancy
				,AdjacentOccupancies_JMIC								AS AdjacentOccupancies
				,CAST(SharedPremises_JMIC AS INT64)						AS SharedPremises
				,PremisesSharedWith_JMIC								AS PremisesSharedWith

				-- Pawn Questions
				,CAST(pcx_bopundinformation_jmic.UIHanPawProOthJew AS INT64)			AS BldgHandlePawnPropOtherJwlry -- Handle Pawn Property other than jewelry pawn? 
				,CAST(pcx_bopundinformation_jmic.UINatPawAssociation	 AS INT64)		AS BldgNatlPawnAssociation -- Are you member of national pawnbroking association? 
				,pcx_bopundinformation_jmic.UIMemberOtherPawnAssociations				AS BldgMemberOtherPawnAssociations  -- member of Other pawnbroking association (names)
				,CAST(pcx_bopundinformation_jmic.UIHaveLocalLicense AS INT64)			AS BldgHavePawnLocalLicense 
				,CAST(pcx_bopundinformation_jmic.UIHaveStateLicense AS INT64)			AS BldgHavePawnStateLicense 
				,CAST(pcx_bopundinformation_jmic.UIHaveFederalLicense AS INT64)			AS BldgHavePawnFederalLicense
				,CAST(pcx_bopundinformation_jmic.UIAreYouBounded	 AS INT64)			AS BldgPawnLicenseAreYouBounded 
				,pcx_bopundinformation_jmic.UITotalAnnualSales							AS BldgTotalAnnualSales 
				,pcx_bopundinformation_jmic.UISalesPawnPercent							AS BldgSalesPawnPercent 
				,pcx_bopundinformation_jmic.UISalesRetailPercent						AS BldgSalesRetailPercent 
				,pcx_bopundinformation_jmic.UISalesCheckCashingPercent					AS BldgSalesCheckCashingPercent 
				,pcx_bopundinformation_jmic.UISalesGunsAndAmmoPercent					AS BldgSalesGunsAndAmmunitionPercent 
				,pcx_bopundinformation_jmic.UISalesAutoPawnPercent						AS BldgSalesAutoPawnPercent 
				,pcx_bopundinformation_jmic.UISalesTitlePawnPercent						AS BldgSalesTitlePawnPercent 
				,pcx_bopundinformation_jmic.UISalesOtherPercent							AS BldgSalesOtherPercent 
				,pcx_bopundinformation_jmic.UISalesOtherPercentDescription				AS BldgSalesOtherPercentDescription 
				,pcx_bopundinformation_jmic.UIAverageDailyAmount						AS BldgAvgDailyAmtOfNonJewelryPawnInventory 
				,CAST(pcx_bopundinformation_jmic.UIReportTransactionsRealTime AS INT64)	AS BldgReportTransactionsRealTime --how often do you report to local authorities?
				,CAST(pcx_bopundinformation_jmic.UIReportTransactionsDaily AS INT64)	AS BldgReportTransactionsDaily --how often do you report to local authorities?
				,CAST(pcx_bopundinformation_jmic.UIReportTransactionsWeekly AS INT64)	AS BldgReportTransactionsWeekly --how often do you report to local authorities?
				,CAST(pcx_bopundinformation_jmic.UIReportTransactionsOther AS INT64)	AS BldgReportTransactionsOther --how often do you report to local authorities?
				,pcx_bopundinformation_jmic.UIReportTransOtherDesc						AS BldgReportTransactionsOtherDesc 
				,CAST(pcx_bopundinformation_jmic.UILoaPosOfSelFireArms AS INT64)		AS BldgLoanPossessOrSellFireArms -- Do you loan on, take possession of, or sell firearms?
				,CAST(pcx_bopundinformation_jmic.UIFedFireDeaLicense AS INT64)			AS BldgFedFirearmsDealerLicense -- Do you have a federal firearms dealer license?
				,pcx_bopundinformation_jmic.UITypesFederalFirearmsLicenses				AS BldgTypesFedFirearmsLicenses -- What type(s) of federal firearms licenses do you hold?
				,CAST(pcx_bopundinformation_jmic.UIFirearmsDisplayLockAndKey AS INT64)	AS BldgFirearmsDisplayLockAndKey -- Are firearms displayed under lock and key when closed to business?
				,pcx_bopundinformation_jmic.UIHowSecureLongGuns							AS BldgHowSecureLongGuns -- How do you secure your long guns when closed to business?
				,pcx_bopundinformation_jmic.UIHowSecureOtherFirearms					AS BldgHowSecureOtherFirearms -- How do you secure all other firearms when closed to business?
				,CAST(pcx_bopundinformation_jmic.UIShootingRangeOnPrem	 AS INT64)		AS BldgShootingRangeOnPrem -- Is there a shooting range on premises to test firearms which are for sale?
				,pcx_bopundinformation_jmic.UIHowSafeguardAmmoGunPowder					AS BldgHowSafeguardAmmoGunPowder -- If ammunition or gun powder is sold, how is it safeguarded?
				,pcx_bopundinformation_jmic.UIAddComYouBus								AS BuildingAddlCommentsAboutBusiness -- Additional Comments about your business

			FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
				INNER JOIN `{project}.{pc_dataset}.pc_job` AS pc_job 
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN `{project}.{pc_dataset}.pc_bopbuilding` AS pc_bopbuilding
					ON pc_bopbuilding.BranchID = pc_policyperiod.ID
				INNER JOIN `{project}.{pc_dataset}.pc_policyline` AS pc_policyline
					ON pc_policyline.BranchID = pc_policyperiod.ID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.SubType
				INNER JOIN RiskBuildingConfig AS ConfigBOPLine ON ConfigBOPLine.Value = pctl_policyline.TYPECODE AND ConfigBOPLine.Key = 'LineCode' 
				INNER JOIN RiskBuildingConfig AS ConfigBOPBuildingRisk ON ConfigBOPBuildingRisk.Key='BuildingLevelRisk'
				INNER JOIN RiskBuildingConfig AS ConfigBOPLocationRisk ON ConfigBOPLocationRisk.Key='LocationLevelRisk'
				INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` AS pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
				
				LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation -- 02/07/2023
					ON pc_boplocation.BranchID = pc_bopbuilding.BranchID
					AND pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
					AND pc_boplocation.BOPLine = pc_policyline.FixedID
					--AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
					--AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
					--AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart)
					--AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) -- 03/29/2023

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost 
					ON pc_bopcost.BranchID = pc_policyperiod.ID
					AND pc_bopcost.BOPLocation = pc_boplocation.FixedID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_building` WHERE _PARTITIONTIME = {partition_date}) AS pc_building
					ON pc_building.BranchID = pc_bopbuilding.BranchID
					AND pc_building.FixedID = pc_bopbuilding.Building
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_building.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_building.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				-- "Revisioned" location for BOP Location
				LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
					ON pc_policylocation.FixedID = pc_boplocation.Location
					AND pc_policylocation.BranchID = pc_boplocation.BranchID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` AS pctl_fireprotectclass
					ON pc_policylocation.FireProtectClass = pctl_fireprotectclass.ID
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonbuilding_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_commonbuilding_jmic
					ON pcx_commonbuilding_jmic.BranchID = pc_bopbuilding.BranchID
					AND pcx_commonbuilding_jmic.FixedID = pc_bopbuilding.CommonBuilding
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonbuilding_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonbuilding_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
					ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
				
			--	LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			--		ON  pc_policylocation.BranchID = pc_policyperiod.ID
			--		AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			--		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			--		AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				--Join in the CTE_PolicyVerLOB_PrimaryRatingLocn Table to map the Natural Key for RatingLocationKey		
				LEFT JOIN CTE_PolicyVerLOB_PrimaryRatingLocn AS CTE_PolicyVerLOB_PrimaryRatingLocn
					ON CTE_PolicyVerLOB_PrimaryRatingLocn.PolicyPeriodID = pc_policyperiod.ID
					AND CTE_PolicyVerLOB_PrimaryRatingLocn.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
					--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
					AND ((CTE_PolicyVerLOB_PrimaryRatingLocn.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
					or 
						(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and CTE_PolicyVerLOB_PrimaryRatingLocn.PolicyLineOfBusiness = 'BusinessOwnersLine'))
					AND CTE_PolicyVerLOB_PrimaryRatingLocn.MinimumBuildingFixedID = pc_building.FixedID

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopriskinformation_JMIC` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopriskinformation_JMIC
					ON pcx_bopriskinformation_JMIC.BranchID = pc_bopbuilding.BranchID
					AND pcx_bopriskinformation_JMIC.FixedID = pc_bopbuilding.RiskInformation_JMIC
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_bopriskinformation_JMIC.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_bopriskinformation_JMIC.ExpirationDate,pc_policyperiod.PeriodEnd)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopundinformation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopundinformation_jmic
					ON pcx_bopundinformation_jmic.BranchID = pc_bopbuilding.BranchID
					AND pcx_bopundinformation_jmic.FixedID = pc_bopbuilding.UndInformation_JMIC
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_bopundinformation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_bopundinformation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
				
				LEFT JOIN `{project}.{pc_dataset}.pctl_bopconstructiontype` AS pctl_bopconstructiontype
					ON pctl_bopconstructiontype.ID = pcx_commonbuilding_jmic.ConstructionType
				LEFT JOIN `{project}.{pc_dataset}.pctl_sprinklered` AS pctl_sprinklered
					ON pctl_sprinklered.ID = pc_building.SprinklerCoverage
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopclasscode` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopclasscode
					ON pc_bopclasscode.ID = pc_bopbuilding.ClassCodeID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopclasscode.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopclasscode.ExpirationDate,pc_policyperiod.PeriodEnd)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopclasscode` WHERE _PARTITIONTIME = {partition_date}) AS pcBOPInsClassCode
					ON pcBOPInsClassCode.ID = pcx_commonbuilding_jmic.BOPLocInsBusCCID
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcBOPInsClassCode.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_bopbuilding.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcBOPInsClassCode.ExpirationDate,pc_policyperiod.PeriodEnd)
				LEFT JOIN `{project}.{pc_dataset}.pctl_publicutilities_jmic` AS pctl_publicutilities_jmic
					ON pctl_publicutilities_jmic.ID = pc_bopbuilding.PublicUtilities_JMIC
				LEFT JOIN `{project}.{pc_dataset}.pctl_buildingclass_jmic` AS pctl_buildingclass_jmic
					ON pctl_buildingclass_jmic.ID = pcx_commonbuilding_jmic.BuildingClass_JMIC
				LEFT JOIN `{project}.{pc_dataset}.pctl_bopbuildingeffgrade_jmic` AS pctl_bopbuildingeffgrade_jmic
					ON pctl_bopbuildingeffgrade_jmic.ID = pc_bopbuilding.BOPBuildingEffGrade_JMIC
				LEFT JOIN `{project}.{pc_dataset}.pctl_limittovalueprcnt_jmic` AS pctlBldgLimitToValue
					ON pctlBldgLimitToValue.ID = pc_bopbuilding.BldgLimitToValuePct_JMIC
				LEFT JOIN `{project}.{pc_dataset}.pctl_limittovalueprcnt_jmic` AS pctlBPPLimitToValue
					ON pctlBPPLimitToValue.ID = pc_bopbuilding.BPPLimitToValuePct_JMIC

				LEFT JOIN 
					(	SELECT 
							BldgImprBreakdown.BranchID
							,BldgImprBreakdown.Building
							,MAX(CASE WHEN BldgImprName = 'HeatingYear' THEN BldgImprBreakdown.YearAdded END) AS HeatingYear
							,MAX(CASE WHEN BldgImprName = 'PlumbingYear' THEN BldgImprBreakdown.YearAdded END) AS PlumbingYear
							,MAX(CASE WHEN BldgImprName = 'RoofingYear' THEN BldgImprBreakdown.YearAdded END) AS RoofingYear
							,MAX(CASE WHEN BldgImprName = 'WiringYear' THEN BldgImprBreakdown.YearAdded END) AS WiringYear

						FROM BldgImprBreakdown AS BldgImprBreakdown						
						GROUP BY BldgImprBreakdown.BranchID
								,BldgImprBreakdown.Building
					)	AS cBldgImpr
				ON cBldgImpr.BranchID = pc_bopbuilding.BranchID
				AND cBldgImpr.Building = pc_bopbuilding.Building

			WHERE 1 = 1
			AND pc_policyperiod._PARTITIONTIME = {partition_date}
			AND pc_job._PARTITIONTIME = {partition_date}
			AND pc_bopbuilding._PARTITIONTIME = {partition_date}
			AND pc_policyline._PARTITIONTIME = {partition_date}

			/**** TEST *****/
			--AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)


		) RiskBuilding

			INNER JOIN RiskBuildingConfig AS sourceConfig
				ON sourceConfig.Key='SourceSystem'
			INNER JOIN RiskBuildingConfig AS hashKeySeparator
				ON hashKeySeparator.Key='HashKeySeparator'
			--INNER JOIN RiskBuildingConfig AS hashAlgorithm ON hashAlgorithm.Key='HashingAlgorithm'

		WHERE 1 = 1
		AND RiskBuilding.BuildingRank = 1
		AND RiskBuilding.CostRank = 1

	) outerselect
--) extractdata
