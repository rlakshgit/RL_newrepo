-- tag: RiskLocationIM - tag ends/			
/********************************************************************************************************************************
	KIMBERLITE
		RiskLocationIM.sql
			Converted to BQ
				Contains Pivots, Temp Tables
*********************************************************************************************************************************/
/*
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/08/2021	DAROBAK		Cost Join fix applied (now a left join)
	04/07/2021	JM / DR		Field rename, cleanup
	04/22/2021	DROBAK		Added pcx_ilmlocation_jmic.EffectiveDate and ExpirationDate
	05/26/2021	DROBAK		Added fixes for dupes to Pivot and MAX() related sub-selects
	05/27/2021	DROBAK		Added IsTransactionSliceEffective field for use in BB layer to select Policy Slice Eff records
	05/27/2021	DROBAK		Added Primary Location
	06/08/2021	DROBAK		Turn Inner to Left Joins for pcx_commonlocation_jmic and pc_policylocation; adjust DG Check Missing
	08/05/2022	DROBAK		Added Line/LOB unioned section and to config table
	08/15/2022	DROBAK		Remove: PrimaryLocationNumber; Add: IsPrimaryLocation (converted number to flag field for easier analysis)
							Added Primary Rating Location code to get LocationPublicID from pcx_ilmlocation_jmic
	09/06/2022	DROBAK		Comment out the Primary Location section since it is now redundant (included in CTE)
	09/30/2022	DROBAK		Converted IsPrimaryLocation to a STRING/VARCHAR to be compatible with other tables
	12/20/2022	DROBAK/SLJ	Corrected join for Location Number for the LocationRiskLevel section
	12/21/2022	DROBAK		Added CASE Stmnt for IS NULL checks for the two Primary Key fields; Add PolicyPeriodStatus
	04/27/2023	SLJ			Remove LOB/Line Level UNION
							Remove RiskLevel from inner to outer query join statement as every level need to be location
							Change cte for primary location to t_PrimaryRatingLocationIM. Rating Location logic modified.
							IsPrimaryLocation flag removed from primary location cte and added to select statement directly comparing the location numbers
--------------------------------------------------------------------------------------------
-- T - Details
	-- ilm location additional interests(Job:1093622)
	-- Underwriting validation method in details
-- T - Coverages (check if available in kimberlite coverage tables)
-- T - Sub-Coverages (check if available in kimberlite coverage tables)
-- T - Security Information
	-- Alarm Information
	-- Loss Prevention Mechanisms
-- T - Hazard Categories
	-- Choice code lookup
	-- Hazard Code
-- T - Safes, Vaults, and Stockrooms
	-- Safe type and details
-- T - Exclusions & Conditions
	-- Exclusions: Manage Warranties available for patterncode ILMLocExclBrglJwlWtchSpec_JMIC and ILMLocExclBrglJwlySpecAmt_JMIC (205057)
	-- Policy Conditions: Manage Warranties and Manage Schedules (Refer to Excel sheet for codes that are applicable)
-- T - Deductible Provision (check if available in kimberlite coverage tables)
-- T - Location-Coverage Questions (complete)
----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_RiskLocationIM`
AS SELECT outerquery.*
FROM (
		--with
) outerquery;
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

----------------------------------------------------------------------------
---BldgImprBreakdown
----------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE BldgImprBreakdown
AS SELECT *
FROM (
		SELECT DISTINCT
			pc_buildingimpr.BranchID
			,pc_buildingimpr.Building
			,pc_buildingimpr.EffectiveDate
			,pc_buildingimpr.ExpirationDate
			,pctl_buildingimprtype.NAME || 'Year' AS BldgImprName
			,pc_buildingimpr.YearAdded
		
		FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON pc_boplocation.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding`  WHERE _PARTITIONTIME = {partition_date}) AS pc_bopbuilding
				ON pc_bopbuilding.BranchID = pc_boplocation.BranchID
				AND pc_bopbuilding.BOPLocation = pc_boplocation.FixedID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_buildingimpr`  WHERE _PARTITIONTIME = {partition_date}) AS pc_buildingimpr
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

	) AS RankedBldgImprResults;

----------------------------------------------------------------------------
---RiskLocation
----------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE RiskLocationInfo
AS SELECT *
FROM
(
	SELECT 
		pcx_ilmlocation_jmic.PublicID									AS IMLocationPublicID
		,pc_policyperiod.EditEffectiveDate	
		,pc_policyperiod.PeriodStart	
		,pc_policyperiod.PeriodEnd
		,pcx_ilmlocation_jmic.BranchID
		,pcx_ilmlocation_jmic.FixedID									AS IMLocationFixedID
		,pc_policylocation.FixedID										AS PolicyLocationFixedID

	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
		INNER JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_commonlocation_jmic
			ON  pcx_commonlocation_jmic.BranchID = pcx_ilmlocation_jmic.BranchID
			AND pcx_commonlocation_jmic.FixedID = pcx_ilmlocation_jmic.CommonLocation
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = pcx_ilmlocation_jmic.BranchID
			AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

	WHERE 1=1
		AND pc_policyperiod._PARTITIONTIME = {partition_date}

 UNION DISTINCT

 	SELECT DISTINCT 
		pcx_ilmlocation_jmic.PublicID									AS IMLocationPublicID
		,pc_policyperiod.EditEffectiveDate
		,pc_policyperiod.PeriodStart	
		,pc_policyperiod.PeriodEnd
		,pc_policylocation.BranchID
		,pcx_ilmlocation_jmic.FixedID									AS IMLocationFixedID
		,pc_policylocation.FixedID										AS PolicyLocationFixedID
	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID		
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID			
			AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'
		--Join in the t_PrimaryRatingLocationIM Table to map the Natural Key for RatingLocationKey		
		LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationIM` AS t_PrimaryRatingLocationIM
			ON t_PrimaryRatingLocationIM.PolicyPeriodID = pc_policyperiod.ID
			AND t_PrimaryRatingLocationIM.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((t_PrimaryRatingLocationIM.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				OR 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationIM.PolicyLineOfBusiness = 'BusinessOwnersLine'))
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = t_PrimaryRatingLocationIM.PolicyPeriodID  
			AND pc_policylocation.LocationNum = t_PrimaryRatingLocationIM.RatingLocationNum  
			--ON  pc_policylocation.BranchID = pc_policyperiod.ID
			--AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
			AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_commonlocation_jmic
			ON  pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_commonlocation_jmic.FixedID = pcx_ilmlocation_jmic.CommonLocation
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	WHERE 1=1
		AND pc_policyperiod._PARTITIONTIME = {partition_date}

) AS RiskLocationResults;

			
CREATE OR REPLACE TEMP TABLE RiskLocationIMPivot
AS
WITH RiskLocationIMConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'IMLineCode','ILMLine_JMIC' UNION ALL
  SELECT 'BOPLineCode','BusinessOwnersLine' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
  SELECT 'LOBLevelRisk','Line' UNION ALL
  SELECT 'IMLocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'BOPLocationLevelRisk','BusinessOwnersLocation' UNION ALL
  SELECT 'IMHazardCategory','ILMLocHazardCodes_JMIC' UNION ALL
  SELECT 'IMLocationCoverageQuestion','BOPHomeBasedBusinessQuestion_JMIC'
) 
SELECT * FROM (
				SELECT 
					RiskLocationInfo.IMLocationPublicID
					,pc_locationanswer.QuestionCode
					,COALESCE(CAST(pc_locationanswer.BooleanAnswer AS STRING)
								,CAST(pc_locationanswer.TextAnswer AS STRING)
								,CAST(pc_locationanswer.IntegerAnswer AS STRING)
								,CAST(pc_locationanswer.DateAnswer AS STRING)
								,CAST(pc_locationanswer.ChoiceAnswerCode AS STRING)) AS Answer
					
				FROM RiskLocationInfo AS RiskLocationInfo
					INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_locationanswer` WHERE _PARTITIONTIME = {partition_date} ) AS pc_locationanswer
						ON pc_locationanswer.BranchID = RiskLocationInfo.BranchID
						AND pc_locationanswer.PolicyLocation = RiskLocationInfo.PolicyLocationFixedID
						AND RiskLocationInfo.EditEffectiveDate >= COALESCE(pc_locationanswer.EffectiveDate,RiskLocationInfo.PeriodStart)
						AND RiskLocationInfo.EditEffectiveDate <  COALESCE(pc_locationanswer.ExpirationDate,RiskLocationInfo.PeriodEnd)
					INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_questionlookup` WHERE _PARTITIONTIME = {partition_date} ) AS pc_questionlookup
						ON pc_questionlookup.QuestionCode = pc_locationanswer.QuestionCode
					INNER JOIN RiskLocationIMConfig AS ConfigHC
						ON ConfigHC.Key = 'IMHazardCategory' 
					INNER JOIN RiskLocationIMConfig AS ConfigCQ
						ON ConfigCQ.Key = 'IMLocationCoverageQuestion' 
					
				WHERE 1=1
				AND	(pc_questionlookup.SourceFile like '%'||ConfigHC.Value||'%' OR pc_questionlookup.SourceFile like '%'||ConfigCQ.Value||'%')
				
			)t;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskLocationIMPivot' # source table
  ,'{project}.{dest_dataset}.t_RiskLocationIM_HazardCat_LocAnsPivot' # destination table
  , ['IMLocationPublicID'] # row_ids
  , 'QuestionCode'-- # pivot_col_name
  , 'Answer'--# pivot_col_value
  , 29--# max_columns
  , 'MAX' --# aggregation
  , '' --# optional_limit
);
DROP table RiskLocationIMPivot;		

/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.DAR_RiskLocationIM`
AS SELECT outerquery.*
FROM (
*/
--DELETE `{project}.{dest_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date});
INSERT INTO `{project}.{dest_dataset}.RiskLocationIM`
	( 
		SourceSystem,	
		RiskLocationKey,
		PolicyTransactionKey,	
		FixedLocationRank,
		IsTransactionSliceEffective,
		LocationPublicID,
		PolicyPeriodPublicID,
		RiskLevel,
		EffectiveDate,
		ExpirationDate,
		PolicyNumber,	
		JobNumber,
		PolicyPeriodStatus,
		--PrimaryLocationNumber
		LocationNumber,
		IsPrimaryLocation,
		LocationFixedID,
		LocationAddress1,	
		LocationAddress2,	
		LocationCity,	
		LocationState,	
		LocationStateCode,	
		LocationCountry,	
		LocationPostalCode,	
		LocationAddressStatus,	
		LocationCounty,	
		LocationCountyFIPS,	
		LocationCountyPopulation,	
		TerritoryCode,	
		Coastal,	
		CoastalZone,	
		SegmentationCode,	
		RetailSale,	
		RepairSale,	
		AppraisalSale,	
		WholesaleSale,	
		ManufacturingSale,	
		RefiningSale,	
		GoldBuyingSale,	
		PawnSale,	
		RatedAs,	
		FullTimeEmployees,	
		PartTimeEmployees,	
		Owners,	
		PublicProtection,	
		LocationTypeCode,	
		LocationTypeName,	
		LocationTypeOtherDescription,	
		AnnualSales,	
		AreSalesPerformedViaInternet,	
		InternetSalesAmount,	
		NormalBusinessHours,	
		TotalValueShippedInLast12Months,	
		ConstructionCode,	
		ConstructionType,	
		ConstructionYearBuilt,	
		NumberOfFloors,	
		FloorNumbersOccupied,	
		TotalBuildngAreaSQFT,	
		AreaOccupiedSQFT,	
		Basement,	
		SmokeDetectors,	
		PercentSprinklered,	
		BuildingClass,	
		LocationClassInsuredBusinessCode,	
		LocationClassInsuredBusinessClassification,	
		HeatingYear,	
		PlumbingYear,	
		RoofingYear,	
		WiringYear,	
		AdjacentOccupancies,	
		SharedPremises,	
		PremisesSharedWith,	
		HasShowCaseWindows,	
		NumberOfShowWindows,	
		WindowsEquippedWithLocks,	
		WindowsKeptLocked,	
		WindowsKeptUnlockedReason,	
		WindowsMaxValue,	
		LockedDoorBuzzer,	
		BarsOnWindows,	
		SteelCurtainsGates,	
		MonitoredFireAlarm,	
		DoubleCylDeadBoltLocks,	
		SafetyBollardCrashProt,	
		GlassProtection,	
		ManTrapEntry,	
		RecordingCameras,	
		DaytimeArmedUniformGuard,	
		ElecTrackingRFTags,	
		MultipleAlarmSystems,	
		DualMonitoring,	
		SecuredBuilding,	
		AddtlProtectionNotMentioned,	
		OperatingCameraSystem,	
		CameraCovPremExclRestrooms,	
		CameraOperRecording,	
		CameraBackUpRecOffSite,	
		CameraAccessRemotely,	
		IsCamerasOnExterior,	
		PremiseBurglarAlarmDetMotion,	
		CoverAreaMerchLeftOutSafeVault,	
		CoverAlarmControlPanel,	
		CoverSafeVaultArea,	
		HoldupAlarmSystem,	
		MobileDevicesUsed,	
		BurglarAlarmSysMonitored,	
		BurglarAlarmWhenClosed,	
		BurglarAlarmWhenClosedReason,	
		OpeningClosingMonitored,	
		OpeningClosingSupervised,	
		NbrOnCallInAlarmConditions,	
		RespondToAlarmConditions,	
		RespondToAlarmConditionsNoReason,	
		OtherEmployAbleDeactivateAlarm,	
		SafeScatteredOnPremiseOrInOneArea,	
		SafeVaultStkroomUsedByOther,	
		SafeVaultStkroomUsedByOtherReason,	
		AnySafeVaultStkroomsOnExtWall,	
		IsSafeOnExterior,	
		LeastNbrEmployOnPremiseBusHrs,	
		YearsInBusiness,	
		JBTFinancial,	
		Inventory,	
		ClaimsFree,	
		PhysicalProtection,	
		ArmedUniformGuard,	
		MetalDetManTrap,	
		ElecTrackDevices,	
		HoldUpAlarm,	
		ElectronicProt,	
		PhysProtectedDoorWindow,	
		NoExtGrndFloorExposure,	
		LockedDoorBuzzSys,	
		SecuredBldg,	
		MonitoredFireAlarmSys,	
		ClosedBusStoragePractices,	
		CameraSystem,	
		DblCylDeadBoltLocks,	
		AddlProtNotMentioned,	
		MltplAlarmsMonitoring,	
		TotalInStoragePercent,	
		BankVaultPercent,	
		OutOfStorageAmount,	
		BurglarylInclSpecProp,	
		BurglaryInclSpecPropValue,	
		ExclBurglary,	
		ExclBurglaryClosed,	
		ExclBurglaryClosedHoursOpen,	
		ExclBurglaryClosedTimeZone,	
		ExclBurglaryClosedTravelLimit,	
		ExclBurglaryJwlWtchSpecified,	
		ExclBurglaryJwlWtchSpecifiedValue,	
		ExclBurglaryJwlySpecifiedAmt,	
		ExclBurglaryJwlySpecifiedAmtValue,	
		ExclOutOfSafe,	
		ExclFireLightningSmoke,	
		HasSpecifiedBurglaryLimit,	
		SpecifiedBurglaryLimitValue,	
		AlarmSignalResponseReq,	
		BankVaultReq,	
		BankVaultReqOutOfSafeVaultPercent,	
		BankVaultReqInSafeVaultPercent,	
		BankVaultReqBankVaultPercent,	
		BurglaryDeductible,	
		BurglaryDeductibleValue,	
		EstimatedInventory,	
		IndivSafeMaxLimit,	
		IndivSafeMaxLimitInSafeVaultStkPercent,	
		IndivSafeMaxLimitMfg,	
		IndivSafeVaultMaxCap,	
		InnerSafeChest,	
		InnerSafeChestInSafePercent,	
		InSafePercent,	
		InSafePercentIndivSafeVaultMaxCapacity,	
		KeyedInvestigatorResponse,	
		KeyedInvestigatorResponseReq,	
		LockedCabinets,	
		LockedCabinetsPercentKept,	
		IsMaxDollarLimit,	
		MaxLimitOutOfSafeAmt,	
		MaxLimitBurglary,	
		MaxLimitBurglaryInSafeVaultStkPct,	
		MaxLimitBurglaryBurgLimit,	
		MaxLimitBurglaryAOPLimit,	
		MaxLimitFinishedMerch,	
		MaxLimitFinishedMerchOutOfSafeVaultAmt,	
		MaxLimitWarranty,	
		MaxLimitWarrantyOutOfSafeVaultAmt,	
		MaxStockValueOutWhenClosed,	
		MaxJwlryValueOutWhenClosed,	
		MaxNonJwlyValueOutWhenClosed,	
		MaxOutofSafeWhenClosed,	
		MaxOutWhenClosedMaxOutOfSafeVault,	
		MaxOutWhenClosedWithWarranty,	
		MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault,	
		MaxOutOfLockedSafeVaultLimitSched,	
		MaxPerItemSafeVault,	
		MaxPerItemSafeVaultCostPerItem,	
		MaxPerItemSafeVaultStkroom,	
		MaxPerItemSafeVaultStkroomCostPerItem,	
		MaxValueInVault,	
		MaxValueInVaultInSafePercent,	
		MinMaxProportionInSafe,	
		MinMaxProportionInSafePercent,	
		MinNbrEmployeeCond,	
		MinNbrEmployeeCondNumber,	
		MinProportionValueSafeVault,	
		MinProportionValueSafeVaultInnerSafe,	
		MinProportionValueStkroom,	
		RobberyDeductible,	
		RobberyDeductibleValue,	
		SafeBurglaryDeductible,	
		SafeBurglaryDeductibleValue,	
		SafeMinLimit,	
		SafeVaultHurrWarningReq,	
		SaefVaultHurrWarningReqTX,	
		SafeVaultHurrWarningReqDeductible,	
		SafeVaultHurrWarningReqDeductibleValue,	
		ShowJewelryConditions,	
		SharedPremiseSecurity,	
		StkroomMaxDollarLimit,	
		StkroomMaxLimitOutOfSafeVaultAmt,	
		StkroomMaxLimit,	
		StkroomMaxLimitInSafePercent,	
		TheftProtection,	
		TheftProtectionDesc,	
		TheftProtectionGuardWarranty,	
		TheftProtectionSecurityDevice,	
		TheftProtectionSecurityDeviceDesc,	
		TotalPercentInSafe,	
		TotalPercentInSafeInSafeVaultStkrmPct,	
		TotalPercentInSafeNotToExceedAmt,	
		ShowcaseOrWindowCondition,	
		BusinessPremise,	
		InviteJewelryClients,	
		SignageAtHome,	
		AnimalsOnPremise,	
		AnimalSpecies,	
		SwimmingPool,	
		Trampoline,	
		MerchandiseAccessibleToChildren,	
		DescribeHowAccessRestricted,	
		bq_load_date
	)
WITH RiskLocationIMConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'IMLineCode','ILMLine_JMIC' UNION ALL
  SELECT 'BOPLineCode','BusinessOwnersLine' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
  SELECT 'IMLocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'LOBLevelRisk','Line' UNION ALL
  SELECT 'BOPLocationLevelRisk','BusinessOwnersLocation' UNION ALL
  SELECT 'IMHazardCategory','ILMLocHazardCodes_JMIC' UNION ALL
  SELECT 'IMLocationCoverageQuestion','BOPHomeBasedBusinessQuestion_JMIC'
)
SELECT
	ConfigSource.Value AS SourceSystem
	--SK For PK <Source>_<LocationPublicID>_<Level>
	,CASE WHEN LocationPublicID IS NOT NULL THEN 
			SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskLocIM.LocationPublicID,ConfigHashSep.Value, ConfigRiskLevel.Value))  -- 04/27/2023
		END AS RiskLocationKey
	--SK For FK <Source>_<PolicyPeriodPublicID>
	,CASE WHEN PolicyPeriodPublicID IS NOT NULL THEN 
			SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskLocIM.PolicyPeriodPublicID)) 
		END AS PolicyTransactionKey
	,DENSE_RANK() OVER(PARTITION BY	cRiskLocIM.LocationFixedID
									,cRiskLocIM.PolicyPeriodPublicID
									--,cRiskLocIM.RiskLevel -- 04/27/2023
					   ORDER BY		IsTransactionSliceEffective DESC
									,cRiskLocIM.FixedLocationRank) AS FixedLocationRank

	,cRiskLocIM.IsTransactionSliceEffective
	,cRiskLocIM.LocationPublicID
	,cRiskLocIM.PolicyPeriodPublicID
	,ConfigRiskLevel.Value AS RiskLevel -- 04/27/2023
	,cRiskLocIM.EffectiveDate
	,cRiskLocIM.ExpirationDate
	,cRiskLocIM.PolicyNumber
	,cRiskLocIM.JobNumber
	,cRiskLocIM.PolicyPeriodStatus

-- T - Details
	-- Location
--	,cRiskLocIM.PrimaryLocationNumber
	,cRiskLocIM.LocationNumber
	,cRiskLocIM.IsPrimaryLocation
	,cRiskLocIM.LocationFixedID
	,cRiskLocIM.LocationAddress1
	,cRiskLocIM.LocationAddress2
	,cRiskLocIM.LocationCity
	,cRiskLocIM.LocationState
	,cRiskLocIM.LocationStateCode
	,cRiskLocIM.LocationCountry
	,cRiskLocIM.LocationPostalCode
	,cRiskLocIM.LocationAddressStatus
	,cRiskLocIM.LocationCounty
	,cRiskLocIM.LocationCountyFIPS
	,cRiskLocIM.LocationCountyPopulation
	--,phone
	,cRiskLocIM.TerritoryCode
	,cRiskLocIM.Coastal
	,cRiskLocIM.CoastalZone
	,cRiskLocIM.SegmentationCode

	-- General Information (Type of Business based on sales)
	,cRiskLocIM.RetailSale	
	,cRiskLocIM.RepairSale
	,cRiskLocIM.AppraisalSale		
	,cRiskLocIM.WholesaleSale		
	,cRiskLocIM.ManufacturingSale	
	,cRiskLocIM.RefiningSale		
	,cRiskLocIM.GoldBuyingSale	
	,cRiskLocIM.PawnSale
	,cRiskLocIM.RatedAs

	-- Employee Informatioin
	,cRiskLocIM.FullTimeEmployees
	,cRiskLocIM.PartTimeEmployees
	,cRiskLocIM.Owners

	-- Public Protection
	,cRiskLocIM.PublicProtection									-- FireProtectionClassCode

	-- Location Information
	,cRiskLocIM.LocationTypeCode
	,cRiskLocIM.LocationTypeName
	,cRiskLocIM.LocationTypeOtherDescription
	,cRiskLocIM.AnnualSales
	,cRiskLocIM.AreSalesPerformedViaInternet						-- Are sales performed via the internet?
	,cRiskLocIM.InternetSalesAmount								-- If yes, what is the total internet sales amount?
	--,Name/Address of other jlry business/loc owned-managed by principals/officers
	,cRiskLocIM.NormalBusinessHours
	,cRiskLocIM.TotalValueShippedInLast12Months					-- Total value of prop shipped in last 12 months via reg/exp mail, armored car, or private paid delivery service

	-- Building Construction
	,cRiskLocIM.ConstructionCode
	,cRiskLocIM.ConstructionType
	,cRiskLocIM.ConstructionYearBuilt
	,cRiskLocIM.NumberOfFloors
	,cRiskLocIM.FloorNumbersOccupied							--Floor Number(s) Occupied
	,cRiskLocIM.TotalBuildngAreaSQFT
	,cRiskLocIM.AreaOccupiedSQFT
	,cRiskLocIM.Basement
	,cRiskLocIM.SmokeDetectors
	,cRiskLocIM.PercentSprinklered
	,cRiskLocIM.BuildingClass
	,cRiskLocIM.LocationClassInsuredBusinessCode
	,cRiskLocIM.LocationClassInsuredBusinessClassification

	-- Building Improvement
	,cRiskLocIM.HeatingYear
	,cRiskLocIM.PlumbingYear
	,cRiskLocIM.RoofingYear
	,cRiskLocIM.WiringYear

	-- Occupancy Information
	,cRiskLocIM.AdjacentOccupancies
	,cRiskLocIM.SharedPremises
	,cRiskLocIM.PremisesSharedWith

	-- Display Cases and/or Show windows
	,cRiskLocIM.HasShowCaseWindows								-- Do you have any showcases and/or show windows?
	,cRiskLocIM.NumberOfShowWindows								-- Number of Show Windows
	--,Are all of your display cases and/or show windows:
	,cRiskLocIM.WindowsEquippedWithLocks						-- Equipped with Locks?
	,cRiskLocIM.WindowsKeptLocked								-- Kept locked except when removing property?
	,cRiskLocIM.WindowsKeptUnlockedReason						-- If no, explain
	,cRiskLocIM.WindowsMaxValue									-- Max value kept in show windows when open to business?
	
-- T - Security Information
	,cRiskLocIM.LockedDoorBuzzer										-- JB/JS - Locked Door Buzzer
	,cRiskLocIM.BarsOnWindows											-- JB/JS - Bars on Windows
	,cRiskLocIM.SteelCurtainsGates									-- JB/JS - Steel Curtains / Gates
	,cRiskLocIM.MonitoredFireAlarm									-- JB/JS - Monitored Fire Alarm
	,cRiskLocIM.DoubleCylDeadBoltLocks								-- JB/JS - Double Cylinder Dead Bolt Locks
	,cRiskLocIM.SafetyBollardCrashProt								-- JB/JS - Safety Bollards (Crash Protectors)
	,cRiskLocIM.GlassProtection										-- JB/JS - Glass Protection
	,cRiskLocIM.ManTrapEntry											-- JB/JS - Man Trap Entry
	,cRiskLocIM.RecordingCameras										-- JB/JS - Recording Cameras
	,cRiskLocIM.DaytimeArmedUniformGuard							-- JB/JS - Daytime Armed Uniform Guard
	,cRiskLocIM.ElecTrackingRFTags								-- JB/JS - Electronic Tracking / RF Tags
	,cRiskLocIM.MultipleAlarmSystems									-- JB/JS - Multiple Alarm Systems
	,cRiskLocIM.DualMonitoring										-- JB/JS - Dual Monitoring
	,cRiskLocIM.SecuredBuilding										-- JB/JS - Secured Building
	,cRiskLocIM.AddtlProtectionNotMentioned						-- JB/JS - Additional Protection Not Mentioned:

	-- Camera Information
	,cRiskLocIM.OperatingCameraSystem							-- JB/JS - Operating Camera System?
	,cRiskLocIM.CameraCovPremExclRestrooms						-- JB/JS - Covers all areas of premises except restrooms (populated when OpeCameraSystem_JMIC = 1 on GW screen but data is available with yes (job: 140685))
	,cRiskLocIM.CameraOperRecording								-- JB/JS - Operating / Recording 24/7? (populated when OpeCameraSystem_JMIC = 1 on GW screen but data is available with yes (job: 140685))
	,cRiskLocIM.CameraBackUpRecOffSite							-- JB/JS - Back-Up Recordings stored off-site? (populated when OpeCameraSystem_JMIC = 1 on GW screen but data is available with yes (job: 140685))
	,cRiskLocIM.CameraAccessRemotely							-- JB/JS - Cameras accessed remotely? (populated when OpeCameraSystem_JMIC = 1 on GW screen but data is available with yes (job: 140685))
	,cRiskLocIM.IsCamerasOnExterior								-- JB/JS - Are there cameras located on the exterior of the premises? 

	-- Motion Information
	,cRiskLocIM.PremiseBurglarAlarmDetMotion							-- JB/JS - Premise Burglar Alarm Detect Motion?
	,cRiskLocIM.CoverAreaMerchLeftOutSafeVault					-- JB/JS - Covers areas where merchanidse left out of safe / vault? (populated when PreBurAlarmDetMotion_JMIC = 1 on GW screen but data is available with yes (job: 123141, 1247875))
	,cRiskLocIM.CoverAlarmControlPanel									-- JB/JS - Covers alarm control panel? (populated when PreBurAlarmDetMotion_JMIC = 1 on GW screen but data is available with yes (job: 123141, 1247875))
	,cRiskLocIM.CoverSafeVaultArea									-- JB/JS - Covers safe / vault area? (populated when PreBurAlarmDetMotion_JMIC = 1 on GW screen but data is available with yes (job: 123141, 1247875))
		
	-- Holdup Alarm Information
	,cRiskLocIM.HoldupAlarmSystem								-- JB/JS - Holdup Alarm System?
	,cRiskLocIM.MobileDevicesUsed								-- JB/JS - Mobile devices used? (populated when HoldupAlarmSystem_JMIC = 1 on GW screen but data is available with yes (job: 123141, 1247875))

	-- Additional Security Information
	,cRiskLocIM.BurglarAlarmSysMonitored								-- JB/JS - Where is premises burglar alarm system monitored?
	,cRiskLocIM.BurglarAlarmWhenClosed									-- JB/JS - Burglar alarm when closed to business?
	,cRiskLocIM.BurglarAlarmWhenClosedReason					-- JB/JS - Explain (populated when BurAlaWhenClosed_JMIC = 0 on GW screen but data is available with yes (job: 02260))
	,cRiskLocIM.OpeningClosingMonitored								-- JB/JS - Openings / Closings Monitored?
	,cRiskLocIM.OpeningClosingSupervised									-- JB/JS - Openings/Closings Supervised?
	,cRiskLocIM.NbrOnCallInAlarmConditions						-- JB/JS - Number of Individuals on call list in the event of an alarm condition?
	,cRiskLocIM.RespondToAlarmConditions									-- JB/JS - Do you or someone from your business respond in person to all alarm conditions?
	,cRiskLocIM.RespondToAlarmConditionsNoReason						-- JB/JS - Explain (populated when RespToAlaCond_JMIC = 0 on GW screen but data is available with yes (job: 1493396, 1524317))
	,cRiskLocIM.OtherEmployAbleDeactivateAlarm					-- JB/JS - Who, other than an employee or officer of the company, has the ability to deactivate alarm?
	,cRiskLocIM.SafeScatteredOnPremiseOrInOneArea							-- JB/JS - If more than one safe, are safes scattered throughout premise or concentrated in one area?
	,cRiskLocIM.SafeVaultStkroomUsedByOther								-- JB/JS - Any safes, vaults, or stockrooms used by a business / individual not related to your business?
	,cRiskLocIM.SafeVaultStkroomUsedByOtherReason					-- JB/JS - Explain (populated when SafVauStoRooByBI_JMIC = 1)
	,cRiskLocIM.AnySafeVaultStkroomsOnExtWall							-- JB/JS - Any safes vaults or stockrooms located on exterior wall
	,cRiskLocIM.IsSafeOnExterior												-- JB/JS - Are any of your safes located on a party wall (or common wall)?
	,cRiskLocIM.LeastNbrEmployOnPremiseBusHrs						-- JB/JS - Least number of employees, officers, or owners customarily on described premies at any time during business hours or when opening / closing for business?

-- T - Hazard Categories
	-- ILM Hazard Categories
	,cRiskLocIM.YearsInBusiness									-- JS    - Years in Business
	,cRiskLocIM.JBTFinancial										-- JS    - JBT/Financial
	,cRiskLocIM.Inventory										-- JS    - Inventory
	,cRiskLocIM.ClaimsFree										-- JS    - Claims Free
	,cRiskLocIM.PhysicalProtection								-- JB/JS - Physical Protection
	,cRiskLocIM.ArmedUniformGuard									-- JB    - Armed Uniform Guard
	,cRiskLocIM.MetalDetManTrap									-- JB    - Metal Detector with Man Trap
	,cRiskLocIM.ElecTrackDevices									-- JB    - Electronic Tracking Devices
	,cRiskLocIM.HoldUpAlarm										-- JB    - Hold Up Alarm
	,cRiskLocIM.ElectronicProt									-- JB/JS - Electronic Protection
	,cRiskLocIM.PhysProtectedDoorWindow									-- JB    - Physically Protected Windows and Doors
	,cRiskLocIM.NoExtGrndFloorExposure									-- JB    - No Exterior Ground Floor Exposure
	,cRiskLocIM.LockedDoorBuzzSys									-- JB    - Locked Door Buzzer System
	,cRiskLocIM.SecuredBldg										-- JB    - Secured Building
	,cRiskLocIM.MonitoredFireAlarmSys									-- JB    - Monitored Fire Alarm System
	,cRiskLocIM.ClosedBusStoragePractices								-- JB    - Closed to Business Storage Practices
	,cRiskLocIM.CameraSystem											-- JB    - Camera System
	,cRiskLocIM.DblCylDeadBoltLocks									-- JB    - Double Cylinder Dead Bolt Locks
	,cRiskLocIM.AddlProtNotMentioned									-- JB    - Additional Protection Not Mentioned
	,cRiskLocIM.MltplAlarmsMonitoring									-- JB    - Multiple Alarms or Monitoring

-- T - Safes, Vaults, and Stockrooms
	,cRiskLocIM.TotalInStoragePercent							-- Total Location In-Safe/Vault/Stockroom % -- check if it is available for js(job: 121483), values present for js but not shown on gw screen
	,cRiskLocIM.BankVaultPercent								-- % in Bank Vault -- check if it is available for js
	,cRiskLocIM.OutOfStorageAmount								-- Out of Safe/Vault/Stockroom Limit -- check if it is available for js

-- T - Exclusions & Conditions
	-- Exclusions
	,cRiskLocIM.BurglarylInclSpecProp							-- Burglary - Included Specified Property (Flag) 										-- (395334,438023)
	,cRiskLocIM.BurglaryInclSpecPropValue						-- Burglary - Included Specified Property (Specified Property)
	,cRiskLocIM.ExclBurglary									-- Exclude Burglary (Flag)																-- (No Value) (268323)
	,cRiskLocIM.ExclBurglaryClosed								-- Exclude Burglary - Closed to Business (Flag)											-- (268323,493463,565015)
	,cRiskLocIM.ExclBurglaryClosedHoursOpen						-- Exclude Burglary - Closed to Business (Hours Open to Business)
	,cRiskLocIM.ExclBurglaryClosedTimeZone						-- Exclude Burglary - Closed to Business (Time Zone)
	,cRiskLocIM.ExclBurglaryClosedTravelLimit					-- Exclude Burglary - Closed to Business (Travel Limit)
	,cRiskLocIM.ExclBurglaryJwlWtchSpecified						-- Exclude Burglary - Jewelry and Watches Specified Dollar Amt (Flag) 					-- (436619)
	,cRiskLocIM.ExclBurglaryJwlWtchSpecifiedValue					-- Exclude Burglary - Jewelry and Watches Specified Dollar Amt Value	 
	,cRiskLocIM.ExclBurglaryJwlySpecifiedAmt						-- Exclude Burglary - Jewelry greater than Specified Dollar Amt (Flag)					-- (205057)
	,cRiskLocIM.ExclBurglaryJwlySpecifiedAmtValue					-- Exclude Burglary - Jewelry greater than Specified Dollar Amt (Dollar Amount)			
	,cRiskLocIM.ExclOutOfSafe									-- Exclude Out of Safe (Flag)															-- (No Value) (395334)
	,cRiskLocIM.ExclFireLightningSmoke							-- Fire, Lightening, and Smoke Exclusion (Flag)											-- (No Value) (218862)
	,cRiskLocIM.HasSpecifiedBurglaryLimit								-- Specified Burglary Limit of Insurance (Flag) 										-- (100920,350314)
	,cRiskLocIM.SpecifiedBurglaryLimitValue							-- Specified Burglary Limit of Insurance (Closed To Business Burglary Limit)

	-- Policy Conditions
	,cRiskLocIM.AlarmSignalResponseReq								-- Alarm Signal Response Requirement													-- (No Value) (903001,6728107)
	,cRiskLocIM.BankVaultReq									-- Bank Vault Requirement																-- (994709,6701342,check 888234,check 6565557)
	,cRiskLocIM.BankVaultReqOutOfSafeVaultPercent				-- Bank Vault Requirement (Percent Kept Out of Safe and Vaults on Premise)	
	,cRiskLocIM.BankVaultReqInSafeVaultPercent					-- Bank Vault Requirement (Total Percent in Safe - Vault - Stockroom) 
	,cRiskLocIM.BankVaultReqBankVaultPercent					-- Bank Vault Requirement (Percent in Bank Vault)									
	,cRiskLocIM.BurglaryDeductible									-- Burglary Deductible																	-- (729770)
	,cRiskLocIM.BurglaryDeductibleValue							-- Burglary Deductible (Burglary Deductible)
	,cRiskLocIM.EstimatedInventory										-- Estimated Inventory																	-- (No Value) (633697,1662375)
	,cRiskLocIM.IndivSafeMaxLimit								-- Individual Safe - Vault Max Limit													-- (2849208,296098,903001)
	,cRiskLocIM.IndivSafeMaxLimitInSafeVaultStkPercent				-- Individual Safe - Vault Max Limit (Total % in Safe - Vault - Stockroom)
	,cRiskLocIM.IndivSafeMaxLimitMfg							-- Individual Safe - Vault Max Limit (Manufacturing) 
	,cRiskLocIM.IndivSafeVaultMaxCap							-- Individual Safe/Vault/Stockroom Maximum Capacity										-- (5268142,5983582)
	,cRiskLocIM.InnerSafeChest									-- Inner Safe - Chest Limitation														-- (148206,2849208)
	,cRiskLocIM.InnerSafeChestInSafePercent						-- Inner Safe - Chest Limitation (Total % in Safe)
	,cRiskLocIM.InSafePercent									-- In-Safe Percentage																	-- (5424819,4233311)
	,cRiskLocIM.InSafePercentIndivSafeVaultMaxCapacity					-- In-Safe Percentage with Individual Safe/Vault/Stockroom Maximum Capacity				-- (4154969)
	,cRiskLocIM.KeyedInvestigatorResponse							-- Keyed Investigator Response Requirement												-- (2293804)
	,cRiskLocIM.KeyedInvestigatorResponseReq							-- Keyed Investigator Response Requirement (Response Required)
	,cRiskLocIM.LockedCabinets									-- Locked Cabinets - Drawers															-- (1385689,check 4850218)
	,cRiskLocIM.LockedCabinetsPercentKept						-- Locked Cabinets - Drawers (% Kept in Locked Cabinets and Drawers on Premise)
	,cRiskLocIM.IsMaxDollarLimit								-- Individual Safe - Vault Max Dollar Limit												-- (553575,check 3566400)
	,cRiskLocIM.MaxLimitOutOfSafeAmt							-- Individual Safe - Vault Max Dollar Limit (Amount out of Safe-Vault)
	,cRiskLocIM.MaxLimitBurglary								-- Max Dollar Limit for Burglary and AOP												-- (888184,1134089)
	,cRiskLocIM.MaxLimitBurglaryInSafeVaultStkPct				-- Max Dollar Limit for Burglary and AOP (Total % in Safe-Vault-Stockroom)
	,cRiskLocIM.MaxLimitBurglaryBurgLimit						-- Max Dollar Limit for Burglary and AOP (Burglary Limit) 
	,cRiskLocIM.MaxLimitBurglaryAOPLimit						-- Max Dollar Limit for Burglary and AOP (AOP Limit)
	,cRiskLocIM.MaxLimitFinishedMerch							-- Max Dollar Limit - Finished Merch Excluding Loose Diamonds							-- (2922690,3771105)
	,cRiskLocIM.MaxLimitFinishedMerchOutOfSafeVaultAmt			-- Max Dollar Limit - Finished Merch Excluding Loose Diamonds (Amount out of Safe-Vault)	
	,cRiskLocIM.MaxLimitWarranty								-- Individual Safe - Vault Max Dollar Limit w/Warranties								-- (385531,811822)
	,cRiskLocIM.MaxLimitWarrantyOutOfSafeVaultAmt				-- Individual Safe - Vault Max Dollar Limit w/Warranties (Amount out of Safe-Vault) 
	,cRiskLocIM.MaxStockValueOutWhenClosed								-- Max Value of Jewelry Stock Out of Safe - Vault When Closed							-- (4422794,check 544351)
	,cRiskLocIM.MaxJwlryValueOutWhenClosed						-- Max Value of Jewelry Stock Out of Safe - Vault When Closed (Amount of Jewelry Stock Out of Safe - Vault) 
	,cRiskLocIM.MaxNonJwlyValueOutWhenClosed					-- Max Value of Jewelry Stock Out of Safe - Vault When Closed (Amount of Non-Jewelry Stock Out of Safe - Vault)
	,cRiskLocIM.MaxOutofSafeWhenClosed									-- Max Value Out of Safe - Vault When Closed											-- (1764534,1065236,check 3645484,check 2884862)
	,cRiskLocIM.MaxOutWhenClosedMaxOutOfSafeVault					-- Max Value Out of Safe - Vault When Closed (Maximum Value Kept Out of Vault or Safe When Closed) 
	,cRiskLocIM.MaxOutWhenClosedWithWarranty								-- Max Value Out of Safe - Vault When Closed w/Warranties								-- (1665028,4301912)
	,cRiskLocIM.MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault				-- Max Value Out of Safe - Vault When Closed w/Warranties (Maximum Value Kept Out of Vault or Safe When Closed) 
	,cRiskLocIM.MaxOutOfLockedSafeVaultLimitSched					-- Maximum Out of Locked Safe/Vault/Stockroom Limitation Schedule						-- (4540268)
	,cRiskLocIM.MaxPerItemSafeVault								-- Max Per Item - Safe-Vault															-- (526909,758924)
	,cRiskLocIM.MaxPerItemSafeVaultCostPerItem					-- Max Per Item - Safe-Vault (Cost Value Per Item) 
	,cRiskLocIM.MaxPerItemSafeVaultStkroom								-- Max Per Item Value - Stockroom														-- (4453295, check 881607)
	,cRiskLocIM.MaxPerItemSafeVaultStkroomCostPerItem					-- Max Per Item Value - Stockroom (Cost Value Per Item) 
	,cRiskLocIM.MaxValueInVault									-- Max Value in Vault																	-- (838170,check 2147717)
	,cRiskLocIM.MaxValueInVaultInSafePercent						-- Max Value in Vault (Total % in Safe) 
	,cRiskLocIM.MinMaxProportionInSafe								-- Min-Max Proportion by Value in Safes													-- (4311387,1182300)	
	,cRiskLocIM.MinMaxProportionInSafePercent					-- Min-Max Proportion by Value in Safes (Total Percent In Safe)
	,cRiskLocIM.MinNbrEmployeeCond								-- Minimum Number of Employees Condition												-- (1167083,1994821)					
	,cRiskLocIM.MinNbrEmployeeCondNumber								-- Minimum Number of Employees Condition (Minimum # of Employees)
	,cRiskLocIM.MinProportionValueSafeVault							-- Min Proportion by Value in Safe - Vault												-- (2253050)
	,cRiskLocIM.MinProportionValueSafeVaultInnerSafe						-- Min Proportion by Value in Safe - Vault - Inner Safe									-- (568683)
	,cRiskLocIM.MinProportionValueStkroom								-- Min Proportion by Value in Stockroom													-- (750068)
	,cRiskLocIM.RobberyDeductible								-- Robbery Deductible																	-- (1844551,4223419)
	,cRiskLocIM.RobberyDeductibleValue 							-- Robbery Deductible (Robbery Deductible)
	,cRiskLocIM.SafeBurglaryDeductible								-- Safe Burglary Deductible																-- (4418545)
	,cRiskLocIM.SafeBurglaryDeductibleValue						-- Safe Burglary Deductible (Safe Burglary Deductible)
	,cRiskLocIM.SafeMinLimit									-- Safe Min Limit - Refined and Unrefined												-- (2994210 only one policy)
	,cRiskLocIM.SafeVaultHurrWarningReq								-- Safe-Vault Hurricane Warning Req														-- (No Value) (1219198,12241827)
	,cRiskLocIM.SaefVaultHurrWarningReqTX							-- Safe-Vault Hurricane Warning Req														-- (No Value) (709327 available for only one policy, looks like deprecated)
	,cRiskLocIM.SafeVaultHurrWarningReqDeductible							-- Safe-Vault Hurricane Warning Req Deductible											-- (609323,6363208)
	,cRiskLocIM.SafeVaultHurrWarningReqDeductibleValue				-- Safe-Vault Hurricane Warning Req Deductible (Hurricane Deductible) 
	,cRiskLocIM.ShowJewelryConditions							-- Showing of Jewelry Conditions														-- (No Value) (365803,2946502)
	,cRiskLocIM.SharedPremiseSecurity								-- Shared Premises Security Device Condition											-- (No Value) (70929,1662375)
	,cRiskLocIM.StkroomMaxDollarLimit								-- Individual Stockroom Max Dollar Limit												-- (446997)
	,cRiskLocIM.StkroomMaxLimitOutOfSafeVaultAmt				-- Individual Stockroom Max Dollar Limit (Amount out of Safe-Vault)
	,cRiskLocIM.StkroomMaxLimit									-- Individual Stockroom Max Limit														-- (446997)
	,cRiskLocIM.StkroomMaxLimitInSafePercent						-- Individual Stockroom Max Limit (Total % in Safe)
	,cRiskLocIM.TheftProtection									-- Theft Protection																		-- (712746,2681249)
	,cRiskLocIM.TheftProtectionDesc								-- Theft Protection (Theft Protection Desc) 
	,cRiskLocIM.TheftProtectionGuardWarranty						-- Theft Protection Armed Guard Warranty												-- (No Value) (1371692,5000432)
	,cRiskLocIM.TheftProtectionSecurityDevice								-- Theft Protection Security Device														-- (3856001,6938238)
	,cRiskLocIM.TheftProtectionSecurityDeviceDesc						-- Theft Protection Security Device (Security Device Description)
	,cRiskLocIM.TotalPercentInSafe								-- Total Percent In Safe - Not to Exceed Dollar Limit									-- (1186804,check 1587083; only two policies) 
	,cRiskLocIM.TotalPercentInSafeInSafeVaultStkrmPct				-- Total Percent In Safe - Not to Exceed Dollar Limit (Total % in Safe-Vault-Stockroom)
	,cRiskLocIM.TotalPercentInSafeNotToExceedAmt				-- Total Percent In Safe - Not to Exceed Dollar Limit (Not to Exceed Amount)
	,cRiskLocIM.ShowcaseOrWindowCondition							-- Showcase or Window Coverage															-- (142833,check 456734; only two policies)

-- T - Location-Coverage Questions
	-- BOP/Inland Marine Loc Home Based Business Questions (183596)
	,cRiskLocIM.BusinessPremise								-- Describe your specific business premises (basement, attached garage, detached garage, outbuilding, etc.)
	,cRiskLocIM.InviteJewelryClients						-- Do you invite jewelry clients or the general public into your home / jewelry business?
	,cRiskLocIM.SignageAtHome								-- Do you have signage at your home that discloses your business operations?
	,cRiskLocIM.AnimalsOnPremise							-- Do you have animals on the premise?
	,cRiskLocIM.AnimalSpecies								-- What Species? (populated when AnimalsInPremise_JMIC = 1)
	,cRiskLocIM.SwimmingPool								-- Do you have a swimming pool?
	,cRiskLocIM.Trampoline									-- Do you have a trampoline?
	,cRiskLocIM.MerchandiseAccessibleToChildren				-- Is business / merchandise accessible to children under the age of 18?
	,cRiskLocIM.DescribeHowAccessRestricted					-- Describe how access is restricted? (mostly populated when MerchandiseAccessChildren_JMIC = 1)
	
    ,DATE('{date}') as bq_load_date			
FROM
(
	----------------------------------------------------------------------------
	---Risk Location Location
	----------------------------------------------------------------------------
	SELECT 
		DENSE_RANK() OVER(PARTITION BY	pcx_ilmlocation_jmic.ID
							ORDER BY	IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS LocationRank
		,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocation_jmic.ID
							ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pcx_ilmcost_jmic.ID DESC
							) AS CostRank
		,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocation_jmic.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
							) AS FixedLocationRank
		
		,pcx_ilmlocation_jmic.PublicID								AS LocationPublicID
		,pc_policyperiod.PublicID									AS PolicyPeriodPublicID
		--,ConfigILMLocationRisk.Value								AS RiskLevel -- 04/27/2023
		,pcx_ilmlocation_jmic.EffectiveDate
		,pcx_ilmlocation_jmic.ExpirationDate
		,pc_policyperiod.PolicyNumber
		,pc_job.JobNumber
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 END 
			AS IsTransactionSliceEffective
		,pc_policyperiod.status											AS PolicyPeriodStatus

	-- T - Details
		-- Location
		--,t_PrimaryRatingLocationIM.PrimaryLocationNumber		AS PrimaryLocationNumber
		,pc_policylocation.LocationNum									AS LocationNumber
		,CASE WHEN t_PrimaryRatingLocationIM.RatingLocationNum = pc_policylocation.LocationNum THEN 'Y' ELSE 'N' END AS IsPrimaryLocation -- 04/27/2023
		,pc_policylocation.FixedID										AS LocationFixedID
		,pc_policylocation.AddressLine1Internal							AS LocationAddress1
		,pc_policylocation.AddressLine2Internal							AS LocationAddress2
		,pc_policylocation.CityInternal									AS LocationCity
		,pctl_state.NAME												AS LocationState
		,pctl_state.TYPECODE											AS LocationStateCode
		,pctl_country.NAME												AS LocationCountry
		,pc_policylocation.PostalCodeInternal							AS LocationPostalCode
		,pctl_addressstatus_jmic.NAME									AS LocationAddressStatus
		,pc_policylocation.CountyInternal								AS LocationCounty
		,pc_policylocation.FIPSCode_JMICInternal						AS LocationCountyFIPS
		,pc_policylocation.CountyPopulation_JMIC						AS LocationCountyPopulation
		--,phone
		,pc_territorycode.Code											AS TerritoryCode
		,pctl_bopcoastal_jmic.TYPECODE									AS Coastal
		,pctl_bopcoastalzones_jmic.TYPECODE								AS CoastalZone
		,pc_policylocation.SegmentationCode_JMIC						AS SegmentationCode

		-- General Information (Type of Business based on sales)
		,pcx_commonlocation_jmic.RetailSale_JMIC						AS RetailSale	
		,pcx_commonlocation_jmic.ServiceSale_JMIC						AS RepairSale
		,pcx_commonlocation_jmic.AppraisalSale_JMIC						AS AppraisalSale		
		,pcx_commonlocation_jmic.WholesaleSale_JMIC						AS WholesaleSale		
		,pcx_commonlocation_jmic.ManufacturingSale_JMIC					AS ManufacturingSale	
		,pcx_commonlocation_jmic.RefiningSale_JMIC						AS RefiningSale		
		,pcx_commonlocation_jmic.GoldBuyingSale_JMIC					AS GoldBuyingSale	
		,pcx_commonlocation_jmic.PawnSale_JM							AS PawnSale
		,pctl_ilmlocationratedas_jmic.TYPECODE							AS RatedAs

		-- Employee Informatioin
		,pcx_commonlocation_jmic.FullTimeEmployees_JMIC					AS FullTimeEmployees
		,pcx_commonlocation_jmic.PartTimeEmployees_JMIC					AS PartTimeEmployees
		,pcx_commonlocation_jmic.Owners_JMIC							AS Owners

		-- Public Protection
		,pctl_fireprotectclass.TYPECODE									AS PublicProtection

		-- Location Information
		,pctl_boplocationtype_jmic.TYPECODE									AS LocationTypeCode
		,pctl_boplocationtype_jmic.NAME										AS LocationTypeName
		,pcx_commonlocation_jmic.OtherDescription_JMIC						AS LocationTypeOtherDescription
		,pcx_commonlocation_jmic.AnnualSales_JMIC							AS AnnualSales
		,CAST(pcx_ilmlocation_jmic.AreInetSalesDone_JMIC AS INT64)						AS AreSalesPerformedViaInternet 
		,pcx_ilmlocation_jmic.InternetSalesAmt_JMIC						AS InternetSalesAmount 
		--,Name/Address of other jlry business/loc owned-managed by principals/officers
		,pcx_ilmlocation_jmic.NormalBusHours_JMIC							AS NormalBusinessHours
		,pcx_ilmlocation_jmic.TotalValShip12Mos_JMIC						AS TotalValueShippedInLast12Months 

		-- Building Construction
		,pctl_bopconstructiontype.TYPECODE							AS ConstructionCode
		,pctl_bopconstructiontype.DESCRIPTION						AS ConstructionType
		,pc_building.YearBuilt										AS ConstructionYearBuilt
		,pc_building.NumStories										AS NumberOfFloors
		,pcx_commonbuilding_jmic.FloorNumberOccupied_JMIC			AS FloorNumbersOccupied 
		,pc_building.TotalArea										AS TotalBuildngAreaSQFT
		,pcx_commonbuilding_jmic.BOPAreaOccupied_JMIC				AS AreaOccupiedSQFT
		,CAST(pcx_commonbuilding_jmic.Basement_JMIC	AS INT64)		AS Basement
		,CAST(pcx_commonbuilding_jmic.SmokeDetectors_JMIC AS INT64)		AS SmokeDetectors
		,pctl_sprinklered.NAME										AS PercentSprinklered
		,pctl_buildingclass_jmic.NAME								AS BuildingClass
		,pc_bopclasscode.Code										AS LocationClassInsuredBusinessCode
		,pc_bopclasscode.Classification								AS LocationClassInsuredBusinessClassification

		-- Building Improvement
		,cBldgImpr.HeatingYear										AS HeatingYear
		,cBldgImpr.PlumbingYear										AS PlumbingYear
		,cBldgImpr.RoofingYear										AS RoofingYear
		,cBldgImpr.WiringYear										AS WiringYear

		-- Occupancy Information
		,pcx_commonbuilding_jmic.AdjacentOccupancies_JMIC					AS AdjacentOccupancies
		,CAST(pcx_commonbuilding_jmic.SharedPremises_JMIC AS INT64)			AS SharedPremises
		,pcx_commonbuilding_jmic.PremisesSharedWith_JMIC					AS PremisesSharedWith

		-- Display Cases and/or Show windows
		,CAST(pcx_ilmlocation_jmic.HasShowcasesOrWindows AS INT64)			AS HasShowCaseWindows 
		,pcx_ilmlocation_jmic.NbrShowWindows_JMIC							AS NumberOfShowWindows
		--,Are all of your display cases and/or show windows:
		,CAST(pcx_ilmlocation_jmic.AllWndwsCasesLockEquipped_JMIC AS INT64)	AS WindowsEquippedWithLocks 
		,CAST(pcx_ilmlocation_jmic.AllWndwsCasesKeptLocked_JMIC	 AS INT64)	AS WindowsKeptLocked 
		,pcx_ilmlocation_jmic.WndwsCasesUnlckReason_JMIC					AS WindowsKeptUnlockedReason 
		,pcx_ilmlocation_jmic.MaxValueShowWndwsClsd_JMIC					AS WindowsMaxValue 
	
	-- T - Security Information
		,CAST(pcx_commonbuilding_jmic.LockedDoorBuzzer_JMIC AS INT64)		AS LockedDoorBuzzer					
		,CAST(pcx_commonbuilding_jmic.BarsonWindows_JMIC AS INT64)			AS BarsOnWindows						
		,CAST(pcx_commonbuilding_jmic.SteelCurtainsGates_JMIC AS INT64)		AS SteelCurtainsGates						
		,CAST(pcx_commonbuilding_jmic.MonitoredFireAlarm_JMIC AS INT64)		AS MonitoredFireAlarm					
		,CAST(pcx_commonbuilding_jmic.DoubleCylDeadBoltLocks_JMIC AS INT64)	AS DoubleCylDeadBoltLocks				
		,CAST(pcx_commonbuilding_jmic.SafetyBolCrashProt_JMIC AS INT64)		AS SafetyBollardCrashProt				
		,CAST(pcx_commonbuilding_jmic.GlassProtection_JMIC AS INT64)		AS GlassProtection						
		,CAST(pcx_commonbuilding_jmic.ManTrapEntry_JMIC AS INT64)			AS ManTrapEntry					
		,CAST(pcx_commonbuilding_jmic.RecordingCameras_JMIC AS INT64)		AS RecordingCameras					
		,CAST(pcx_commonbuilding_jmic.DaytimeArmedUniGuard_JMIC AS INT64)	AS DaytimeArmedUniformGuard				
		,CAST(pcx_commonbuilding_jmic.EleTrackingRFTags_JMIC AS INT64)		AS ElecTrackingRFTags					
		,CAST(pcx_commonbuilding_jmic.MutAlarmSystems_JMIC AS INT64)		AS MultipleAlarmSystems						
		,CAST(pcx_commonbuilding_jmic.DualMonitoring_JMIC AS INT64)			AS DualMonitoring					
		,CAST(pcx_commonbuilding_jmic.SecuredBuilding_JMIC AS INT64)		AS SecuredBuilding						
		,pcx_commonbuilding_jmic.AddProNotMentioned_JMIC					AS AddtlProtectionNotMentioned	

		-- Camera Information
		,CAST(pcx_commonbuilding_jmic.OpeCameraSystem_JMIC AS INT64)		AS OperatingCameraSystem							
		,CAST(pcx_commonbuilding_jmic.CovPreExcRestrooms_JMIC AS INT64)		AS CameraCovPremExclRestrooms						
		,CAST(pcx_commonbuilding_jmic.OpeRecording_JMIC AS INT64)			AS CameraOperRecording								
		,CAST(pcx_commonbuilding_jmic.BackUpRecStOffSite_JMIC AS INT64)		AS CameraBackUpRecOffSite						
		,CAST(pcx_commonbuilding_jmic.CamAccRemotely_JMIC AS INT64)			AS CameraAccessRemotely							
		,CAST(pcx_commonbuilding_jmic.IsCamerasOnExterior AS INT64)			AS IsCamerasOnExterior				

		-- Motion Information
		,CAST(pcx_commonbuilding_jmic.PreBurAlarmDetMotion_JMIC AS INT64)	AS PremiseBurglarAlarmDetMotion						
		,CAST(pcx_commonbuilding_jmic.CovArMerLeftOSafeVault_JMIC AS INT64)	AS CoverAreaMerchLeftOutSafeVault				
		,CAST(pcx_commonbuilding_jmic.CovAlarmConPanel_JMIC AS INT64)		AS CoverAlarmControlPanel						
		,CAST(pcx_commonbuilding_jmic.CovSafeVaultArea_JMIC AS INT64)		AS CoverSafeVaultArea							
		
		-- Holdup Alarm Information
		,CAST(pcx_commonbuilding_jmic.HoldupAlarmSystem_JMIC AS INT64)		AS HoldupAlarmSystem				
		,CAST(pcx_commonbuilding_jmic.MobileDevicesUsed_JMIC AS INT64)		AS MobileDevicesUsed				

		-- Additional Security Information
		,pcx_commonbuilding_jmic.BurAlaSysMonitored_JMIC				AS BurglarAlarmSysMonitored			
		,CAST(pcx_ilmlocation_jmic.BurAlaWhenClosed_JMIC AS INT64)		AS BurglarAlarmWhenClosed					
		,pcx_ilmlocation_jmic.BurAlaWhenClosedReason_JMIC				AS BurglarAlarmWhenClosedReason
		,CAST(pcx_commonbuilding_jmic.OpeCloMonitored_JMIC AS INT64)	AS OpeningClosingMonitored					
		,CAST(pcx_commonbuilding_jmic.OpeCloSupervised_JMIC AS INT64)	AS OpeningClosingSupervised					
		,pcx_commonbuilding_jmic.NumOnCallAlaCond_JMIC					AS NbrOnCallInAlarmConditions
		,CAST(pcx_commonbuilding_jmic.RespToAlaCond_JMIC AS INT64)		AS RespondToAlarmConditions						
		,pcx_commonbuilding_jmic.RespToAlaCondNOExp_JMIC				AS RespondToAlarmConditionsNoReason	
		,pcx_commonbuilding_jmic.OthEmpAbilDeaAlm_JMIC					AS OtherEmployAbleDeactivateAlarm
		,pctl_boppresafersposition_jmic.NAME							AS SafeScatteredOnPremiseOrInOneArea		
		,CAST(pcx_commonbuilding_jmic.SafVauStoRooByBI_JMIC	AS INT64)	AS SafeVaultStkroomUsedByOther					
		,pcx_commonbuilding_jmic.SafVauStoRooByBIExp_JMIC				AS SafeVaultStkroomUsedByOtherReason
		,CAST(pcx_commonbuilding_jmic.AnySafVauStoRooms_JMIC AS INT64)	AS AnySafeVaultStkroomsOnExtWall						
		,CAST(pcx_commonbuilding_jmic.IsSafeOnExterior AS INT64)		AS IsSafeOnExterior							
		,pcx_commonbuilding_jmic.LeaNumEmpPreBusHr_JMIC					AS LeastNbrEmployOnPremiseBusHrs

	-- T - Hazard Categories
		-- ILM Hazard Categories
		,cHazardCategory.YearsInBusiness_JMIC			AS YearsInBusiness			
		,cHazardCategory.JBTFinancial_JMIC				AS JBTFinancial					
		,cHazardCategory.Inventory_JMIC					AS Inventory						
		,cHazardCategory.ClaimsFree_JMIC				AS ClaimsFree				
		,cHazardCategory.PhysicalProtection_JMIC		AS PhysicalProtection			
		,cHazardCategory.ArmedUniformGuard_JMIC			AS ArmedUniformGuard				
		,cHazardCategory.MetalDetManTrap_JMIC			AS MetalDetManTrap				
		,cHazardCategory.ElecTrackDevices_JMIC			AS ElecTrackDevices				
		,cHazardCategory.HoldUpAlarm_JMIC				AS HoldUpAlarm				
		,cHazardCategory.ElectronicProt_JMIC			AS ElectronicProt				
		,cHazardCategory.PhysProtDoorWindw_JMIC			AS PhysProtectedDoorWindow					
		,cHazardCategory.NoExtGrndFloorExp_JMIC			AS NoExtGrndFloorExposure					
		,cHazardCategory.LckdDoorBuzzSys_JMIC			AS LockedDoorBuzzSys					
		,cHazardCategory.SecuredBldg_JMIC				AS SecuredBldg							
		,cHazardCategory.MontrdFireAlrmSys_JMIC			AS MonitoredFireAlarmSys				
		,cHazardCategory.ClsdBsnssStrgPract_JMIC		AS ClosedBusStoragePractices		
		,cHazardCategory.CameraSys_JMIC					AS CameraSystem						
		,cHazardCategory.DblCylDdBltLcks_JMIC			AS DblCylDeadBoltLocks				
		,cHazardCategory.AddlProtNotMentnd_JMIC			AS AddlProtNotMentioned				
		,cHazardCategory.MltplAlmsMontrng_JMIC			AS MltplAlarmsMonitoring				

	-- T - Safes, Vaults, and Stockrooms
		,pcx_ilmlocation_jmic.TotalInStrgPrcnt_JMIC		AS 	TotalInStoragePercent				
		,pcx_ilmlocation_jmic.BankVaultPercent_JMIC		AS 	BankVaultPercent					
		,pcx_ilmlocation_jmic.OutOfStorageAmt_JMIC		AS 	OutOfStorageAmount					

	-- T - Exclusions & Conditions
		-- Exclusions
		,cLocExcl.BrglInclSpecProp						AS BurglarylInclSpecProp				
		,cLocExcl.BrglInclSpecPropValue					AS BurglaryInclSpecPropValue			
		,cLocExcl.ExclBrgl								AS ExclBurglary						
		,cLocExcl.ExclBrglClosed						AS ExclBurglaryClosed					
		,cLocExcl.ExclBrglClosedHoursOpen				AS ExclBurglaryClosedHoursOpen			
		,cLocExcl.ExclBrglClosedTimeZone				AS ExclBurglaryClosedTimeZone			
		,cLocExcl.ExclBrglClosedTravelLimit				AS ExclBurglaryClosedTravelLimit		
		,cLocExcl.ExclBrglJwlWtchSpec					AS ExclBurglaryJwlWtchSpecified		
		,cLocExcl.ExclBrglJwlWtchSpecValue				AS ExclBurglaryJwlWtchSpecifiedValue	
		,cLocExcl.ExclBrglJwlySpecAmt					AS ExclBurglaryJwlySpecifiedAmt		
		,cLocExcl.ExclBrglJwlySpecAmtValue				AS ExclBurglaryJwlySpecifiedAmtValue	
		,cLocExcl.ExclOutOfSafe							AS ExclOutOfSafe						
		,cLocExcl.FireLightSmkExcl						AS ExclFireLightningSmoke				
		,cLocExcl.SpecBrglLimit							AS HasSpecifiedBurglaryLimit			
		,cLocExcl.SpecBrglLimitValue					AS SpecifiedBurglaryLimitValue			

		-- Policy Conditions
		,cLocCond.AlrmSgnlRsp							AS AlarmSignalResponseReq									
		,cLocCond.BankVaultReq							AS BankVaultReq											
		,cLocCond.BankVaultReq_OutOfSafVltPrcnt			AS BankVaultReqOutOfSafeVaultPercent						
		,cLocCond.BankVaultReq_InSafVltStkPrcnt			AS BankVaultReqInSafeVaultPercent							
		,cLocCond.BankVaultReq_BankVltPrcnt				AS BankVaultReqBankVaultPercent							
		,cLocCond.BrglDeduct							AS BurglaryDeductible										
		,cLocCond.BrglDeduct_Value						AS BurglaryDeductibleValue									
		,cLocCond.EstInv								AS EstimatedInventory										
		,cLocCond.IndSafeMaxLim							AS IndivSafeMaxLimit										
		,cLocCond.IndSafeMaxLim_InSafVltStkPrcnt		AS IndivSafeMaxLimitInSafeVaultStkPercent					
		,cLocCond.IndSafeMaxLim_Mfg						AS IndivSafeMaxLimitMfg									
		,cLocCond.IndSafeVltMaxCap						AS IndivSafeVaultMaxCap									
		,cLocCond.InnerSafChest							AS InnerSafeChest											
		,cLocCond.InnerSafChest_InSafPrcnt				AS InnerSafeChestInSafePercent								
		,cLocCond.InSafePercent							AS InSafePercent											
		,cLocCond.InSafePerIndSafeVltMaxCap				AS InSafePercentIndivSafeVaultMaxCapacity					
		,cLocCond.KeyedInvestRsp						AS KeyedInvestigatorResponse								
		,cLocCond.KeyedInvestRsp_Rsp					AS KeyedInvestigatorResponseReq							
		,cLocCond.LckdCabinets							AS LockedCabinets											
		,cLocCond.LckdCabinets_PrcntKept				AS LockedCabinetsPercentKept								
		,cLocCond.IsMaxDollarLimit						AS IsMaxDollarLimit										
		,cLocCond.MaxDlrLim_OutOfSafAmt					AS MaxLimitOutOfSafeAmt									
		,cLocCond.MaxDlrLimBrgl							AS MaxLimitBurglary										
		,cLocCond.MaxDlrLimBrgl_InSafVltStkPrcnt		AS MaxLimitBurglaryInSafeVaultStkPct						
		,cLocCond.MaxDlrLimBrgl_BurglaryLmt				AS MaxLimitBurglaryBurgLimit								
		,cLocCond.MaxDlrLimBrgl_AOPLmt					AS MaxLimitBurglaryAOPLimit								
		,cLocCond.MaxDlrLimFinMrch						AS MaxLimitFinishedMerch									
		,cLocCond.MaxDlrLimFinMrch_OutOfSafVltAmt		AS MaxLimitFinishedMerchOutOfSafeVaultAmt					
		,cLocCond.MaxDlrLimWnty							AS MaxLimitWarranty										
		,cLocCond.MaxDlrLimWnty_OutOfSafVltAmt			AS MaxLimitWarrantyOutOfSafeVaultAmt						
		,cLocCond.MaxJwlyOutClsd						AS MaxStockValueOutWhenClosed								
		,cLocCond.MaxJwlyOutClsd_JwlryAmt				AS MaxJwlryValueOutWhenClosed								
		,cLocCond.MaxJwlyOutClsd_NonJwlryAmt			AS MaxNonJwlyValueOutWhenClosed							
		,cLocCond.MaxOutClsd							AS MaxOutofSafeWhenClosed									
		,cLocCond.MaxOutClsd_MaxOutOfSafVlt				AS MaxOutWhenClosedMaxOutOfSafeVault						
		,cLocCond.MaxOutClsdWnty						AS MaxOutWhenClosedWithWarranty							
		,cLocCond.MaxOutClsdWnty_MaxOutOfSafVlt			AS MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault			
		,cLocCond.MaxOutOfLckedSafeVltLmtSch			AS MaxOutOfLockedSafeVaultLimitSched						
		,cLocCond.MaxPrItmSafVlt						AS MaxPerItemSafeVault										
		,cLocCond.MaxPrItmSafVlt_CostPerItem			AS MaxPerItemSafeVaultCostPerItem							
		,cLocCond.MaxPrItmStk							AS MaxPerItemSafeVaultStkroom								
		,cLocCond.MaxPrItmStk_CostPerItem				AS MaxPerItemSafeVaultStkroomCostPerItem					
		,cLocCond.MaxValInVlt							AS MaxValueInVault											
		,cLocCond.MaxValInVlt_InSafPrcnt				AS MaxValueInVaultInSafePercent							
		,cLocCond.MinMaxPrptnSaf						AS MinMaxProportionInSafe									
		,cLocCond.MinMaxPrptnSaf_InSafPrcnt				AS MinMaxProportionInSafePercent							
		,cLocCond.MinNbrEmp								AS MinNbrEmployeeCond										
		,cLocCond.MinNbrEmp_Num							AS MinNbrEmployeeCondNumber								
		,cLocCond.MinPrptnValSafVlt						AS MinProportionValueSafeVault								
		,cLocCond.MinPrptnValSafVltIS					AS MinProportionValueSafeVaultInnerSafe					
		,cLocCond.MinPrptnValStk						AS MinProportionValueStkroom								
		,cLocCond.RobberyDeduct							AS RobberyDeductible										
		,cLocCond.RobberyDeduct_Deduct					AS RobberyDeductibleValue 									
		,cLocCond.SafeBrglDeduct						AS SafeBurglaryDeductible									
		,cLocCond.SafeBrglDeduct_Deduct					AS SafeBurglaryDeductibleValue								
		,cLocCond.SafMinLim								AS SafeMinLimit											
		,cLocCond.SafVltHurrWrn							AS SafeVaultHurrWarningReq									
		,cLocCond.SafVltHurrWrnTX						AS SaefVaultHurrWarningReqTX								
		,cLocCond.SafVltHurrWrnDed						AS SafeVaultHurrWarningReqDeductible						
		,cLocCond.SafVltHurrWrnDed_Deduct				AS SafeVaultHurrWarningReqDeductibleValue					
		,cLocCond.ShowJwlCond							AS ShowJewelryConditions									
		,cLocCond.ShrdPremSec							AS SharedPremiseSecurity									
		,cLocCond.StkMaxDlrLim							AS StkroomMaxDollarLimit									
		,cLocCond.StkMaxDlrLim_OutOfSafVltAmt			AS StkroomMaxLimitOutOfSafeVaultAmt						
		,cLocCond.StkMaxLim								AS StkroomMaxLimit											
		,cLocCond.StkMaxLim_InSafPrcnt					AS StkroomMaxLimitInSafePercent							
		,cLocCond.ThftProt								AS TheftProtection											
		,cLocCond.ThftProt_Desc							AS TheftProtectionDesc										
		,cLocCond.ThftPRotGrd							AS TheftProtectionGuardWarranty							
		,cLocCond.ThftProtSecDev						AS TheftProtectionSecurityDevice							
		,cLocCond.ThftProtSecDev_Desc					AS TheftProtectionSecurityDeviceDesc						
		,cLocCond.TotalPctInSaf							AS TotalPercentInSafe										
		,cLocCond.TotalPctInSaf_InSafVltStkPrcnt		AS TotalPercentInSafeInSafeVaultStkrmPct					
		,cLocCond.TotalPctInSaf_NotToExceedAmt			AS TotalPercentInSafeNotToExceedAmt						
		,cLocCond.ShwcsShwWndwCond						AS ShowcaseOrWindowCondition							

	-- T - Location-Coverage Questions
		-- BOP/Inland Marine Loc Home Based Business Questions (183596)
		,cHazardCategory.BusinessPremise_JMIC							AS BusinessPremise				
		,cHazardCategory.InviteJewelryClients_JMIC						AS InviteJewelryClients
		,cHazardCategory.SignageAtHome_JMIC								AS SignageAtHome
		,cHazardCategory.AnimalsInPremise_JMIC							AS AnimalsOnPremise
		,cHazardCategory.WhatSpecies_JMIC								AS AnimalSpecies
		,cHazardCategory.SwimmingPool_JMIC								AS SwimmingPool
		,cHazardCategory.Trampoline_JMIC								AS Trampoline	
		,cHazardCategory.MerchandiseAccessChildren_JMIC					AS MerchandiseAccessibleToChildren	
		,cHazardCategory.DescribeHow_JMIC								AS DescribeHowAccessRestricted		

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date} ) pc_policyperiod
 
 		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcx_ilmlocation_jmic
		ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date} ) pc_policyline
		ON  pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType

		INNER JOIN RiskLocationIMConfig AS ConfigILMLine
		ON  ConfigILMLine.Value = pctl_policyline.TYPECODE
		AND ConfigILMLine.Key = 'IMLineCode' 

		--INNER JOIN RiskLocationIMConfig AS ConfigILMLocationRisk -- 04/27/2023
		--ON ConfigILMLocationRisk.Key='IMLocationLevelRisk'

		--INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date} ) pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		--Join in the t_PrimaryRatingLocationIM Table to map the Natural Key for RatingLocationKey		
		LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationIM` AS t_PrimaryRatingLocationIM -- 04/27/2023
			ON t_PrimaryRatingLocationIM.PolicyPeriodID = pc_policyperiod.ID
			AND t_PrimaryRatingLocationIM.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((t_PrimaryRatingLocationIM.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				OR 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationIM.PolicyLineOfBusiness = 'BusinessOwnersLine'))
/*
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) AS pc_policylocation
		ON pc_policylocation.BranchID = t_PrimaryRatingLocationIM.PolicyPeriodID  
		AND pc_policylocation.LocationNum = t_PrimaryRatingLocationIM.RatingLocationNum  
		AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
*/
				
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) pc_policylocation
		ON  pc_policylocation.BranchID = pcx_ilmlocation_jmic.BranchID
		AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcx_ilmcost_jmic
		ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
		AND pcx_ilmcost_jmic.ILMLocation_JMIC = pcx_ilmlocation_jmic.FixedID
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` pctl_state
		ON pctl_state.ID = pc_policylocation.StateInternal

		LEFT JOIN `{project}.{pc_dataset}.pctl_country` pctl_country
		ON pctl_country.ID = pc_policylocation.CountryInternal
	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcx_commonlocation_jmic   
		ON  pcx_commonlocation_jmic.BranchID = pcx_ilmlocation_jmic.BranchID
		AND pcx_commonlocation_jmic.FixedID = pcx_ilmlocation_jmic.CommonLocation
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_territorycode` WHERE _PARTITIONTIME = {partition_date} ) pc_territorycode
		ON  pc_territorycode.BranchID = pc_policylocation.BranchID
		AND pc_territorycode.PolicyLocation = pc_policylocation.FixedID 
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_territorycode.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_territorycode.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_boplocationtype_jmic` pctl_boplocationtype_jmic
		ON pctl_boplocationtype_jmic.ID = pcx_commonlocation_jmic.LocationType_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastal_jmic`  pctl_bopcoastal_jmic
		ON pctl_bopcoastal_jmic.ID = pc_policylocation.Coastal_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastalzones_jmic` pctl_bopcoastalzones_jmic
		ON pctl_bopcoastalzones_jmic.ID = pc_policylocation.CoastalZones_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` pctl_fireprotectclass
		ON pctl_fireprotectclass.ID = pc_policylocation.FireProtectClass	
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_ilmlocationratedas_jmic` pctl_ilmlocationratedas_jmic
		ON pctl_ilmlocationratedas_jmic.ID = pcx_ilmlocation_jmic.RatedAs_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_addressstatus_jmic` pctl_addressstatus_jmic    
		ON pctl_addressstatus_jmic.ID = pc_policylocation.AddressStatus_JMICInternal

		-- Building
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonbuilding_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcx_commonbuilding_jmic 
		ON  pcx_commonbuilding_jmic.BranchID = pcx_ilmlocation_jmic.BranchID
		AND pcx_commonbuilding_jmic.FixedID = pcx_ilmlocation_jmic.CommonBuilding
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonbuilding_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonbuilding_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_building` WHERE _PARTITIONTIME = {partition_date} ) pc_building
		ON  pc_building.BranchID = pcx_commonbuilding_jmic.BranchID
		AND pc_building.FixedID = pcx_commonbuilding_jmic.Building
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_building.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_building.ExpirationDate,pc_policyperiod.PeriodEnd)

/*		-- Primary Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date} ) AS pc_effectivedatedfields
			ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) AS PrimaryPolicyLocation
			ON  PrimaryPolicyLocation.BranchID = pcx_ilmlocation_jmic.BranchID	--pc_policyperiod.ID
			AND PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)
*/
		LEFT JOIN `{project}.{pc_dataset}.pctl_bopconstructiontype` pctl_bopconstructiontype
		ON pctl_bopconstructiontype.ID = pcx_commonbuilding_jmic.ConstructionType

		LEFT JOIN `{project}.{pc_dataset}.pctl_sprinklered` pctl_sprinklered
		ON pctl_sprinklered.ID = pc_building.SprinklerCoverage

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopclasscode` WHERE _PARTITIONTIME = {partition_date} ) pc_bopclasscode
		ON pc_bopclasscode.ID = pcx_commonbuilding_jmic.BOPLocInsBusCCID
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopclasscode.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopclasscode.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_buildingclass_jmic` pctl_buildingclass_jmic    
		ON pctl_buildingclass_jmic.ID = pcx_commonbuilding_jmic.BuildingClass_JMIC

		-- Security Information
		LEFT JOIN `{project}.{pc_dataset}.pctl_boppresafersposition_jmic` pctl_boppresafersposition_jmic
		ON pctl_boppresafersposition_jmic.ID = pcx_commonbuilding_jmic.SafScaThrPreConOneArea_JMIC

		--Building Improvement Breakdown
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
		ON cBldgImpr.BranchID = pcx_commonbuilding_jmic.BranchID
		AND cBldgImpr.Building = pcx_commonbuilding_jmic.Building

		-- Hazard Categories &
		-- Location-Coverage Questions
		LEFT JOIN `{project}.{dest_dataset}.t_RiskLocationIM_HazardCat_LocAnsPivot` AS cHazardCategory
 		ON  cHazardCategory.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID
		--cHazardCategory.BranchID = pcx_ilmlocation_jmic.BranchID
 		--AND cHazardCategory.PolicyLocation = pcx_ilmlocation_jmic.Location
 		--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(cHazardCategory.EffectiveDate,pc_policyperiod.PeriodStart)
 		--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(cHazardCategory.ExpirationDate,pc_policyperiod.PeriodEnd)

		-- Exclusions
		LEFT JOIN
			(SELECT 
				RiskLocationInfo.IMLocationPublicID
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocBrglInclSpecProp_JMIC'	 THEN CAST(pcxILMExcl.MediumStringTerm1Avl AS INT64)			END) AS BrglInclSpecProp
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocBrglInclSpecProp_JMIC'	 THEN pcxILMExcl.MediumStringTerm1							END) AS BrglInclSpecPropValue
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrgl_JMIC'			 THEN 1														END) AS ExclBrgl
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglClosed_JMIC'		 THEN CAST(COALESCE(pcxILMExcl.ShortStringTerm1Avl
																													,pcxILMExcl.ShortStringTerm2Avl
																													,pcxILMExcl.DirectTerm1Avl) AS INT64)	END) AS ExclBrglClosed
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglClosed_JMIC'		 THEN pcxILMExcl.ShortStringTerm1							END) AS ExclBrglClosedHoursOpen
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglClosed_JMIC'		 THEN pcxILMExcl.ShortStringTerm2							END) AS ExclBrglClosedTimeZone
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglClosed_JMIC'		 THEN pcxILMExcl.DirectTerm1								END) AS ExclBrglClosedTravelLimit
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglJwlWtchSpec_JMIC' THEN CAST(pcxILMExcl.DirectTerm1Avl AS INT64)				END) AS ExclBrglJwlWtchSpec
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglJwlWtchSpec_JMIC' THEN pcxILMExcl.DirectTerm1								END) AS ExclBrglJwlWtchSpecValue
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglJwlySpecAmt_JMIC' THEN CAST(pcxILMExcl.DirectTerm1Avl AS INT64)				END) AS ExclBrglJwlySpecAmt
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclBrglJwlySpecAmt_JMIC' THEN pcxILMExcl.DirectTerm1								END) AS ExclBrglJwlySpecAmtValue
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocExclOutOfSafe_JMIC'		 THEN 1														END) AS ExclOutOfSafe
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocFireLightSmkExcl_JMIC'	 THEN 1														END) AS FireLightSmkExcl
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocSpecBrglLimit_JMIC'		 THEN CAST(pcxILMExcl.DirectTerm1Avl AS INT64)				END) AS SpecBrglLimit
				,MAX(CASE WHEN pcxILMExcl.PatternCode = 'ILMLocSpecBrglLimit_JMIC'		 THEN pcxILMExcl.DirectTerm1								END) AS SpecBrglLimitValue

			FROM RiskLocationInfo AS RiskLocationInfo
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationexcl_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcxILMExcl
				ON  pcxILMExcl.BranchID = RiskLocationInfo.BranchID
				AND pcxILMExcl.ILMLocation = RiskLocationInfo.IMLocationFixedID
				AND RiskLocationInfo.EditEffectiveDate >= COALESCE(pcxILMExcl.EffectiveDate,RiskLocationInfo.PeriodStart)
				AND RiskLocationInfo.EditEffectiveDate <  COALESCE(pcxILMExcl.ExpirationDate,RiskLocationInfo.PeriodEnd)
			GROUP BY 
				RiskLocationInfo.IMLocationPublicID
			) AS cLocExcl
		ON cLocExcl.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID

		-- Conditions
		LEFT JOIN
			(SELECT 
				RiskLocationInfo.IMLocationPublicID
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocAlrmSgnlRsp_JMIC'				THEN 1	END)													AS AlrmSgnlRsp							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm3Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS BankVaultReq							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN pcxILMCond.DirectTerm2 END)								AS BankVaultReq_OutOfSafVltPrcnt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS BankVaultReq_InSafVltStkPrcnt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN pcxILMCond.DirectTerm3 END)								AS BankVaultReq_BankVltPrcnt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBrglDeduct_JMIC'					THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.ChoiceTerm2Avl) AS INT64) END)	AS BrglDeduct							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocBrglDeduct_JMIC'					THEN pcxILMCond.ChoiceTerm1 END)								AS BrglDeduct_Value						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocEstInv_JMIC'						THEN 1	END)													AS EstInv								
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CAST(COALESCE(pcxILMCond.BooleanTerm1Avl
																															,pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS IndSafeMaxLim						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS IndSafeMaxLim_InSafVltStkPrcnt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CAST(pcxILMCond.BooleanTerm1 AS INT64) END)					AS IndSafeMaxLim_Mfg					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocIndSafeVltMaxCap_JM'				THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS IndSafeVltMaxCap						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocInnerSafChest_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS InnerSafChest						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocInnerSafChest_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS InnerSafChest_InSafPrcnt				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocInSafePercent_JM'				THEN CAST(pcxILMCond.ChoiceTerm1Avl AS INT64) END)				AS InSafePercent						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocInSafePerIndSafeVltMaxCap_JM'	THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.DirectTerm3Avl) AS INT64) END)	AS InSafePerIndSafeVltMaxCap			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocKeyedInvestRsp_JMIC'				THEN CAST(pcxILMCond.ChoiceTerm1Avl AS INT64) END)				AS KeyedInvestRsp						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocKeyedInvestRsp_JMIC'				THEN pcxILMCond.ChoiceTerm1 END)								AS KeyedInvestRsp_Rsp					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocLckdCabinets_JMIC'				THEN CAST(pcxILMCond.DirectTerm1Avl AS INT64) END)				AS LckdCabinets							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocLckdCabinets_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS LckdCabinets_PrcntKept				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLim_JMIC'					THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS IsMaxDollarLimit							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLim_JMIC'					THEN CONCAT('Derived: ',CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS MaxDlrLim_OutOfSafAmt				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.DirectTerm3Avl) AS INT64) END)	AS MaxDlrLimBrgl						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS MaxDlrLimBrgl_InSafVltStkPrcnt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN pcxILMCond.DirectTerm2 END)								AS MaxDlrLimBrgl_BurglaryLmt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN pcxILMCond.DirectTerm3 END)								AS MaxDlrLimBrgl_AOPLmt					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimFinMrch_JMIC'			THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MaxDlrLimFinMrch						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimFinMrch_JMIC'			THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS MaxDlrLimFinMrch_OutOfSafVltAmt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimWnty_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS MaxDlrLimWnty						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxDlrLimWnty_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxDlrLimWnty_OutOfSafVltAmt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MaxJwlyOutClsd						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxJwlyOutClsd_JwlryAmt				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN pcxILMCond.DirectTerm2 END)								AS MaxJwlyOutClsd_NonJwlryAmt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxOutClsd_JMIC'					THEN CAST(pcxILMCond.DirectTerm1Avl AS INT64) END)				AS MaxOutClsd							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxOutClsd_JMIC'					THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS MaxOutClsd_MaxOutOfSafVlt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxOutClsdWnty_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS MaxOutClsdWnty						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxOutClsdWnty_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxOutClsdWnty_MaxOutOfSafVlt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxOutOfLckedSafeVltLmtSch_JM'	THEN CAST(pcxILMCond.ChoiceTerm1Avl AS INT64) END)				AS MaxOutOfLckedSafeVltLmtSch			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxPrItmSafVlt_JMIC'				THEN CAST(pcxILMCond.DirectTerm1Avl AS INT64) END)				AS MaxPrItmSafVlt						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxPrItmSafVlt_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxPrItmSafVlt_CostPerItem			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxPrItmStk_JMIC'				THEN CAST(pcxILMCond.DirectTerm1Avl AS INT64) END)				AS MaxPrItmStk							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxPrItmStk_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxPrItmStk_CostPerItem				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxValInVlt_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MaxValInVlt							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMaxValInVlt_JMIC'				THEN pcxILMCond.DirectTerm1 END)								AS MaxValInVlt_InSafPrcnt				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinMaxPrptnSaf_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm3Avl
																															,pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS MinMaxPrptnSaf						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinMaxPrptnSaf_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm3 AS STRING)) END)		AS MinMaxPrptnSaf_InSafPrcnt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinNbrEmp_JMIC'					THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.ChoiceTerm1Avl) AS INT64) END)	AS MinNbrEmp							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinNbrEmp_JMIC'					THEN pcxILMCond.DirectTerm1 END)								AS MinNbrEmp_Num						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinPrptnValSafVlt_JMIC'			THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValSafVlt					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinPrptnValSafVltIS_JMIC'		THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValSafVltIS					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocMinPrptnValStk_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValStk						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocRobberyDeduct_JMIC'				THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.ChoiceTerm2Avl) AS INT64) END)	AS RobberyDeduct						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocRobberyDeduct_JMIC'				THEN pcxILMCond.ChoiceTerm1 END)								AS RobberyDeduct_Deduct					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafeBrglDeduct_JMIC'				THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.ChoiceTerm2Avl) AS INT64) END)	AS SafeBrglDeduct						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafeBrglDeduct_JMIC'				THEN pcxILMCond.ChoiceTerm1 END)								AS SafeBrglDeduct_Deduct				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafMinLim_JMIC'					THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS SafMinLim							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafVltHurrWrn_JMIC'				THEN 1	END)													AS SafVltHurrWrn						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafVltHurrWrnTX_JMIC'			THEN 1	END)													AS SafVltHurrWrnTX					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafVltHurrWrnDed_JMIC'			THEN CAST(pcxILMCond.ChoiceTerm1Avl AS INT64) END)				AS SafVltHurrWrnDed						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocSafVltHurrWrnDed_JMIC'			THEN pcxILMCond.ChoiceTerm1 END)								AS SafVltHurrWrnDed_Deduct				
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocShowJwlCond_JMIC'				THEN 1	END)													AS ShowJwlCond							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocShrdPremSec_JMIC'				THEN 1	END)													AS ShrdPremSec							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocStkMaxDlrLim_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS StkMaxDlrLim							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocStkMaxDlrLim_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS StkMaxDlrLim_OutOfSafVltAmt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocStkMaxLim_JMIC'					THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS StkMaxLim							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocStkMaxLim_JMIC'					THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS StkMaxLim_InSafPrcnt					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocThftProt_JMIC'					THEN CAST(pcxILMCond.MediumStringTerm1Avl AS INT64) END)			AS ThftProt								
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocThftProt_JMIC'					THEN pcxILMCond.MediumStringTerm1 END)							AS ThftProt_Desc						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocThftPRotGrd_JMIC'				THEN 1	END)													AS ThftPRotGrd							
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocThftProtSecDev_JMIC'				THEN CAST(pcxILMCond.MediumStringTerm1Avl AS INT64) END)			AS ThftProtSecDev						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocThftProtSecDev_JMIC'				THEN pcxILMCond.MediumStringTerm1 END)							AS ThftProtSecDev_Desc					
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN CAST(COALESCE(pcxILMCond.DirectTerm1Avl
																															,pcxILMCond.DirectTerm2Avl) AS INT64) END)	AS TotalPctInSaf						
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN CONCAT('Derived: ', CAST(pcxILMCond.DirectTerm1 AS STRING)) END)		AS TotalPctInSaf_InSafVltStkPrcnt		
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN pcxILMCond.DirectTerm2 END)								AS TotalPctInSaf_NotToExceedAmt			
				,MAX(CASE WHEN pcxILMCond.PatternCode = 'ILMShwcsShwWndwCond_JMIC'				THEN CAST(COALESCE(pcxILMCond.ChoiceTerm1Avl
																															,pcxILMCond.ChoiceTerm2Avl
																															,pcxILMCond.MediumStringTerm1Avl
																															,pcxILMCond.DirectTerm1Avl) AS INT64) END)	AS ShwcsShwWndwCond						

			FROM RiskLocationInfo AS RiskLocationInfo
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcond_jmic` WHERE _PARTITIONTIME = {partition_date} ) pcxILMCond 
				ON  pcxILMCond.BranchID = RiskLocationInfo.BranchID
				AND pcxILMCond.ILMLocation = RiskLocationInfo.IMLocationFixedID
				AND RiskLocationInfo.EditEffectiveDate >= COALESCE(pcxILMCond.EffectiveDate,RiskLocationInfo.PeriodStart)
				AND RiskLocationInfo.EditEffectiveDate <  COALESCE(pcxILMCond.ExpirationDate,RiskLocationInfo.PeriodEnd)
			GROUP BY 
				RiskLocationInfo.IMLocationPublicID
			) AS cLocCond
		ON cLocCond.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID

	WHERE	1 = 1
	--AND _PARTITIONTIME = {partition_date} 
		--AND pc_policyperiod.PolicyNumber = '55-001338'
		--AND pc_job.JobNumber = '183596' --'3928213'--'1093622'--'02266'--'99138'

--	-- 04/27/2023
--	UNION ALL
--	----------------------------------------------------------------------------
--	---Risk Location LOB or Line
--	----------------------------------------------------------------------------
--	SELECT 
--/*		DENSE_RANK() OVER(PARTITION BY	pc_policylocation.ID
--							ORDER BY	ISNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,ISNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--							) AS LocationRank
--		,DENSE_RANK() OVER(PARTITION BY pc_policylocation.FixedID, pcx_ilmlocation_jmic.FixedID, pc_policyperiod.ID
--							ORDER BY	ISNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,ISNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--							) AS FixedLocationRank
--		,DENSE_RANK() OVER(PARTITION BY pc_policylocation.ID
--							ORDER BY	ISNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,pcx_ilmcost_jmic.ID DESC
--							) AS CostRank
--*/
--		DENSE_RANK() OVER(PARTITION BY	pcx_ilmlocation_jmic.ID
--							ORDER BY	IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--							) AS LocationRank
--		,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocation_jmic.FixedID, pc_policyperiod.ID
--							ORDER BY	IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
--							) AS FixedLocationRank
--		,DENSE_RANK() OVER(PARTITION BY pcx_ilmlocation_jmic.ID
--							ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
--										,pcx_ilmcost_jmic.ID DESC
--							) AS CostRank


--		,pcx_ilmlocation_jmic.PublicID								AS LocationPublicID
--		,pc_policyperiod.PublicID									AS PolicyPeriodPublicID
--		,ConfigILMLocationRisk.Value								AS RiskLevel
--		,pcx_ilmlocation_jmic.EffectiveDate
--		,pcx_ilmlocation_jmic.ExpirationDate
--		,pc_policyperiod.PolicyNumber
--		,pc_job.JobNumber
--		--,pc_policyperiod.EditEffectiveDate
--/*		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
--					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) 
--				THEN 1 ELSE 0 END 
--			AS IsTransactionSliceEffective
--*/		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) 
--				THEN 1 ELSE 0 END 
--			AS IsTransactionSliceEffective
--		,pc_policyperiod.status										AS PolicyPeriodStatus

--	-- T - Details
--		-- Location
--		--,pc_policylocation.LocationNum								AS PrimaryLocationNumber
--		,pc_policylocation.LocationNum								AS LocationNumber
--		,t_PrimaryRatingLocationIM.IsPrimaryLocation		AS IsPrimaryLocation
--		,pc_policylocation.FixedID									AS LocationFixedID
--		,pc_policylocation.AddressLine1Internal						AS LocationAddress1
--		,pc_policylocation.AddressLine2Internal						AS LocationAddress2
--		,pc_policylocation.CityInternal								AS LocationCity
--		,pctl_state.NAME											AS LocationState
--		,pctl_state.TYPECODE										AS LocationStateCode
--		,pctl_country.NAME											AS LocationCountry
--		,pc_policylocation.PostalCodeInternal						AS LocationPostalCode
--		,pctl_addressstatus_jmic.NAME								AS LocationAddressStatus
--		,pc_policylocation.CountyInternal							AS LocationCounty
--		,pc_policylocation.FIPSCode_JMICInternal					AS LocationCountyFIPS
--		,pc_policylocation.CountyPopulation_JMIC					AS LocationCountyPopulation

--		--,phone
--		,pc_territorycode.Code											AS TerritoryCode
--		,pctl_bopcoastal_jmic.TYPECODE									AS Coastal
--		,pctl_bopcoastalzones_jmic.TYPECODE								AS CoastalZone
--		,pc_policylocation.SegmentationCode_JMIC						AS SegmentationCode

--		-- General Information (Type of Business based on sales)
--		,pcx_commonlocation_jmic.RetailSale_JMIC						AS RetailSale	
--		,pcx_commonlocation_jmic.ServiceSale_JMIC						AS RepairSale
--		,pcx_commonlocation_jmic.AppraisalSale_JMIC						AS AppraisalSale		
--		,pcx_commonlocation_jmic.WholesaleSale_JMIC						AS WholesaleSale		
--		,pcx_commonlocation_jmic.ManufacturingSale_JMIC					AS ManufacturingSale	
--		,pcx_commonlocation_jmic.RefiningSale_JMIC						AS RefiningSale		
--		,pcx_commonlocation_jmic.GoldBuyingSale_JMIC					AS GoldBuyingSale	
--		,pcx_commonlocation_jmic.PawnSale_JM							AS PawnSale
--		,pctl_ilmlocationratedas_jmic.TYPECODE							AS RatedAS

--		-- Employee Informatioin
--		,pcx_commonlocation_jmic.FullTimeEmployees_JMIC					AS FullTimeEmployees
--		,pcx_commonlocation_jmic.PartTimeEmployees_JMIC					AS PartTimeEmployees
--		,pcx_commonlocation_jmic.Owners_JMIC							AS Owners

--		-- Public Protection
--		,pctl_fireprotectclass.TYPECODE									AS PublicProtection

--		-- Location Information
--		,pctl_boplocationtype_jmic.TYPECODE								AS LocationTypeCode
--		,pctl_boplocationtype_jmic.NAME									AS LocationTypeName
--		,pcx_commonlocation_jmic.OtherDescription_JMIC					AS LocationTypeOtherDescription
--		,pcx_commonlocation_jmic.AnnualSales_JMIC						AS AnnualSales
--,CAST(pcx_ilmlocation_jmic.AreInetSalesDone_JMICASINT64)	AS AreSalesPerformedViaInternet
--		,pcx_ilmlocation_jmic.InternetSalesAmt_JMIC						AS InternetSalesAmount 
--		--,Name/Address of other jlry business/loc owned-managed by principals/officers
--		,pcx_ilmlocation_jmic.NormalBusHours_JMIC						AS NormalBusinessHours
--		,pcx_ilmlocation_jmic.TotalValShip12Mos_JMIC					AS TotalValueShippedInLast12Months 

--		-- Building Construction
--		,pctl_bopconstructiontype.TYPECODE								AS ConstructionCode
--		,pctl_bopconstructiontype.DESCRIPTION							AS ConstructionType
--		,pc_building.YearBuilt											AS ConstructionYearBuilt
--	
--		,pctl_sprinklered.NAME											AS PercentSprinklered
--		,pctl_buildingclass_jmic.NAME									AS BuildingClass
--		,pc_bopclasscode.Code											AS LocationClassInsuredBusinessCode
--		,pc_bopclasscode.Classification									AS LocationClassInsuredBusinessClassification

--		-- Building Improvement
--		,cBldgImpr.HeatingYear											AS HeatingYear
--		,cBldgImpr.PlumbingYear											AS PlumbingYear
--		,cBldgImpr.RoofingYear											AS RoofingYear
--		,cBldgImpr.WiringYear											AS WiringYear

--	-- T - Hazard Categories
--		-- ILM Hazard Categories
--		,cHazardCategory.YearsInBusiness_JMIC					AS YearsInBusiness	
--		,cHazardCategory.JBTFinancial_JMIC						AS JBTFinancial		
--		,cHazardCategory.Inventory_JMIC							AS Inventory			
--		,cHazardCategory.ClaimsFree_JMIC						AS ClaimsFree		
--		,cHazardCategory.PhysicalProtection_JMIC				AS PhysicalProtection
--		,cHazardCategory.ArmedUniformGuard_JMIC					AS ArmedUniformGuard	
--		,cHazardCategory.MetalDetManTrap_JMIC					AS MetalDetManTrap	
--		,cHazardCategory.ElecTrackDevices_JMIC					AS ElecTrackDevices	
--		,cHazardCategory.HoldUpAlarm_JMIC						AS HoldUpAlarm		
--		,cHazardCategory.ElectronicProt_JMIC					AS ElectronicProt	
--		,cHazardCategory.PhysProtDoorWindw_JMIC					AS PhysProtectedDoorWindow	
--		,cHazardCategory.NoExtGrndFloorExp_JMIC					AS NoExtGrndFloorExposure	
--		,cHazardCategory.LckdDoorBuzzSys_JMIC					AS LockedDoorBuzzSys	
--		,cHazardCategory.SecuredBldg_JMIC						AS SecuredBldg		
--		,cHazardCategory.MontrdFireAlrmSys_JMIC					AS MonitoredFireAlarmSys	
--		,cHazardCategory.ClsdBsnssStrgPract_JMIC				AS ClosedBusStoragePractices
--		,cHazardCategory.CameraSys_JMIC							AS CameraSystem			
--		,cHazardCategory.DblCylDdBltLcks_JMIC					AS DblCylDeadBoltLocks	
--		,cHazardCategory.AddlProtNotMentnd_JMIC					AS AddlProtNotMentioned	
--		,cHazardCategory.MltplAlmsMontrng_JMIC					AS MltplAlarmsMonitoring	

--	-- T - Safes, Vaults, and Stockrooms
--		,pcx_ilmlocation_jmic.TotalInStrgPrcnt_JMIC			AS TotalInStoragePercent
--		,pcx_ilmlocation_jmic.BankVaultPercent_JMIC			AS BankVaultPercent
--		,pcx_ilmlocation_jmic.OutOfStorageAmt_JMIC			AS OutOfStorageAmount

--	-- T - Exclusions & Conditions
--		-- Exclusions
--		,cLocExcl.BrglInclSpecProp						AS BurglarylInclSpecProp				
--		,cLocExcl.BrglInclSpecPropValue					AS BurglaryInclSpecPropValue			
--		,cLocExcl.ExclBrgl								AS ExclBurglary						
--		,cLocExcl.ExclBrglClosed						AS ExclBurglaryClosed					
--		,cLocExcl.ExclBrglClosedHoursOpen				AS ExclBurglaryClosedHoursOpen			
--		,cLocExcl.ExclBrglClosedTimeZone				AS ExclBurglaryClosedTimeZone			
--		,cLocExcl.ExclBrglClosedTravelLimit				AS ExclBurglaryClosedTravelLimit		
--		,cLocExcl.ExclBrglJwlWtchSpec					AS ExclBurglaryJwlWtchSpecified		
--		,cLocExcl.ExclBrglJwlWtchSpecValue				AS ExclBurglaryJwlWtchSpecifiedValue	
--		,cLocExcl.ExclBrglJwlySpecAmt					AS ExclBurglaryJwlySpecifiedAmt		
--		,cLocExcl.ExclBrglJwlySpecAmtValue				AS ExclBurglaryJwlySpecifiedAmtValue	
--		,cLocExcl.ExclOutOfSafe							AS ExclOutOfSafe						
--		,cLocExcl.FireLightSmkExcl						AS ExclFireLightningSmoke				
--		,cLocExcl.SpecBrglLimit							AS HasSpecifiedBurglaryLimit			
--		,cLocExcl.SpecBrglLimitValue					AS SpecifiedBurglaryLimitValue

--		-- Policy Conditions
--		,cLocCond.AlrmSgnlRsp							AS AlarmSignalResponseReq									
--		,cLocCond.BankVaultReq							AS BankVaultReq											
--		,cLocCond.BankVaultReq_OutOfSafVltPrcnt			AS BankVaultReqOutOfSafeVaultPercent						
--		,cLocCond.BankVaultReq_InSafVltStkPrcnt			AS BankVaultReqInSafeVaultPercent							
--		,cLocCond.BankVaultReq_BankVltPrcnt				AS BankVaultReqBankVaultPercent							
--		,cLocCond.BrglDeduct							AS BurglaryDeductible										
--		,cLocCond.BrglDeduct_Value						AS BurglaryDeductibleValue									
--		,cLocCond.EstInv								AS EstimatedInventory										
--		,cLocCond.IndSafeMaxLim							AS IndivSafeMaxLimit										
--		,cLocCond.IndSafeMaxLim_InSafVltStkPrcnt		AS IndivSafeMaxLimitInSafeVaultStkPercent					
--		,cLocCond.IndSafeMaxLim_Mfg						AS IndivSafeMaxLimitMfg									
--		,cLocCond.IndSafeVltMaxCap						AS IndivSafeVaultMaxCap									
--		,cLocCond.InnerSafChest							AS InnerSafeChest											
--		,cLocCond.InnerSafChest_InSafPrcnt				AS InnerSafeChestInSafePercent								
--		,cLocCond.InSafePercent							AS InSafePercent											
--		,cLocCond.InSafePerIndSafeVltMaxCap				AS InSafePercentIndivSafeVaultMaxCapacity					
--		,cLocCond.KeyedInvestRsp						AS KeyedInvestigatorResponse								
--		,cLocCond.KeyedInvestRsp_Rsp					AS KeyedInvestigatorResponseReq							
--		,cLocCond.LckdCabinets							AS LockedCabinets											
--		,cLocCond.LckdCabinets_PrcntKept				AS LockedCabinetsPercentKept								
--		,cLocCond.IsMaxDollarLimit						AS IsMaxDollarLimit										
--		,cLocCond.MaxDlrLim_OutOfSafAmt					AS MaxLimitOutOfSafeAmt									
--		,cLocCond.MaxDlrLimBrgl							AS MaxLimitBurglary										
--		,cLocCond.MaxDlrLimBrgl_InSafVltStkPrcnt		AS MaxLimitBurglaryInSafeVaultStkPct						
--		,cLocCond.MaxDlrLimBrgl_BurglaryLmt				AS MaxLimitBurglaryBurgLimit								
--		,cLocCond.MaxDlrLimBrgl_AOPLmt					AS MaxLimitBurglaryAOPLimit								
--		,cLocCond.MaxDlrLimFinMrch						AS MaxLimitFinishedMerch									
--		,cLocCond.MaxDlrLimFinMrch_OutOfSafVltAmt		AS MaxLimitFinishedMerchOutOfSafeVaultAmt					
--		,cLocCond.MaxDlrLimWnty							AS MaxLimitWarranty										
--		,cLocCond.MaxDlrLimWnty_OutOfSafVltAmt			AS MaxLimitWarrantyOutOfSafeVaultAmt						
--		,cLocCond.MaxJwlyOutClsd						AS MaxStockValueOutWhenClosed								
--		,cLocCond.MaxJwlyOutClsd_JwlryAmt				AS MaxJwlryValueOutWhenClosed								
--		,cLocCond.MaxJwlyOutClsd_NonJwlryAmt			AS MaxNonJwlyValueOutWhenClosed							
--		,cLocCond.MaxOutClsd							AS MaxOutofSafeWhenClosed									
--		,cLocCond.MaxOutClsd_MaxOutOfSafVlt				AS MaxOutWhenClosedMaxOutOfSafeVault						
--		,cLocCond.MaxOutClsdWnty						AS MaxOutWhenClosedWithWarranty							
--		,cLocCond.MaxOutClsdWnty_MaxOutOfSafVlt			AS MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault			
--		,cLocCond.MaxOutOfLckedSafeVltLmtSch			AS MaxOutOfLockedSafeVaultLimitSched						
--		,cLocCond.MaxPrItmSafVlt						AS MaxPerItemSafeVault										
--		,cLocCond.MaxPrItmSafVlt_CostPerItem			AS MaxPerItemSafeVaultCostPerItem							
--		,cLocCond.MaxPrItmStk							AS MaxPerItemSafeVaultStkroom								
--		,cLocCond.MaxPrItmStk_CostPerItem				AS MaxPerItemSafeVaultStkroomCostPerItem					
--		,cLocCond.MaxValInVlt							AS MaxValueInVault											
--		,cLocCond.MaxValInVlt_InSafPrcnt				AS MaxValueInVaultInSafePercent							
--		,cLocCond.MinMaxPrptnSaf						AS MinMaxProportionInSafe									
--		,cLocCond.MinMaxPrptnSaf_InSafPrcnt				AS MinMaxProportionInSafePercent							
--		,cLocCond.MinNbrEmp								AS MinNbrEmployeeCond										
--		,cLocCond.MinNbrEmp_Num							AS MinNbrEmployeeCondNumber								
--		,cLocCond.MinPrptnValSafVlt						AS MinProportionValueSafeVault								
--		,cLocCond.MinPrptnValSafVltIS					AS MinProportionValueSafeVaultInnerSafe					
--		,cLocCond.MinPrptnValStk						AS MinProportionValueStkroom								
--		,cLocCond.RobberyDeduct							AS RobberyDeductible										
--		,cLocCond.RobberyDeduct_Deduct					AS RobberyDeductibleValue 									
--		,cLocCond.SafeBrglDeduct						AS SafeBurglaryDeductible									
--		,cLocCond.SafeBrglDeduct_Deduct					AS SafeBurglaryDeductibleValue								
--		,cLocCond.SafMinLim								AS SafeMinLimit											
--		,cLocCond.SafVltHurrWrn							AS SafeVaultHurrWarningReq									
--		,cLocCond.SafVltHurrWrnTX						AS SaefVaultHurrWarningReqTX								
--		,cLocCond.SafVltHurrWrnDed						AS SafeVaultHurrWarningReqDeductible						
--		,cLocCond.SafVltHurrWrnDed_Deduct				AS SafeVaultHurrWarningReqDeductibleValue					
--		,cLocCond.ShowJwlCond							AS ShowJewelryConditions									
--		,cLocCond.ShrdPremSec							AS SharedPremiseSecurity									
--		,cLocCond.StkMaxDlrLim							AS StkroomMaxDollarLimit									
--		,cLocCond.StkMaxDlrLim_OutOfSafVltAmt			AS StkroomMaxLimitOutOfSafeVaultAmt						
--		,cLocCond.StkMaxLim								AS StkroomMaxLimit											
--		,cLocCond.StkMaxLim_InSafPrcnt					AS StkroomMaxLimitInSafePercent							
--		,cLocCond.ThftProt								AS TheftProtection											
--		,cLocCond.ThftProt_Desc							AS TheftProtectionDesc										
--		,cLocCond.ThftPRotGrd							AS TheftProtectionGuardWarranty							
--		,cLocCond.ThftProtSecDev						AS TheftProtectionSecurityDevice							
--		,cLocCond.ThftProtSecDev_Desc					AS TheftProtectionSecurityDeviceDesc						
--		,cLocCond.TotalPctInSaf							AS TotalPercentInSafe										
--		,cLocCond.TotalPctInSaf_InSafVltStkPrcnt		AS TotalPercentInSafeInSafeVaultStkrmPct					
--		,cLocCond.TotalPctInSaf_NotToExceedAmt			AS TotalPercentInSafeNotToExceedAmt						
--		,cLocCond.ShwcsShwWndwCond						AS ShowcaseOrWindowCondition						

--	-- T - Location-Coverage Questions
--		-- BOP/Inland Marine Loc Home Based Business Questions (183596)	
--		,cHazardCategory.BusinessPremise_JMIC						AS BusinessPremise			
--		,cHazardCategory.InviteJewelryClients_JMIC					AS InviteJewelryClients		
--		,cHazardCategory.SignageAtHome_JMIC							AS SignageAtHome				
--		,cHazardCategory.AnimalsInPremise_JMIC						AS AnimalsOnPremise			
--		,cHazardCategory.WhatSpecies_JMIC							AS AnimalSpecies				
--		,cHazardCategory.SwimmingPool_JMIC							AS SwimmingPool				
--		,cHazardCategory.Trampoline_JMIC							AS Trampoline				
--		,cHazardCategory.MerchandiseAccessChildren_JMIC				AS MerchandiseAccessibleToChildren	
--		,cHazardCategory.DescribeHow_JMIC							AS DescribeHowAccessRestricted				

--		--Testing
--		--,pcx_ilmlocation_jmic.BranchID
--		--,pcx_ilmlocation_jmic.Location
--		--, pc_policyline.publicid

--	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date} ) pc_policyperiod
--		INNER JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date} ) AS pc_policyline
--			ON  pc_policyline.BranchID = pc_policyperiod.ID
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
--		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
--			ON pctl_policyline.ID = pc_policyline.SubType
--			--AND pctl_policyline.TYPECODE= 'ILMLine_JMIC' 
--		INNER JOIN RiskLocationIMConfig AS ConfigILMLine
--			ON  ConfigILMLine.Value = pctl_policyline.TYPECODE
--			AND ConfigILMLine.Key = 'IMLineCode' 
--		INNER JOIN RiskLocationIMConfig AS ConfigILMLocationRisk
--			ON ConfigILMLocationRisk.Key='LOBLevelRisk'
--		/* Need?*/
--		--INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` AS pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date} ) AS pc_job
--			ON pc_job.ID = pc_policyperiod.JobID
		
--		--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction
--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date} ) AS pc_effectivedatedfields
--			ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
--		--Join in the t_PrimaryRatingLocationIM Table to map the Natural Key for RatingLocationKey		
--		LEFT OUTER JOIN `{project}.{dest_dataset}.t_PrimaryRatingLocationIM` AS t_PrimaryRatingLocationIM
--			ON t_PrimaryRatingLocationIM.PolicyPeriodID = pc_policyperiod.ID
--			AND t_PrimaryRatingLocationIM.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
--			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
--			AND ((t_PrimaryRatingLocationIM.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
--				or 
--				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and t_PrimaryRatingLocationIM.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) AS pc_policylocation
--			ON pc_policylocation.BranchID = t_PrimaryRatingLocationIM.PolicyPeriodID  
--			AND pc_policylocation.LocationNum = t_PrimaryRatingLocationIM.RatingLocationNum  
--			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(t_PrimaryRatingLocationIM.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
--		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state
--			ON pctl_state.ID = pc_policylocation.StateInternal
--		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country
--			ON pctl_country.ID = pc_policylocation.CountryInternal
--		LEFT JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` AS pctl_fireprotectclass
--			ON pc_policylocation.FireProtectClass = pctl_fireprotectclass.ID
--		LEFT JOIN `{project}.{pc_dataset}.pctl_addressstatus_jmic` AS pctl_addressstatus_jmic
--			ON pctl_addressstatus_jmic.ID = pc_policylocation.AddressStatus_JMICInternal

--		--Skipped RiskUnit from DW SP -- is it needed?

--		--Inland Marine Location
--		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_ilmlocation_jmic
--			ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
--			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
--			AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_commonlocation_jmic
--			ON  pcx_commonlocation_jmic.BranchID = pc_policyperiod.ID	--equivalent to pcx_ilmlocation_jmic.BranchID
--			AND pcx_commonlocation_jmic.FixedID = pcx_ilmlocation_jmic.CommonLocation
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
--		LEFT JOIN `{project}.{pc_dataset}.pctl_ilmlocationratedas_jmic` AS pctl_ilmlocationratedas_jmic
--			ON pctl_ilmlocationratedas_jmic.ID = pcx_ilmlocation_jmic.RatedAs_JMIC
	
	
--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_ilmcost_jmic
--			ON pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
--			AND pcx_ilmcost_jmic.ILMLocation_JMIC = pcx_ilmlocation_jmic.FixedID
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
	
--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_territorycode` WHERE _PARTITIONTIME = {partition_date} ) AS pc_territorycode
--			ON  pc_territorycode.BranchID = pc_policylocation.BranchID
--			AND pc_territorycode.PolicyLocation = pc_policylocation.FixedID 
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_territorycode.EffectiveDate,pc_policyperiod.PeriodStart)
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_territorycode.ExpirationDate,pc_policyperiod.PeriodEnd)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_territorycode.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pc_territorycode.ExpirationDate,pc_policyperiod.PeriodEnd)

--		-- Building
--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonbuilding_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_commonbuilding_jmic
--			ON  pcx_commonbuilding_jmic.BranchID = pcx_ilmlocation_jmic.BranchID
--			AND pcx_commonbuilding_jmic.FixedID = pcx_ilmlocation_jmic.CommonBuilding
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonbuilding_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonbuilding_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_commonbuilding_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pcx_commonbuilding_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_building` WHERE _PARTITIONTIME = {partition_date} ) AS pc_building
--			ON  pc_building.BranchID = pcx_commonbuilding_jmic.BranchID
--			AND pc_building.FixedID = pcx_commonbuilding_jmic.Building
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_building.EffectiveDate,pc_policyperiod.PeriodStart)
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_building.ExpirationDate,pc_policyperiod.PeriodEnd)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_building.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pc_building.ExpirationDate,pc_policyperiod.PeriodEnd)

--		LEFT JOIN `{project}.{pc_dataset}.pctl_bopconstructiontype` AS pctl_bopconstructiontype
--			ON pctl_bopconstructiontype.ID = pcx_commonbuilding_jmic.ConstructionType

--		LEFT JOIN `{project}.{pc_dataset}.pctl_sprinklered` AS pctl_sprinklered
--			ON pctl_sprinklered.ID = pc_building.SprinklerCoverage

--		LEFT JOIN `{project}.{pc_dataset}.pctl_boplocationtype_jmic` AS pctl_boplocationtype_jmic
--			ON pctl_boplocationtype_jmic.ID = pcx_commonlocation_jmic.LocationType_JMIC

--		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastal_jmic` AS pctl_bopcoastal_jmic
--			ON pctl_bopcoastal_jmic.ID = pc_policylocation.Coastal_JMIC

--		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastalzones_jmic` AS pctl_bopcoastalzones_jmic
--			ON pctl_bopcoastalzones_jmic.ID = pc_policylocation.CoastalZones_JMIC


--		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopclasscode` WHERE _PARTITIONTIME = {partition_date} ) AS pc_bopclasscode
--			ON pc_bopclasscode.ID = pcx_commonbuilding_jmic.BOPLocInsBusCCID
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopclasscode.EffectiveDate,pc_policyperiod.PeriodStart)
--			--AND COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopclasscode.ExpirationDate,pc_policyperiod.PeriodEnd)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_bopclasscode.EffectiveDate,pc_policyperiod.PeriodStart)
--			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pc_bopclasscode.ExpirationDate,pc_policyperiod.PeriodEnd)

--		LEFT JOIN `{project}.{pc_dataset}.pctl_buildingclass_jmic` AS pctl_buildingclass_jmic
--			ON pctl_buildingclass_jmic.ID = pcx_commonbuilding_jmic.BuildingClass_JMIC

--		-- Security Information
--		LEFT JOIN `{project}.{pc_dataset}.pctl_boppresafersposition_jmic` AS pctl_boppresafersposition_jmic
--		ON pctl_boppresafersposition_jmic.ID = pcx_commonbuilding_jmic.SafScaThrPreConOneArea_JMIC

--	--Building Improvement Breakdown
--		LEFT JOIN 
--			(	SELECT 
--					BldgImprBreakdown.BranchID
--					,BldgImprBreakdown.Building
--					,MAX(CASE WHEN BldgImprName = 'HeatingYear' THEN BldgImprBreakdown.YearAdded END) AS HeatingYear
--					,MAX(CASE WHEN BldgImprName = 'PlumbingYear' THEN BldgImprBreakdown.YearAdded END) AS PlumbingYear
--					,MAX(CASE WHEN BldgImprName = 'RoofingYear' THEN BldgImprBreakdown.YearAdded END) AS RoofingYear
--					,MAX(CASE WHEN BldgImprName = 'WiringYear' THEN BldgImprBreakdown.YearAdded END) AS WiringYear

--				FROM BldgImprBreakdown AS BldgImprBreakdown						
--				GROUP BY BldgImprBreakdown.BranchID
--						,BldgImprBreakdown.Building
--			)	AS cBldgImpr
--		ON  cBldgImpr.BranchID = pcx_commonbuilding_jmic.BranchID
--		AND cBldgImpr.Building = pcx_commonbuilding_jmic.Building

--		-- Hazard Categories &
--		-- Location-Coverage Questions
--		LEFT JOIN `{project}.{dest_dataset}.t_RiskLocationIM_HazardCat_LocAnsPivot` AS cHazardCategory
-- 		ON  cHazardCategory.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID

--		-- Exclusions
--		LEFT JOIN
--			(SELECT 
--				RiskLocationInfo.IMLocationPublicID
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocBrglInclSpecProp_JMIC'		THEN CAST(pcx_ilmlocationexcl_jmic.MediumStringTerm1Avl AS INT64)	END) AS BrglInclSpecProp
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocBrglInclSpecProp_JMIC'		THEN pcx_ilmlocationexcl_jmic.MediumStringTerm1							END) AS BrglInclSpecPropValue
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrgl_JMIC'				THEN 1																	END) AS ExclBrgl
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglClosed_JMIC'		THEN CAST(COALESCE(pcx_ilmlocationexcl_jmic.ShortStringTerm1Avl
--																												,pcx_ilmlocationexcl_jmic.ShortStringTerm2Avl
--																												,pcx_ilmlocationexcl_jmic.DirectTerm1Avl) AS INT64)			END) AS ExclBrglClosed
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglClosed_JMIC'		THEN pcx_ilmlocationexcl_jmic.ShortStringTerm1							END) AS ExclBrglClosedHoursOpen
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglClosed_JMIC'		THEN pcx_ilmlocationexcl_jmic.ShortStringTerm2							END) AS ExclBrglClosedTimeZone
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglClosed_JMIC'		THEN pcx_ilmlocationexcl_jmic.DirectTerm1								END) AS ExclBrglClosedTravelLimit
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglJwlWtchSpec_JMIC'	THEN CAST(pcx_ilmlocationexcl_jmic.DirectTerm1Avl AS INT64)			END) AS ExclBrglJwlWtchSpec
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglJwlWtchSpec_JMIC'	THEN pcx_ilmlocationexcl_jmic.DirectTerm1								END) AS ExclBrglJwlWtchSpecValue
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglJwlySpecAmt_JMIC'	THEN CAST(pcx_ilmlocationexcl_jmic.DirectTerm1Avl AS INT64)			END) AS ExclBrglJwlySpecAmt
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclBrglJwlySpecAmt_JMIC'	THEN pcx_ilmlocationexcl_jmic.DirectTerm1								END) AS ExclBrglJwlySpecAmtValue
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocExclOutOfSafe_JMIC'		THEN 1																	END) AS ExclOutOfSafe
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocFireLightSmkExcl_JMIC'		THEN 1																	END) AS FireLightSmkExcl
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocSpecBrglLimit_JMIC'		THEN CAST(pcx_ilmlocationexcl_jmic.DirectTerm1Avl AS INT64)			END) AS SpecBrglLimit
--				,MAX(CASE WHEN pcx_ilmlocationexcl_jmic.PatternCode = 'ILMLocSpecBrglLimit_JMIC'		THEN pcx_ilmlocationexcl_jmic.DirectTerm1								END) AS SpecBrglLimitValue

--			FROM RiskLocationInfo
--				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationexcl_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_ilmlocationexcl_jmic
--					ON  pcx_ilmlocationexcl_jmic.BranchID = RiskLocationInfo.BranchID
--					AND pcx_ilmlocationexcl_jmic.ILMLocation = RiskLocationInfo.IMLocationFixedID
--					AND RiskLocationInfo.EditEffectiveDate >= COALESCE(pcx_ilmlocationexcl_jmic.EffectiveDate,RiskLocationInfo.PeriodStart)
--					AND RiskLocationInfo.EditEffectiveDate <  COALESCE(pcx_ilmlocationexcl_jmic.ExpirationDate,RiskLocationInfo.PeriodEnd)
--			GROUP BY 
--				RiskLocationInfo.IMLocationPublicID
--			) AS cLocExcl
--		ON cLocExcl.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID

--		-- Conditions
--		LEFT JOIN
--			(SELECT 
--				RiskLocationInfo.IMLocationPublicID
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocAlrmSgnlRsp_JMIC'				THEN 1	END)													AS AlrmSgnlRsp			
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm3Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS BankVaultReq				
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm2 END)								AS BankVaultReq_OutOfSafVltPrcnt		
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS BankVaultReq_InSafVltStkPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBankVaultReq_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm3 END)								AS BankVaultReq_BankVltPrcnt	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBrglDeduct_JMIC'					THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm2Avl) AS INT64) END)	AS BrglDeduct
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocBrglDeduct_JMIC'					THEN pcx_ilmlocationcond_jmic.ChoiceTerm1 END)								AS BrglDeduct_Value	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocEstInv_JMIC'						THEN 1	END)													AS EstInv
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.BooleanTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS IndSafeMaxLim
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS IndSafeMaxLim_InSafVltStkPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocIndSafeMaxLim_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.BooleanTerm1 AS INT64) END)					AS IndSafeMaxLim_Mfg
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocIndSafeVltMaxCap_JM'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS IndSafeVltMaxCap	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocInnerSafChest_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS InnerSafChest
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocInnerSafChest_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS InnerSafChest_InSafPrcnt	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocInSafePercent_JM'				THEN CAST(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl AS INT64) END)				AS InSafePercent
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocInSafePerIndSafeVltMaxCap_JM'	THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm3Avl) AS INT64) END)	AS InSafePerIndSafeVltMaxCap
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocKeyedInvestRsp_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl AS INT64) END)				AS KeyedInvestRsp
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocKeyedInvestRsp_JMIC'				THEN pcx_ilmlocationcond_jmic.ChoiceTerm1 END)								AS KeyedInvestRsp_Rsp
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocLckdCabinets_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.DirectTerm1Avl AS INT64) END)				AS LckdCabinets	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocLckdCabinets_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS LckdCabinets_PrcntKept
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLim_JMIC'					THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS IsMaxDollarLimit	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLim_JMIC'					THEN CONCAT('Derived: ',CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS MaxDlrLim_OutOfSafAmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm3Avl) AS INT64) END)	AS MaxDlrLimBrgl
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS MaxDlrLimBrgl_InSafVltStkPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm2 END)								AS MaxDlrLimBrgl_BurglaryLmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimBrgl_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm3 END)								AS MaxDlrLimBrgl_AOPLmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimFinMrch_JMIC'			THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MaxDlrLimFinMrch
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimFinMrch_JMIC'			THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS MaxDlrLimFinMrch_OutOfSafVltAmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimWnty_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS MaxDlrLimWnty	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxDlrLimWnty_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxDlrLimWnty_OutOfSafVltAmt	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MaxJwlyOutClsd
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxJwlyOutClsd_JwlryAmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxJwlyOutClsd_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm2 END)								AS MaxJwlyOutClsd_NonJwlryAmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxOutClsd_JMIC'					THEN CAST(pcx_ilmlocationcond_jmic.DirectTerm1Avl AS INT64) END)				AS MaxOutClsd
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxOutClsd_JMIC'					THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS MaxOutClsd_MaxOutOfSafVlt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxOutClsdWnty_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS MaxOutClsdWnty
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxOutClsdWnty_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxOutClsdWnty_MaxOutOfSafVlt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxOutOfLckedSafeVltLmtSch_JM'	THEN CAST(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl AS INT64) END)				AS MaxOutOfLckedSafeVltLmtSch
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxPrItmSafVlt_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.DirectTerm1Avl AS INT64) END)				AS MaxPrItmSafVlt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxPrItmSafVlt_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxPrItmSafVlt_CostPerItem
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxPrItmStk_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.DirectTerm1Avl AS INT64) END)				AS MaxPrItmStk
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxPrItmStk_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxPrItmStk_CostPerItem
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxValInVlt_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MaxValInVlt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMaxValInVlt_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MaxValInVlt_InSafPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinMaxPrptnSaf_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm3Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS MinMaxPrptnSaf
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinMaxPrptnSaf_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm3 AS STRING)) END)		AS MinMaxPrptnSaf_InSafPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinNbrEmp_JMIC'					THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm1Avl) AS INT64) END)	AS MinNbrEmp
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinNbrEmp_JMIC'					THEN pcx_ilmlocationcond_jmic.DirectTerm1 END)								AS MinNbrEmp_Num
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinPrptnValSafVlt_JMIC'			THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValSafVlt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinPrptnValSafVltIS_JMIC'		THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValSafVltIS
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocMinPrptnValStk_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS MinPrptnValStk
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocRobberyDeduct_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm2Avl) AS INT64) END)	AS RobberyDeduct
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocRobberyDeduct_JMIC'				THEN pcx_ilmlocationcond_jmic.ChoiceTerm1 END)								AS RobberyDeduct_Deduct	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafeBrglDeduct_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm2Avl) AS INT64) END)	AS SafeBrglDeduct
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafeBrglDeduct_JMIC'				THEN pcx_ilmlocationcond_jmic.ChoiceTerm1 END)								AS SafeBrglDeduct_Deduct
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafMinLim_JMIC'					THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS SafMinLim	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafVltHurrWrn_JMIC'				THEN 1	END)													AS SafVltHurrWrn
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafVltHurrWrnTX_JMIC'			THEN 1	END)													AS SafVltHurrWrnTX
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafVltHurrWrnDed_JMIC'			THEN CAST(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl AS INT64) END)				AS SafVltHurrWrnDed	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocSafVltHurrWrnDed_JMIC'			THEN pcx_ilmlocationcond_jmic.ChoiceTerm1 END)								AS SafVltHurrWrnDed_Deduct
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocShowJwlCond_JMIC'				THEN 1	END)													AS ShowJwlCond	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocShrdPremSec_JMIC'				THEN 1	END)													AS ShrdPremSec	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocStkMaxDlrLim_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS StkMaxDlrLim	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocStkMaxDlrLim_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS StkMaxDlrLim_OutOfSafVltAmt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocStkMaxLim_JMIC'					THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS StkMaxLim
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocStkMaxLim_JMIC'					THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS StkMaxLim_InSafPrcnt	
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocThftProt_JMIC'					THEN CAST(pcx_ilmlocationcond_jmic.MediumStringTerm1Avl AS INT64) END)			AS ThftProt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocThftProt_JMIC'					THEN pcx_ilmlocationcond_jmic.MediumStringTerm1 END)							AS ThftProt_Desc
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocThftPRotGrd_JMIC'				THEN 1	END)													AS ThftPRotGrd
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocThftProtSecDev_JMIC'				THEN CAST(pcx_ilmlocationcond_jmic.MediumStringTerm1Avl AS INT64) END)			AS ThftProtSecDev
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocThftProtSecDev_JMIC'				THEN pcx_ilmlocationcond_jmic.MediumStringTerm1 END)							AS ThftProtSecDev_Desc
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.DirectTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm2Avl) AS INT64) END)	AS TotalPctInSaf
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN CONCAT('Derived: ', CAST(pcx_ilmlocationcond_jmic.DirectTerm1 AS STRING)) END)		AS TotalPctInSaf_InSafVltStkPrcnt
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMLocTotalPctInSaf_JMIC'				THEN pcx_ilmlocationcond_jmic.DirectTerm2 END)								AS TotalPctInSaf_NotToExceedAmt			
--				,MAX(CASE WHEN pcx_ilmlocationcond_jmic.PatternCode = 'ILMShwcsShwWndwCond_JMIC'				THEN CAST(COALESCE(pcx_ilmlocationcond_jmic.ChoiceTerm1Avl
--																															,pcx_ilmlocationcond_jmic.ChoiceTerm2Avl
--																															,pcx_ilmlocationcond_jmic.MediumStringTerm1Avl
--																															,pcx_ilmlocationcond_jmic.DirectTerm1Avl) AS INT64) END)	AS ShwcsShwWndwCond	

--			FROM RiskLocationInfo
--				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcond_jmic` WHERE _PARTITIONTIME = {partition_date} ) AS pcx_ilmlocationcond_jmic
--					ON  pcx_ilmlocationcond_jmic.BranchID = RiskLocationInfo.BranchID
--					AND pcx_ilmlocationcond_jmic.ILMLocation = RiskLocationInfo.IMLocationFixedID
--					AND RiskLocationInfo.EditEffectiveDate >= COALESCE(pcx_ilmlocationcond_jmic.EffectiveDate,RiskLocationInfo.PeriodStart)
--					AND RiskLocationInfo.EditEffectiveDate <  COALESCE(pcx_ilmlocationcond_jmic.ExpirationDate,RiskLocationInfo.PeriodEnd)
--			GROUP BY 
--				RiskLocationInfo.IMLocationPublicID
--			) AS cLocCond
--		ON cLocCond.IMLocationPublicID = pcx_ilmlocation_jmic.PublicID

--	WHERE 1 = 1
--	/**** TEST *****/
--	--AND pc_policyperiod.PolicyNumber BETWEEN '55-010400' AND '55-010700'

) cRiskLocIM

	INNER JOIN RiskLocationIMConfig ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN RiskLocationIMConfig ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN RiskLocationIMConfig ConfigHashAlgo 
	ON ConfigHashAlgo.Key='HashingAlgorithm'

	INNER JOIN RiskLocationIMConfig ConfigRiskLevel -- 04/27/2023
	ON ConfigRiskLevel.Key='IMLocationLevelRisk'

WHERE	1 = 1
	AND cRiskLocIM.LocationRank = 1
	AND cRiskLocIM.CostRank = 1;

--) outerquery;

----------------
--Self Clean-up 
----------------
DROP TABLE BldgImprBreakdown;
DROP TABLE RiskLocationInfo;
