/****** Script for SelectTopNRows command from SSMS  ******/
with loc_risk_core as (
		SELECT DISTINCT
			  a.[PolicyKey]
			  ,CONCAT([LocationAddress1],[LocationAddress2],LocationCounty, LocationCountyFIPS, LocationPostalCode,LocationGeographyKey) AS temp_location_key
			  ,a.[SourceSystem]
			  ,[RiskType]
			  ,[RiskTypeDescription]
			  ,[LocationAddress1]
			  ,CASE [LocationAddress2] WHEN '?' THEN NULL ELSE [LocationAddress2] END AS [LocationAddress2]
			  ,[LocationCity]
			  ,CASE [LocationCounty] WHEN '?' THEN NULL ELSE [LocationCounty] END AS [LocationCounty]
			  ,CASE [LocationCountyFIPS] WHEN '?' THEN NULL ELSE [LocationCountyFIPS] END AS [LocationCountyFIPS]
			  ,d.GeographyStateCode as LocationStateCode
			  ,d.GeographyStateDesc as LocationStateDesc
			  ,[LocationPostalCode]
			  ,d.GeographyCountryCode as LocationCountryCode
			  ,d.GeographyCountryDesc as LocationCountryDesc
		  FROM [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentCore] a
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimLineOfBusiness] b
				ON a.LineOfBusinessKey = b.LineOfBusinessKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentLocation] c
				ON a.RiskSegmentKey = c.RiskSegmentKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimGeography] d
				ON c.LocationGeographyKey = d.GeographyKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRatedAs] e
				ON c.LocationRatedAsKey = e.RatedAsKey
		  WHERE a.SourceSystem = 'GW'
					AND IsInactive = 0
					AND RiskSegmentLocationKey >= 0
					AND a.LineOfBusinessKey IN (1,2,3,4,6)
					AND a.PolicyKey > 0
					AND RiskType = 'LOC'
					AND LOBCode IN ('BOP', 'JB', 'JS')
					),

policy_core as (
		SELECT
			PolicyKey
			,PolicyNumber
			,a.JobNumber
			,AccountNumber
			,PolicyInsuredContactFullName
		 FROM (SELECT 
					  t.JobNumber
					, t.AccountKey
					, t. PolicyNumber
					, t.PolicyStatus
					, t.PolicyType
					, t.PolicyKey
					, t.PolicyInsuredContactFullName
					, DENSE_RANK() OVER (PARTITION BY t.JobNumber ORDER BY t.PolicyInforceFromDate DESC, t.PolicyKey DESC ) AS ranking
				  FROM [DW_DDS_CURRENT].[bi_dds].[DimPolicy] t
					WHERE t.JobNumber IS NOT NULL
						AND PolicyStatus = 'Bound') a
		 LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimAccount] b
			ON a.AccountKey = b.AccountKey
		 --INNER JOIN (SELECT JobNumber, Max(PolicyInforceFromDate) PolicyInforceFromDate FROM [DW_DDS_CURRENT].[bi_dds].[DimPolicy] GROUP BY JobNumber) c
			--ON a.JobNumber = c.JobNumber
		 WHERE PolicyStatus = 'Bound'
		 AND PolicyType in ('BOP', 'JB', 'JBP', 'JS', 'JSP', 'SEL')
		 AND ranking = 1),

loc_risk_ilm as (
	SELECT 	DISTINCT
			PolicyKey as ILMPolicyKey
			,CONCAT([LocationAddress1],[LocationAddress2],LocationCounty, LocationCountyFIPS, LocationPostalCode,LocationGeographyKey) AS temp_location_key
			--,[RiskSegmentLOBKey]
			,[RiskSegmentLocationKey]
			--,[RiskSegmentStockKey]
			,[RiskSegmentBuildingKey]
			,[LOB_IM] = CASE WHEN LOBCode = 'JB' THEN 'JB' WHEN LOBCode = 'JS' THEN 'JS' ELSE NULL END
			,[LocationGeographyKey]
			,[LocationRatingLocationKey]
			,[RiskType]
			,[RiskTypeDescription]
			,RatedAsDescription
			,[LocationFullTimeEmployees]
			,[LocationPartTimeEmployees]
			,[Location_EPLI_FTE_Employees]
			,[LocationBuildingConstructionCode]
			,[LocationBuildingConstructionDesc]
			,[LocationBuildingProtectionClassCode]
			,[LocationBuildingCompletelySprinklered] = CASE WHEN [LocationBuildingCompletelySprinklered] = 0 THEN 'No' WHEN [LocationBuildingCompletelySprinklered] = 1 THEN 'Yes' ELSE NULL END
			,[LocationBuildingTotalArea]
			,[LocationBuildingAreaOccupied]
			,[LocationBuildingNumStories]
			,CASE [LocationBuildingOccupiedFloorCount] WHEN '?' THEN NULL ELSE [LocationBuildingOccupiedFloorCount] END AS LocationBuildingOccupiedFloorCount
			,[LocationNumber]
			,[LocationAnnualSales]
			,[LocationExcludeFireCoverage] = CASE WHEN [LocationExcludeFireCoverage] = 0 THEN 'No' WHEN [LocationExcludeFireCoverage] = 1 THEN 'Yes' ELSE NULL END
			,[LocationInlandMarineZoneCode]
			,[LocationInlandMarineHazardCode]
			,[LocationInlandMarineExperienceCreditFactor]
			,[LocationInlandMarineTierFactor]
			,[LocationInlandMarineExperienceTierFactor]
			,[LocationInlandMarineMaxTravelAmount]
			,[LocationInlandMarineReplacementCostInd] = CASE WHEN [LocationInlandMarineReplacementCostInd] = 0 THEN 'No' WHEN [LocationInlandMarineReplacementCostInd] = 1 THEN 'Yes' ELSE NULL END
			,[LocationInlandMarineHasShowCaseWindows] = CASE WHEN [LocationInlandMarineHasShowCaseWindows] = 0 THEN 'No' WHEN [LocationInlandMarineHasShowCaseWindows] = 1 THEN 'Yes' ELSE NULL END
			,[LocationInlandMarineNumberShowWindows]
			,[LocationInlandMarineEquippedWithLocks] = CASE WHEN [LocationInlandMarineEquippedWithLocks] = 0 THEN 'No' WHEN [LocationInlandMarineEquippedWithLocks] = 1 THEN 'Yes' ELSE NULL END
			,[LocationInlandMarineWindowsKeptLocked] = CASE WHEN [LocationInlandMarineWindowsKeptLocked] = 0 THEN 'No' WHEN [LocationInlandMarineWindowsKeptLocked] = 1 THEN 'Yes' ELSE NULL END
			,[LocationInlandMarineWindowsKeptUnlockedReason]
			,[LocationInlandMarineMaxValueInWindow]
			,[LocationInlandMarineTotalInSafeVaultStockroom]
			,[LocationInlandMarineAmountOutOfSafeVaultStockroom]
			,[LocationInlandMarinePctInBankVault]
			,[LocationDistrictName]
			,CASE [LocationTypeDesc] WHEN '?' THEN NULL ELSE [LocationTypeDesc] END AS [LocationTypeDesc]
			,CASE [LocationTypeCode] WHEN '?' THEN NULL ELSE [LocationTypeCode] END AS [LocationTypeCode]
			,CASE [LocationTerritoryCode] WHEN '?' THEN NULL ELSE [LocationTerritoryCode] END AS [LocationTerritoryCode]
			,CASE [LocationCoastal] WHEN '?' THEN NULL ELSE [LocationCoastal] END AS [LocationCoastal]
			,CASE [LocationCoastalZone] WHEN '?' THEN NULL ELSE [LocationCoastalZone] END AS [LocationCoastalZone]
			,[LocationILMAdditionalProtectionNotMentioned]
			,[LocationILMArmedUniformGuard]
			,[LocationILMCameraSystem]
			,[LocationILMClaimsFree]
			,[LocationILMClosedToBusinessStoragePractices]
			,[LocationILMDoubleCylinderDeadBoltLocks]
			,[LocationILMElectronicProtection]
			,[LocationILMElectronicTrackingDevices]
			,[LocationILMHoldUpAlarm]
			,[LocationILMInventory]
			,[LocationILMJBTFinancial]
			,[LocationILMLockedDoorBuzzerSystem]
			,[LocationILMMetalDetectorWithManTrap]
			,[LocationILMMonitoredFireAlarmSystem]
			,[LocationILMMultipleAlarmsOrMonitoring]
			,[LocationILMNoExteriorGroundFloorExposure]
			,[LocationILMPhysicallyProtectedDoorsAndWindows]
			,[LocationILMPhysicalProtection]
			,[LocationILMSecuredBuilding]
			,[LocationILMYearsInBusiness]
		FROM [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentCore] a
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimLineOfBusiness] b
				ON a.LineOfBusinessKey = b.LineOfBusinessKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentLocation] c
				ON a.RiskSegmentKey = c.RiskSegmentKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRatedAs] e
				ON c.LocationRatedAsKey = e.RatedAsKey
		WHERE a.SourceSystem = 'GW'
					AND IsInactive = 0
					AND RiskSegmentLocationKey >= 0
					AND a.LineOfBusinessKey IN (1,2,3,4,6)
					AND a.PolicyKey > 0
					AND RiskType = 'LOC'
					AND LOBCode IN ('JB', 'JS')
					),

loc_risk_ilm_final as (
	SELECT a.* FROM loc_risk_ilm a
				 INNER JOIN (SELECT ILMPolicyKey, temp_location_key, Max(RiskSegmentLocationKey) RiskSegmentLocationKey FROM loc_risk_ilm GROUP BY ILMPolicyKey, temp_location_key) r
			ON a.ILMPolicyKey = r.ILMPolicyKey
			AND a.RiskSegmentLocationKey = r.RiskSegmentLocationKey
			),


loc_risk_bop as (
	SELECT 	DISTINCT
			PolicyKey
			,CONCAT([LocationAddress1],[LocationAddress2],LocationCounty, LocationCountyFIPS, LocationPostalCode,LocationGeographyKey) AS temp_location_key
			,[LOB_BOP] = CASE WHEN LOBCode = 'BOP' THEN 'BOP' ELSE NULL END
		FROM [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentCore] a
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimLineOfBusiness] b
				ON a.LineOfBusinessKey = b.LineOfBusinessKey
			LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentLocation] c
				ON a.RiskSegmentKey = c.RiskSegmentKey
		WHERE a.SourceSystem = 'GW'
					AND IsInactive = 0
					AND RiskSegmentLocationKey >= 0
					AND a.LineOfBusinessKey IN (1,2,3,4,6)
					AND a.PolicyKey > 0
					AND RiskType = 'LOC'
					AND LOBCode IN ('BOP')
					),

--SELECT count(DISTINCT(PolicyKey))
--	FROM loc_risk_ilm

risk_all as (SELECT DISTINCT
			  a.PolicyKey
			  ,d.AccountNumber
			  ,d.PolicyNumber
			  ,a.temp_location_key as risk_location_key
			  ,d.JobNumber
			  ,a.[SourceSystem]
			  ,d.PolicyInsuredContactFullName
			  ,[LocationAddress1]
			  ,[LocationAddress2]
			  ,[LocationCity]
			  ,[LocationCounty]
			  ,[LocationCountyFIPS]
			  ,LocationStateCode
			  ,LocationStateDesc
			  ,[LocationPostalCode]
			  ,LocationCountryCode
			  ,LocationCountryDesc
			  ,b.*, c.LOB_BOP
	FROM policy_core d
	INNER JOIN loc_risk_core a
		ON d.PolicyKey = a.PolicyKey
	LEFT JOIN loc_risk_ilm_final  b
		ON a.PolicyKey = b.ILMPolicyKey
		AND a.temp_location_key = b.temp_location_key
	LEFT JOIN loc_risk_bop c
		ON a.PolicyKey = c.PolicyKey
		AND a.temp_location_key = c.temp_location_key
		),

risk_final as (SELECT JobNumber = CONCAT('J_', a.JobNumber)
			,PolicyNumber
			,AccountNumber
			,risk_location_key
			,SourceSystem
			,[PolicyInsuredContactFullName]
			,LocationAddress1
			,LocationAddress2
			,LocationCity
			,LocationCounty
			,LocationStateCode
			,LocationStateDesc
			,LocationPostalCode
			,LocationCountryCode
			,LocationCountryDesc
			,LOB_IM
			,LOB_BOP
			,RiskType
			,RiskTypeDescription
			,RatedAsDescription
			,LocationFullTimeEmployees
			,LocationBuildingConstructionDesc
			,LocationBuildingProtectionClassCode
			,LocationBuildingCompletelySprinklered
			,LocationBuildingTotalArea
			,LocationBuildingAreaOccupied
			,LocationBuildingNumStories
			,LocationBuildingOccupiedFloorCount
			,LocationNumber
			,LocationAnnualSales
			,LocationExcludeFireCoverage
			,[LocationInlandMarineZoneCode]
			,[LocationInlandMarineHazardCode]
			,[LocationInlandMarineExperienceCreditFactor]
			,[LocationInlandMarineTierFactor]
			,[LocationInlandMarineExperienceTierFactor]
			,[LocationInlandMarineMaxTravelAmount]
			,[LocationInlandMarineReplacementCostInd]
			,[LocationInlandMarineHasShowCaseWindows]
			,[LocationInlandMarineNumberShowWindows]
			,[LocationInlandMarineEquippedWithLocks]
			,[LocationInlandMarineWindowsKeptLocked]
			,[LocationInlandMarineWindowsKeptUnlockedReason]
			,[LocationInlandMarineMaxValueInWindow]
			,[LocationInlandMarineTotalInSafeVaultStockroom]
			,[LocationInlandMarineAmountOutOfSafeVaultStockroom]
			,[LocationInlandMarinePctInBankVault]
			,[LocationDistrictName]
			,[LocationTypeDesc]
			,[LocationTypeCode]
			,[LocationTerritoryCode]
			,[LocationCoastal]
			,[LocationCoastalZone]
			,[LocationILMAdditionalProtectionNotMentioned]
			,[LocationILMArmedUniformGuard]
			,[LocationILMCameraSystem]
			,[LocationILMClaimsFree]
			,[LocationILMClosedToBusinessStoragePractices]
			,[LocationILMDoubleCylinderDeadBoltLocks]
			,[LocationILMElectronicProtection]
			,[LocationILMElectronicTrackingDevices]
			,[LocationILMHoldUpAlarm]
			,[LocationILMInventory]
			,[LocationILMJBTFinancial]
			,[LocationILMLockedDoorBuzzerSystem]
			,[LocationILMMetalDetectorWithManTrap]
			,[LocationILMMonitoredFireAlarmSystem]
			,[LocationILMMultipleAlarmsOrMonitoring]
			,[LocationILMNoExteriorGroundFloorExposure]
			,[LocationILMPhysicallyProtectedDoorsAndWindows]
			,[LocationILMPhysicalProtection]
			,[LocationILMSecuredBuilding]
			,[LocationILMYearsInBusiness]
			FROM risk_all a 
			--INNER JOIN (SELECT JobNumber, temp_location_key, Max(PolicyKey) PolicyKey FROM risk_all GROUP BY JobNumber, temp_location_key) b
			--ON a.PolicyKey = b.PolicyKey
			--	AND a.JobNumber = b.JobNumber
			--	AND a.temp_location_key = b.temp_location_key
			)

--SELECT COUNT(*) FROM (SELECT JobNumber, temp_location_key, Max(PolicyKey) PolicyKey FROM risk_all GROUP BY JobNumber, temp_location_key) a

--SELECT * FROM policy_core WHERE PolicyKey = 4260151
SELECT * FROM risk_final a 
		--JOIN (SELECT JobNumber, risk_location_key,  COUNT(*) as num_duplicates
		--	FROM risk_final
		--	GROUP BY JobNumber, risk_location_key
		--	HAVING count(*) > 1 ) b
		--ON a.risk_location_key = b.risk_location_key
		--	AND a.JobNumber = b.JobNumber
		--ORDER BY a.JobNumber, a.risk_location_key DESC


	--risk_alarm as (
	--	SELECT [RiskSegmentKey]
	--		  ,[AlarmType]
	--		  ,[AlarmCertType]
	--		  ,[StorageNumber]
	--		  ,[ExtentName]
	--		  ,[AlarmCompany]
	--		  ,[ULCertified]
	--		  ,[ULCertificateNumber]
	--		  ,[ULCertificateExpirationDate]
	--		  ,[ULAuditedAlarmInstallDate]
	--	  FROM [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentAlarm]
	--	  WHERE SourceSystem = 'GW'
	--	  ),

	--risk_safe as (
	--	SELECT	[RiskSegmentKey]
	--			,[FireBurglaryRating]
	--			,[SafeNumber]
	--			,[TotalLocationInSafeVaultStockroom]
	--			,[AmountOutOfSafeVaultStockroom]
	--			,[PctInBankVault]
	--			,[IsSafeInVault]
	--			,[MaxValueInSafe]
	--			,[IsSafeAnchored]
	--			,[SafeNotAnchoredExplanation]
	--			,[IsSafeLightweight]
	--			,[SafeWeight]
	--	FROM [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentSafe]
	--	WHERE SourceSystem = 'GW'
	--	),



