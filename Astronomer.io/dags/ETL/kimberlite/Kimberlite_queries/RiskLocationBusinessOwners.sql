-- tag: RiskLocationBusinessOwners - tag ends/
/*******************************************************************************************
	Kimberlite Extract Query
		RiskLocationBusinessOwners.sql
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	11/2020		SLJ			Init create
	01/07/2021	DROBAK		Replaced PIVOT code with CASE Stmnts
	03/03/2021	GN			Pivot function conversion for BQ
	03/08/2021	DROBAK		Cost Join fix applied (now a left join)
	04/07/2021	DROBAK		Field clean-up
	04/12/2021	DROBAK		Added Eff & Exp Dates
	06/08/2021	SLJ			FixedRank modified, JOIN modified for Inner select or pivot, Unit tests
	07/12/2021	SLJ			Add CASE WHEN NOT NULL condition for Key
	08/19/2022	DROBAK		Add: IsPrimaryLocation (flag field for easier analysis)
							Added Primary Rating Location code to get LocationPublicID from pc_boplocation
	01/26/2023	DROBAK		Added Line/LOB unioned section and to config table
	02/02/2023	SLJ/DAR		IsTransactionSliceEffective logic added: AND pc_boplocation.BOPLine = pc_policyline.FixedID
	04/18/2023	DROBAK		Fix LocationLevelRisk in Config table; align with FinTrans query; Use BOPLineCode for Risk Level too
							Fix CTE_PrimaryRatingLocationBOP (Add BOPLine logic) to get correct LocationNum

----------------------------------------------------------------------------------------------

-- T - Details
	-- additional insureds
	-- one-time credits
	-- Underwriting validation method in details
-- T - Package Coverages (check if available in kimberlite coverage tables)
-- T - Income/Expense Coverages (check if available in kimberlite coverage tables)
-- T - Additional Coverages (check if available in kimberlite coverage tables)
-- T - Exclusions & Conditions
	-- Exclusions (complete)
	-- Conditions (complete) 
-- T - Location Information (complete)

************************************************************************************************/
CREATE OR REPLACE TEMP TABLE RiskLocBusinessOwnerCombinations AS 
(
	SELECT 
		pc_boplocation.PublicID											AS BOPLocationPublicID
		,pc_policyperiod.PublicID										AS PolicyPeriodPublicID
		,pc_policyperiod.EditEffectiveDate								AS EditEffectiveDate	
		,pc_policyperiod.PeriodStart									AS PeriodStart
		,pc_policyperiod.PeriodEnd										AS PeriodEnd
		,pc_boplocation.BranchID										AS BranchID
		,pc_boplocation.FixedID											AS BOPLocationFixedID
		,pc_boplocation.Location										AS Location
		,pc_boplocation.CommonLocation									AS CommonLocation
		,pc_policylocation.FixedID										AS PolicyLocationFixedID

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
			ON pc_boplocation.BranchID = pc_policyperiod.ID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pc_boplocation.BranchID
			AND pc_policylocation.FixedID = pc_boplocation.Location
			AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
);
--This code is also used in RiskLocationBusinessOwners, so the two tables use SAME PublicID from SAME Table (pc_boplocation)
CREATE OR REPLACE TEMP TABLE CTE_PrimaryRatingLocationBOP
AS SELECT *
FROM (
	SELECT 
		pc_policyperiod.ID AS PolicyPeriodID
		,pc_policyperiod.EditEffectiveDate AS EditEffectiveDate
		,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
		--This flag displays whether or not the LOB Location matches the PrimaryLocation
		,MAX(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
			--1. If the Primary loc matches the LOB loc, use it
			--2. Else use Coverage Effective Location (Same as RatingLocation and may be different than the min or primary)
			--3. Otherwise use the MIN location num's corresponding LOB Location
		--If the Primary loc matches the LOB loc, use it, otherwise use the MIN location num's corresponding LOB Location
		,COALESCE(MIN(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
				,MIN(CASE WHEN pc_boplocation.BOPLine = pc_policyline.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
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
		--LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS PrimaryPolicyLocation_state
		--	ON PrimaryPolicyLocation_state.ID = PrimaryPolicyLocation.StateInternal  
	WHERE 1 = 1
		AND pc_policyperiod._PARTITIONTIME = {partition_date}
	GROUP BY
		pc_policyperiod.ID
		,pc_policyperiod.EditEffectiveDate
		,pctl_policyline.TYPECODE

	) AS PrimaryRatingLocations;

CREATE OR REPLACE TEMP TABLE RiskLocationBOPPivot AS 
(
	WITH RiskLocationBOPConfig AS (
		SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
		SELECT 'HashKeySeparator','_' UNION ALL
		SELECT 'HashingAlgorithm', 'SHA2_256' UNION ALL 

		SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
		SELECT 'ILMLineCode','ILMLine_JMIC' UNION ALL
		SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
		SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL

		SELECT 'BOPILMLocHomeQuestion', 'BOPHomeBasedBusinessQuestion_JMIC' UNION ALL
		SELECT 'BOPLocationLevelRisk', 'BusinessOwnersLocation' UNION ALL

		SELECT 'BOPLocUndInfo','BOPUnderwritingInformationQuestions_JMIC' UNION ALL
		SELECT 'BOPLineCode', 'BusinessOwnersLine' UNION ALL
		-----RISK------
		SELECT 'LineLevelRisk', 'BusinessOwnersLine' UNION ALL
		SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
		SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
	) 

	SELECT *
	FROM (

		SELECT 
			RiskLocBusinessOwnerCombinations.BOPLocationPublicID
			,pc_locationanswer.QuestionCode
			,COALESCE(CAST(pc_locationanswer.BooleanAnswer AS STRING),pc_locationanswer.TextAnswer) AS Answer
					
		FROM RiskLocBusinessOwnerCombinations

			INNER JOIN `{project}.{pc_dataset}.pc_locationanswer` AS pc_locationanswer
			ON pc_locationanswer.BranchID = RiskLocBusinessOwnerCombinations.BranchID
			AND pc_locationanswer.PolicyLocation = RiskLocBusinessOwnerCombinations.PolicyLocationFixedID
			AND RiskLocBusinessOwnerCombinations.EditEffectiveDate >= COALESCE(pc_locationanswer.EffectiveDate,RiskLocBusinessOwnerCombinations.PeriodStart)
			AND RiskLocBusinessOwnerCombinations.EditEffectiveDate <  COALESCE(pc_locationanswer.ExpirationDate,RiskLocBusinessOwnerCombinations.PeriodEnd)

			INNER JOIN `{project}.{pc_dataset}.pc_questionlookup` AS pc_questionlookup
			ON pc_questionlookup.QuestionCode = pc_locationanswer.QuestionCode

			INNER JOIN RiskLocationBOPConfig AS ConfigHQ
			ON ConfigHQ.Key = 'BOPILMLocHomeQuestion' 

			INNER JOIN RiskLocationBOPConfig AS ConfigUI
			ON ConfigUI.Key = 'BOPLocUndInfo' 

		WHERE	1 = 1
			AND pc_locationanswer._PARTITIONTIME = {partition_date}
			AND pc_questionlookup._PARTITIONTIME = {partition_date}
			AND	(pc_questionlookup.SourceFile like '%'||ConfigHQ.Value||'%' OR pc_questionlookup.SourceFile like '%'||ConfigUI.Value||'%')
			AND pc_locationanswer.QuestionCode in ('BusinessPremise_JMIC','InviteJewelryClients_JMIC','SignageAtHome_JMIC','AnimalsInPremise_JMIC','WhatSpecies_JMIC','SwimmingPool_JMIC','Trampoline_JMIC','MerchandiseAccessChildren_JMIC','DescribeHow_JMIC','HasBeenInBusiness_JMIC','PreviousLocAdd_JMIC','PastLosses_JMIC','ExplainLoss_JMIC','ExposureToFlammables_JMIC','ExplainExposure_JMIC','ContainLead_JMIC','ExplainContainLead_JMIC')
	)t
);
CALL `{project}.custom_functions.sp_pivot`(
  'RiskLocationBOPPivot' # source table
  ,'{project}.{dest_dataset}.t_RiskLocationBOP_Pivot' # destination table
  , ['BOPLocationPublicID'] # row_ids
  , 'QuestionCode'-- # pivot_col_name
  , 'Answer'--# pivot_col_value
  , 17--# max_columns
  , 'MAX' --# aggregation
  , '' --# optional_limit
);
DROP table RiskLocationBOPPivot;

/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.RiskLocationBusinessOwners`
AS SELECT outerquery.*
FROM (
*/
--DELETE `{project}.{dest_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date});
INSERT INTO `{project}.{dest_dataset}.RiskLocationBusinessOwners`
	(  
		SourceSystem,
		RiskLocationKey,
		PolicyTransactionKey,
		FixedLocationRank,
		LocationPublicID,
		PolicyPeriodPublicID,
		JobNumber,
		PolicyNumber,
		RiskLevel,
		EffectiveDate,
		ExpirationDate,
		IsTransactionSliceEffective,
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
		Casting,
		Plating,
		AllOtherMfg,
		FullTimeEmployees,
		PartTimeEmployees,
		Owners,
		PublicProtection,
		LocationType,
		LocationTypeName,
		LocationTypeOtherDescription,
		AnnualSales,
		AnnualSalesAttributableToMfg,
		WindOrHailDeductiblePercent,
		IsBusIncomeAndExtraExpDollarLimit,
		NuclBioChemRadExcl,
		JwlryExclBusIncomeExtraExpTheftExcl,
		RemoveInsToValueProvision,
		OutdoorTheftExcl,
		PunitiveDamagesCertTerrorismExcl,
		AmdCancProvisionsCovChg,
		AmdCancProvisionsCovChgDaysAdvNotice,
		ForkliftExtCond,
		ForkliftExtCondBlanketLimit,
		ForkliftExtCondDeduct,
		PattrnFrmsCond,
		PattrnFrmsCondLimit,
		BusinessPremise,
		InviteJewelryClients,
		SignageAtHome,
		AnimalsInPremise,
		WhatSpecies,
		SwimmingPool,
		Trampoline,
		MerchandiseAccessChildren,
		DescribeHow,
		HasBeenInBusiness,
		PreviousLocationAddress,
		PastLosses,
		ExplainLoss,
		ExposureToFlammables,
		ExplainExposure,
		ContainLead,
		ExplainContainLead,
		bq_load_date
)

WITH RiskLocationBOPConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm', 'SHA2_256' UNION ALL 

  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'ILMLineCode','ILMLine_JMIC' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
  SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL

  SELECT 'BOPILMLocHomeQuestion', 'BOPHomeBasedBusinessQuestion_JMIC' UNION ALL
  SELECT 'BOPLocationLevelRisk', 'BusinessOwnersLocation' UNION ALL

  SELECT 'BOPLocUndInfo','BOPUnderwritingInformationQuestions_JMIC' UNION ALL
  SELECT 'BOPLineCode', 'BusinessOwnersLine' UNION ALL
-----RISK------
  SELECT 'LineLevelRisk', 'BusinessOwnersLine' UNION ALL
  SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
  SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
) 
SELECT
	ConfigSource.Value AS SourceSystem
	--SK For PK [<Source>_<LocationPublicID>_<Level>]
	,CASE WHEN cRiskLocBOP.LocationPublicID IS NOT NULL 
		THEN SHA256 (CONCAT(ConfigSource.Value,ConfigHashSep.Value,cRiskLocBOP.LocationPublicID,ConfigHashSep.Value,cRiskLocBOP.RiskLevel)) END AS RiskLocationKey
	--SK For FK [<Source>_<PolicyPeriodPublicID>]
	,CASE WHEN cRiskLocBOP.PolicyPeriodPublicID IS NOT NULL 
		THEN SHA256 (CONCAT(ConfigSource.Value,ConfigHashSep.Value,cRiskLocBOP.PolicyPeriodPublicID)) END AS PolicyTransactionKey 
	,DENSE_RANK() OVER(PARTITION BY	cRiskLocBOP.LocationFixedID
									,cRiskLocBOP.PolicyPeriodPublicID
									,cRiskLocBOP.RiskLevel
					   ORDER BY		cRiskLocBOP.IsTransactionSliceEffective DESC
									,cRiskLocBOP.FixedLocationRank) AS FixedLocationRank
	,cRiskLocBOP.LocationPublicID
	,cRiskLocBOP.PolicyPeriodPublicID
	,cRiskLocBOP.JobNumber
	,cRiskLocBOP.PolicyNumber
	,cRiskLocBOP.RiskLevel
	,cRiskLocBOP.EffectiveDate
	,cRiskLocBOP.ExpirationDate
	,cRiskLocBOP.IsTransactionSliceEffective

-- T - Details
	-- Location
	,cRiskLocBOP.LocationNumber
	,cRiskLocBOP.IsPrimaryLocation
	,cRiskLocBOP.LocationFixedID
	,cRiskLocBOP.LocationAddress1
	,cRiskLocBOP.LocationAddress2
	,cRiskLocBOP.LocationCity
	,cRiskLocBOP.LocationState
	,cRiskLocBOP.LocationStateCode
	,cRiskLocBOP.LocationCountry
	,cRiskLocBOP.LocationPostalCode
	,cRiskLocBOP.LocationAddressStatus
	,cRiskLocBOP.LocationCounty
	,cRiskLocBOP.LocationCountyFIPS
	,cRiskLocBOP.LocationCountyPopulation
	--,phone
	,cRiskLocBOP.TerritoryCode
	,cRiskLocBOP.Coastal
	,cRiskLocBOP.CoastalZone
	,cRiskLocBOP.SegmentationCode

	-- General Information (Type of Business based on sales)
	,cRiskLocBOP.RetailSale	
	,cRiskLocBOP.RepairSale
	,cRiskLocBOP.AppraisalSale		
	,cRiskLocBOP.WholesaleSale		
	,cRiskLocBOP.ManufacturingSale	
	,cRiskLocBOP.RefiningSale		
	,cRiskLocBOP.GoldBuyingSale	
	,cRiskLocBOP.PawnSale		
		
	-- Manufacturing Process
	,cRiskLocBOP.Casting
	,cRiskLocBOP.Plating
	,cRiskLocBOP.AllOtherMfg

	-- Employee Informatioin
	,cRiskLocBOP.FullTimeEmployees
	,cRiskLocBOP.PartTimeEmployees
	,cRiskLocBOP.Owners

	-- Public Protection
	,cRiskLocBOP.PublicProtection							-- FireProtectionClassCode

	-- Location Information
	,cRiskLocBOP.LocationType
	,cRiskLocBOP.LocationTypeName
	,cRiskLocBOP.LocationTypeOtherDescription
	,cRiskLocBOP.AnnualSales
	,cRiskLocBOP.AnnualSalesAttributableToMfg
	--,coinsurance%
	,cRiskLocBOP.WindOrHailDeductiblePercent
	,cRiskLocBOP.IsBusIncomeAndExtraExpDollarLimit

-- T - Exclusions & Conditions
	-- Exclusions
	,cRiskLocBOP.NuclBioChemRadExcl							-- Nuclear, Biological, Chemical and Radiological Hazards																-- (5812770)
	,cRiskLocBOP.JwlryExclBusIncomeExtraExpTheftExcl		-- Jewelry Exclusion Endorsement with Business Income and Extra Expense Theft Exclusion									-- (5812770)
	,cRiskLocBOP.RemoveInsToValueProvision					-- Removal of Insurance To Value Provision																				-- (5812770)
	,cRiskLocBOP.OutdoorTheftExcl							-- Outdoor Theft Exclusion																								-- (365803)
	,cRiskLocBOP.PunitiveDamagesCertTerrorismExcl			-- Exclusion of Punitive Damages Related to a Certified Act of Terrorism												-- (5812770)

	-- Conditions
	,cRiskLocBOP.AmdCancProvisionsCovChg					-- Amendment of Cancellation Provisions or Coverage Change																-- (716003)			
	,cRiskLocBOP.AmdCancProvisionsCovChgDaysAdvNotice		-- Amendment of Cancellation Provisions or Coverage Change (Number of Days Advance Notice)
	,cRiskLocBOP.ForkliftExtCond							-- Forklift Extension																									-- (1565522)			
	,cRiskLocBOP.ForkliftExtCondBlanketLimit				-- Forklift Extension (Blanket Limit)
	,cRiskLocBOP.ForkliftExtCondDeduct						-- Forklift Extension (Deductible)
	,cRiskLocBOP.PattrnFrmsCond								-- Patterns, Dies, Molds and Forms Amendment																			-- (1565522)			
	,cRiskLocBOP.PattrnFrmsCondLimit						-- Patterns, Dies, Molds and Forms Amendment (Limit (Equals the sum of underlying BPP Limit))							-- (check the value as each record has the same value 500)

-- T - Location Information
	-- BOP/Inland Marine Loc Home Based Business Questions
	,cRiskLocBOP.BusinessPremise							-- Describe your specific business premises (basement, attached garage, detached garage, outbuilding, etc.)				-- (183596)
	,cRiskLocBOP.InviteJewelryClients						-- Do you invite jewelry clients or the general public into your home / jewelry business?
	,cRiskLocBOP.SignageAtHome								-- Do you have signage at your home that discloses your business operations?
	,cRiskLocBOP.AnimalsInPremise							-- Do you have animals on the premise?
	,cRiskLocBOP.WhatSpecies								-- What Species? (populated when AnimalsInPremise = 1)
	,cRiskLocBOP.SwimmingPool								-- Do you have a swimming pool?
	,cRiskLocBOP.Trampoline									-- Do you have a trampoline?
	,cRiskLocBOP.MerchandiseAccessChildren					-- Is business / merchandise accessible to children under the age of 18?
	,cRiskLocBOP.DescribeHow								-- Describe how access is restricted? (mostly populated when MerchandiseAccessChildren = 1)

	-- BOP Loc Underwriting Information
	,cRiskLocBOP.HasBeenInBusiness							-- Has business been in operation less than two years at this location?
	,cRiskLocBOP.PreviousLocationAddress					-- Previous location address? (mostly populated when HasBeenInBusiness = 1)
	,cRiskLocBOP.PastLosses									-- Any past losses, claims, or allegations relating to sexual abuse, molestation or negligent hiring or firing?
	,cRiskLocBOP.ExplainLoss								-- Explain (mostly populated when PastLosses = 1; values when past losses = 0 are 'argon and oxygen' and 'TBD')
	,cRiskLocBOP.ExposureToFlammables						-- Any exposure to flammables, explosive, or chemicals?
	,cRiskLocBOP.ExplainExposure							-- Explain (populated when ExposureToFlammables = 1)																-- (99138)
	,cRiskLocBOP.ContainLead								-- Do current or past products, or their components, contain lead?
	,cRiskLocBOP.ExplainContainLead							-- Explain (populated when ContainLead = 1)

	,DATE('{date}') as bq_load_date	

FROM
(
	----------------------------------------------------------------------------
	---Risk Location Location
	----------------------------------------------------------------------------
	SELECT 
		DENSE_RANK() OVER(PARTITION BY	pc_boplocation.ID
							ORDER BY	IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS LocationRank
		,DENSE_RANK() OVER(PARTITION BY pc_boplocation.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
							) AS FixedLocationRank
		,DENSE_RANK() OVER(PARTITION BY pc_boplocation.ID
							ORDER BY	IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pc_bopcost.ID DESC
							) AS CostRank
	
		,pc_boplocation.PublicID										AS LocationPublicID
		,pc_policyperiod.PublicID										AS PolicyPeriodPublicID
		,pc_job.JobNumber
		,pc_policyperiod.PolicyNumber
		,ConfigBOPLocationRisk.Value									AS RiskLevel
		,pc_boplocation.EffectiveDate
		,pc_boplocation.ExpirationDate
		,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
					AND pc_boplocation.BOPLine = pc_policyline.FixedID
			THEN 1 ELSE 0 
			END AS IsTransactionSliceEffective
	-- T - Details
		-- Location
		,pc_policylocation.LocationNum									AS LocationNumber
		,CTE_PrimaryRatingLocationBOP.IsPrimaryLocation
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
		
		-- Manufacturing Process
		,pc_boplocation.Casting_JMIC									AS Casting
		,pc_boplocation.Plating_JMIC									AS Plating
		,pc_boplocation.AllOther_JMIC									AS AllOtherMfg

		-- Employee Informatioin
		,pcx_commonlocation_jmic.FullTimeEmployees_JMIC					AS FullTimeEmployees
		,pcx_commonlocation_jmic.PartTimeEmployees_JMIC					AS PartTimeEmployees
		,pcx_commonlocation_jmic.Owners_JMIC							AS Owners

		-- Public Protection
		,pctl_fireprotectclass.TYPECODE									AS PublicProtection 

		-- Location Information
		,pctl_boplocationtype_jmic.TYPECODE								AS LocationType
		,pctl_boplocationtype_jmic.NAME									AS LocationTypeName
		,pcx_commonlocation_jmic.OtherDescription_JMIC					AS LocationTypeOtherDescription
		,pcx_commonlocation_jmic.AnnualSales_JMIC						AS AnnualSales
		,pcx_commonlocation_jmic.AnnualSalesAttrToManufact_JMIC			AS AnnualSalesAttributableToMfg
		--,coinsurance%
		,pctl_windorhaildedpercent_jmic.TYPECODE						AS WindOrHailDeductiblePercent
		,CAST(pc_boplocation.ExtraExpIndicator_JMIC	AS INT64)			AS IsBusIncomeAndExtraExpDollarLimit
			
	-- T - Exclusions & Conditions
		-- Exclusions
		,cLocExcl.NuclBioChemRadExcl							
		,cLocExcl.JwlryExclBusIncomeExtraExpTheftExcl					
		,cLocExcl.RemoveInsToValueProvision									
		,cLocExcl.OutdoorTheftExcl								
		,cLocExcl.PunitiveDamagesCertTerrorismExcl						

		-- Conditions
		,cLocCond.AmdCancProvisionsCovChg								
		,cLocCond.AmdCancProvisionsCovChgDaysAdvNotice				
		,cLocCond.ForkliftExtCond										
		,cLocCond.ForkliftExtCondBlanketLimit						
		,cLocCond.ForkliftExtCondDeduct							
		,cLocCond.PattrnFrmsCond									
		,cLocCond.PattrnFrmsCondLimit							

	-- T - Location Information
		-- BOP/Inland Marine Loc Home Based Business Questions
		,cBOPLocAns.BusinessPremise_JMIC						AS BusinessPremise			
		,cBOPLocAns.InviteJewelryClients_JMIC					AS InviteJewelryClients		
		,cBOPLocAns.SignageAtHome_JMIC							AS SignageAtHome				
		,cBOPLocAns.AnimalsInPremise_JMIC						AS AnimalsInPremise			
		,cBOPLocAns.WhatSpecies_JMIC							AS WhatSpecies				
		,cBOPLocAns.SwimmingPool_JMIC							AS SwimmingPool				
		,cBOPLocAns.Trampoline_JMIC								AS Trampoline					
		,cBOPLocAns.MerchandiseAccessChildren_JMIC				AS MerchandiseAccessChildren	
		,cBOPLocAns.DescribeHow_JMIC							AS DescribeHow				

		-- BOP Loc Underwriting Information
		,cBOPLocAns.HasBeenInBusiness_JMIC						AS HasBeenInBusiness		
		,cBOPLocAns.PreviousLocAdd_JMIC							AS PreviousLocationAddress			
		,cBOPLocAns.PastLosses_JMIC								AS PastLosses				
		,cBOPLocAns.ExplainLoss_JMIC							AS ExplainLoss			
		,cBOPLocAns.ExposureToFlammables_JMIC					AS ExposureToFlammables	
		,cBOPLocAns.ExplainExposure_JMIC						AS ExplainExposure		
		,cBOPLocAns.ContainLead_JMIC							AS ContainLead			
		,cBOPLocAns.ExplainContainLead_JMIC						AS ExplainContainLead		

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
		ON pc_boplocation.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType

		INNER JOIN RiskLocationBOPConfig AS ConfigBOPLine
		ON ConfigBOPLine.Value = pctl_policyline.TYPECODE
		AND ConfigBOPLine.Key = 'BOPLineCode' 

		INNER JOIN RiskLocationBOPConfig AS ConfigBOPLocationRisk
		ON ConfigBOPLocationRisk.Key='LocationLevelRisk'

		INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost
		ON pc_bopcost.BranchID = pc_policyperiod.ID
		AND pc_bopcost.BOPLocation = pc_boplocation.FixedID
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)

		--Join in the CTE_PrimaryRatingLocationBOP Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN CTE_PrimaryRatingLocationBOP AS CTE_PrimaryRatingLocationBOP
		ON CTE_PrimaryRatingLocationBOP.PolicyPeriodID = pc_policyperiod.ID
		AND CTE_PrimaryRatingLocationBOP.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((CTE_PrimaryRatingLocationBOP.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and CTE_PrimaryRatingLocationBOP.PolicyLineOfBusiness = 'BusinessOwnersLine'))

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
		ON pc_policylocation.BranchID = pc_boplocation.BranchID
		AND pc_policylocation.FixedID = pc_boplocation.Location
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state
		ON pctl_state.ID = pc_policylocation.StateInternal

		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country
		ON pctl_country.ID = pc_policylocation.CountryInternal
	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_commonlocation_jmic
		ON pcx_commonlocation_jmic.BranchID = pc_boplocation.BranchID
		AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_territorycode` WHERE _PARTITIONTIME = {partition_date}) AS pc_territorycode
		ON pc_territorycode.BranchID = pc_policylocation.BranchID
		AND pc_territorycode.PolicyLocation = pc_policylocation.FixedID 
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_territorycode.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_territorycode.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_boplocationtype_jmic` AS pctl_boplocationtype_jmic
		ON pctl_boplocationtype_jmic.ID = pcx_commonlocation_jmic.LocationType_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastal_jmic` AS pctl_bopcoastal_jmic
		ON pctl_bopcoastal_jmic.ID = pc_policylocation.Coastal_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastalzones_jmic` AS pctl_bopcoastalzones_jmic
		ON pctl_bopcoastalzones_jmic.ID = pc_policylocation.CoastalZones_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` AS pctl_fireprotectclass
		ON pctl_fireprotectclass.ID = pc_policylocation.FireProtectClass

		LEFT JOIN `{project}.{pc_dataset}.pctl_windorhaildedpercent_jmic` AS pctl_windorhaildedpercent_jmic
		ON pctl_windorhaildedpercent_jmic.ID = pc_boplocation.WindOrHailDedPercent_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_addressstatus_jmic` AS pctl_addressstatus_jmic
		ON pctl_addressstatus_jmic.ID = pc_policylocation.AddressStatus_JMICInternal

		LEFT JOIN `{project}.{dest_dataset}.t_RiskLocationBOP_Pivot` AS cBOPLocAns
		ON cBOPLocAns.BOPLocationPublicID = pc_boplocation.PublicID
		
		-- Exclusions
		LEFT JOIN
			(SELECT 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPJwlryExclBusIncExtExpThftExcl_JMIC'	THEN 1	END) AS JwlryExclBusIncomeExtraExpTheftExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPNuclBioChemRadExcl_JMIC'				THEN 1	END) AS NuclBioChemRadExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPOutdoorTheftExcl_JMIC'				THEN 1	END) AS OutdoorTheftExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPPunDamCertTerExcl_JMIC'				THEN 1	END) AS PunitiveDamagesCertTerrorismExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPRemInsValProv_JMIC'					THEN 1	END) AS RemoveInsToValueProvision

			FROM RiskLocBusinessOwnerCombinations

				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boplocationexcl_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_boplocationexcl_jmic
				ON  pcx_boplocationexcl_jmic.BranchID = RiskLocBusinessOwnerCombinations.BranchID
				AND pcx_boplocationexcl_jmic.BOPLocation = RiskLocBusinessOwnerCombinations.BOPLocationFixedID
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate >= COALESCE(pcx_boplocationexcl_jmic.EffectiveDate,RiskLocBusinessOwnerCombinations.PeriodStart)
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate <  COALESCE(pcx_boplocationexcl_jmic.ExpirationDate,RiskLocBusinessOwnerCombinations.PeriodEnd)

			GROUP BY 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
			) AS cLocExcl
		ON cLocExcl.BOPLocationPublicID = pc_boplocation.PublicID

		-- Conditions
		LEFT JOIN
			(SELECT 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPAmdCancProvCovChg_JMIC'	THEN CAST(pcx_boplocationcond_jmic.DirectTerm1Avl AS INT64) END)			AS AmdCancProvisionsCovChg				-- Amendment of Cancellation Provisions or Coverage Change										-- (716003)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPAmdCancProvCovChg_JMIC'	THEN pcx_boplocationcond_jmic.DirectTerm1 END)								AS AmdCancProvisionsCovChgDaysAdvNotice	-- Amendment of Cancellation Provisions or Coverage Change (Number of Days Advance Notice) 
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN CAST(COALESCE(pcx_boplocationcond_jmic.DirectTerm1Avl
																												,pcx_boplocationcond_jmic.DirectTerm2Avl) AS INT64) END)		AS ForkliftExtCond						-- Forklift Extension																			-- (1565522)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN pcx_boplocationcond_jmic.DirectTerm1 END)								AS ForkliftExtCondBlanketLimit			-- Forklift Extension (Blanket Limit)
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN pcx_boplocationcond_jmic.DirectTerm2 END)								AS ForkliftExtCondDeduct				-- Forklift Extension (Deductible)
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPPattrnFrmsCond_JMIC'		THEN CAST(pcx_boplocationcond_jmic.DirectTerm1Avl AS INT64) END)			AS PattrnFrmsCond						-- Patterns, Dies, Molds and Forms Amendment													-- (1565522)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPPattrnFrmsCond_JMIC'		THEN CONCAT('Derived: ',CAST(pcx_boplocationcond_jmic.DirectTerm1 AS STRING)) END)	AS PattrnFrmsCondLimit			-- Patterns, Dies, Molds and Forms Amendment (Limit (Equals the sum of underlying BPP Limit))
																								
			FROM RiskLocBusinessOwnerCombinations
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boplocationcond_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_boplocationcond_jmic
				ON  pcx_boplocationcond_jmic.BranchID = RiskLocBusinessOwnerCombinations.BranchID
				AND pcx_boplocationcond_jmic.BOPLocation = RiskLocBusinessOwnerCombinations.BOPLocationFixedID
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate >= COALESCE(pcx_boplocationcond_jmic.EffectiveDate,RiskLocBusinessOwnerCombinations.PeriodStart)
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate <  COALESCE(pcx_boplocationcond_jmic.ExpirationDate,RiskLocBusinessOwnerCombinations.PeriodEnd)
			GROUP BY 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
			) AS cLocCond
		ON cLocCond.BOPLocationPublicID = pc_boplocation.PublicID

	--WHERE	1 = 1
	
	UNION ALL
	----------------------------------------------------------------------------
	---Risk Location LOB or Line
	----------------------------------------------------------------------------
	SELECT 
		DENSE_RANK() OVER(PARTITION BY	pc_boplocation.ID
							ORDER BY	IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS LocationRank
		,DENSE_RANK() OVER(PARTITION BY pc_boplocation.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
							) AS FixedLocationRank
		,DENSE_RANK() OVER(PARTITION BY pc_boplocation.ID
							ORDER BY	IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pc_bopcost.ID DESC
							) AS CostRank
	
		,pc_boplocation.PublicID										AS LocationPublicID
		,pc_policyperiod.PublicID										AS PolicyPeriodPublicID
		,pc_job.JobNumber
		,pc_policyperiod.PolicyNumber
		,ConfigBOPLocationRisk.Value									AS RiskLevel
		,pc_boplocation.EffectiveDate
		,pc_boplocation.ExpirationDate
		,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
					AND pc_boplocation.BOPLine = pc_policyline.FixedID
			THEN 1 ELSE 0 
			END AS IsTransactionSliceEffective
	-- T - Details
		-- Location
		,pc_policylocation.LocationNum									AS LocationNumber
		,CTE_PrimaryRatingLocationBOP.IsPrimaryLocation
		--,NULL As IsPrimaryLocation --testing
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
		
		-- Manufacturing Process
		,pc_boplocation.Casting_JMIC									AS Casting
		,pc_boplocation.Plating_JMIC									AS Plating
		,pc_boplocation.AllOther_JMIC									AS AllOtherMfg

		-- Employee Informatioin
		,pcx_commonlocation_jmic.FullTimeEmployees_JMIC					AS FullTimeEmployees
		,pcx_commonlocation_jmic.PartTimeEmployees_JMIC					AS PartTimeEmployees
		,pcx_commonlocation_jmic.Owners_JMIC							AS Owners

		-- Public Protection
		,pctl_fireprotectclass.TYPECODE									AS PublicProtection 

		-- Location Information
		,pctl_boplocationtype_jmic.TYPECODE								AS LocationType
		,pctl_boplocationtype_jmic.NAME									AS LocationTypeName
		,pcx_commonlocation_jmic.OtherDescription_JMIC					AS LocationTypeOtherDescription
		,pcx_commonlocation_jmic.AnnualSales_JMIC						AS AnnualSales
		,pcx_commonlocation_jmic.AnnualSalesAttrToManufact_JMIC			AS AnnualSalesAttributableToMfg
		--,coinsurance%
		,pctl_windorhaildedpercent_jmic.TYPECODE						AS WindOrHailDeductiblePercent
		,CAST(pc_boplocation.ExtraExpIndicator_JMIC	AS INT64)			AS IsBusIncomeAndExtraExpDollarLimit
			
	-- T - Exclusions & Conditions
		-- Exclusions
		,cLocExcl.NuclBioChemRadExcl							
		,cLocExcl.JwlryExclBusIncomeExtraExpTheftExcl					
		,cLocExcl.RemoveInsToValueProvision									
		,cLocExcl.OutdoorTheftExcl								
		,cLocExcl.PunitiveDamagesCertTerrorismExcl						

		-- Conditions
		,cLocCond.AmdCancProvisionsCovChg								
		,cLocCond.AmdCancProvisionsCovChgDaysAdvNotice				
		,cLocCond.ForkliftExtCond										
		,cLocCond.ForkliftExtCondBlanketLimit						
		,cLocCond.ForkliftExtCondDeduct							
		,cLocCond.PattrnFrmsCond									
		,cLocCond.PattrnFrmsCondLimit							

	-- T - Location Information
		-- BOP/Inland Marine Loc Home Based Business Questions
		,cBOPLocAns.BusinessPremise_JMIC						AS BusinessPremise			
		,cBOPLocAns.InviteJewelryClients_JMIC					AS InviteJewelryClients		
		,cBOPLocAns.SignageAtHome_JMIC							AS SignageAtHome				
		,cBOPLocAns.AnimalsInPremise_JMIC						AS AnimalsInPremise			
		,cBOPLocAns.WhatSpecies_JMIC							AS WhatSpecies				
		,cBOPLocAns.SwimmingPool_JMIC							AS SwimmingPool				
		,cBOPLocAns.Trampoline_JMIC								AS Trampoline					
		,cBOPLocAns.MerchandiseAccessChildren_JMIC				AS MerchandiseAccessChildren	
		,cBOPLocAns.DescribeHow_JMIC							AS DescribeHow				

		-- BOP Loc Underwriting Information
		,cBOPLocAns.HasBeenInBusiness_JMIC						AS HasBeenInBusiness		
		,cBOPLocAns.PreviousLocAdd_JMIC							AS PreviousLocationAddress			
		,cBOPLocAns.PastLosses_JMIC								AS PastLosses				
		,cBOPLocAns.ExplainLoss_JMIC							AS ExplainLoss			
		,cBOPLocAns.ExposureToFlammables_JMIC					AS ExposureToFlammables	
		,cBOPLocAns.ExplainExposure_JMIC						AS ExplainExposure		
		,cBOPLocAns.ContainLead_JMIC							AS ContainLead			
		,cBOPLocAns.ExplainContainLead_JMIC						AS ExplainContainLead		

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
			AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType

		INNER JOIN RiskLocationBOPConfig AS ConfigBOPLine
			ON ConfigBOPLine.Value = pctl_policyline.TYPECODE
			AND ConfigBOPLine.Key = 'BOPLineCode' 
		INNER JOIN RiskLocationBOPConfig AS ConfigBOPLocationRisk
			ON ConfigBOPLocationRisk.Key = 'LineLevelRisk'

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		/* Need?*/
		--INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctl_policyperiodstatus	ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date} ) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
		--Join in the CTE_PrimaryRatingLocationBOP Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN CTE_PrimaryRatingLocationBOP AS CTE_PrimaryRatingLocationBOP
			ON CTE_PrimaryRatingLocationBOP.PolicyPeriodID = pc_policyperiod.ID
			AND CTE_PrimaryRatingLocationBOP.EditEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((CTE_PrimaryRatingLocationBOP.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				or 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and CTE_PrimaryRatingLocationBOP.PolicyLineOfBusiness = 'BusinessOwnersLine'))

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = CTE_PrimaryRatingLocationBOP.PolicyPeriodID  
			AND pc_policylocation.LocationNum = CTE_PrimaryRatingLocationBOP.RatingLocationNum  
			AND COALESCE(CTE_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(CTE_PrimaryRatingLocationBOP.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state
			ON pctl_state.ID = pc_policylocation.StateInternal

		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country
			ON pctl_country.ID = pc_policylocation.CountryInternal

		--BOP Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
			ON pc_boplocation.BranchID = pc_policyperiod.ID
			AND pc_boplocation.Location = pc_policylocation.FixedID
			--tie striaght to BOP
			AND ((pc_boplocation.BOPLine = pc_policyline.FixedID)
					OR 
				(pc_boplocation.BOPLine = pc_policyline.FixedID AND pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC'))
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost
			ON pc_bopcost.BranchID = pc_policyperiod.ID
			AND pc_bopcost.BOPLocation = pc_boplocation.FixedID
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) <  COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_commonlocation_jmic
		ON pcx_commonlocation_jmic.BranchID = pc_boplocation.BranchID
		AND pcx_commonlocation_jmic.FixedID = pc_boplocation.CommonLocation
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_territorycode` WHERE _PARTITIONTIME = {partition_date}) AS pc_territorycode
		ON pc_territorycode.BranchID = pc_policylocation.BranchID
		AND pc_territorycode.PolicyLocation = pc_policylocation.FixedID 
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_territorycode.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_territorycode.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_boplocationtype_jmic` AS pctl_boplocationtype_jmic
		ON pctl_boplocationtype_jmic.ID = pcx_commonlocation_jmic.LocationType_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastal_jmic` AS pctl_bopcoastal_jmic
		ON pctl_bopcoastal_jmic.ID = pc_policylocation.Coastal_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_bopcoastalzones_jmic` AS pctl_bopcoastalzones_jmic
		ON pctl_bopcoastalzones_jmic.ID = pc_policylocation.CoastalZones_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` AS pctl_fireprotectclass
		ON pctl_fireprotectclass.ID = pc_policylocation.FireProtectClass

		LEFT JOIN `{project}.{pc_dataset}.pctl_windorhaildedpercent_jmic` AS pctl_windorhaildedpercent_jmic
		ON pctl_windorhaildedpercent_jmic.ID = pc_boplocation.WindOrHailDedPercent_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_addressstatus_jmic` AS pctl_addressstatus_jmic
		ON pctl_addressstatus_jmic.ID = pc_policylocation.AddressStatus_JMICInternal

		LEFT JOIN `{project}.{dest_dataset}.t_RiskLocationBOP_Pivot` AS cBOPLocAns
		ON cBOPLocAns.BOPLocationPublicID = pc_boplocation.PublicID
	
		
		-- Exclusions
		LEFT JOIN
			(SELECT 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPJwlryExclBusIncExtExpThftExcl_JMIC'	THEN 1	END) AS JwlryExclBusIncomeExtraExpTheftExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPNuclBioChemRadExcl_JMIC'				THEN 1	END) AS NuclBioChemRadExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPOutdoorTheftExcl_JMIC'				THEN 1	END) AS OutdoorTheftExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPPunDamCertTerExcl_JMIC'				THEN 1	END) AS PunitiveDamagesCertTerrorismExcl
				,MAX(CASE WHEN pcx_boplocationexcl_jmic.PatternCode = 'BOPRemInsValProv_JMIC'					THEN 1	END) AS RemoveInsToValueProvision

			FROM RiskLocBusinessOwnerCombinations

				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boplocationexcl_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_boplocationexcl_jmic
				ON  pcx_boplocationexcl_jmic.BranchID = RiskLocBusinessOwnerCombinations.BranchID
				AND pcx_boplocationexcl_jmic.BOPLocation = RiskLocBusinessOwnerCombinations.BOPLocationFixedID
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate >= COALESCE(pcx_boplocationexcl_jmic.EffectiveDate,RiskLocBusinessOwnerCombinations.PeriodStart)
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate <  COALESCE(pcx_boplocationexcl_jmic.ExpirationDate,RiskLocBusinessOwnerCombinations.PeriodEnd)

			GROUP BY 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
			) AS cLocExcl
		ON cLocExcl.BOPLocationPublicID = pc_boplocation.PublicID

		-- Conditions
		LEFT JOIN
			(SELECT 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPAmdCancProvCovChg_JMIC'	THEN CAST(pcx_boplocationcond_jmic.DirectTerm1Avl AS INT64) END)			AS AmdCancProvisionsCovChg				-- Amendment of Cancellation Provisions or Coverage Change										-- (716003)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPAmdCancProvCovChg_JMIC'	THEN pcx_boplocationcond_jmic.DirectTerm1 END)								AS AmdCancProvisionsCovChgDaysAdvNotice	-- Amendment of Cancellation Provisions or Coverage Change (Number of Days Advance Notice) 
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN CAST(COALESCE(pcx_boplocationcond_jmic.DirectTerm1Avl
																												,pcx_boplocationcond_jmic.DirectTerm2Avl) AS INT64) END)		AS ForkliftExtCond						-- Forklift Extension																			-- (1565522)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN pcx_boplocationcond_jmic.DirectTerm1 END)								AS ForkliftExtCondBlanketLimit			-- Forklift Extension (Blanket Limit)
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPFrklftExtCond_JMIC'		THEN pcx_boplocationcond_jmic.DirectTerm2 END)								AS ForkliftExtCondDeduct				-- Forklift Extension (Deductible)
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPPattrnFrmsCond_JMIC'		THEN CAST(pcx_boplocationcond_jmic.DirectTerm1Avl AS INT64) END)			AS PattrnFrmsCond						-- Patterns, Dies, Molds and Forms Amendment													-- (1565522)			
				,MAX(CASE WHEN pcx_boplocationcond_jmic.PatternCode = 'BOPPattrnFrmsCond_JMIC'		THEN CONCAT('Derived: ',CAST(pcx_boplocationcond_jmic.DirectTerm1 AS STRING)) END)	AS PattrnFrmsCondLimit			-- Patterns, Dies, Molds and Forms Amendment (Limit (Equals the sum of underlying BPP Limit))
																								
			FROM RiskLocBusinessOwnerCombinations
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boplocationcond_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_boplocationcond_jmic
				ON  pcx_boplocationcond_jmic.BranchID = RiskLocBusinessOwnerCombinations.BranchID
				AND pcx_boplocationcond_jmic.BOPLocation = RiskLocBusinessOwnerCombinations.BOPLocationFixedID
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate >= COALESCE(pcx_boplocationcond_jmic.EffectiveDate,RiskLocBusinessOwnerCombinations.PeriodStart)
				AND RiskLocBusinessOwnerCombinations.EditEffectiveDate <  COALESCE(pcx_boplocationcond_jmic.ExpirationDate,RiskLocBusinessOwnerCombinations.PeriodEnd)
			GROUP BY 
				RiskLocBusinessOwnerCombinations.BOPLocationPublicID
			) AS cLocCond
		ON cLocCond.BOPLocationPublicID = pc_boplocation.PublicID
		
	--WHERE	1 = 1

)cRiskLocBOP

	INNER JOIN RiskLocationBOPConfig AS ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN RiskLocationBOPConfig AS ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN RiskLocationBOPConfig AS ConfigHashAlgo
	ON ConfigHashAlgo.Key='HashingAlgorithm'

WHERE 1 = 1
	AND cRiskLocBOP.LocationRank = 1
	AND cRiskLocBOP.CostRank = 1

--) outerquery
;DROP table RiskLocBusinessOwnerCombinations;
