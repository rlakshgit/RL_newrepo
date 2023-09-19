-- tag: RiskStockIM - tag ends/
/*********************************************************************************************************************************************************************
  RiskStockIM.sql
	Converted to BigQuery
		Contains Pivots
**********************************************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/23/2021	SLJ			Init
	04/29/2021	DROBAK		Add unit test cases
	04/29/2021	DROBAK		Duplicates found: commented out Inventory sections
	04/30/2021	DROBAK		Field Name clean-up for BB layer
	06/09/2021	SLJ			FixedRank modified, Cleanup, JOIN modified for Inner select or pivot, Unit tests	
	08/08/2022	DROBAK		Added stockRisk parameter: ('StockLevelRisk','ILMStock') to Config table and RiskStockKey
	05/02/2023	SLJ			Modify the date logic for join to pcx_ilmlocation_jmic

-----------------------------------------------------------------------------------------------------------------

-- T - Details
	-- property categories: Exclude from theft and exclude from insurance to include: multi-pivot issue 1752669 or 1239306
	-- property sub categories: dups  2221299,5963725
-- T - Earthquake & Flood Information	: Add coverage condition in building blocks
	-- Earthquake Information: earthquake zone 3982951
	-- Flood Information: flood info not available for stock but populated 1618383 or 2004399
-- T - Exclusions & Conditions
	-- Exclusions: Manage Schedules available for patterncode ILMStockExclSlThftCnd_JMIC and ILMStockNonJwlInv_JMIC
	-- Policy Conditions: 
			-- Manage Schedules and Manage warranties available for patternCode ILMStockStockLimClsd_JMIC
			-- Choice option code lookup
			-- Duplicates 6621810
*/
-- Combinations
CREATE OR REPLACE TEMP TABLE RiskStockIMCombinations AS 
(
	SELECT 
		pcx_jewelrystock_jmic.PublicID									AS StockPublicID
		,pc_policyperiod.PublicID										AS PolicyPeriodPublicID
		,pc_policyperiod.EditEffectiveDate								AS EditEffectiveDate	
		,pc_policyperiod.PeriodStart	
		,pc_policyperiod.PeriodEnd
		,pcx_jewelrystock_jmic.BranchID									AS BranchID
		,pcx_jewelrystock_jmic.FixedID									AS StockFixedID
	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod

		INNER JOIN `{project}.{pc_dataset}.pcx_jewelrystock_jmic` AS pcx_jewelrystock_jmic
		ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID	

	WHERE	1 = 1  
			AND pc_policyperiod._PARTITIONTIME= {partition_date}
			AND pcx_jewelrystock_jmic._PARTITIONTIME= {partition_date}
);
-- Inventory
CREATE OR REPLACE TEMP TABLE RiskStockIMInventoryBreakdown AS 
(
WITH cInvBreak_pivot AS
(
	SELECT DISTINCT
		RiskStockIMCombinations.StockPublicID
		,pctl_jewelrystockpctype_jmic.TYPECODE 
		,pctl_jewelrystockpctype_jmic.DESCRIPTION
		,pcx_jewelrystockpropcat.PercentOfTotal
		,CAST(pcx_jewelrystockpropcat.IsExcludedFromTheft AS INT64) AS IsExcludedFromTheft
		,CAST(pcx_jewelrystockpropcat.IsExcludedFromInsurance AS INT64) AS IsExcludedFromInsurance
			
	FROM RiskStockIMCombinations AS RiskStockIMCombinations

		INNER JOIN `{project}.{pc_dataset}.pcx_jewelrystockpropcat_jmic` AS pcx_jewelrystockpropcat
		ON  pcx_jewelrystockpropcat.BranchID = RiskStockIMCombinations.BranchID
		AND pcx_jewelrystockpropcat.JewelryStock = RiskStockIMCombinations.StockFixedID	
		AND RiskStockIMCombinations.EditEffectiveDate >= COALESCE(pcx_jewelrystockpropcat.EffectiveDate,RiskStockIMCombinations.PeriodStart)
		AND RiskStockIMCombinations.EditEffectiveDate <  COALESCE(pcx_jewelrystockpropcat.ExpirationDate,RiskStockIMCombinations.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_jewelrystockpctype_jmic` AS pctl_jewelrystockpctype_jmic
		ON pctl_jewelrystockpctype_jmic.ID = pcx_jewelrystockpropcat.JewelryStockPCType

	WHERE	1 = 1  
			AND pcx_jewelrystockpropcat._PARTITIONTIME= {partition_date}
)
SELECT 
	cInvBreak_pivot.StockPublicID
	,cInvBreak_pivot.TYPECODE
	,CONCAT(CONCAT('{lbracket}"DESCRIPTION"',':',TO_JSON_STRING(cInvBreak_pivot.DESCRIPTION),',') 
	 ,CONCAT('"PercentOfTotal"',':',TO_JSON_STRING(cInvBreak_pivot.PercentOfTotal),',')
	 ,CONCAT('"IsExcludedFromTheft"',':',TO_JSON_STRING(cInvBreak_pivot.IsExcludedFromTheft),',')
	  ,CONCAT('"IsExcludedFromInsurance"',':',TO_JSON_STRING(cInvBreak_pivot.IsExcludedFromInsurance)),'{rbracket}')  TypeCodeValues
FROM cInvBreak_pivot
GROUP BY
	cInvBreak_pivot.StockPublicID
	,cInvBreak_pivot.TYPECODE
	,cInvBreak_pivot.DESCRIPTION
	,cInvBreak_pivot.PercentOfTotal
	,cInvBreak_pivot.IsExcludedFromTheft
	,cInvBreak_pivot.IsExcludedFromInsurance
);
CALL `{project}.custom_functions.sp_pivot`(
  'RiskStockIMInventoryBreakdown' # source table
  ,'{project}.{dest_dataset}.t_RiskStockIMInventoryBreakdownPivot' # destination table
  , ['StockPublicID'] # row_ids
  , 'TYPECODE'-- # pivot_col_name
  , 'TypeCodeValues'--# pivot_col_value
  , 15--# max_columns
  , 'MAX' --# aggregation
  , '' --# optional_limit
);
DROP TABLE RiskStockIMInventoryBreakdown;

-- Watch
CREATE OR REPLACE TEMP TABLE RiskStockIMWatch AS 
(
	SELECT 
		RiskStockIMCombinations.StockPublicID
		,pctl_watchbrands_jm.TYPECODE
		,COALESCE(CAST(IsCarried AS INT64),0) AS IsCarried 
	FROM RiskStockIMCombinations AS RiskStockIMCombinations

		INNER JOIN `{project}.{pc_dataset}.pcx_watchbrandcontainer_jm` AS pcx_watchbrandcontainer_jm
		ON pcx_watchbrandcontainer_jm.BranchID = RiskStockIMCombinations.BranchID
		AND pcx_watchbrandcontainer_jm.JewelryStock_JMIC = RiskStockIMCombinations.StockFixedID
		AND RiskStockIMCombinations.EditEffectiveDate >= COALESCE(pcx_watchbrandcontainer_jm.EffectiveDate,RiskStockIMCombinations.PeriodStart)
		AND RiskStockIMCombinations.EditEffectiveDate <  COALESCE(pcx_watchbrandcontainer_jm.ExpirationDate,RiskStockIMCombinations.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_watchbrands_jm` AS pctl_watchbrands_jm
		ON pctl_watchbrands_jm.ID = pcx_watchbrandcontainer_jm.WatchBrand

	WHERE	1 = 1  
			AND pcx_watchbrandcontainer_jm._PARTITIONTIME= {partition_date}
);
CALL `{project}.custom_functions.sp_pivot`(
  'RiskStockIMWatch' # source table
  ,'{project}.{dest_dataset}.t_RiskStockIMWatchPivot' # destination table
  , ['StockPublicID'] # row_ids
  , 'TYPECODE'-- # pivot_col_name
  , 'IsCarried'--# pivot_col_value
  , 10--# max_columns
  , 'MAX' --# aggregation
  , '' --# optional_limit
);
DROP TABLE RiskStockIMWatch;

-- Exclusions and Conditions
CREATE OR REPLACE TEMP TABLE RiskStockIMExclCond AS 
(
	SELECT 
		RiskStockIMCombinations.StockPublicID
		-- Policy Conditions
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnPledgedVal_JM'	THEN CAST(COALESCE(pcx_jewelrystockcond.MediumStringTerm1Avl
																												,pcx_jewelrystockcond.ChoiceTerm1Avl
																							 					,pcx_jewelrystockcond.ChoiceTerm2Avl) AS INT64)	END)		AS IsJwlryPawnPledgedVal										
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnPledgedVal_JM'	THEN pcx_jewelrystockcond.ChoiceTerm1						END)		AS JwlryPawnPledgedValMethod							
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnPledgedVal_JM'	THEN pcx_jewelrystockcond.MediumStringTerm1					END)		AS JwlryPawnPledgedValOtherDesc											
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnPledgedVal_JM'	THEN pcx_jewelrystockcond.ChoiceTerm2						END)		AS JwlryPawnPledgedValMethodMultiplier					
				
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnUnpledgedVal_JM'	THEN CAST(COALESCE(pcx_jewelrystockcond.MediumStringTerm1Avl
																												,pcx_jewelrystockcond.ChoiceTerm1Avl
																												,pcx_jewelrystockcond.ChoiceTerm2Avl) AS INT64)	END)		AS IsJwlryPawnUnpledgedVal					
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnUnpledgedVal_JM'	THEN pcx_jewelrystockcond.ChoiceTerm1					END)		AS JwlryPawnUnpledgedValMethod				
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnUnpledgedVal_JM'	THEN pcx_jewelrystockcond.MediumStringTerm1				END)		AS JwlryPawnUnpledgedValOtherDesc			
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockJwlryPawnUnpledgedVal_JM'	THEN pcx_jewelrystockcond.ChoiceTerm2					END)		AS JwlryPawnUnpledgedValMethodMultiplier
				
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockUnrepInv_JMIC'				THEN CAST(pcx_jewelrystockcond.MediumStringTerm1Avl AS INT64)	END)	AS IsUnrepInv									
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockUnrepInv_JMIC'				THEN pcx_jewelrystockcond.MediumStringTerm1				END)		AS UnrepInvDescValue							

		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockStockLimClsd_JMIC'			THEN CAST(COALESCE(pcx_jewelrystockcond.DirectTerm1Avl
																												,pcx_jewelrystockcond.DirectTerm2Avl
																												,pcx_jewelrystockcond.DirectTerm3Avl
																												,pcx_jewelrystockcond.DirectTerm4Avl
																												,pcx_jewelrystockcond.ChoiceTerm1Avl
																												,pcx_jewelrystockcond.DateTerm1Avl
																												,pcx_jewelrystockcond.DateTerm2Avl) AS INT64)	END)		AS IsStockLimitClsd								
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockStockLimClsd_JMIC'			THEN pcx_jewelrystockcond.DirectTerm1					END)		AS StockLimitClsdOpenStockLimit				
		,MAX(CASE WHEN pcx_jewelrystockcond.PatternCode = 'ILMStockStockLimClsd_JMIC'			THEN pcx_jewelrystockcond.DirectTerm2					END)		AS StockLimitClsdClsdStockLimit

		-- Exclusions
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockExclSlThftCnd_JMIC'	 THEN CAST(COALESCE(pcx_jewelrystockexcl.MediumStringTerm1Avl
																											,pcx_jewelrystockexcl.DirectTerm1Avl) AS INT64)		END)		AS IsExclStkForSaleFromTheft				
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockExclSlThftCnd_JMIC'	 THEN pcx_jewelrystockexcl.DirectTerm1							END)		AS ExclStkForSaleFromTheftPremium			
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockExclThftExcBrgl_JMIC'	 THEN 1	END)															AS IsExclFromTheftExclBurglary					
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockNonJwlInv_JMIC'		 THEN CAST(pcx_jewelrystockexcl.MediumStringTerm1Avl AS INT64)	END)		AS IsExcludeNonJwlryInv					
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockSpecStkSalInv_JMIC'	 THEN CAST(pcx_jewelrystockexcl.MediumStringTerm1Avl AS INT64)	END)		AS IsExclSpecStkForSaleInv				
		,MAX(CASE WHEN pcx_jewelrystockexcl.PatternCode = 'ILMStockSpecStkSalInv_JMIC'	 THEN pcx_jewelrystockexcl.MediumStringTerm1					END)		AS ExclSpecStkForSaleInvPropNotIncl
	 
	FROM RiskStockIMCombinations AS RiskStockIMCombinations

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcond_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockcond
		ON  pcx_jewelrystockcond.BranchID = RiskStockIMCombinations.BranchID
		AND pcx_jewelrystockcond.JewelryStock = RiskStockIMCombinations.StockFixedID
		AND RiskStockIMCombinations.EditEffectiveDate >= COALESCE(pcx_jewelrystockcond.EffectiveDate,RiskStockIMCombinations.PeriodStart)
		AND RiskStockIMCombinations.EditEffectiveDate <  COALESCE(pcx_jewelrystockcond.ExpirationDate,RiskStockIMCombinations.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockexcl_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockexcl
		ON  pcx_jewelrystockexcl.BranchID = RiskStockIMCombinations.BranchID
		AND pcx_jewelrystockexcl.JewelryStock = RiskStockIMCombinations.StockFixedID
		AND RiskStockIMCombinations.EditEffectiveDate >= COALESCE(pcx_jewelrystockexcl.EffectiveDate,RiskStockIMCombinations.PeriodStart)
		AND RiskStockIMCombinations.EditEffectiveDate <  COALESCE(pcx_jewelrystockexcl.ExpirationDate,RiskStockIMCombinations.PeriodEnd)

	GROUP BY 
		RiskStockIMCombinations.StockPublicID
);

--DELETE `{project}.{dest_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date});
/*CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.DAR_RiskStockIM`
AS SELECT outerquery.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.RiskStockIM` (
		SourceSystem,
		RiskStockKey,
		RiskLocationKey,
		PolicyTransactionKey,
		FixedStockRank,
		StockPublicID,
		StockFixedID,
		LocationPublicID,
		PolicyPeriodPublicID,
		RiskLevel,
		EffectiveDate,
		ExpirationDate,
		JobNumber,
		PolicyNumber,
		LocationNumber,
		IsTransactionSliceEffective,
		IsCompleteInvAnnualOrMore,
		LastInventoryTotal,
		LastInventoryDate,
		InventoryType,
		HasDetailedInvRecords,
		HasPurchaseInvoices,
		HasAwayListings,
		PriorInventoryTotal,
		PriorInventoryDate,
		MaxStockValue,
		MaxDailyScrapValue,
		IsCustPropertyRecorded,
		CustomerProperyAverage,
		ConsignmentPropertyAverage,
		InventoryPremiumBase,
		OutofSafeVltStkrmExposure,
		PawnPropertyHandled,
		PawnCoverageIncluded,
		PawnLastInventoryTotal,
		PawnLastInventoryDate,
		PawnPriorInventoryTotal,
		PawnPriorInventoryDate,
		PawnMaxStockValue,
		InventoryLooseDiamonds,
		InventoryWatchesLowValue,
		InventoryWatchesHighValue,
		InventoryHighValue,
		InventoryLowValue,
		InventoryScrap,
		InventoryNonJewelry,
		InventoryOther,
		HasLuxuryBrandWatches,
		HasWatchBlancpain,
		HasWatchBreitling,
		HasWatchCartier,
		HasWatchOmega,
		HasWatchPatekphilippe,
		HasWatchRolex,
		HasWatchOther,
		WatchOtherExplanation,
		IsOfficialRolexDealer,
		ProtectionClassCode,
		IsExclStkForSaleFromTheft,
		ExclStkForSaleFromTheftPremium,
		IsExclFromTheftExclBurglary,
		IsExcludeNonJwlryInv,
		IsExclSpecStkForSaleInv,
		ExclSpecStkForSaleInvPropNotIncl,
		IsJwlryPawnPledgedVal,
		JwlryPawnPledgedValMethod,
		JwlryPawnPledgedValOtherDesc,
		JwlryPawnPledgedValMethodMultiplier,
		IsJwlryPawnUnpledgedVal,
		JwlryPawnUnpledgedValMethod,
		JwlryPawnUnpledgedValOtherDesc,
		JwlryPawnUnpledgedValMethodMultiplier,
		IsUnrepInv,
		UnrepInvDescValue,
		IsStockLimitClsd,
		StockLimitClsdOpenStockLimit,
		StockLimitClsdClsdStockLimit,
		EQTerritory,
		EQZone,
		EQZoneDesc,
		FloodZone,
		FloodZoneDesc,
		FirmIndicator,
		FloodInceptionDate,
		OtherFloodInsurance,
		OtherFloodInsuranceCarrier,
		OtherFloodPolicyNumber,
		OtherFloodPrimaryNFIP,
		OtherFloodInformation,
		OtherFloodInsUndWaiverJMPrimary,
		bq_load_date

)

-- Main
WITH RiskStockIMConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'ILMLineCode','ILMLine_JMIC' UNION ALL
  SELECT 'BOPLineCode','BusinessOwnersLine' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
  SELECT 'ILMLocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'BOPLocationLevelRisk','BusinessOwnersLocation' UNION ALL
  SELECT 'StockLevelRisk','ILMStock' UNION ALL
  SELECT 'ILMHazardCategory','ILMLocHazardCodes_JMIC' UNION ALL
  SELECT 'ILMLocationCoverageQuestion','BOPHomeBasedBusinessQuestion_JMIC'
)

SELECT 
	ConfigSource.Value AS SourceSystem
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value,cRiskStock.StockPublicID,ConfigHashSep.Value, stockRisk.Value)) AS RiskStockKey
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value,cRiskStock.LocationPublicID,ConfigHashSep.Value,cRiskStock.RiskLevel)) AS RiskLocationKey
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value,cRiskStock.PolicyPeriodPublicID)) AS PolicyTransactionKey 
	,DENSE_RANK() OVER(PARTITION BY	cRiskStock.StockFixedID
									,cRiskStock.PolicyPeriodPublicID
					   ORDER BY		cRiskStock.IsTransactionSliceEffective DESC
									,cRiskStock.FixedStockRank) AS FixedStockRank
	
	,cRiskStock.StockPublicID
	,cRiskStock.StockFixedID
	,cRiskStock.LocationPublicID
	,cRiskStock.PolicyPeriodPublicID
	,cRiskStock.RiskLevel
	,cRiskStock.EffectiveDate
	,cRiskStock.ExpirationDate
	,cRiskStock.JobNumber
	,cRiskStock.PolicyNumber
	,cRiskStock.LocationNumber
	,cRiskStock.IsTransactionSliceEffective

-- T - Details
	-- Inventory
	,cRiskStock.IsCompleteInvAnnualOrMore					-- Complete inventory of at least once per year?
	,cRiskStock.LastInventoryTotal							-- Recent inventory total
	,cRiskStock.LastInventoryDate							-- Taken on
	,cRiskStock.InventoryType								-- Inventory Type

	-- Do you keep the following records?
	,cRiskStock.HasDetailedInvRecords						-- Detailed & Itemized inventory of your stock, incl quantity, description and value
	,cRiskStock.HasPurchaseInvoices							-- Purchase invoices, sales receipts and related documents	
	,cRiskStock.HasAwayListings								-- Detailed listing of property away from described premises
	,cRiskStock.PriorInventoryTotal							-- Previous inventory total
	,cRiskStock.PriorInventoryDate							-- Taken on
	,cRiskStock.MaxStockValue								-- Maximum stock value
	,cRiskStock.MaxDailyScrapValue							-- Highest Daily Value of scrap, precious alloys and metals on premises?
	,cRiskStock.IsCustPropertyRecorded						-- Maintain records of other people's property?
	,cRiskStock.CustomerProperyAverage						-- Avg. Amt. Customer's Property
	,cRiskStock.ConsignmentPropertyAverage					-- Avg. Amt. Memo/Consignment
	,cRiskStock.InventoryPremiumBase						-- Inventory Premium Base
	,cRiskStock.OutofSafeVltStkrmExposure					-- Out of Safe/Vault/Stockroom Exposure

	-- Pawned Property
	,cRiskStock.PawnPropertyHandled							-- Is pawn property handled?
	,cRiskStock.PawnCoverageIncluded						-- Include coverage for pawned property?
	,cRiskStock.PawnLastInventoryTotal						-- Last / Recent Pawn Total
	,cRiskStock.PawnLastInventoryDate						-- Taken on
	,cRiskStock.PawnPriorInventoryTotal						-- Previous Pawn Total
	,cRiskStock.PawnPriorInventoryDate						-- Taken on
	,cRiskStock.PawnMaxStockValue							-- Maximum Pawn Value
		
	-- Inventory Breakdown
	,cRiskStock.InventoryLooseDiamonds		
	,cRiskStock.InventoryWatchesLowValue		
	,cRiskStock.InventoryWatchesHighValue	
	,cRiskStock.InventoryHighValue			
	,cRiskStock.InventoryLowValue			
	,cRiskStock.InventoryScrap				
	,cRiskStock.InventoryNonJewelry			
	,cRiskStock.InventoryOther	

	-- Watch Information
	,cRiskStock.HasLuxuryBrandWatches						-- Do you carry luxury brand watches?
	,cRiskStock.HasWatchBlancpain							-- Watch Brand: Blancpain
	,cRiskStock.HasWatchBreitling							-- Watch Brand: Breitling
	,cRiskStock.HasWatchCartier								-- Watch Brand: Cartier
	,cRiskStock.HasWatchOmega								-- Watch Brand: Omega
	,cRiskStock.HasWatchPatekphilippe						-- Watch Brand: Patek Philippe
	,cRiskStock.HasWatchRolex								-- Watch Brand: Rolex
	,cRiskStock.HasWatchOther								-- Watch Brand: Other
	,cRiskStock.WatchOtherExplanation						-- Please explain what other watch brands are carried
	,cRiskStock.IsOfficialRolexDealer						-- Are they an official Rolex Dealer?

	-- Public Protection
	,cRiskStock.ProtectionClassCode

-- T - Exclusions & Conditions
	-- Exclusions
	,cRiskStock.IsExclStkForSaleFromTheft					-- Exclude Stock for Sale from Theft (Flag)														-- (747552)
	,cRiskStock.ExclStkForSaleFromTheftPremium				-- Exclude Stock for Sale from Theft - Premium				
	,cRiskStock.IsExclFromTheftExclBurglary					-- Exclude from Theft - Except Burglary-Robbery	(Flag)											-- (1791046)	
	,cRiskStock.IsExcludeNonJwlryInv						-- Exclude non jewelry inventory items (Flag)													-- (4791147)
	,cRiskStock.IsExclSpecStkForSaleInv						-- Excluding specified stock for sale and from inventory (Flag)									-- (5862050)
	,cRiskStock.ExclSpecStkForSaleInvPropNotIncl			-- Excluding specified stock for sale and from inventory - Property Not Included for Inventory	

	-- Policy Conditions
	,cRiskStock.IsJwlryPawnPledgedVal						-- Jewelry Pawn Pledged Valuation (Flag)														-- (4463528)				
	,cRiskStock.JwlryPawnPledgedValMethod					-- Jewelry Pawn Pledged Valuation - Valuation Method											-- (4323926)				
	,cRiskStock.JwlryPawnPledgedValOtherDesc				-- Jewelry Pawn Pledged Valuation - Valuation Method - Other Description						-- (4323926)								
	,cRiskStock.JwlryPawnPledgedValMethodMultiplier			-- Jewelry Pawn Pledged Valuation - Valuation Method Multiplier									-- (4518496)				
	,cRiskStock.IsJwlryPawnUnpledgedVal						-- Jewelry Pawn Unpledged Valuation (Flag)														-- (4463528)
	,cRiskStock.JwlryPawnUnpledgedValMethod					-- Jewelry Pawn Unpledged Valuation	- Valuation Method											-- (4323926)	
	,cRiskStock.JwlryPawnUnpledgedValOtherDesc				-- Jewelry Pawn Unpledged Valuation	- Valuation Method - Other Description						-- (5311457)
	,cRiskStock.JwlryPawnUnpledgedValMethodMultiplier		-- Jewelry Pawn Unpledged Valuation	- Valuation Method Multiplier								-- (4518496)		
	,cRiskStock.IsUnrepInv									-- Unreported Inventory Desc (Flag)																-- (222470)
	,cRiskStock.UnrepInvDescValue							-- Unreported Inventory Desc - Value
	,cRiskStock.IsStockLimitClsd							-- Stock Limitation-Closed to Business (Flag)													-- (494067)
	,cRiskStock.StockLimitClsdOpenStockLimit				-- Stock Limitation-Closed to Business - Open to Business Stock Limit					
	,cRiskStock.StockLimitClsdClsdStockLimit				-- Stock Limitation-Closed to Business - Closed to Business Stock Limit							
				
-- T - Earthquake & Flood Information	
	-- Earthquake Information
	,cRiskStock.EQTerritory									-- EQ Territory																					-- (7182366)
		
	--,cRiskStock.EarthquakeZone_JMIC
	,cRiskStock.EQZone
	,cRiskStock.EQZoneDesc

	-- Flood Information
	,cRiskStock.FloodZone									-- Flood Zone																					-- (7038204)
	,cRiskStock.FloodZoneDesc
	,cRiskStock.FirmIndicator								-- Pre-Firm / Post-Firm Indicator
	,cRiskStock.FloodInceptionDate							-- Flood Inception Date
	,cRiskStock.OtherFloodInsurance							-- Flood Inception Date
	,cRiskStock.OtherFloodInsuranceCarrier					-- Other Flood Insurance Carrier
	,cRiskStock.OtherFloodPolicyNumber						-- Other Flood Policy Number
	,cRiskStock.OtherFloodPrimaryNFIP						-- Other Flood Primary (NFIP)
	,cRiskStock.OtherFloodInformation						-- Other Flood Other Information
	,cRiskStock.OtherFloodInsUndWaiverJMPrimary				-- Other Flood Insurance Underlying Waiver (JM Primary)
	,bq_load_date 

FROM
(
	SELECT 
		DENSE_RANK() OVER(PARTITION BY	pcx_jewelrystock_jmic.ID
							ORDER BY	IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS StockRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jewelrystock_jmic.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC										
										,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS FixedStockRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jewelrystock_jmic.ID
							ORDER BY	IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pcx_ilmcost_jmic.ID DESC
							) AS CostRank
		
		,pcx_jewelrystock_jmic.PublicID									AS StockPublicID
		,pcx_jewelrystock_jmic.FixedID									AS StockFixedID
		,pcx_ilmlocation_jmic.PublicID									AS LocationPublicID
		,pc_policyperiod.PublicID										AS PolicyPeriodPublicID
		,ConfigILMLocationRisk.Value									AS RiskLevel
		,pcx_jewelrystock_jmic.EffectiveDate							AS EffectiveDate
		,pcx_jewelrystock_jmic.ExpirationDate							AS ExpirationDate
		,pc_job.JobNumber
		,pc_policyperiod.PolicyNumber
		,pc_policylocation.LocationNum									AS LocationNumber
		,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) THEN 1 ELSE 0 END AS IsTransactionSliceEffective

	-- T - Details
		-- Inventory
		,CAST(pcx_jewelrystock_jmic.IsCompleteInvAnnualOrMore AS INT64)	AS IsCompleteInvAnnualOrMore	
		,pcx_jewelrystock_jmic.LastInvTotal								AS LastInventoryTotal			
		,pcx_jewelrystock_jmic.LastInvDate								AS LastInventoryDate			
		,pctl_inventorytype_jm.NAME										AS InventoryType				

		-- Do you keep the following records?
		,CAST(pcx_jewelrystock_jmic.HasDetailedInvRecords AS INT64)		AS HasDetailedInvRecords		
		,CAST(pcx_jewelrystock_jmic.HasPurchaseInvoices AS INT64)		AS HasPurchaseInvoices			
		,CAST(pcx_jewelrystock_jmic.HasAwayListings AS INT64)			AS HasAwayListings				
		,pcx_jewelrystock_jmic.PriorInvTotal							AS PriorInventoryTotal			
		,pcx_jewelrystock_jmic.PriorInvDate								AS PriorInventoryDate			
		,pcx_jewelrystock_jmic.MaxStockValue							AS MaxStockValue				
		,pcx_jewelrystock_jmic.MaxDailyScrapValue						AS MaxDailyScrapValue			
		,CAST(pcx_jewelrystock_jmic.IsCustPropertyRecorded AS INT64)	AS IsCustPropertyRecorded		
		,pcx_jewelrystock_jmic.CustomerPropertyAvg						AS CustomerProperyAverage		
		,pcx_jewelrystock_jmic.ConsignmentPropertyAvg					AS ConsignmentPropertyAverage	
		,pcx_jewelrystock_jmic.InvPremiumBase							AS InventoryPremiumBase			
		,pcx_jewelrystock_jmic.OOSVSExposure							AS OutofSafeVltStkrmExposure				

		-- Pawned Property
		,CAST(pcx_jewelrystock_jmic.IsPawnPropertyHandled AS INT64)		AS PawnPropertyHandled			
		,CAST(pcx_jewelrystock_jmic.IsPawnPropCvgIncluded AS INT64)		AS PawnCoverageIncluded			
		,pcx_jewelrystock_jmic.LastPawnInvTotal							AS PawnLastInventoryTotal		
		,pcx_jewelrystock_jmic.LastPawnInvDate							AS PawnLastInventoryDate		
		,pcx_jewelrystock_jmic.PriorPawnInvTotal						AS PawnPriorInventoryTotal		
		,pcx_jewelrystock_jmic.PriorPawnInvDate							AS PawnPriorInventoryDate		
		,pcx_jewelrystock_jmic.MaxPawnStockValue						AS PawnMaxStockValue
		
		-- Inventory Breakdown
		,cInvBreak.loosediamonds										AS InventoryLooseDiamonds		
		,cInvBreak.watcheslow											AS InventoryWatchesLowValue		
		,cInvBreak.watcheshigh										AS InventoryWatchesHighValue	
		,cInvBreak.valuedhigh											AS InventoryHighValue			
		,cInvBreak.valuedlow											AS InventoryLowValue			
		,cInvBreak.scrap												AS InventoryScrap				
		,cInvBreak.nonjewelry											AS InventoryNonJewelry			
		,cInvBreak.allother											AS InventoryOther			

		-- Watch Information
		,CAST(pcx_jewelrystock_jmic.HasLuxuryWatches AS INT64)			AS HasLuxuryBrandWatches		
		,cInvWatch.blancpain											AS HasWatchBlancpain			
		,cInvWatch.breitling											AS HasWatchBreitling			
		,cInvWatch.cartier												AS HasWatchCartier				
		,cInvWatch.omega												AS HasWatchOmega				
		,cInvWatch.patekphilippe										AS HasWatchPatekphilippe		
		,cInvWatch.rolex												AS HasWatchRolex				
		,cInvWatch.other												AS HasWatchOther				
		,pcx_jewelrystock_jmic.OtherBrandExplanation					AS WatchOtherExplanation		
		,CAST(pcx_jewelrystock_jmic.IsOfficialRolexDealer AS INT64)		AS IsOfficialRolexDealer		

		-- Public Protection
		,pctl_fireprotectclass.TYPECODE									AS ProtectionClassCode

	-- T - Exclusions & Conditions
		-- Exclusions
		,CAST(RiskStockIMExclCond.IsExclStkForSaleFromTheft AS INT64)	AS IsExclStkForSaleFromTheft		
		,RiskStockIMExclCond.ExclStkForSaleFromTheftPremium				AS ExclStkForSaleFromTheftPremium	
		,CAST(RiskStockIMExclCond.IsExclFromTheftExclBurglary AS INT64)	AS IsExclFromTheftExclBurglary	
		,CAST(RiskStockIMExclCond.IsExcludeNonJwlryInv	 AS INT64)		AS IsExcludeNonJwlryInv
		,CAST(RiskStockIMExclCond.IsExclSpecStkForSaleInv AS INT64)		AS IsExclSpecStkForSaleInv	
		,RiskStockIMExclCond.ExclSpecStkForSaleInvPropNotIncl			AS ExclSpecStkForSaleInvPropNotIncl									

		-- Policy Conditions
		,CAST(RiskStockIMExclCond.IsJwlryPawnPledgedVal AS INT64)		AS IsJwlryPawnPledgedVal
		,RiskStockIMExclCond.JwlryPawnPledgedValMethod					AS JwlryPawnPledgedValMethod	
		,RiskStockIMExclCond.JwlryPawnPledgedValOtherDesc				AS JwlryPawnPledgedValOtherDesc			
		,RiskStockIMExclCond.JwlryPawnPledgedValMethodMultiplier		AS JwlryPawnPledgedValMethodMultiplier	
		,CAST(RiskStockIMExclCond.IsJwlryPawnUnpledgedVal AS INT64)		AS IsJwlryPawnUnpledgedVal			
		,RiskStockIMExclCond.JwlryPawnUnpledgedValMethod				AS JwlryPawnUnpledgedValMethod		
		,RiskStockIMExclCond.JwlryPawnUnpledgedValOtherDesc				AS JwlryPawnUnpledgedValOtherDesc	
		,RiskStockIMExclCond.JwlryPawnUnpledgedValMethodMultiplier		AS JwlryPawnUnpledgedValMethodMultiplier	
		,CAST(RiskStockIMExclCond.IsUnrepInv AS INT64)					AS IsUnrepInv			
		,RiskStockIMExclCond.UnrepInvDescValue							AS UnrepInvDescValue	
		,CAST(RiskStockIMExclCond.IsStockLimitClsd AS INT64)			AS IsStockLimitClsd
		,RiskStockIMExclCond.StockLimitClsdOpenStockLimit				AS StockLimitClsdOpenStockLimit			
		,RiskStockIMExclCond.StockLimitClsdClsdStockLimit				AS StockLimitClsdClsdStockLimit											
				
	-- T - Earthquake & Flood Information	
		-- Earthquake Information 
		,pcx_commonbuilding_jmic.EQTerritoryCode_JMIC					AS EQTerritory
		
		--,pcx_commonbuilding_jmic.EarthquakeZone_JMIC
		,pctl_earthquakezone_jmic.NAME									AS EQZone
		,pctl_earthquakezone_jmic.DESCRIPTION							AS EQZoneDesc

		-- Flood Information
		,pctl_floodzone_jmic.NAME											AS FloodZone					
		,pctl_floodzone_jmic.DESCRIPTION									AS FloodZoneDesc
		,pctl_firmindicator_jmic.NAME										AS FirmIndicator				
		,pcx_floodinformation_JMIC.FloodInceptionDate_JMIC					AS FloodInceptionDate
		,CAST(pcx_floodinformation_JMIC.OtherFloodInsurance_JMIC AS INT64)	AS OtherFloodInsurance
		,pcx_floodinformation_JMIC.OFInsuranceCarrier_JMIC					AS OtherFloodInsuranceCarrier			
		,pcx_floodinformation_JMIC.OthFloPolicyNumber_JMIC					AS OtherFloodPolicyNumber			
		,CAST(pcx_floodinformation_JMIC.OthFloPrimaryNFIP_JMIC AS INT64)	AS OtherFloodPrimaryNFIP			
		,pcx_floodinformation_JMIC.OFOtherInformation_JMIC					AS OtherFloodInformation			
		,CAST(pcx_floodinformation_JMIC.OFIUndWaiJMPrimary_JMIC AS INT64)	AS OtherFloodInsUndWaiverJMPrimary		
		,DATE('{date}') as bq_load_date

		--pcx_eqterritorycodes_jmic

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME= {partition_date}) AS pc_policyperiod

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME= {partition_date}) AS pcx_jewelrystock_jmic
		ON  pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME= {partition_date}) AS pc_policyline
		ON  pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctlPolLine
		ON pctlPolLine.ID = pc_policyline.SubType

		INNER JOIN RiskStockIMConfig AS ConfigILMLine
		ON  ConfigILMLine.Value = pctlPolLine.TYPECODE
		AND ConfigILMLine.Key = 'ILMLineCode' 

		INNER JOIN RiskStockIMConfig AS ConfigILMLocationRisk
		ON ConfigILMLocationRisk.Key='ILMLocationLevelRisk'

		INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME= {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME= {partition_date}) AS pcx_ilmlocation_jmic
		ON  pcx_ilmlocation_jmic.BranchID = pcx_jewelrystock_jmic.BranchID
		AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
		-- AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) -- 05/02/2023
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME= {partition_date}) AS pcx_ilmcost_jmic
		ON pcx_ilmcost_jmic.BranchID = pcx_jewelrystock_jmic.BranchID
		AND pcx_ilmcost_jmic.ILMLocation_JMIC = pcx_jewelrystock_jmic.ILMLocation
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
				
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME= {partition_date}) AS pc_policylocation
		ON  pc_policylocation.BranchID = pcx_jewelrystock_jmic.BranchID
		AND pc_policylocation.FixedID = pcx_jewelrystock_jmic.Location
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state
		ON pctl_state.ID = pc_policylocation.StateInternal

		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country
		ON pctl_country.ID = pc_policylocation.CountryInternal

		LEFT JOIN `{project}.{pc_dataset}.pctl_inventorytype_jm` AS pctl_inventorytype_jm
		ON pctl_inventorytype_jm.ID = pcx_jewelrystock_jmic.InventoryType

		LEFT JOIN `{project}.{pc_dataset}.pctl_fireprotectclass` AS pctl_fireprotectclass
		ON pctl_fireprotectclass.ID = pc_policylocation.FireProtectClass
		
		-- Inventory Breakdown
		LEFT JOIN `{project}.{dest_dataset}.t_RiskStockIMInventoryBreakdownPivot` cInvBreak
		ON  cInvBreak.StockPublicID = pcx_jewelrystock_jmic.PublicID

		-- Watch Information
		LEFT JOIN `{project}.{dest_dataset}.t_RiskStockIMWatchPivot` cInvWatch
		ON  cInvWatch.StockPublicID = pcx_jewelrystock_jmic.PublicID

		-- T - Earthquake & Flood Information
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_commonbuilding_jmic` WHERE _PARTITIONTIME= {partition_date}) AS pcx_commonbuilding_jmic
		ON  pcx_commonbuilding_jmic.BranchID = pcx_ilmlocation_jmic.BranchID
		AND pcx_commonbuilding_jmic.FixedID = pcx_ilmlocation_jmic.CommonBuilding
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_commonbuilding_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_commonbuilding_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_earthquakezone_jmic` AS pctl_earthquakezone_jmic
		ON pctl_earthquakezone_jmic.ID = pcx_commonbuilding_jmic.EarthquakeZone_JMIC

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_floodinformation_JMIC` WHERE _PARTITIONTIME= {partition_date}) AS pcx_floodinformation_JMIC
		ON  pcx_floodinformation_JMIC.BranchID = pcx_commonbuilding_jmic.BranchID
		AND pcx_floodinformation_JMIC.FixedID = pcx_commonbuilding_jmic.FloodInformation_JMIC
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_floodinformation_JMIC.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_floodinformation_JMIC.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_floodzone_jmic` AS pctl_floodzone_jmic
		ON pctl_floodzone_jmic.ID = pcx_floodinformation_JMIC.FloodZone_JMIC

		LEFT JOIN `{project}.{pc_dataset}.pctl_firmindicator_jmic` AS pctl_firmindicator_jmic
		ON pctl_firmindicator_jmic.ID = pcx_floodinformation_JMIC.FirmIndicator_JMIC

		-- T - Exclusions & Conditions
		LEFT JOIN RiskStockIMExclCond AS RiskStockIMExclCond
		ON RiskStockIMExclCond.StockPublicID = pcx_jewelrystock_jmic.PublicID

	WHERE	1 = 1
			--AND pctl_policyperiodstatus.NAME = 'Bound'
			--AND pc_policyperiod.PolicyNumber = '55-001338'
			--AND pcx_jewelrystock_jmic.PublicID  IN ('pc:374322', 'pc:374324', 'pc:463728', 'pc:513832', 'pc:513831', 'pc:557530', 'pc:557529')
)cRiskStock

	INNER JOIN RiskStockIMConfig AS ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN RiskStockIMConfig AS ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN RiskStockIMConfig AS stockRisk
	ON stockRisk.Key = 'StockLevelRisk'

	INNER JOIN RiskStockIMConfig AS ConfigHashAlgo
	ON ConfigHashAlgo.Key='HashingAlgorithm'

WHERE	1 = 1
		AND cRiskStock.StockRank = 1
		AND cRiskStock.CostRank = 1
;
--) outerquery;
DROP TABLE RiskStockIMExclCond;
DROP TABLE RiskStockIMCombinations;

