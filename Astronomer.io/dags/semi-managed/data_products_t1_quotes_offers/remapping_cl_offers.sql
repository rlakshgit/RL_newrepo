
USE DW_DDS_CURRENT;

DROP TABLE IF EXISTS #QuotesDetails

;WITH CTE_Rel AS (
SELECT
	ContactKey
	,RelationshipIDNumber
	,JBTRating
FROM (
	SELECT
		ContactKey
		,RelationshipIDNumber
		,JBTRating
		,ROW_NUMBER() OVER(PARTITION BY ContactKey
							   ORDER BY CreateTimeStamp DESC
										,InsertAuditKey DESC
										,RelationshipIDNumber DESC
		) AS RowNumber
	FROM bi_dds.DimContactRelationship
	) AS Rel
WHERE RowNumber = 1
)

-- First pass to capture details
,source_details AS (
SELECT
	D_policy.PolicyKey															AS POLICY_KEY
	,D_policy.PolicyNumber														AS POLICY_NUMBER
	,D_policy.JobNumber															AS SUBMISSION_NUMBER	
	,D_account.AccountNumber													AS ACCOUNT_NUMBER																					
	,D_agency.AgencyMasterCode													AS AGENCY_MASTER_CODE
	,D_agency.AgencyMasterName													AS AGENCY_MASTER_CODE_NAME
	,D_agency.AgencyCode														AS AGENCY_CODE
	,D_agency.AgencyName														AS AGENCY_NAME
	,D_agency.AgencySubCode														AS AGENCY_SUB_CODE
	,D_policy.PolicyInsuredContactFullName										AS QUOTE_NAME
	,F_quote.QuoteCreateDateKey													AS QUOTE_CREATE_DATE
	,F_quote.QuoteIssuedDateKey													AS QUOTE_ISSUED_DATE
	,F_quote.QuoteExpirationDate												AS QUOTE_EXPIRATION_DATE
	,D_policy.PolicyEffectiveDate												AS POL_EFF_DATE
	,D_policy.PolicyExpirationDate												AS POL_EXP_DATE
	,D_policy.PolicyGroup														AS POL_GROUP
	,D_policy.PolicyVersion														AS POL_VERSION
	,DENSE_RANK() OVER(PARTITION BY D_account.AccountNumber
						   ORDER BY	D_policy.JOBCLOSEDATE DESC)					AS QUOTE_RANK
	,D_policy.AccountSegment													AS ACCT_SEGMENT
	,D_policy.PolicyType														AS POLICY_TYPE
	,D_primary_geo.GeographyStateCode											AS PRIMARY_STATE
	,D_policy.PolicyStatus														AS POLICY_STATUS
	,F_quote.QuoteStatus														AS QUOTE_STATUS
	,D_policy.TotalPremiumRPT													AS TOTAL_PREMIUM
	,COALESCE(Rel_Overlay.RelationshipIDNumber,Rel_Policy.RelationshipIDNumber)	AS JBTNumber
	,COALESCE(Rel_Overlay.JBTRating,Rel_Policy.JBTRating)						AS JBTRating
	,CASE 
		WHEN D_core.RiskType = 'LOB' THEN D_risk_lob.PrimaryRatingLocationNumber
		ELSE D_risk_loc.LocationNumber
		END																		AS LOCATION_NUMBER
	,CASE 
		WHEN D_core.RiskType = 'LOB' THEN D_risk_lob.PrimaryRatingBuildingNumber
		WHEN D_core.RiskType = 'BLD' THEN D_risk_bld.BuildingNumber
		ELSE D_risk_loc.LocationBuildingNumber
		END																		AS BUILDING_NUMBER
	,CASE 
		WHEN D_core.RiskType = 'LOB' THEN D_primary_geo.GeographyStateCode 
		ELSE D_loc_geo.GeographyStateCode
		END																		AS LOCATION_STATE
	,CASE 
		WHEN D_LOB.LOBCode = 'JS' THEN 1 ELSE 0 
		END																		AS HAS_JS
	,CASE 
		WHEN D_LOB.LOBCode = 'JB' THEN 1 ELSE 0
		END																		AS HAS_JB
	,CASE 
		WHEN D_LOB.LOBCode = 'BOP' THEN 1 ELSE 0
		END																		AS HAS_BOP
	,CASE 
		WHEN D_LOB.LOBCode = 'UMB' THEN 1 ELSE 0
		END																		AS HAS_UMB
	,CASE 
		WHEN D_risk_loc.LocationSegmentationCode LIKE '%Rol%' THEN 1 ELSE 0 
		END																		AS ROLEX_LOCATION
	,CASE 
		WHEN D_risk_loc.LocationSegmentationCode LIKE '%Pan%' THEN 1 ELSE 0
		END																		AS PANDORA_LOCATION
	,D_risk_loc.LocationTypeDesc														AS LOCATION_TYPE
	,D_risk_stock.StockPremiumBaseAOP													AS INVENTORY_PREMIUM_BASE
	,D_risk_loc.LocationInlandMarineHazardCode											AS HAZARD_CODE
	,D_risk_loc.LocationBuildingConstructionCode										AS CONST_CODE
	,D_risk_loc.LocationInlandMarineExperienceTierFactor								AS EXPERIENCE_TIER_FACTOR
	,D_risk_bld.BuildingYearOfConstruction												AS YR_CONSTRUCTION
	,D_risk_stock.StockIsPawnPropertyCoverageIncluded									AS JB_JS_PAWN_COVERED_INDICATOR
	,D_cov_type.ConformedCoverageTypeCode										AS COV_TYPE
	,D_cov.PerOccurenceLimit													AS LIMIT
	,D_cov.PerOccurenceDeductible												AS DEDUCTIBLE
	,DENSE_RANK() OVER(PARTITION BY D_policy.Policykey
									,D_policy.PolicyEffectiveDate
									,D_policy.PolicyGroup
									,D_policy.PolicyVersion
									,CASE 
										WHEN D_core.RiskType = 'LOB' THEN D_risk_lob.PrimaryRatingLocationNumber 
										ELSE D_risk_loc.LocationNumber
										END
									,CASE 
										WHEN D_core.RiskType = 'LOB' THEN D_risk_lob.PrimaryRatingBuildingNumber 
										WHEN D_core.RiskType = 'BLD' THEN D_risk_bld.BuildingNumber 
										ELSE D_risk_loc.LocationBuildingNumber
										END	
									,D_cov_type.ConformedCoverageTypeCode
						   ORDER BY	D_core.RiskSegmentKey DESC
									,COALESCE(D_cov.CoverageKey,999999999) DESC
						)														AS COV_RANK
	,D_risk_stock.StockInventoryLooseDiamonds											AS StockInventoryLooseDiamonds				
	,D_risk_stock.StockInventoryLowValue													AS StockInventoryLowValue
	,D_risk_stock.StockInventoryHighValue												AS StockInventoryHighValue
	,D_risk_stock.StockInventoryLowValueWatches											AS StockInventoryLowValueWatches
	,D_risk_stock.StockInventoryHighValueWatches											AS StockInventoryHighValueWatches
	,D_risk_stock.StockInventoryScrapPreciousMetals										AS StockInventoryScrapPreciousMetals
	,D_risk_stock.StockInventoryNonJewelryPreciousMetals									AS StockInventoryNonJewelryPreciousMetals
	,D_risk_stock.StockInventoryOther													AS StockInventoryOther
	,D_risk_stock.StockHasLuxuryBrandWatches												AS StockHasLuxuryBrandWatches
	,D_risk_stock.StockHasWatchBlancpain													AS StockHasWatchBlancpain
	,D_risk_stock.StockHasWatchBreitling													AS StockHasWatchBreitling
	,D_risk_stock.StockHasWatchCartier													AS StockHasWatchCartier
	,D_risk_stock.StockHasWatchOmega														AS StockHasWatchOmega
	,D_risk_stock.StockHasWatchPatekPhilippe												AS StockHasWatchPatekPhilippe
	,D_risk_stock.StockHasWatchRolex														AS StockHasWatchRolex
	,D_risk_stock.StockHasWatchOther														AS StockHasWatchOther
	,D_risk_stock.IsStockDealerAnOfficialRolexDealer										AS IsStockDealerAnOfficialRolexDealer
	,CASE
		WHEN D_risk_bld.BuildingBOPInsuredsBusinessClassCode = '59982'
		THEN D_risk_bld.BuildingBOPInsuredsBusinessClassCode ELSE 0
		END																		AS PAWN_CLASS

FROM bi_dds.FactQuote AS F_quote
	
	INNER JOIN bi_dds.DimPolicy AS D_policy
		ON	F_quote.PolicyKey = D_policy.PolicyKey
/*
	INNER JOIN bi_DDS.vwDimRiskSegmentAll (NOEXPAND) AS vRS
		ON vRS.PolicyKey = D_policy.PolicyKey
		AND vRS.IsInactive = 0
		AND vRS.RiskSegmentKey <> -1
*/
	LEFT JOIN bi_dds.DimRiskSegmentCore AS D_core
		ON	D_core.PolicyKey = D_policy.PolicyKey

	LEFT JOIN bi_dds.DimRiskSegmentAlarm AS D_risk_alarm
		ON	D_risk_alarm.RiskSegmentKey = D_core.RiskSegmentKey
	
	LEFT JOIN bi_dds.DimRiskSegmentBuilding AS D_risk_bld
		ON	D_risk_bld.RiskSegmentKey = D_core.RiskSegmentBuildingKey
			
	LEFT JOIN bi_dds.DimRiskSegmentLOB AS D_risk_lob
		ON	D_risk_lob.RiskSegmentKey = D_core.RiskSegmentLOBKey
	
	LEFT JOIN bi_dds.DimRiskSegmentLocation AS D_risk_loc
		ON	D_risk_loc.RiskSegmentKey	= D_core.RiskSegmentLocationKey

	LEFT JOIN bi_dds.DimRiskSegmentSafe AS D_risk_safe
		ON	D_risk_safe.RiskSegmentKey	= D_core.RiskSegmentKey

	LEFT JOIN bi_dds.DimRiskSegmentStock AS D_risk_stock
		ON	D_risk_stock.RiskSegmentKey	= D_core.RiskSegmentStockKey

	INNER JOIN bi_dds.DimLineOfBusiness AS D_LOB
		ON D_LOB.LineOfBusinessKey = D_core.LineOfBusinessKey

	INNER JOIN bi_dds.DimCoverage AS D_cov
		ON	D_cov.RiskSegmentKey = D_core.RiskSegmentKey

	INNER JOIN bi_dds.DimCoverageType AS D_cov_type
		ON D_cov_type.CoverageTypeKey = D_cov.CoverageTypeKey

	INNER JOIN bi_dds.DimGeography AS D_loc_geo
		ON D_loc_geo.GeographyKey = D_risk_loc.LocationGeographyKey--vRS.LocationGeographyKey

	INNER JOIN bi_dds.DimGeography AS D_primary_geo
		ON D_primary_geo.GeographyKey = D_risk_lob.PrimaryRatingGeographyKey--vRS.PrimaryRatingGeographyKey

	INNER JOIN bi_dds.DimContact AS D_pol_ins_contact
		ON D_policy.PolicyInsuredContactKey = D_pol_ins_contact.ContactKey 

	INNER JOIN bi_dds.DimAgency AS D_agency
		ON D_policy.AgencyKey = D_agency.AgencyKey

	INNER JOIN bi_dds.DimAccount AS D_account
		ON D_policy.AccountKey = D_account.AccountKey

	------------ JOIN through the Overlay to get proper contact relationship
	LEFT OUTER JOIN bi_dds.DimContactOverlay
		ON DimContactOverlay.OriginalSK = D_pol_ins_contact.ContactKey
	LEFT OUTER JOIN CTE_Rel Rel_Overlay 
		ON DimContactOverlay.ReplacedBySK = Rel_Overlay.ContactKey 
	LEFT OUTER JOIN CTE_Rel Rel_Policy
		ON D_pol_ins_contact.ContactKey = Rel_Policy.ContactKey 

WHERE
	D_cov.PerOccurenceLimit IS NOT NULL --007041 1 1
	AND D_policy.AccountSegment <> 'Personal Lines'
	AND D_policy.PolicyTransactionTypeKey=11 ---11 equal Submission
	AND F_quote.QuoteCreateDateKey >= 20150101
	
GROUP BY
	D_policy.PolicyKey
	,D_policy.PolicyNumber
	,D_policy.JobNumber
	,D_account.AccountNumber
	,D_agency.AgencyMasterCode
	,D_agency.AgencyMasterName
	,D_agency.AgencyCode
	,D_agency.AgencyName
	,D_agency.AgencySubCode
	,D_policy.PolicyInsuredContactFullName
	,F_quote.QuoteCreateDateKey
	,F_quote.QuoteIssuedDateKey
	,F_quote.QuoteExpirationDate
	,D_policy.PolicyEffectiveDate
	,D_policy.PolicyExpirationDate
	,D_policy.PolicyGroup
	,D_policy.PolicyVersion
	,D_policy.JOBCLOSEDATE
	,D_policy.AccountSegment
	,D_policy.PolicyType
	,D_primary_geo.GeographyStateCode
	,D_policy.PolicyStatus
	,F_quote.QuoteStatus
	,D_policy.TotalPremiumRPT
	,COALESCE(Rel_Overlay.RelationshipIDNumber,Rel_Policy.RelationshipIDNumber)
	,COALESCE(Rel_Overlay.JBTRating,Rel_Policy.JBTRating)
	,D_core.RiskType
	,D_risk_lob.PrimaryRatingLocationNumber
	,D_risk_loc.LocationNumber
	,D_risk_lob.PrimaryRatingBuildingNumber
	,D_risk_bld.BuildingNumber
	,D_risk_loc.LocationBuildingNumber
	,D_primary_geo.GeographyStateCode
	,D_loc_geo.GeographyStateCode
	,D_LOB.LOBCode
	,D_risk_loc.LocationSegmentationCode
	,D_risk_loc.LocationTypeDesc
	,D_risk_stock.StockPremiumBaseAOP
	,D_risk_loc.LocationInlandMarineHazardCode
	,D_risk_loc.LocationBuildingConstructionCode
	,D_risk_loc.LocationInlandMarineExperienceTierFactor
	,D_risk_bld.BuildingYearOfConstruction
	,D_risk_stock.StockIsPawnPropertyCoverageIncluded
	,D_cov_type.ConformedCoverageTypeCode
	,D_cov.PerOccurenceLimit
	,D_cov.PerOccurenceDeductible
	,D_core.RiskSegmentKey
	,D_cov.CoverageKey
	,D_risk_stock.StockInventoryLooseDiamonds
	,D_risk_stock.StockInventoryLowValue
	,D_risk_stock.StockInventoryHighValue
	,D_risk_stock.StockInventoryLowValueWatches
	,D_risk_stock.StockInventoryHighValueWatches
	,D_risk_stock.StockInventoryScrapPreciousMetals
	,D_risk_stock.StockInventoryNonJewelryPreciousMetals
	,D_risk_stock.StockInventoryOther
	,D_risk_stock.StockHasLuxuryBrandWatches
	,D_risk_stock.StockHasWatchBlancpain
	,D_risk_stock.StockHasWatchBreitling
	,D_risk_stock.StockHasWatchCartier
	,D_risk_stock.StockHasWatchOmega
	,D_risk_stock.StockHasWatchPatekPhilippe
	,D_risk_stock.StockHasWatchRolex
	,D_risk_stock.StockHasWatchOther
	,D_risk_stock.IsStockDealerAnOfficialRolexDealer
	,D_risk_bld.BuildingBOPInsuredsBusinessClassCode
)

,grouped_details AS (
SELECT
	POLICY_KEY
	,POLICY_NUMBER
	,ACCOUNT_NUMBER
	,QUOTE_RANK
	,SUBMISSION_NUMBER
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME
	,JBTNumber
	,JBTRating
	,POL_EFF_DATE
	,POL_EXP_DATE
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,POL_GROUP
	,POL_VERSION
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,POLICY_STATUS
	,QUOTE_STATUS
	,LOCATION_NUMBER
	,MAX(EXPERIENCE_TIER_FACTOR)												AS EXPERIENCE_TIER_FACTOR
	,LOCATION_STATE
	,MAX(TOTAL_PREMIUM)															AS TOTAL_PREMIUM
	,MAX(HAS_JS)																AS HAS_JS
	,MAX(HAS_JB)																AS HAS_JB
	,MAX(HAS_BOP)																AS HAS_BOP
	,MAX(HAS_UMB)																AS HAS_UMB
	,MAX(ROLEX_LOCATION)														AS ROLEX_LOCATION
	,MAX(PANDORA_LOCATION)														AS PANDORA_LOCATION
	,MAX(LOCATION_TYPE)															AS LOCATIONTYPE
	,MAX(INVENTORY_PREMIUM_BASE)												AS INVENTORY_PREMIUM_BASE
	,MAX(HAZARD_CODE)															AS HAZARD_CODE
	,MAX(CONST_CODE)															AS CONST_CODE
	,MAX(YR_CONSTRUCTION)														AS YR_CONSTRUCTION
	,SUM(CASE
			WHEN COV_TYPE = '401A'
			THEN LIMIT ELSE 0 END)												AS STOCK_LIMIT
	,MAX(CASE
			WHEN COV_TYPE = '401A'
			THEN DEDUCTIBLE ELSE 0 END)											AS STOCK_DEDUCTIBLE
	,SUM(CASE
			WHEN COV_TYPE LIKE '871%'
			THEN LIMIT ELSE 0 END)												AS BPP_LIMIT
	,MAX(CASE
			WHEN COV_TYPE LIKE '871%'
			THEN DEDUCTIBLE ELSE 0 END)											AS BPP_DEDUCTIBLE
	,SUM(CASE
			WHEN COV_TYPE ='850A'
			THEN LIMIT ELSE 0 END)												AS BLDG_LIMIT
	,MAX(CASE
			WHEN COV_TYPE = '850A'
			THEN DEDUCTIBLE ELSE 0 END)											AS BLDG_DEDUCTIBLE
	,MAX(CASE
			WHEN COV_TYPE = '463A'
			THEN LIMIT ELSE 0 END)												AS TRAVEL_LIMIT
	,MAX(CAST(JB_JS_PAWN_COVERED_INDICATOR AS int))								AS JB_JS_PAWN_COVERED_INDICATOR
	,SUM(CASE
			WHEN COV_TYPE IN ('871E','871F','871G','871I','871J','871K','871L')
			THEN LIMIT ELSE 0 END)												AS BPP_PAWN_LIMIT
	,MAX(PAWN_CLASS)															AS PAWN_CLASS	
	,MAX(CASE
			WHEN COV_TYPE = '407A'
			THEN LIMIT ELSE 0 END)												AS OUT_OF_SAFE_LIMIT
	,MAX(StockInventoryLooseDiamonds)											AS StockInventoryLooseDiamonds						
	,MAX(StockInventoryLowValue)												AS StockInventoryLowValue
	,MAX(StockInventoryHighValue)												AS StockInventoryHighValue
	,MAX(StockInventoryLowValueWatches)											AS StockInventoryLowValueWatches
	,MAX(StockInventoryHighValueWatches)										AS StockInventoryHighValueWatches
	,MAX(StockInventoryScrapPreciousMetals)										AS StockInventoryScrapPreciousMetals
	,MAX(StockInventoryNonJewelryPreciousMetals)								AS StockInventoryNonJewelryPreciousMetals
	,MAX(StockInventoryOther)													AS StockInventoryOther
	,MAX(ISNULL(CAST(StockHasLuxuryBrandWatches AS int),0))						AS StockHasLuxuryBrandWatches
	,MAX(ISNULL(CAST(StockHasWatchBlancpain AS int),0))							AS StockHasWatchBlancpain
	,MAX(ISNULL(CAST(StockHasWatchBreitling AS int),0))							AS StockHasWatchBreitling
	,MAX(ISNULL(CAST(StockHasWatchCartier AS int),0))							AS StockHasWatchCartier
	,MAX(ISNULL(CAST(StockHasWatchOmega AS int),0))								AS StockHasWatchOmega
	,MAX(ISNULL(CAST(StockHasWatchPatekPhilippe AS int),0))						AS StockHasWatchPatekPhilippe
	,MAX(ISNULL(CAST(StockHasWatchRolex AS int),0))								AS StockHasWatchRolex
	,MAX(ISNULL(CAST(StockHasWatchOther AS int),0))								AS StockHasWatchOther
	,MAX(ISNULL(CAST(IsStockDealerAnOfficialRolexDealer AS int), 0))			AS IsStockDealerAnOfficialRolexDealer

FROM source_details

WHERE 
	COV_RANK = 1

GROUP BY
	POLICY_KEY
	,POLICY_NUMBER
	,ACCOUNT_NUMBER
	,SUBMISSION_NUMBER
	,QUOTE_RANK
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME
	,JBTNumber
	,JBTRating
	,POL_EFF_DATE
	,POL_EXP_DATE
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,POL_GROUP
	,POL_VERSION
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,POLICY_STATUS
	,QUOTE_STATUS
	,LOCATION_NUMBER
	,LOCATION_STATE
)

,details_subset AS (
SELECT
	grouped_details.POLICY_KEY
	,POLICY_NUMBER
	,grouped_details.ACCOUNT_NUMBER
	,grouped_details.SUBMISSION_NUMBER
	,QUOTE_RANK
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME
	,JBTNumber
	,JBTRating
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,POL_EFF_DATE
	,POL_EXP_DATE
	,POL_GROUP
	,POL_VERSION
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,grouped_details.POLICY_STATUS
	,QUOTE_STATUS
	,LOCATION_NUMBER
	,LOCATION_STATE
	,EXPERIENCE_TIER_FACTOR
	,grouped_details.TOTAL_PREMIUM
	,HAS_JS
	,HAS_JB
	,HAS_BOP
	,HAS_UMB
	,ROLEX_LOCATION
	,PANDORA_LOCATION
	,LOCATIONTYPE
	,INVENTORY_PREMIUM_BASE
	,HAZARD_CODE
	,CONST_CODE
	,YR_CONSTRUCTION
	,STOCK_LIMIT
	,STOCK_DEDUCTIBLE
	,BPP_LIMIT
	,BPP_DEDUCTIBLE
	,BLDG_LIMIT
	,BLDG_DEDUCTIBLE
	,TRAVEL_LIMIT
	,JB_JS_PAWN_COVERED_INDICATOR
	,BPP_PAWN_LIMIT
	,OUT_OF_SAFE_LIMIT
	,StockInventoryLooseDiamonds				
	,StockInventoryLowValue
	,StockInventoryHighValue
	,StockInventoryLowValueWatches
	,StockInventoryHighValueWatches
	,StockInventoryScrapPreciousMetals
	,StockInventoryNonJewelryPreciousMetals
	,StockInventoryOther
	,StockHasLuxuryBrandWatches
	,StockHasWatchBlancpain
	,StockHasWatchBreitling
	,StockHasWatchCartier
	,StockHasWatchOmega
	,StockHasWatchPatekPhilippe
	,StockHasWatchRolex
	,StockHasWatchOther
	,IsStockDealerAnOfficialRolexDealer
	,PAWN_CLASS

FROM grouped_details
)

,IRPM AS (
SELECT
	POLICYKEY
	,LOC_ITEM
	,(CASE
		WHEN LOBCode='BOP' and RateModificationType IN ('IRPM','Judgment')
		THEN RateModificationFactor ELSE 0 END)									AS BOPIRPM
	,(CASE
		WHEN LOBCode='JS' and RateModificationType IN ('IRPM','Judgment')
		THEN RateModificationFactor ELSE 0 END)									AS JSIRPM
	,(CASE
		WHEN LOBCode='JB' and RateModificationType IN ('IRPM','Judgment')
		THEN RateModificationFactor ELSE 0 END)									AS JBIRPM

FROM bi_dds.vwFactRateModificationFactorSummary

WHERE RowIsCurrent = 1
)

,IRPMSummary AS (
SELECT
	POLICYKEY
	,LOC_ITEM
	,CASE WHEN SUM(BOPIRPM) = 0 THEN 1 ELSE SUM(BOPIRPM) END					AS BOPIRPM
	,CASE WHEN SUM(JSIRPM) = 0 THEN 1 ELSE SUM(JSIRPM) END						AS JSIRPM
	,CASE WHEN SUM(JBIRPM) = 0 THEN 1 ELSE SUM(JBIRPM) END						AS JBIRPM

FROM IRPM

GROUP BY
	PolicyKey
	,LOC_ITEM
)

SELECT
	details_subset.POLICY_KEY
	,POLICY_NUMBER
	,ACCOUNT_NUMBER
	,QUOTE_RANK
	,SUBMISSION_NUMBER
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME
	,JBTNumber
	,JBTRating
	,POL_EFF_DATE
	,POL_EXP_DATE
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,POL_GROUP
	,POL_VERSION
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,POLICY_STATUS
	,QUOTE_STATUS
	,details_subset.LOCATION_NUMBER
	,LOCATION_STATE
	,EXPERIENCE_TIER_FACTOR
	,TOTAL_PREMIUM
	,HAS_JS
	,HAS_JB
	,HAS_BOP
	,HAS_UMB
	,ROLEX_LOCATION
	,PANDORA_LOCATION
	,LOCATIONTYPE
	,INVENTORY_PREMIUM_BASE
	,HAZARD_CODE
	,CONST_CODE
	,YR_CONSTRUCTION
	,STOCK_LIMIT
	,STOCK_DEDUCTIBLE
	,BPP_LIMIT
	,BPP_DEDUCTIBLE
	,BLDG_LIMIT
	,BLDG_DEDUCTIBLE
	,STOCK_LIMIT + BPP_LIMIT + BLDG_LIMIT										AS ROW_LIMIT
	,	SUM(STOCK_LIMIT) OVER(PARTITION BY POLICY_KEY)	+
		SUM(BPP_LIMIT) OVER(PARTITION BY POLICY_KEY)	+
		SUM(BLDG_LIMIT) OVER(PARTITION BY POLICY_KEY)							AS POLICY_LIMIT
	,TRAVEL_LIMIT
	,JB_JS_PAWN_COVERED_INDICATOR
	,BPP_PAWN_LIMIT
	,OUT_OF_SAFE_LIMIT
	,StockInventoryLooseDiamonds				
	,StockInventoryLowValue
	,StockInventoryHighValue
	,StockInventoryLowValueWatches
	,StockInventoryHighValueWatches
	,StockInventoryScrapPreciousMetals
	,StockInventoryNonJewelryPreciousMetals
	,StockInventoryOther
	,StockHasLuxuryBrandWatches
	,StockHasWatchBlancpain
	,StockHasWatchBreitling
	,StockHasWatchCartier
	,StockHasWatchOmega
	,StockHasWatchPatekPhilippe
	,StockHasWatchRolex
	,StockHasWatchOther
	,IsStockDealerAnOfficialRolexDealer
	,PAWN_CLASS
	,ISNULL(BOPIRPM, 999)														AS BOPIRPM
	,ISNULL(JBIRPM, 999)														AS JBIRPM
	,ISNULL(JSIRPM, 999)														AS JSIRPM

INTO #QuotesDetails

FROM details_subset

	LEFT JOIN IRPMSummary
		ON IRPMSummary.PolicyKey = details_subset.POLICY_KEY
		AND IRPMSummary.Loc_Item = details_subset.LOCATION_NUMBER


---Alarm and OutOfSafeLimit
;WITH AlarmPremises AS (
SELECT DISTINCT
	a.AlarmCompany																AS PremAlarmCompany
	,a.ULCertificateExpirationDate												AS ULCertExpDate
	,a.ULCertificateNumber														AS ULCertNbr
	,a.AlarmCertType															AS AlarmCertType
	,c.PolicyKey
	,p.PolicyNumber
	,a.RiskSegmentKey
	,l.LocationNumber
	,ROW_NUMBER() OVER(PARTITION BY c.PolicyKey
									,l.LocationNumber
						   ORDER BY ISNULL(a.ULCertificateExpirationDate,'1799-01-01') DESC
	)																			AS LocationAlarmIndex

FROM bi_dds.DimRiskSegmentAlarm AS a
	LEFT JOIN bi_dds.DimRiskSegmentCore AS c ON c.RiskSegmentKey = a.RiskSegmentKey
	LEFT JOIN bi_dds.DimRiskSegmentLocation AS l ON l.RiskSegmentKey = c.RiskSegmentLocationKey
	LEFT JOIN bi_dds.DimPolicy AS p ON p.PolicyKey = c.PolicyKey

WHERE a.AlarmType = 'Premises' 
)

,FinalAlarmInfo1 AS (
SELECT 
	AlarmPremises.PolicyNumber
	,AlarmPremises.LocationNumber
	,AlarmPremises.PolicyKey
	,AlarmPremises.PremAlarmCompany
	,AlarmPremises.ULCertExpDate
	,AlarmPremises.ULCertNbr
	,AlarmPremises.AlarmCertType
	,CASE
		WHEN AlarmPremises.ULCertExpDate IS NULL
		THEN 'Non-Cert' ELSE 'Cert' END AS CertAlarm

FROM AlarmPremises

WHERE LocationAlarmIndex=1

Group by
AlarmPremises.PolicyNumber
,AlarmPremises.LocationNumber
,AlarmPremises.PolicyKey
,AlarmPremises.PremAlarmCompany
,AlarmPremises.ULCertExpDate
,AlarmPremises.ULCertNbr
,AlarmPremises.AlarmCertType
,CASE WHEN AlarmPremises.ULCertExpDate IS NULL THEN 'Non-Cert' ELSE 'Cert' END
)

,FinalSafeInfo AS (
SELECT
	PolicyNumber
	,LocationNumber
	,c.PolicyKey
	,COUNT(SafeNumber) AS NbrSafes
	,MIN(FireBurglaryRating) AS MinFireBurgRtg
	,MAX(FireBurglaryRating) AS MaxFireBurgRtg
	,MAX(TotalLocationInSafeVaultStockroom) AS MaxTotLocInSafeVaultStkrm
	,MAX(AmountOutOfSafeVaultStockroom) AS MaxAmtOutOfSafeVaultStkrm

FROM bi_dds.DimRiskSegmentSafe s

	LEFT JOIN bi_dds.DimRiskSegmentCore c ON c.RiskSegmentKey = S.RiskSegmentKey
    LEFT JOIN bi_dds.DimRiskSegmentLocation l ON l.RiskSegmentKey = c.RiskSegmentLocationKey
    LEFT JOIN bi_dds.DimPolicy p ON p.PolicyKey =c.PolicyKey

GROUP BY
	PolicyNumber
	,LocationNumber
	,c.PolicyKey
)

,CombineInfo AS (
SELECT
	COALESCE(FinalAlarmInfo1.PolicyNumber,FinalSafeInfo.PolicyNumber)			AS PolicyNumber
	,COALESCE(FinalAlarmInfo1.LocationNumber,FinalSafeInfo.LocationNumber)		AS LocationNumber
	,COALESCE(FinalAlarmInfo1.PolicyKey, FinalSafeInfo.PolicyKey)				AS PolicyKey
	,CertAlarm																	AS CertAlarm
	,AlarmCertType																AS AlarmCertType
	,FinalSafeInfo.NbrSafes
	,FinalSafeInfo.MaxTotLocInSafeVaultStkrm
	,FinalSafeInfo.MaxAmtOutOfSafeVaultStkrm

FROM FinalAlarmInfo1

Full OUTER JOIN FinalSafeInfo
	ON FinalAlarmInfo1.PolicyKey = FinalSafeInfo.PolicyKey 
	AND FinalAlarmInfo1.PolicyNumber = FinalSafeInfo.PolicyNumber
	AND FinalAlarmInfo1.LocationNumber = FinalSafeInfo.LocationNumber

GROUP BY
	COALESCE(FinalAlarmInfo1.PolicyNumber,FinalSafeInfo.PolicyNumber)
	,COALESCE(FinalAlarmInfo1.LocationNumber,FinalSafeInfo.LocationNumber)
	,COALESCE(FinalAlarmInfo1.PolicyKey, FinalSafeInfo.PolicyKey)
	,CertAlarm
	,AlarmCertType
	,FinalSafeInfo.NbrSafes
	,FinalSafeInfo.MaxTotLocInSafeVaultStkrm
	,FinalSafeInfo.MaxAmtOutOfSafeVaultStkrm
) 

-- This is effectively Loretta's original query results + the first pass at feature engineering pulled out of Excel formulas
,OG AS (
SELECT
	POLICY_KEY
	,POLICY_NUMBER
	,ACCOUNT_NUMBER
	,SUBMISSION_NUMBER
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME 
	,JBTNumber
	,JBTRating
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,CONVERT(varchar(10), PolEffDate.FullDate, 101)								AS PolEffDate
	,CONVERT(varchar(10), PolExpDate.FullDate, 101)								AS PolExpDate
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,POLICY_STATUS
	,QUOTE_STATUS
	,LOCATION_NUMBER
	,LOCATION_STATE
	,EXPERIENCE_TIER_FACTOR
	,TOTAL_PREMIUM
	,HAS_JS
	,HAS_JB
	,HAS_BOP
	,HAS_UMB
	,ROLEX_LOCATION
	,PANDORA_LOCATION
	,LOCATIONTYPE
	,INVENTORY_PREMIUM_BASE
	,HAZARD_CODE
	,CONST_CODE
	,YR_CONSTRUCTION
	,STOCK_LIMIT
	,STOCK_DEDUCTIBLE
	,BPP_LIMIT
	,BPP_DEDUCTIBLE
	,BLDG_LIMIT
	,BLDG_DEDUCTIBLE
	,ROW_LIMIT
	,POLICY_LIMIT
	-- ,ROW_LIMIT / POLICY_LIMIT AS PROPORTION
	,CASE WHEN POLICY_LIMIT = 0 THEN 0 ELSE ROW_LIMIT / POLICY_LIMIT END AS PROPORTION
	,TRAVEL_LIMIT
	,JB_JS_PAWN_COVERED_INDICATOR
	,BPP_PAWN_LIMIT
	,OUT_OF_SAFE_LIMIT
	,StockInventoryLooseDiamonds				
	,StockInventoryLowValue
	,StockInventoryHighValue
	,StockInventoryLowValueWatches
	,StockInventoryHighValueWatches
	,StockInventoryScrapPreciousMetals
	,StockInventoryNonJewelryPreciousMetals
	,StockInventoryOther
	,StockHasLuxuryBrandWatches
	,StockHasWatchBlancpain
	,StockHasWatchBreitling
	,StockHasWatchCartier
	,StockHasWatchOmega
	,StockHasWatchPatekPhilippe
	,StockHasWatchRolex
	,StockHasWatchOther
	,IsStockDealerAnOfficialRolexDealer
	,BOPIRPM
	,JBIRPM
	,JSIRPM
	--,ISNULL(CertAlarm, '')														AS CertAlarm
	,CertAlarm
	,ISNULL(NbrSafes, 0)														AS NbrSafes
	,ISNULL(MaxAmtOutOfSafeVaultStkrm, 0)										AS MaxAmtOutOfSafeVaultStkrm
	,ISNULL(MaxTotLocInSafeVaultStkrm, 0)										AS MaxTotLocInSafeVaultStkrm
	,QUOTECREATEDATE.CalendarYear												AS QuoteYear
	,QUOTECREATEDATE.MonthNumber												AS QuoteMonth
	,QUOTECREATEDATE.QuarterNumber												AS QuoteQtr
	,CASE
		WHEN TOTAL_PREMIUM> 25000
		THEN 1 ELSE 0 END														AS LARGE_ACCT_FLAG_GT_25K
	,CASE
		WHEN TOTAL_PREMIUM> 50000
		THEN 1 ELSE 0 END														AS LARGE_ACCT_FLAG_GT_50K
	,CASE
		WHEN PAWN_CLASS <> 0 OR JB_JS_PAWN_COVERED_INDICATOR <> 0 OR BPP_PAWN_LIMIT <> 0
		THEN 1 ELSE 0 END														AS PAWN_FLAG
	--,ISNULL(AlarmCertType, '')													AS AlarmCertType
	,AlarmCertType
	,PAWN_CLASS
	,CASE
		WHEN (HAS_JS = 1 OR HAS_JB = 1)
		THEN CASE
				WHEN ISNULL(MaxAmtOutOfSafeVaultStkrm, 0) >= OUT_OF_SAFE_LIMIT
				THEN ISNULL(MaxAmtOutOfSafeVaultStkrm, 0)
				ELSE OUT_OF_SAFE_LIMIT
				END
		ELSE NULL
		END																		AS JB_JS_Out_of_Safe_Limit
	,CASE
		WHEN COUNT(*) OVER (PARTITION BY POLICY_KEY) > 1
		THEN 'Multi' ELSE 'Single' END											AS Single_Multiple_Location
	,CASE
		WHEN ROW_LIMIT = 0 OR POLICY_LIMIT = 0
		THEN 0
		ELSE (ROW_LIMIT / POLICY_LIMIT) * TOTAL_PREMIUM
		END																		AS Estimated_Location_Premium
	,CASE
		WHEN (HAS_JS = 1 OR HAS_JB = 1)
		THEN CASE
				WHEN STOCK_LIMIT < 1.0			THEN 'No Stock'
				WHEN STOCK_LIMIT < 250001.0		THEN 'Less than Equal $250K'
				WHEN STOCK_LIMIT < 1000001.0	THEN '$250K - $1M'
				WHEN STOCK_LIMIT < 5000001.0	THEN '$1M - $5M'
												ELSE 'Greater than $5M'
				END
		ELSE ''
		END																		AS Stock_Limit_Range
	,CASE
		WHEN HAS_JB = 1
		THEN CASE
				WHEN JBIRPM = 999					THEN NULL
				WHEN JBIRPM < 0.4					THEN 'Less than .4'
				WHEN JBIRPM < 0.5					THEN '.40-.49'
				WHEN JBIRPM < 0.75					THEN '.50-.74'
				WHEN JBIRPM < 0.85					THEN '.75-.84'
				WHEN JBIRPM < 1.0					THEN '.85-.99'
				WHEN JBIRPM = 1.0					THEN '1'
				WHEN JBIRPM < 1.1					THEN '1.01-1.09'
				WHEN JBIRPM < 1.25					THEN '1.10-1.24'
				WHEN JBIRPM < 1.4					THEN '1.25-1.39'
				WHEN JBIRPM >= 1.4					THEN 'Over 1.4'
													ELSE NULL
				END
		END																		AS JB_IRPM_Range
	,CASE
		WHEN HAS_JS = 1
		THEN CASE
				WHEN JSIRPM = 999					THEN NULL
				WHEN JSIRPM < 0.4					THEN 'Less than .4'
				WHEN JSIRPM < 0.5					THEN '.40-.49'
				WHEN JSIRPM < 0.75					THEN '.50-.74'
				WHEN JSIRPM < 0.85					THEN '.75-.84'
				WHEN JSIRPM < 1.0					THEN '.85-.99'
				WHEN JSIRPM = 1.0					THEN '1'
				WHEN JSIRPM < 1.1					THEN '1.01-1.09'
				WHEN JSIRPM < 1.25					THEN '1.10-1.24'
				WHEN JSIRPM < 1.4					THEN '1.25-1.39'
				WHEN JSIRPM >= 1.4					THEN 'Over 1.4'
													ELSE NULL
				END
		END																		AS JS_IRPM_Range
	,CASE
		WHEN HAS_BOP = 1
		THEN CASE
				WHEN BOPIRPM = 999					THEN NULL
				WHEN BOPIRPM < 0.4					THEN 'Less than .4'
				WHEN BOPIRPM < 0.5					THEN '.40-.49'
				WHEN BOPIRPM < 0.75					THEN '.50-.74'
				WHEN BOPIRPM < 0.85					THEN '.75-.84'
				WHEN BOPIRPM < 1.0					THEN '.85-.99'
				WHEN BOPIRPM = 1.0					THEN '1'
				WHEN BOPIRPM < 1.1					THEN '1.01-1.09'
				WHEN BOPIRPM < 1.25					THEN '1.10-1.24'
				WHEN BOPIRPM < 1.4					THEN '1.25-1.39'
				WHEN BOPIRPM >= 1.4					THEN 'Over 1.4'
													ELSE NULL
				END
		END																		AS BOP_IRPM_Range
	,CASE
		WHEN (ISNULL(HAS_JB, 0) + ISNULL(HAS_JS, 0) + ISNULL(HAS_BOP, 0)) > 1
		THEN 'Package' ELSE 'Monoline' END										AS Package_Monoline
	,CASE
		WHEN QUOTE_STATUS = 'Issued' THEN 'Yes' ELSE 'No' END					AS Issued_YN
	,CASE
		WHEN AGENCY_CODE = '332' OR AGENCY_CODE = 'MJS' OR
			 AGENCY_MASTER_CODE IN ('200', '332', '405', '638', 'A05', 'B50', 'C04', 'C51', 'CJB', 'D01', 'D10'
									,'D52', 'F50', 'F51', 'H51', 'I50', 'INV', 'J51', 'M01', 'M50', 'M80', 'MAC'
									,'MJS', 'MLK', 'P02', 'P05', 'P13', 'P17', 'P19', 'Q50', 'Q52', 'R01', 'R53'
									,'SOP', 'T01', 'T50', 'U09'
			 /*
									'200', '332', '405', '638',	'A05', 'B50', 'C04', 'C51',	'CJB', 'D01', 'D10', -- Updated 31 Oct 2019
								   	'D52', 'F50', 'F51', 'H51',	'I50', 'INV', 'J51', 'M01',	'M50', 'M80', 'MAC',
									'MLK', 'P02', 'P05', 'P13',	'P17', 'P19', 'Q50', 'Q52',	'R01', 'R53', 'SOP',
									'T01',	'T50', 'U09'
									*/)

		THEN 'Yes' ELSE 'No'
		END																		AS Top_40
	,CASE
		WHEN PRIMARY_STATE IN ('AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'XC', 'YT')
		THEN 'CAN' ELSE 'USA' END												AS Country
	,CASE
		WHEN TOTAL_PREMIUM IS NULL
		THEN 'Yes' ELSE 'No' END												AS Exclude_No_Premium
	,CASE
		WHEN StockInventoryHighValueWatches IS NULL		THEN NULL
		WHEN StockInventoryHighValueWatches < 0.0001	THEN '0%'
		WHEN StockInventoryHighValueWatches < 0.06		THEN '1-5%'
		WHEN StockInventoryHighValueWatches < 0.11		THEN '6-10%'
		WHEN StockInventoryHighValueWatches < 0.16		THEN '11-15%'
		WHEN StockInventoryHighValueWatches < 0.26		THEN '16-25%'
		WHEN StockInventoryHighValueWatches < 0.51		THEN '26-50%'
		WHEN StockInventoryHighValueWatches < 0.76		THEN '51-75%'
		WHEN StockInventoryHighValueWatches < 0.86		THEN '76-85%'
		WHEN StockInventoryHighValueWatches < 1.0		THEN '86-99%'
		WHEN StockInventoryHighValueWatches = 1.0		THEN '100%'
														ELSE NULL
		END																		AS Perc_of_High_Value_Watches
	,CASE
		WHEN TOTAL_PREMIUM IS NULL						THEN NULL
		WHEN TOTAL_PREMIUM < 1001						THEN '0 - 1,000'
		WHEN TOTAL_PREMIUM < 2501						THEN '1,001 - 2,500'
		WHEN TOTAL_PREMIUM < 5001						THEN '2,501 - 5,000'
		WHEN TOTAL_PREMIUM < 10001						THEN '5,001 - 10,000'
		WHEN TOTAL_PREMIUM < 15001						THEN '10,001 - 15,000'
		WHEN TOTAL_PREMIUM < 25001						THEN '15,001 - 25,000'
		WHEN TOTAL_PREMIUM < 50001						THEN '25,001 - 50,000'
														ELSE '> $50K'
		END																		AS Quote_Premium_Range

FROM #QuotesDetails 

LEFT JOIN CombineInfo
	ON CombineInfo.PolicyKey = #QuotesDetails.POLICY_KEY
	AND CombineInfo.LocationNumber=#QuotesDetails.LOCATION_NUMBER

INNER JOIN bief_dds.DimDate AS POLEFFDATE
	ON POLEFFDATE.DateKey = POL_EFF_DATE

INNER JOIN bief_dds.DimDate AS POLEXPDATE
	ON POLEXPDATE.DateKey = POL_EXP_DATE

INNER JOIN bief_dds.DimDate AS QUOTECREATEDATE
	ON QUOTECREATEDATE.DateKey = QUOTE_CREATE_DATE

WHERE QUOTE_STATUS<>'WITHDRAWN' 

GROUP BY
	POLICY_KEY
	,POLICY_NUMBER
	,ACCOUNT_NUMBER
	,SUBMISSION_NUMBER
	,AGENCY_MASTER_CODE
	,AGENCY_MASTER_CODE_NAME
	,AGENCY_CODE
	,AGENCY_NAME
	,AGENCY_SUB_CODE
	,QUOTE_NAME 
	,JBTNumber
	,JBTRating
	,QUOTE_CREATE_DATE
	,QUOTE_ISSUED_DATE
	,QUOTE_EXPIRATION_DATE
	,CONVERT(varchar(10), PolEffDate.FullDate, 101)
	,CONVERT(varchar(10), PolExpDate.FullDate, 101)
	,ACCT_SEGMENT
	,POLICY_TYPE
	,PRIMARY_STATE
	,POLICY_STATUS
	,QUOTE_STATUS
	,LOCATION_NUMBER
	,LOCATION_STATE
	,EXPERIENCE_TIER_FACTOR
	,TOTAL_PREMIUM
	,HAS_JS
	,HAS_JB
	,HAS_BOP
	,HAS_UMB
	,ROLEX_LOCATION
	,PANDORA_LOCATION
	,LOCATIONTYPE
	,INVENTORY_PREMIUM_BASE
	,HAZARD_CODE
	,CONST_CODE
	,YR_CONSTRUCTION
	,STOCK_LIMIT
	,STOCK_DEDUCTIBLE
	,BPP_LIMIT
	,BPP_DEDUCTIBLE
	,BLDG_LIMIT
	,BLDG_DEDUCTIBLE
	,ROW_LIMIT
	,POLICY_LIMIT
	,TRAVEL_LIMIT
	,JB_JS_PAWN_COVERED_INDICATOR
	,BPP_PAWN_LIMIT
	,OUT_OF_SAFE_LIMIT
	,StockInventoryLooseDiamonds				
	,StockInventoryLowValue
	,StockInventoryHighValue
	,StockInventoryLowValueWatches
	,StockInventoryHighValueWatches
	,StockInventoryScrapPreciousMetals
	,StockInventoryNonJewelryPreciousMetals
	,StockInventoryOther
	,StockHasLuxuryBrandWatches
	,StockHasWatchBlancpain
	,StockHasWatchBreitling
	,StockHasWatchCartier
	,StockHasWatchOmega
	,StockHasWatchPatekPhilippe
	,StockHasWatchRolex
	,StockHasWatchOther
	,IsStockDealerAnOfficialRolexDealer
	,BOPIRPM
	,JBIRPM
	,JSIRPM
	--,ISNULL(CertAlarm, '')
	,CertAlarm
	,ISNULL(NbrSafes, 0)
	,ISNULL(MaxAmtOutOfSafeVaultStkrm, 0)
	,ISNULL(MaxTotLocInSafeVaultStkrm, 0)
	,QUOTECREATEDATE.CalendarYear
	,QUOTECREATEDATE.MonthNumber
	,QUOTECREATEDATE.QuarterNumber
	,AlarmCertType
	,PAWN_CLASS
)

,feature_engineered AS (
-- Second pass at feature engineering using some of the fields defined by the previous CTE
SELECT
	OG.*
	,CASE
		WHEN JB_JS_Out_of_Safe_Limit IS NULL
		THEN NULL
		WHEN (HAS_JS = 1 OR HAS_JB = 1) AND [JB_JS_Out_of_Safe_Limit] IS NOT NULL
		THEN CASE 
				WHEN STOCK_LIMIT = 0 THEN 0 ELSE JB_JS_Out_of_Safe_Limit / STOCK_LIMIT END 
										ELSE ''
		END																		AS Perc_of_Stock_Limit_Out_of_Safe
	,CASE
		WHEN (HAS_JB = 1 AND JBIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN JBIRPM * Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (JBIRPM * Estimated_Location_Premium * STOCK_LIMIT) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Wtd_JB_IRPM_Premium
	,CASE
		WHEN (HAS_JB = 1 AND JBIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (Estimated_Location_Premium * STOCK_LIMIT) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Estimated_JB_Premium
	,CASE
		WHEN (HAS_JS = 1 AND JSIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN JSIRPM * Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (JSIRPM * Estimated_Location_Premium * STOCK_LIMIT) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Wtd_JS_IRPM_Premium
	,CASE
		WHEN (HAS_JS = 1 AND JSIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (Estimated_Location_Premium * STOCK_LIMIT) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Estimated_JS_Premium
	,CASE
		WHEN (HAS_BOP = 1 AND BOPIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN BOPIRPM * Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (BOPIRPM * Estimated_Location_Premium * (BPP_LIMIT + BLDG_LIMIT)) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Wtd_BOP_IRPM_Premium
	,CASE
		WHEN (HAS_BOP = 1 AND BOPIRPM <> 999)
		THEN CASE
				WHEN Single_Multiple_Location = 'Single'
				THEN Estimated_Location_Premium
				ELSE CASE
					WHEN (ROW_LIMIT) = 0
					THEN 0
					ELSE (Estimated_Location_Premium * (BPP_LIMIT + BLDG_LIMIT)) /  (ROW_LIMIT)
					END
				END
		ELSE 0
		END																		AS Estimated_BOP_Premium
FROM OG
)

SELECT
	POLICY_KEY AS Policy_Key
	,POLICY_NUMBER AS Policy_Number
	,ACCOUNT_NUMBER AS Account_Number
	,SUBMISSION_NUMBER AS Submission_Number
	,AGENCY_MASTER_CODE AS Agency_Master_Code
	,AGENCY_MASTER_CODE_NAME AS Agency_Master_Code_Name
	,AGENCY_CODE AS Agency_Code
	,AGENCY_NAME AS Agency_Name
	,AGENCY_SUB_CODE AS Agency_Sub_Code
	,QUOTE_NAME AS Quote_Name
	,JBTNumber
	,JBTRating
	,QUOTE_CREATE_DATE AS Quote_Create_Date
	,QUOTE_ISSUED_DATE AS Quote_Issued_Date
	,QUOTE_EXPIRATION_DATE AS Quote_Expiration_Date
	,CAST(PolEffDate AS datetime) AS PolEffDate
	,CAST(PolExpDate AS datetime) AS PolExpDate
	,ACCT_SEGMENT AS Acct_Segment
	,POLICY_TYPE AS Policy_Type
	,PRIMARY_STATE AS Primary_State
	,POLICY_STATUS AS Policy_Status
	,QUOTE_STATUS AS Quote_Status
	,LOCATION_NUMBER AS Location_Number
	,LOCATION_STATE AS Location_State
	,EXPERIENCE_TIER_FACTOR AS Experience_Tier_Factor
	,TOTAL_PREMIUM AS Total_Premium
	,HAS_JS AS Has_JS
	,HAS_JB AS Has_JB
	,HAS_BOP AS Has_BOP
	,HAS_UMB AS Has_UMB
	,ROLEX_LOCATION AS Rolex_Location
	,PANDORA_LOCATION AS Pandora_Location
	,LOCATIONTYPE AS Location_Type
	,INVENTORY_PREMIUM_BASE AS Inventory_Premium_Base
	,HAZARD_CODE AS Hazard_Code
	,CONST_CODE AS Const_Code
	,YR_CONSTRUCTION AS YR_Construction
	,STOCK_LIMIT AS Stock_Limit
	,STOCK_DEDUCTIBLE AS Stock_Deductible
	,BPP_LIMIT AS BPP_Limit
	,BPP_DEDUCTIBLE AS BPP_Deductible
	,BLDG_LIMIT AS BLDG_Limit
	,BLDG_DEDUCTIBLE AS BLDG_Deductible
	,ROW_LIMIT AS Row_Limit
	,POLICY_LIMIT AS Policy_Limit
	,PROPORTION AS Proportion
	,PROPORTION * TOTAL_PREMIUM AS Allocated_Premium
	,TRAVEL_LIMIT AS Travel_Limit
	,JB_JS_PAWN_COVERED_INDICATOR AS JB_JS_Pawn_Covered_Indicator
	,BPP_PAWN_LIMIT AS BPP_Pawn_Limit
	,OUT_OF_SAFE_LIMIT AS Out_Of_Safe_Limit
	,StockInventoryLooseDiamonds
	,StockInventoryLowValue
	,StockInventoryHighValue
	,StockInventoryLowValueWatches
	,StockInventoryHighValueWatches
	,StockInventoryScrapPreciousMetals
	,StockInventoryNonJewelryPreciousMetals
	,StockInventoryOther
	,StockHasLuxuryBrandWatches
	,StockHasWatchBlancpain
	,StockHasWatchBreitling
	,StockHasWatchCartier
	,StockHasWatchOmega
	,StockHasWatchPatekPhilippe
	,StockHasWatchRolex
	,StockHasWatchOther
	,IsStockDealerAnOfficialRolexDealer
	,BOPIRPM
	,JBIRPM
	,JSIRPM
	,CertAlarm
	,NbrSafes
	,MaxAmtOutOfSafeVaultStkrm
	,MaxTotLocInSafeVaultStkrm
	,QuoteYear
	,QuoteMonth
	,QuoteQtr
	,LARGE_ACCT_FLAG_GT_25K AS Large_Acct_Flag_Gt_25K
	,LARGE_ACCT_FLAG_GT_50K AS Large_Acct_Flag_Gt_50K
	,CASE
		WHEN PAWN_FLAG = 1
		THEN 'Yes' ELSE 'No' END												AS PAWN_FLAG
	,AlarmCertType
	,PAWN_CLASS AS Pawn_Class
	,CASE WHEN AGENCY_CODE = 'DIRD' THEN 'Y' ELSE 'N' END AS BallparkQuote
	,JB_JS_Out_of_Safe_Limit
	,Single_Multiple_Location
	,ROUND(Estimated_Location_Premium, 0, 0)									AS Estimated_Location_Premium
	,Stock_Limit_Range
	,JB_IRPM_Range
	,JS_IRPM_Range
	,BOP_IRPM_Range
	,Package_Monoline
	,Perc_of_Stock_Limit_Out_of_Safe
	,Issued_YN
	,Top_40
	,Country
	,Exclude_No_Premium
--	,ROUND(Wtd_JB_IRPM_Premium, 0, 0)											AS Wtd_JB_IRPM_Premium
--	,ROUND(Estimated_JB_Premium, 0, 0)											AS Estimated_JB_Premium
--	,ROUND(Wtd_JS_IRPM_Premium, 0, 0)											AS Wtd_JS_IRPM_Premium
--	,ROUND(Estimated_JS_Premium, 0, 0)											AS Estimated_JS_Premium
--	,ROUND(Wtd_BOP_IRPM_Premium, 0, 0)											AS Wtd_BOP_IRPM_Premium
--	,ROUND(Estimated_BOP_Premium, 0, 0)											AS Estimated_BOP_Premium
	,CASE
		WHEN Perc_of_Stock_Limit_Out_of_Safe IS NULL	THEN 'NA'
		WHEN Perc_of_Stock_Limit_Out_of_Safe = 0		THEN '0%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.0001	THEN '0%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.06		THEN '1-5%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.11		THEN '6-10%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.16		THEN '11-15%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.26		THEN '16-25%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.51		THEN '26-50%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.76		THEN '51-75%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 0.76		THEN '51-75%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe < 1.0		THEN '76-99%'
		WHEN Perc_of_Stock_Limit_Out_of_Safe = 1.0		THEN '100%'
														ELSE 'NA'
		END																		AS Perc_Stock_Out_of_Safe_Range
	,[Perc_of_High_Value_Watches]
--	,CASE
--		WHEN [Wtd_JB_IRPM_Premium] = 0 OR [Wtd_JB_IRPM_Premium] IS NULL
--		THEN 1 ELSE 0 END														AS Exclude_for_JB_IRPM
--	,CASE
--		WHEN [Wtd_JS_IRPM_Premium] = 0 OR [Wtd_JS_IRPM_Premium] IS NULL
--		THEN 1 ELSE 0 END														AS Exclude_for_JS_IRPM
--	,CASE
--		WHEN [Wtd_BOP_IRPM_Premium] = 0 OR [Wtd_BOP_IRPM_Premium] IS NULL
--		THEN 1 ELSE 0 END														AS Exclude_for_BOP_IRPM
	,[Quote_Premium_Range]

FROM feature_engineered
