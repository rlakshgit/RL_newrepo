-- tag: CLCoverageLevelAttributes - tag ends/
/******************************************************************************************************************
	Kimberlite - Building Blocks
	CLCoverageLevelAttributes.sql 
		CL Jewelry Attributes and Feature Details
	----------------------------------------------------------------------------------------------------------------
	--Change Log--
	----------------------------------------------------------------------------------------------------------------
	06/14/2021	SAI			Init (is a table, but could be a view)
	08/25/2021	DROBAK		Added column: IsTransactionSliceEffective; mapped CoverageNumber, BOPLocationPublicID
	05/25/2022	DROBAK		Added v_ProductHierarchy.ProductType; and source ProductCode from here too
	02/15/2023	DROBAK		Remove: PrimaryLocationNumber (keep only in Risk Tables)

	-----------------------------------------------------------------------------------------------------------------
*/
--DELETE `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.CLCoverageLevelAttributes`(
SourceSystem		,	
ProductCode		,	
ProductType,
CoverageKey		,	
RiskBOPLocationKey		,	
RiskIMLocationKey		,	
RiskBOPBuildingKey		,	
RiskIMStockKey		,	
PolicyTransactionKey		,	
CoveragePublicID		,	
BOPLocationPublicID		,	
IMLocationPublicID		,	
BOPBuildingPublicId		,	
IMStockPublicID		,	
PolicyLocationPublicID		,	
PolicyPeriodPublicID		,	
JobNumber		,	
PolicyNumber		,	
--PrimaryLocationNumber		,	
CoverageLevel		,	
EffectiveDate		,	
ExpirationDate		,	
IsTempCoverage		,	
PerOccurenceLimit		,	
PerOccurenceDeductible		,	
CoverageCode		,	
CoverageNumber		,	
BOPEPLICode		,	
BOPPropertyRateNum		,	
BOPTheftAdjPropertyRateNum		,	
BOPRetroactiveDate		,	
BOPRateBookUsed		,	
IMSpecifiedCarrier		,	
SpecifiedCarrierExposure	,
IsTransactionSliceEffective	,
bq_load_date	
)

SELECT 
    CoverageBOP.SourceSystem    
    --,'BOP'                      AS Product
	--,'CMP Liability and Non-Liability' AS ProductType
    ,v_ProductHierarchy.ProductCode
	,v_ProductHierarchy.ProductType
    --Keys
    ,BOPCoverageKey             AS CoverageKey
    ,RiskLocationKey            AS RiskBOPLocationKey
    ,NULL                       AS RiskIMLocationKey
    ,RiskBuildingKey            AS RiskBOPBuildingKey
    ,NULL                       AS RiskIMStockKey
    ,PolicyTransactionKey       AS PolicyTransactionKey
    --PublicID
    ,CoveragePublicID           AS CoveragePublicID
    ,BOPLocationPublicID        AS BOPLocationPublicID
    ,NULL                       AS IMLocationPublicID
    ,BOPBuildingPublicId        AS BOPBuildingPublicId
    ,NULL                       AS IMStockPublicID
    ,PolicyLocationPublicID     AS PolicyLocationPublicID
    ,PolicyPeriodPublicID       AS PolicyPeriodPublicID
    --Details
    ,JobNumber                  AS JobNumber
    ,PolicyNumber               AS PolicyNumber
    --,PrimaryLocationNumber      AS PrimaryLocationNumber
    ,CoverageLevel              AS CoverageLevel
    ,EffectiveDate              AS EffectiveDate
    ,ExpirationDate				AS ExpirationDate
    ,IsTempCoverage				AS IsTempCoverage
    ,BOPLimit					AS PerOccurenceLimit
    ,BOPDeductible				AS PerOccurenceDeductible
    ,CoverageCode				AS CoverageCode
    ,CoverageNumber				AS CoverageNumber
    ,EPLICode					AS BOPEPLICode
    ,PropertyRateNum			AS BOPPropertyRateNum
    ,TheftAdjPropertyRateNum	AS BOPTheftAdjPropertyRateNum
    ,RetroactiveDate			AS BOPRetroactiveDate
    ,RateBookUsed				AS BOPRateBookUsed
    ,NULL                       AS IMSpecifiedCarrier
    ,NULL                       AS SpecifiedCarrierExposure
	,IsTransactionSliceEffective
	,DATE('{partition_date}')	AS bq_load_date

--FROM `qa-edl.B_QA_ref_kimberlite.CoverageBOP` 
FROM `{project}.{core_dataset}.CoverageBOP` AS CoverageBOP
LEFT JOIN `{project}.{dest_dataset}.v_ProductHierarchy` AS v_ProductHierarchy
  ON v_ProductHierarchy.ProductCode ='BOP'
  AND v_ProductHierarchy.SourceSystem = 'GW'
WHERE   1 = 1
    AND CoverageBOP.bq_load_date = DATE({partition_date})
    AND FixedCoverageInBranchRank = 1
    -- AND IsTransactionSliceEffective = 1 -- Add later
    -- AND JobNumber = '4526242'

UNION ALL

SELECT 
    CoverageIM.SourceSystem
	--,CASE WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
	--	WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
	--END		                    AS Product
	--,'Other Commercial Inland Marine' AS ProductType
	,v_ProductHierarchy.ProductCode
	,v_ProductHierarchy.ProductType
    --Keys
    ,IMCoverageKey						AS CoverageKey
    ,NULL								AS RiskBOPLocationKey
    ,RiskLocationKey					AS RiskIMLocationKey
    ,NULL								AS RiskBOPBuildingKey
    ,RiskStockKey						AS RiskIMStockKey
    ,CoverageIM.PolicyTransactionKey	AS PolicyTransactionKey
    --PublicID
    ,CoveragePublicID					AS CoveragePublicID
    ,NULL								AS BOPLocationPublicID
    ,IMLocationPublicID					AS IMLocationPublicID
    ,NULL								AS BOPBuildingPublicId
    ,IMStockPublicID					AS IMStockPublicID
    ,CoverageIM.PolicyLocationPublicID	AS PolicyLocationPublicID
    ,CoverageIM.PolicyPeriodPublicID	AS PolicyPeriodPublicID
    --Details
    ,CoverageIM.JobNumber               AS JobNumber
    ,CoverageIM.PolicyNumber            AS PolicyNumber
    --,NULL								AS PrimaryLocationNumber -- Missing
    ,CoverageLevel						AS CoverageLevel
    ,EffectiveDate						AS EffectiveDate
    ,ExpirationDate						AS ExpirationDate
    ,IsTempCoverage						AS IsTempCoverage
    ,IMLimit							AS PerOccurenceLimit
    ,IMDeductible						AS PerOccurenceDeductible
    ,CoverageCode						AS CoverageCode
    ,CoverageNumber						AS CoverageNumber
	,NULL								AS BOPEPLICode
    ,NULL								AS BOPPropertyRateNum
    ,NULL								AS BOPTheftAdjPropertyRateNum
    ,NULL								AS BOPRetroactiveDate
    ,NULL								AS BOPRateBookUsed
    ,SpecifiedCarrier					AS IMSpecifiedCarrier
    ,SpecifiedCarrierExposure			AS SpecifiedCarrierExposure
	,IsTransactionSliceEffective
	,DATE('{partition_date}')			AS bq_load_date

--FROM `qa-edl.B_QA_ref_kimberlite.CoverageIM` AS CoverageIM INNER JOIN `qa-edl.B_QA_ref_kimberlite.PolicyTransaction` PolicyTransaction
FROM `{project}.{core_dataset}.CoverageIM` AS CoverageIM
INNER JOIN `{project}.{core_dataset}.PolicyTransaction` PolicyTransaction
	ON PolicyTransaction.PolicyTransactionKey = CoverageIM.PolicyTransactionKey
LEFT JOIN `{project}.{dest_dataset}.v_ProductHierarchy` AS v_ProductHierarchy
	ON CASE WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
			WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
		END = v_ProductHierarchy.ProductCode
	AND v_ProductHierarchy.SourceSystem = 'GW'
WHERE   1 = 1
    AND CoverageIM.bq_load_date = DATE({partition_date})
    AND FixedCoverageInBranchRank = 1
	AND PolicyTransaction.bq_load_date = DATE({partition_date})
    -- AND IsTransactionSliceEffective = 1 -- Add later
    -- AND JobNumber = '4526242'

UNION ALL

SELECT 
    CoverageUMB.SourceSystem    
    --,'UMB'                      AS Product
	--,'Other Liability'			AS ProductType
	,v_ProductHierarchy.ProductCode
	,v_ProductHierarchy.ProductType
    --Keys
    ,UMBCoverageKey             AS CoverageKey
    ,RiskLocationKey            AS RiskBOPLocationKey -- Is it primary location key? do we have separate key --Dave: it is based on PrimaryLocation
    ,NULL                       AS RiskIMLocationKey
    ,NULL                       AS RiskBOPBuildingKey -- verify null.. Is UMB applied to primary location or primary building?
    ,NULL                       AS RiskIMStockKey
    ,PolicyTransactionKey       AS PolicyTransactionKey
    --PublicID
    ,CoveragePublicID           AS CoveragePublicID
    ,BOPLocationPublicID        AS BOPLocationPublicID
    ,NULL                       AS IMLocationPublicID
    ,NULL                       AS BOPBuildingPublicId
    ,NULL                       AS IMStockPublicID
    ,NULL     					AS PolicyLocationPublicID
    ,PolicyPeriodPublicID       AS PolicyPeriodPublicID
    --Details
    ,JobNumber                  AS JobNumber
    ,PolicyNumber               AS PolicyNumber
    --,PrimaryLocationNumber      AS PrimaryLocationNumber
    ,CoverageLevel              AS CoverageLevel
    ,EffectiveDate              AS EffectiveDate
    ,ExpirationDate				AS ExpirationDate
    ,IsTempCoverage				AS IsTempCoverage
    ,UMBLimit					AS PerOccurenceLimit
    ,UMBDeductible				AS PerOccurenceDeductible
    ,CoverageCode				AS CoverageCode
    ,CoverageNumber				AS CoverageNumber
	,NULL						AS BOPEPLICode
    ,NULL						AS BOPPropertyRateNum
    ,NULL						AS BOPTheftAdjPropertyRateNum
    ,NULL						AS BOPRetroactiveDate
    ,NULL						AS BOPRateBookUsed
    ,NULL                       AS IMSpecifiedCarrier
    ,NULL                       AS SpecifiedCarrierExposure
	,IsTransactionSliceEffective
	,DATE('{partition_date}')	AS bq_load_date

--FROM `qa-edl.B_QA_ref_kimberlite.CoverageUMB`
FROM `{project}.{core_dataset}.CoverageUMB` AS CoverageUMB
LEFT JOIN `{project}.{dest_dataset}.v_ProductHierarchy` AS v_ProductHierarchy
  ON v_ProductHierarchy.ProductCode ='UMB'
  AND v_ProductHierarchy.SourceSystem = 'GW'
WHERE   1 = 1
    AND CoverageUMB.bq_load_date = DATE({partition_date})
    AND FixedCoverageInBranchRank = 1
    -- AND IsTransactionSliceEffective = 1 -- Add later
    -- AND JobNumber = '4526242'


