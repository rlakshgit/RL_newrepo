DELETE `{project}.{dest_dataset}.PJCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.PJCoverageLevelAttributes` 
	( 
		SourceSystem,
		ProductCode,
		ProductType,
		ItemCoverageKey,
		PolicyTransactionKey,
		RiskJewelryItemKey,
		CoverageLevel,
		JobNumber,
		TransactionStatus,
		CoverageTypeCode,
		PolicyNumber,
		CoverageNumber,
		EffectiveDate,
		ExpirationDate,
		IsTempCoverage,
		PerOccurenceLimit,
		PerOccurenceDeductible,
		ItemValue,
		ItemAnnualPremium,
		CoverageCode,
		bq_load_date	
	)

/**********************************************************************************************
	Kimberlite - Building Blocks
		PJCoverageLevelAttributes.sql
			Personal Jewelry Attributes and Feature Details
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	06/08/2021	DROBAK		Init (is a table, but could be a view)
	07/08/2021	DROBAK		Add PolicyTransaction.TransactionStatus and join to PolicyTransaction
	08/26/2021	DROBAK		Make join to PolicyTransaction a LEFT JOIN
	11/09/2021	DROBAK		Added date code; fixed BQ dataset variable
	06/03/2022	DROBAK		Added: ProductCode, ProductType

	-------------------------------------------------------------------------------------------
*/
SELECT
	CoverageJewelryItem.SourceSystem
    ,v_ProductHierarchy.ProductCode
	,v_ProductHierarchy.ProductType
	,CoverageJewelryItem.ItemCoverageKey
	,CoverageJewelryItem.PolicyTransactionKey
	,CoverageJewelryItem.RiskJewelryItemKey
	,CoverageJewelryItem.CoverageLevel
	,CoverageJewelryItem.JobNumber
	,PolicyTransaction.TransactionStatus
	,CoverageJewelryItem.CoverageTypeCode
	,CoverageJewelryItem.PolicyNumber
	,CoverageJewelryItem.CoverageNumber
	,CoverageJewelryItem.EffectiveDate
	,CoverageJewelryItem.ExpirationDate
	,CoverageJewelryItem.IsTempCoverage
	,CoverageJewelryItem.ItemLimit				AS PerOccurenceLimit
	,CoverageJewelryItem.ItemDeductible			AS PerOccurenceDeductible
	,CoverageJewelryItem.ItemValue
	,CoverageJewelryItem.ItemAnnualPremium
	,CoverageJewelryItem.CoverageCode
	,DATE('{date}')								AS bq_load_date
	

--FROM `qa-edl.B_QA_ref_kimberlite.CoverageJewelryItem` AS CoverageJewelryItem
--INNER JOIN `qa-edl.B_QA_ref_kimberlite.PolicyTransaction` AS PolicyTransaction
FROM `{project}.{core_dataset}.CoverageJewelryItem` AS CoverageJewelryItem
LEFT JOIN `{project}.{core_dataset}.PolicyTransaction` AS PolicyTransaction
	ON PolicyTransaction.PolicyTransactionKey = CoverageJewelryItem.PolicyTransactionKey
	--AND PolicyTransaction.SourceSystem = CoverageJewelryItem.SourceSystem
LEFT JOIN `{project}.{dest_dataset}.v_ProductHierarchy` AS v_ProductHierarchy
	ON v_ProductHierarchy.ProductCode ='PJ'
	AND v_ProductHierarchy.SourceSystem = 'GW'
WHERE 1 = 1
AND CoverageJewelryItem.bq_load_date = DATE({partition_date})
AND PolicyTransaction.bq_load_date = DATE({partition_date})
--AND IsTransactionSliceEffective = 1	--not sure if to add yet
