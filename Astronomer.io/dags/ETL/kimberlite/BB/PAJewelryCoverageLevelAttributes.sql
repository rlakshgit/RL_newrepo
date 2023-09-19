DELETE `{project}.{dest_dataset}.PAJewelryCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.PAJewelryCoverageLevelAttributes` 
	( 
		SourceSystem,
		ProductCode,
		ProductType,
		PAJewelryCoverageKey,
		PolicyTransactionKey,
		RiskPAJewelryKey,
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
		PAJewelryCoverageLevelAttributes.sql
			Personal Article Jewelry Attributes and Feature Details
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	06/08/2021	DROBAK		Init (is a table, but could be a view)
	07/08/2021	DROBAK		Add PolicyTransaction.TransactionStatus and join to PolicyTransaction
	08/26/2021	DROBAK		Make join to PolicyTransaction a LEFT JOIN
	11/09/2021	DROBAK		Added date code; fixed BQ dataset variable
	06/03/2022	DROBAK		Added: AND IsTransactionSliceEffective = 1, ProductCode, ProductType

	-------------------------------------------------------------------------------------------
*/
SELECT
	CoveragePAJewelry.SourceSystem
    ,v_ProductHierarchy.ProductCode
	,v_ProductHierarchy.ProductType
	,CoveragePAJewelry.PAJewelryCoverageKey
	,CoveragePAJewelry.PolicyTransactionKey
	,CoveragePAJewelry.RiskPAJewelryKey
	,CoveragePAJewelry.CoverageLevel
	,CoveragePAJewelry.JobNumber
	,PolicyTransaction.TransactionStatus
	,CoveragePAJewelry.CoverageTypeCode
	,CoveragePAJewelry.PolicyNumber
	,CoveragePAJewelry.CoverageNumber
	,CoveragePAJewelry.EffectiveDate
	,CoveragePAJewelry.ExpirationDate
	,CoveragePAJewelry.IsTempCoverage
	,CoveragePAJewelry.ItemLimit				AS PerOccurenceLimit
	,CoveragePAJewelry.ItemDeductible			AS PerOccurenceDeductible
	,CoveragePAJewelry.ItemValue
	,CoveragePAJewelry.ItemAnnualPremium
	,CoveragePAJewelry.CoverageCode
	,DATE('{date}')								AS bq_load_date

--FROM `qa-edl.B_QA_ref_kimberlite.CoveragePAJewelry` AS CoveragePAJewelry
--LEFT JOIN (SELECT * FROM `qa-edl.B_QA_ref_kimberlite.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction
FROM `{project}.{core_dataset}.CoveragePAJewelry` AS CoveragePAJewelry
LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction
	ON PolicyTransaction.PolicyTransactionKey = CoveragePAJewelry.PolicyTransactionKey
	--AND PolicyTransaction.SourceSystem = CoveragePAJewelry.SourceSystem
LEFT JOIN `{project}.{dest_dataset}.v_ProductHierarchy` AS v_ProductHierarchy
	ON v_ProductHierarchy.ProductCode ='PA'
	AND v_ProductHierarchy.SourceSystem = 'GW'
WHERE 1 = 1
AND CoveragePAJewelry.bq_load_date = DATE({partition_date})
AND IsTransactionSliceEffective = 1