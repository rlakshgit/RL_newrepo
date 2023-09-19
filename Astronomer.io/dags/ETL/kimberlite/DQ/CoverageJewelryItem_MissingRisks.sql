/**********************************************************************************************
	Kimberlite - DQ Checks
	CoverageJewelryItem_MissingRisks
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/01/2021	DROBAK		Added CoverageTypeCode = 'SCH'
	12/22/2021	SLJ			Added missing key check for transaction

	-------------------------------------------------------------------------------------------
*/
SELECT 
	'MISSING RISK KEY'			AS UnitTest
	,CoverageLevel
	,CoveragePublicID
	,RiskJewelryItemKey
	,PolicyNumber
	,JobNumber
	,DATE('{date}')				AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) 
WHERE RiskJewelryItemKey IS NULL AND CoverageCode = 'JewelryItemCov_JMIC_PL'
UNION ALL
SELECT 
	'MISSING TRANSACTION KEY'	AS UnitTest
	,CoverageLevel
	,CoveragePublicID
	,RiskJewelryItemKey
	,PolicyNumber
	,JobNumber
	,DATE('{date}')				AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) 
WHERE PolicyTransactionKey IS NULL 