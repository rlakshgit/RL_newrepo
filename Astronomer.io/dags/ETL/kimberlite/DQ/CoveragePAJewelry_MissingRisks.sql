
/***** Missing Keys *****/
SELECT 
	'MISSING RISK KEY' as UnitTest
	,CoverageLevel
	,CoveragePublicID
	,RiskPAJewelryKey
	,DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.CoveragePAJewelry` WHERE bq_load_date = DATE({partition_date})) 
WHERE RiskPAJewelryKey IS NULL AND CoverageCode = 'JewelryItemCov_JM'
UNION ALL
SELECT 
	'MISSING Transaction KEY' as UnitTest
	,CoverageLevel
	,CoveragePublicID
	,RiskPAJewelryKey
	,DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.CoveragePAJewelry` WHERE bq_load_date = DATE({partition_date})) 
WHERE PolicyTransactionKey IS NULL 