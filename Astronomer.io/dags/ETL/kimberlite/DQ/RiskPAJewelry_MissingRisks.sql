--Missing Article Jewelery Key
SELECT 
	'Missing Article Key' AS UnitTest
	,RiskPAJewelryKey
	,JobNumber
	, DATE('{date}') AS bq_load_date 

FROM (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelry` WHERE bq_load_date = DATE({partition_date}))
WHERE RiskPAJewelryKey IS NULL  --all item coverage should be tied to a risk 