--Dupes by Tran Article
SELECT 
	'Duplicates by ID' AS UnitTest
	--,PAJewelryPublicID
	,JobNumber
	,JewelryArticleFixedID
	,FixedArticleRank
	, count(*) as NumRecords
	, DATE('{date}') AS bq_load_date 

FROM `{project}.{dest_dataset}.RiskPAJewelry` WHERE bq_load_date = DATE({partition_date})
GROUP BY 
	PAJewelryPublicID
	,JobNumber
	,JewelryArticleFixedID
	,FixedArticleRank
HAVING COUNT(*) > 1


