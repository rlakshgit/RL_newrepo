/**** RiskPAJewelryFeature_DupesOverall.sql ********

 *****  Change History  *****

	06/28/2021	SLJ		Init

****************************************************/
--Dupes by key
	SELECT 
		'Duplicates by ID' AS UnitTest
		,CAST(NULL as STRING)as PAJewelryPublicID
		,PAJewelryFeaturePublicID
		,JobNumber
		,CAST(NULL AS INT64) as PAJewelryFeatureFixedID
		,CAST(NULL AS INT64) as FixedFeatureRank
		, count(*) as NumRecords
		, DATE('{date}') AS bq_load_date 

	FROM (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelryFeature` WHERE bq_load_date = DATE({partition_date})) RiskPAJewelryFeature	--#PAJwlryFeatureData
	GROUP BY 
		PAJewelryFeaturePublicID
		,JobNumber
	HAVING COUNT(*) > 1

	UNION ALL
	
--Dupes by Tran Article feature
	SELECT 
		'Duplicates by ID' AS UnitTest
		,PAJewelryPublicID
		,CAST(NULL AS STRING) as PAJewelryFeaturePublicID
		,JobNumber
		,PAJewelryFeatureFixedID
		,FixedFeatureRank
		, count(*) as NumRecords
		, DATE('{date}') AS bq_load_date 		

	FROM (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelryFeature` WHERE bq_load_date = DATE({partition_date})) RiskPAJewelryFeature	--#PAJwlryFeatureData
	GROUP BY 
		PAJewelryPublicID
		,JobNumber
		,PAJewelryFeatureFixedID
		,FixedFeatureRank
	HAVING COUNT(*) > 1

