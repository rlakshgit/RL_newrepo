	
--unit test w/ Dupe records
SELECT 
	'DUPE by ID' as UnitTest
	,dt.LocationPublicID
	,dt.JobNumber 
	,dt.LocationNumber
	,CAST(NULL AS INT64) as FixedLocationRank
	,Count(1) as NumRecords
	,DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date})) AS dt	--#RiskLocBOPTemp 
WHERE 1 = 1
GROUP BY 
	dt.LocationPublicID
	,dt.JobNumber 
	,dt.LocationNumber
HAVING count(1)>1

UNION ALL

--unit test w/ Dupe records
SELECT 
	'DUPE by Tran-Loc' as UnitTest
	,CAST(NULL AS STRING) as LocationPublicID
	,dt.JobNumber 
	,dt.LocationNumber
	,dt.FixedLocationRank
	,Count(1) as NumRecords
	,DATE('{date}') AS bq_load_date 	
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date})) AS dt	
WHERE 1 = 1
GROUP BY 
	dt.LocationPublicID
	,dt.JobNumber 
	,dt.LocationNumber
	,dt.FixedLocationRank
HAVING count(1)>1