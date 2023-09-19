--MISSING Risks In Extract

SELECT 'MISSING RISKS' as UnitTest,  BuildingPublicID, LocationPublicID, RiskBuildingKey, RiskLocationKey, DATE('{date}') AS bq_load_date 
	from (SELECT * FROM `{project}.{dest_dataset}.RiskBuilding` WHERE bq_load_date = DATE({partition_date}))
WHERE ((RiskBuildingKey IS NULL) or (RiskLocationKey IS NULL))  AND IsTransactionSliceEffective = 1 --all item coverage should be tied to a risk 
--AND Jobnumber=@jobnumber 

/*
--MISSING Risks In Extract
SELECT 'MISSING RISKS' as UnitTest, BuildingPublicID, LocationPublicID, RiskBuildingKey, RiskLocationKey, PolicyNumber, JobNumber
		, DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskBuilding` WHERE bq_load_date = DATE({partition_date}))
WHERE ((RiskBuildingKey IS NULL) OR (RiskLocationKey IS NULL)) 
--AND Jobnumber=@jobnumber 


*/