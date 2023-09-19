--MISSING Risks In Extract
SELECT 'MISSING RISKS' as UnitTest,  LocationPublicID, RiskLocationKey, DATE('{date}') AS bq_load_date 
	from (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date}))
WHERE RiskLocationKey IS NULL  --all item coverage should be tied to a risk 
--AND Jobnumber=@jobnumber 