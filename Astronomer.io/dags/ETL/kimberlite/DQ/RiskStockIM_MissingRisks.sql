-- tag: RiskStockIM_MissingByX - tag ends/
/**** RiskStockIM_MissingByX.sql ********
 
 *****  Change History  *****
 --------------------------------------------------------------------------------------------------------------------
	06/01/2021	SLJ		Init
--------------------------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
SELECT 
	'MISSING RISKS' as UnitTest
	,StockPublicID
	,RiskStockKey
	, DATE('{date}') AS bq_load_date 	
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date}))
WHERE	1 = 1
	AND RiskStockKey IS NULL