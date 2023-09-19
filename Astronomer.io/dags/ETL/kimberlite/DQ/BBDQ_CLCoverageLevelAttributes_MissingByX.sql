/*** BBDQ_CLCoverageLevelAttributes_MissingByX.sql	***
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/12/2021	SLJ			Init
	01/18/2022	DROBAK		Not needed, so removed section 'Missing Transaction Key' AS UnitTest

---------------------------------------------------------------------------------------------------
*/
/*
SELECT 
    'Missing Transaction Key' AS UnitTest
	,JobNumber
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
--	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
--      AND TransactionStatus IS NULL
UNION ALL 
*/
SELECT 
    'Missing IM Location Key' AS UnitTest
    ,JobNumber 
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
 --   ,TransactionStatus 
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND CoverageLevel IN ('Line','SubLine','Location','SubLoc')
        AND RiskIMLocationKey IS NULL
        AND ProductCode = 'IM'
UNION ALL 
SELECT 
    'Missing IM Stock Key' AS UnitTest
    ,JobNumber 
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
--    ,TransactionStatus 
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND CoverageLevel IN ('Stock','SubStock')
        AND RiskIMStockKey IS NULL
        AND ProductCode = 'IM'
UNION ALL 
SELECT 
    'Missing BOP Location Key' AS UnitTest
    ,JobNumber 
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
--    ,TransactionStatus 
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND CoverageLevel IN ('Line','SubLine','Location','SubLoc')
        AND RiskBOPLocationKey IS NULL
        AND ProductCode = 'BOP'
UNION ALL 
SELECT 
    'Missing BOP Building Key' AS UnitTest
    ,JobNumber 
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
 --   ,TransactionStatus 
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND CoverageLevel IN ('Building')
        AND RiskBOPBuildingKey IS NULL
        AND ProductCode = 'BOP'
UNION ALL 
SELECT 
    'Missing UMB Location Key' AS UnitTest
    ,JobNumber 
    ,RiskBOPLocationKey 
    ,RiskIMLocationKey 
    ,RiskBOPBuildingKey 
    ,RiskIMStockKey 
--    ,TransactionStatus 
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND RiskBOPLocationKey IS NULL
        AND ProductCode = 'UMB'