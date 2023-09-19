/***Building Block***
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/12/2021	SLJ			Init
---------------------------------------------------------------------------------------------------
*/

SELECT 
    'Missing Transaction' AS UnitTest
	,JobNumber
    ,RiskLocationKey
    ,RiskBuildingKey
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND TransactionStatus IS NULL

UNION ALL
 
SELECT 
    'Missing Location' AS UnitTest
	,JobNumber
    ,RiskLocationKey
    ,RiskBuildingKey
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND LocationNumber IS NULL 

UNION ALL
 
SELECT 
    'Missing Building' AS UnitTest
	,JobNumber
    ,RiskLocationKey
    ,RiskBuildingKey
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
        AND BuildingNumber IS NULL