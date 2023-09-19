/***Building Block***
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/12/2021	SLJ			Init
---------------------------------------------------------------------------------------------------
*/

SELECT 
    'Duplicates - Bound' AS UnitTest
	,JobNumber
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
		AND TransactionStatus = 'Bound'
GROUP BY
	JobNumber
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
HAVING COUNT(*) > 1

UNION ALL 

SELECT 
    'Duplicates - Not Bound' AS UnitTest
	,JobNumber
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
	, DATE('{date}') AS bq_load_date	
FROM `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date})
		AND TransactionStatus != 'Bound'
GROUP BY
	PolicyTransactionKey
	,JobNumber
	,LocationNumber
	,BuildingNumber
	,TransactionStatus
HAVING COUNT(*) > 1