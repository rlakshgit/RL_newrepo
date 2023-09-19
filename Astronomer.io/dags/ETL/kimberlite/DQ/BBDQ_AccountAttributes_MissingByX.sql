-- tag: BBDQ_AccountAttributes_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_AccountAttributes_MissingByX.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create

---------------------------------------------------------------------------------------------------
*/
--Missing Key
SELECT
		'MISSING KEY'			AS UnitTest
		, AccountPublicID					--natural key
		, AccountNumber
		, AccountCreatedDate
		, DATE('{date}')		AS bq_load_date

FROM `{project}.{dest_dataset}.AccountAttributes` AS AccountAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND AccountKey IS NULL

UNION ALL

--Missing Foreign Key
SELECT
		'MISSING AccountHolderContactKey'		AS UnitTest
		, AccountPublicID					--natural key
		, AccountNumber
		, AccountCreatedDate
		, DATE('{date}')			AS bq_load_date

FROM `{project}.{dest_dataset}.AccountAttributes` AS AccountAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND AccountHolderContactKey IS NULL

UNION ALL

SELECT  'MISSING Acconts'				AS UnitTest
        ,PolicyTransaction.AccountKey
        ,COUNT(*)						AS Kount
        ,DATE('{date}')					AS bq_load_date
FROM (SELECT DISTINCT AccountKey FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date}) AS PolicyTransaction
WHERE NOT EXISTS 
	(SELECT AccountKey FROM  `{project}.{dest_dataset}.Account` AS Account WHERE PolicyTransaction.AccountKey = Account.AccountKey)
GROUP BY PolicyTransaction.AccountKey;