-- tag: BBDQ_AccountContactRoleAttributes_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_AccountContactRoleAttributes_MissingByX.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create

---------------------------------------------------------------------------------------------------
*/
--Missing Key
SELECT
		'MISSING KEY'			AS UnitTest
		, AccountContactRolePublicID					--natural key
		, AccountNumber
		, AccountCreatedDate
		, DATE('{date}')		AS bq_load_date

FROM `{project}.{dest_dataset}.AccountContactRoleAttributes` AS AccountContactRoleAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND AccountContactRoleKey IS NULL

UNION ALL

--Missing Foreign Key
SELECT
		'MISSING AccountHolderContactKey'		AS UnitTest
		, AccountContactRolePublicID
		, AccountPublicID
		, AccountNumber
		, AccountCreatedDate
		, DATE('{date}')			AS bq_load_date

FROM `{project}.{dest_dataset}.AccountContactRoleAttributes` AS AccountContactRoleAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND AccountKey IS NULL