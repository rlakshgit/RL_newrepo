-- tag: AccountContactRole_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		AccountContactRole_MissingByX.sql

	*****  Change History  *****

	03/23/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--Missing By X
	SELECT	'MISSING KEY'				AS UnitTest
			,AccountContactRoleKey
			,AccountContactRolePublicID
			,DATE('{date}')				AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date}))
	WHERE AccountContactRoleKey IS NULL
  
UNION ALL

--Missing By X
	SELECT	'MISSING AccountKey'				AS UnitTest
			,AccountContactRoleKey
			,AccountContactRolePublicID
			,DATE('{date}')						AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date}))
	WHERE AccountKey IS NULL

UNION ALL

--Missing By X
	SELECT	'MISSING ContactKey'				AS UnitTest
			,AccountContactRoleKey
			,AccountContactRolePublicID
			,DATE('{date}')						AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date}))
	WHERE ContactKey IS NULL

