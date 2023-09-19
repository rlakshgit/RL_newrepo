-- tag: AccountContactRole_DupesOverall - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		AccountContactRole_DupesOverall.sql

	*****  Change History  *****

	03/23/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT	'DUPES OVERALL'				AS UnitTest
			,AccountContactRoleKey
			,AccountContactRolePublicID
			,COUNT(*)					AS NumRecords
			,DATE('{date}')				AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY AccountContactRoleKey, AccountContactRolePublicID 
	HAVING COUNT(*)>1 --dupe check