-- tag: BBDQ_AccountContactRoleAttributes_Dupes - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_AccountContactRoleAttributes_Dupes.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/
--DUPES In Extract
	SELECT	'DUPES OVERALL'					AS UnitTest
			, AccountContactRoleKey
			, AccountContactRolePublicID
			, COUNT(*)						AS NumRecords
			, DATE('{date}')				AS bq_load_date

	FROM (SELECT * FROM `{project}.{dest_dataset}.AccountContactRoleAttributes` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY AccountContactRoleKey, AccountContactRolePublicID 
	HAVING COUNT(*)>1 --dupe check