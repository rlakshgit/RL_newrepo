-- tag: Account_DupesOverall - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Account_DupesOverall.sql

	*****  Change History  *****

	03/23/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT	'DUPES OVERALL'		AS UnitTest
			,AccountKey
			,AccountPublicID
			,COUNT(*)			AS NumRecords
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY AccountKey, AccountPublicID 
	HAVING COUNT(*)>1 --dupe check