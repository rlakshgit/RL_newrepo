-- tag: BBDQ_AccountAttributes_Dupes - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_AccountAttributes_Dupes.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/
--DUPES In Extract
	SELECT	'DUPES OVERALL'			AS UnitTest
			, AccountKey
			, AccountPublicID
			, COUNT(*)				AS NumRecords
			, DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{dest_dataset}.AccountAttributes` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY AccountKey, AccountPublicID 
	HAVING COUNT(*)>1 --dupe check