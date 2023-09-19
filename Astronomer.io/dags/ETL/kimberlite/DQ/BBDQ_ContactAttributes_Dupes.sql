-- tag: BBDQ_ContactAttributes_Dupes - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ContactAttributes_Dupes.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/
--DUPES In Extract
	SELECT	'DUPES OVERALL'			AS UnitTest
			, ContactKey
			, ContactPublicID
			, COUNT(*)				AS NumRecords
			, DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{dest_dataset}.ContactAttributes` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY ContactKey, ContactPublicID 
	HAVING COUNT(*)>1 --dupe check