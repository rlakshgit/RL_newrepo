-- tag: Contact_DupesOverall - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Contact_DupesOverall.sql

	*****  Change History  *****

	04/11/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT	'DUPES OVERALL'		AS UnitTest
			,ContactKey
			,ContactPublicID
			,COUNT(*)			AS NumRecords
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY ContactKey, ContactPublicID 
	HAVING COUNT(*)>1 --dupe check