-- tag: BBDQ_ClaimAttributes_Dupes - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimAttributes_Dupes.sql

	*****  Change History  *****

	03/22/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT 'DUPES OVERALL' as UnitTest, ClaimKey, ClaimPublicId, COUNT(*) AS NumRecords, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimAttributes` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY ClaimKey, ClaimPublicId 
	HAVING COUNT(*)>1 --dupe check