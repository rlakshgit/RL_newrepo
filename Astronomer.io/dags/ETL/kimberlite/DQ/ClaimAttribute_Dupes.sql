-- tag: BBDQ_ClaimAttribute_Dupes - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimAttribute_Dupes.sql

	*****  Change History  *****

	11/11/2022	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT 'DUPES OVERALL' as UnitTest, ClaimAttributeKey, ClaimPublicId, COUNT(*) AS NumRecords, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimAttribute` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY ClaimAttributeKey, ClaimPublicId 
	HAVING COUNT(*)>1 --dupe check