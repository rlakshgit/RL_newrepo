-- tag: Address_DupesOverall - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Address_DupesOverall.sql

	*****  Change History  *****

	04/11/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--DUPES In Extract
	SELECT	'DUPES OVERALL'		AS UnitTest
			,AddressKey
			,AddressPublicID
			,COUNT(*)			AS NumRecords
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date}))
	GROUP BY AddressKey, AddressPublicID 
	HAVING COUNT(*)>1 --dupe check