-- tag: RiskLocationIM_DupesOverall - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Checks
	RiskLocationIM_DupesOverall.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/01/2021	SLJ			INIT
	08/11/2022	DROBAK		Added RiskLevel, w/addition of PrimaryLocation recs, & FixedLocationRank = 1
	
	-------------------------------------------------------------------------------------------
*/
--DUPES In Extract
SELECT	'DUPES OVERALL'		AS UnitTest
		,LocationPublicID
		, RiskLevel
		,COUNT(*)			AS NumRecords
		,DATE('{date}')		AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date}))
WHERE FixedLocationRank = 1
GROUP BY LocationPublicID, RiskLevel
HAVING COUNT(*) > 1

	
	