/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimPJCeded_Dupes.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Duplicates'				AS UnitTest
	, TransactionPublicID
	, ClaimPublicId
	, PolicyNumber
	, COUNT(*)						AS NumRecords
	, DATE('{date}')			AS bq_load_date	

FROM `{project}.{dest_dataset}.ClaimPJCeded`
WHERE 1=1
  AND bq_load_date = DATE({partition_date}) 
GROUP BY
	 TransactionPublicID
	, ClaimPublicId
	, PolicyNumber
HAVING COUNT(*)>1
