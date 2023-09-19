/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimPJDirect_Dupes.sql
-------------------------------------------------------------------------------------------
	--Change Log--
-------------------------------------------------------------------------------------------
	01/23/2023	DROBAK		Init

-------------------------------------------------------------------------------------------
*/
SELECT 
    'Duplicates'				AS UnitTest
	, TransactionPublicID
	, TransactionLinePublicID
	, ClaimPublicId
	, PolicyNumber
	, COUNT(*)						AS NumRecords
	, DATE('{date}')			AS bq_load_date	

FROM `{project}.{dest_dataset}.ClaimPJDirect`
WHERE 1=1
  AND bq_load_date = DATE({partition_date}) 
GROUP BY
	 TransactionPublicID
	, TransactionLinePublicID
	, ClaimPublicId
	, PolicyNumber
HAVING COUNT(*)>1
