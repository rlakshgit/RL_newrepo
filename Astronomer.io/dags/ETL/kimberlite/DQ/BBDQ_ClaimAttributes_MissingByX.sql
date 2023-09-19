-- tag: BBDQ_ClaimAttributes_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimAttributes_MissingByX.sql

	*****  Change History  *****

	03/22/2023	DROBAK		Initial Create

---------------------------------------------------------------------------------------------------
*/
--Missing Key
SELECT
		'MISSING KEY'			AS UnitTest
		, ClaimPublicId					--natural key
		, ClaimNumber
		, PolicyNumber
		, CreatedDate
		, DATE('{date}')		AS bq_load_date

FROM `{project}.{dest_dataset}.ClaimAttributes` AS ClaimAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND ClaimAttributes.ClaimKey IS NULL
  
UNION ALL

--Missing Foreign Key
SELECT
		'MISSING POLICY KEY'			AS UnitTest
		, PolicyPeriodPublicID
		, ClaimPublicId					--natural key
		, ClaimNumber
		, PolicyNumber
		, CreatedDate
		, DATE('{date}')		AS bq_load_date

FROM `{project}.{dest_dataset}.ClaimAttributes` AS ClaimAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND ClaimAttributes.PolicyTransactionKey IS NULL
  AND IsVerified = 1		--Want to eliminate false positives from Legacy claims (pre-GW) with shell Policy number, Mistakes
  

