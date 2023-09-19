-- tag: BBDQ_ClaimAttribute_Missing - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ClaimAttribute_Missing.sql

	*****  Change History  *****

	11/11/2022	DROBAK		Initial Create

---------------------------------------------------------------------------------------------------
*/
--Missing Key
SELECT
		'MISSING KEY'		AS UnitTest
		, ClaimPublicId					--natural key
		, ClaimNumber
		, PolicyNumber
		, CreatedDate
		, DATE('{date}')	AS bq_load_date

FROM `{project}.{dest_dataset}.ClaimAttribute` AS ClaimAttribute
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND ClaimAttribute.ClaimAttributeKey IS NULL
  
UNION ALL

--Missing By X
SELECT	'MISSING CLAIM'				AS UnitTest
		, cc_claim.PublicId			AS ClaimPublicId			--natural key
		, cc_claim.ClaimNumber
		, cc_policy.PolicyNumber
		, cc_claim.CreateTime		AS CreatedDate
		, DATE('{date}')			AS bq_load_date

FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy
	ON cc_policy.ID = cc_claim.PolicyID
LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype
	ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
WHERE cc_claim.PublicId NOT IN (SELECT ClaimPublicId FROM `{project}.{dest_dataset}.ClaimAttribute` WHERE bq_load_date = DATE({partition_date}))
--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat claims from being processed; add TWICO
