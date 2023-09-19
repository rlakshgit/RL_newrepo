-- tag: BBDQ_ContactAttributes_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check for Building Block 
		BBDQ_ContactAttributes_MissingByX.sql

	*****  Change History  *****

	03/27/2023	DROBAK		Initial Create

---------------------------------------------------------------------------------------------------
*/
--Missing Key
SELECT
		'MISSING KEY'			AS UnitTest
		, ContactPublicID
		, AddressPublicID
		, MailingAddressID
		, DATE('{date}')		AS bq_load_date

FROM `{project}.{dest_dataset}.ContactAttributes` AS ContactAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND ContactKey IS NULL

UNION ALL

--Missing Foreign Key
SELECT
		'MISSING AddressPublicID'		AS UnitTest
		, ContactPublicID					--natural key
		, MailingAddressID
		, DATE('{date}')			AS bq_load_date

FROM `{project}.{dest_dataset}.ContactAttributes` AS ContactAttributes
WHERE 1=1
  AND bq_load_date = DATE({partition_date})
  AND AddressPublicID IS NULL