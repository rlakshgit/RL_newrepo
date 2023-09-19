-- tag: Address_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Address_MissingByX.sql

	*****  Change History  *****

	04/11/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--Missing By X
	SELECT	'MISSING KEY'		AS UnitTest
			,AddressKey
			,AddressPublicID
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date}))
	WHERE AddressKey IS NULL
  
UNION ALL

--Missing by X
	SELECT	'MISSING PC ADDRESSS'	AS UnitTest
			,pc_address.PublicID	AS SourceAddressPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS pc_address
	WHERE pc_address.PublicID NOT IN (SELECT AddressPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Account` WHERE bq_load_date = DATE({partition_date})))
  
UNION ALL

--Missing by X
	SELECT	'MISSING BC ADDRESSS'	AS UnitTest
			,bc_address.PublicID	AS SourceAddressPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address
	WHERE bc_address.PublicID NOT IN (SELECT AddressPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Account` WHERE bq_load_date = DATE({partition_date})))
  
UNION ALL

--Missing by X
	SELECT	'MISSING CC ADDRESSS'	AS UnitTest
			,cc_address.PublicID	AS SourceAddressPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
	WHERE cc_address.PublicID NOT IN (SELECT AddressPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Account` WHERE bq_load_date = DATE({partition_date})))
  
UNION ALL

--Missing by X
	SELECT	'MISSING CM ADDRESSS'	AS UnitTest
			,ab_address.PublicID	AS SourceAddressPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address
	WHERE ab_address.PublicID NOT IN (SELECT AddressPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Account` WHERE bq_load_date = DATE({partition_date})))
