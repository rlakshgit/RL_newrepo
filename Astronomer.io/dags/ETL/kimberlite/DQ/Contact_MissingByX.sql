-- tag: Contact_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Contact_MissingByX.sql

	*****  Change History  *****

	04/11/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--Missing By X
	SELECT	'MISSING KEY'		AS UnitTest
			,ContactPublicID
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date}))
	WHERE ContactKey IS NULL
	
	UNION ALL

--Missing by X
	SELECT	'MISSING PC CONTACT'	AS UnitTest
			,pc_contact.PublicID	AS SourceContactPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact
	WHERE pc_contact.PublicID NOT IN (SELECT ContactPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})))

	UNION ALL

--Missing by X
	SELECT	'MISSING BC CONTACT'	AS UnitTest
			,bc_contact.PublicID	AS SourceContactPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{bc_dataset}.bc_contact` WHERE _PARTITIONTIME = {partition_date}) AS bc_contact
	WHERE bc_contact.PublicID NOT IN (SELECT ContactPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})))

	UNION ALL

--Missing by X
	SELECT	'MISSING CC CONTACT'	AS UnitTest
			,cc_contact.PublicID	AS SourceContactPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact
	WHERE cc_contact.PublicID NOT IN (SELECT ContactPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})))

	UNION ALL

--Missing by X
	SELECT	'MISSING CM CONTACT'	AS UnitTest
			,ab_abcontact.PublicID	AS SourceContactPublicID
			,DATE('{date}')			AS bq_load_date

	FROM (SELECT * FROM `{project}.{cm_dataset}.ab_abcontact` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontact
	WHERE ab_abcontact.PublicID NOT IN (SELECT ContactPublicID FROM (SELECT * FROM `{project}.{dest_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})))
