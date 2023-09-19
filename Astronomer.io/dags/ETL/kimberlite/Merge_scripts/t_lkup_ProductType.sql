-- tag: t_lkup_ProductType - tag ends/
/**********************************************************************************************
	Kimberlite - Reference Tables
		t_lkup_ProductType.sql
			Product Class - Product Type - Product
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	05/25/2022	DROBAK		Init (are tables, but view ties all values together for lookup)

	-------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------
--Table Name: t_lkup_ProductType
-------------------------------------------------------------------------------------------
	CREATE OR REPLACE TABLE  `{project}.{core_dataset}.t_lkup_ProductType`
	--CREATE TABLE IF NOT EXISTS `{project}.{dest_dataset}.t_lkup_ProductType`
	(
		SourceSystem		STRING,
		ProductTypeID		INT64,	
		ProductType			STRING,
		ProductClassID		INT64,
		ModifiedOn			Timestamp
	)
	OPTIONS(
			description="This reference table is part of hierarchy of Product Class - Product Type - Product"
			);

	--Init Fill Table
	INSERT INTO `{project}.{core_dataset}.t_lkup_ProductType`
	VALUES	('GW', 1,	'Other Personal Inland Marine', 1, CURRENT_TIMESTAMP() )
			,('GW', 2,	'Commercial Multiperil Liability and Non-liability', 1, CURRENT_TIMESTAMP() )
			,('GW', 3,	'Other Commercial Inland Marine', 1, CURRENT_TIMESTAMP() )
			,('GW', 4,	'Other Liability', 1, CURRENT_TIMESTAMP() )
			,('GW', 5,	'Ocean Marine', 1, CURRENT_TIMESTAMP() )