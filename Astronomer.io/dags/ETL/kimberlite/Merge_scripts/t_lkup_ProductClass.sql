-- tag: t_lkup_ProductClass - tag ends/
/**********************************************************************************************
	Kimberlite - Reference Tables
		t_lkup_ProductClass.sql
			Product Class - Product Type - Product
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	05/25/2022	DROBAK		Init (are tables, but view ties all values together for lookup)
	06/24/2022	DROBAK		Fixed Marketplace in ProductClass

	-------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------
--Table Name: t_lkup_ProductClass
-------------------------------------------------------------------------------------------
	--CREATE TABLE IF NOT EXISTS `{project}.{dest_dataset}.t_lkup_ProductClass`
	CREATE OR REPLACE TABLE  `{project}.{core_dataset}.t_lkup_ProductClass`
	(
		SourceSystem		STRING,
		ProductClassID		INT64,	
		ProductClass		STRING,
		ModifiedOn			Timestamp
	)
	OPTIONS(
			description="This reference table is part of hierarchy of Product Class - Product Type - Product"
			);
	
	--Init Fill Table
	INSERT INTO `{project}.{core_dataset}.t_lkup_ProductClass`
	VALUES	('GW', 1,	'Insurance', CURRENT_TIMESTAMP() )
			,('GW', 2,	'Shipping', CURRENT_TIMESTAMP() )
			,('GW', 3,	'Warranty', CURRENT_TIMESTAMP() )
			,('ZING', 4, 'Marketplace', CURRENT_TIMESTAMP() )
