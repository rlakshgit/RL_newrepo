-- tag: t_lkup_Product - tag ends/
/**********************************************************************************************
	Kimberlite - Core
		Reference and Lookup Tables
			t_lkup_Product.sql
				Product Class - Product Type - Product
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	05/25/2022	DROBAK		Init (are tables, but view ties all values together for lookup)
	06/24/2022	DROBAK		DG Added Cargo - US and Canada
	04/30/2023	DROBAK		Add BusinessUnitID
	05/25/2023	DROBK		Add DefaultASL

	-------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------
--Table Name: t_lkup_Product
-------------------------------------------------------------------------------------------
	CREATE OR REPLACE TABLE  `{project}.{core_dataset}.t_lkup_Product`
	(
		SourceSystem		STRING,
		ProductID			INT64,	
		Product				STRING,
		ProductCode			STRING,
		ProductTypeID		INT64,
		ProductClassID		INT64,
		BusinessUnitID		INT64,
		DefaultASL			INT64,
		--CCProductCode		STRING,
		--PCProductCode		STRING,
		ModifiedOn			Timestamp
	)
	OPTIONS(
			description="This reference table is part of hierarchy of Product Class - Product Type - Product plus link to BusinessUnit "
			);

	--Init Fill Table
	INSERT INTO `{project}.{core_dataset}.t_lkup_Product`
	VALUES	('GW', 1,	'Jewelers Block', 'JB', 3, 1, 2, 90, CURRENT_TIMESTAMP() )
			,('GW', 2,	'Jewelers Standard', 'JS', 3, 1, 2, 90, CURRENT_TIMESTAMP() )
			,('GW', 3,	'Businessowners Policy', 'BOP', 2, 1, 2, NULL, CURRENT_TIMESTAMP() )
			,('GW', 4,	'Umbrella Liability', 'UMB', 4, 1, 2, 52, CURRENT_TIMESTAMP() )
			,('GW', 5,	'Personal Jewelry', 'PJ', 1, 1, 1, 90, CURRENT_TIMESTAMP() )
			,('GW', 6,	'Personal Article', 'PA',  1, 1, 1, 90, CURRENT_TIMESTAMP() )
			,('GW', 7,	'Cargo (Canada)', 'CC',  3, 1, 2, 90, CURRENT_TIMESTAMP() )
			,('GW', 8,	'Cargo (US)', 'CU', 5, 1, 2, 80, CURRENT_TIMESTAMP() )
			,('ZING', 9, 'Shipping Label', 'SL',  NULL, 2, 3, NULL, CURRENT_TIMESTAMP() )
			,('GW', 10, 'Shipping Insurance', 'SI', NULL, 2, 3, NULL, CURRENT_TIMESTAMP() )
/*
	VALUES	('GW', 1,	'Jewelers Block', 'JB', 3, 1, 2, 'JB_ILMLine', CURRENT_TIMESTAMP() )
			,('GW', 2,	'Jewelers Standard', 'JS', 3, 1, 2, 'JS_ILMLine', CURRENT_TIMESTAMP() )
			,('GW', 3,	'Businessowners Policy', 'BOP', 2, 1, 2, 'BOPLine', CURRENT_TIMESTAMP() )
			,('GW', 4,	'Umbrella Liability', 'UMB', 4, 1, 2, 'UMBLine', CURRENT_TIMESTAMP() )
			,('GW', 5,	'Personal Jewelry', 'PJ', 1, 1, 1, 'JMICPJBLine', CURRENT_TIMESTAMP() )
			,('GW', 6,	'Personal Article', 'PA',  1, 1, 1, 'JPALine', CURRENT_TIMESTAMP() )
			,('GW', 7,	'Cargo (Canada)', 'CC',  3, 1, 2, '55-016512', CURRENT_TIMESTAMP() )
			,('GW', 8,	'Cargo (US)', 'CU', 5, 1, 2, '55-013722', CURRENT_TIMESTAMP() )
			,('ZING', 9, 'Shipping Label', 'SL',  NULL, 2, 3, NULL, CURRENT_TIMESTAMP() )
			,('GW', 10, 'Shipping Insurance', 'SI', NULL, 2, 3, NULL, CURRENT_TIMESTAMP() )
*/