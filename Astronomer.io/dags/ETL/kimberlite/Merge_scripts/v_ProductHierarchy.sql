-- tag: v_ProductHierarchy - tag ends/
/**********************************************************************************************
	Kimberlite - Building Block
		Reference and Lookup Tables
			v_ProductHierarchy.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	05/25/2022	DROBAK		Init (are tables, but view ties all values together for lookup)
	05/16/2023	DROBAK		Add BusinessUnit fields

	-------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------
--View Name: v_ProductHierarchy
-------------------------------------------------------------------------------------------

--CREATE VIEW `qa-edl.B_QA_ref_kimberlite.v_ProductHierarchy`
CREATE OR REPLACE VIEW `{project}.{dest_dataset}.v_ProductHierarchy`
AS(
SELECT
    t_lkup_Product.SourceSystem,
    t_lkup_Product.Product,
	t_lkup_Product.ProductCode,
    t_lkup_ProductType.ProductType,
    t_lkup_ProductClass.ProductClass,
    BusinessUnit.BusinessUnitCode,
    BusinessUnit.BusinessUnitShortName
FROM `{project}.{core_dataset}.t_lkup_Product` AS t_lkup_Product
LEFT JOIN `{project}.{core_dataset}.t_lkup_ProductType` AS t_lkup_ProductType
  ON t_lkup_Product.ProductTypeID = t_lkup_ProductType.ProductTypeID
LEFT JOIN `{project}.{core_dataset}.t_lkup_ProductClass` AS t_lkup_ProductClass
  ON t_lkup_Product.ProductClassID = t_lkup_ProductClass.ProductClassID
LEFT JOIN `{project}.{core_dataset}.BusinessUnit` AS BusinessUnit
  ON t_lkup_Product.BusinessUnitID = BusinessUnit.BusinessUnitID
);
--SELECT * FROM `qa-edl.B_QA_ref_kimberlite.v_ProductHierarchy`