/*****************************************
  BIGQUERY FUNCTION
    fn_GetDefaultMinDateTime.sql
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
-- Place in custom_functions
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	05/15/2023	DROBAK		Initial build

-------------------------------------------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION
  `{project}.custom_functions.fn_GetDefaultMinDateTime`()
  RETURNS DATETIME AS ( CAST("1990-01-01" AS DATETIME)) OPTIONS (description = "Returns default DATETIME for 1990-01-01");