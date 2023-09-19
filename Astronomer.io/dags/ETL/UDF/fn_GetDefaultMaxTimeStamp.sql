/*****************************************
  BIGQUERY FUNCTION
    fn_GetDefaultMaxDateTime.sql
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
-- Place in custom_functions
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	10/24/2022	DROBAK		Initial build
  2/1/2023  DROBAK		replicate fn_GetDefaultMaxDateTime(), returning a timestamp 

-------------------------------------------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION
  `{project}.custom_functions.fn_GetDefaultMaxTimeStamp`()
  RETURNS TIMESTAMP AS ( CAST("9999-12-31" AS TIMESTAMP)) OPTIONS (description = 'Returns 9999-12-31');