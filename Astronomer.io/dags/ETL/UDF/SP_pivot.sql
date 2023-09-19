################################################################################################
####Function to get Maximum date from two dates#############
CREATE OR REPLACE FUNCTION `{project}.{dataset}.fn_GetMaxDate`(DateOne DATE, DateTwo DATE) RETURNS DATE AS (
CASE WHEN DateOne > DateTwo
     THEN DateOne
  ELSE
     DateTwo
  END
);

###################################################################################################

##Creating Function to replace invalid characters from column name (used for pivot Stored Procedure)###

CREATE OR REPLACE FUNCTION
`{project}.{dataset}.fn_normalize_col_name`(col_name STRING) AS (
  REPLACE(REGEXP_REPLACE(col_name,r'[/+#|]', '_'),' ','')
);


###################################################################################################

###creating pivot functionality through stored procedure ################

CREATE OR REPLACE PROCEDURE `{project}.{dataset}.sp_pivot`(
  table_name STRING
  , destination_table STRING
  , row_ids ARRAY<STRING>
  , pivot_col_name STRING
  , pivot_col_value STRING
  , max_columns INT64
  , aggregation STRING
  , optional_limit STRING
  )
BEGIN
  DECLARE pivotter STRING;
  EXECUTE IMMEDIATE (
    "SELECT STRING_AGG(' "||aggregation
    ||"""(IF('||@pivot_col_name||'="'||x.value||'", '||@pivot_col_value||', null)) '||`{project}.{dataset}.fn_normalize_col_name`(x.value))
   FROM UNNEST((
       SELECT APPROX_TOP_COUNT("""||pivot_col_name||", @max_columns) FROM `"||table_name||"`)) x"
  ) INTO pivotter
  USING pivot_col_name AS pivot_col_name, pivot_col_value AS pivot_col_value, max_columns AS max_columns;


  EXECUTE IMMEDIATE (
   'CREATE OR REPLACE TABLE `'||destination_table
   ||'` AS SELECT '
   ||(SELECT STRING_AGG(x) FROM UNNEST(row_ids) x)
   ||', '||pivotter
   ||' FROM `'||table_name||'` GROUP BY '
   || (SELECT STRING_AGG(''||(i+1)) FROM UNNEST(row_ids) WITH OFFSET i)||' ORDER BY '
   || (SELECT STRING_AGG(''||(i+1)) FROM UNNEST(row_ids) WITH OFFSET i)
   ||' '||optional_limit
  );
END;




