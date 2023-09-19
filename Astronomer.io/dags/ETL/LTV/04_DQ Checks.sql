##### source duplicate check
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_source_duplicates` AS 
SELECT  
  JobNumber
  ,ItemNumber

FROM `{project}.{dataset}.t_ltv_source` AS attr

WHERE   1 = 1
        AND attr.ItemRank = 1
        AND attr.IsInactive = 0
        AND attr.CoverageTypeCode = 'SCH'
        AND attr.InsuredCountry = 'USA'


GROUP BY 
  JobNumber
  ,ItemNumber 

HAVING COUNT(*) > 1
;

##### LTV duplicate check
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_ltv_duplicates` AS 
SELECT  
  JobNumber
  ,allversion_load_date

FROM `{project}.{dataset}.t_ltv_allversions` AS attr

GROUP BY 
  JobNumber
  ,allversion_load_date

HAVING COUNT(DISTINCT Retention) > 1 OR COUNT(DISTINCT PolicyProfile) > 1
;

##### LTV NULL check
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_ltv_null` AS 
SELECT DISTINCT
  JobNumber

FROM `{project}.{dataset}.t_ltv_allversions` AS attr

WHERE Retention IS NULL OR PolicyProfile IS NULL
;

##### current version duplicate check
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_currentversion_ltv_duplicates` AS 
SELECT  
  AccountNumber,
  PolicyNumber,
  currentversion_load_date

FROM `{project}.{dataset}.t_ltv_currentversions` AS attr

GROUP BY 
  AccountNumber,
  PolicyNumber,
  currentversion_load_date

HAVING COUNT(*) > 1