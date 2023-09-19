--#StandardSQL
--CREATE OR REPLACE TABLE
--  `{project}.{dataset}.nonpromoted_CL_Policy_MonthlyInforcePremium`
--  (AsOfDate DATE
--  ,PolicyNumber STRING
--  ,ProductLine STRING
--  ,LOBCode STRING
--  ,PolicyType STRING
--  ,Country STRING
--  ,InforcePolicyPeriodID INTEGER
--  ,ASL INTEGER
--  ,LocationCount INTEGER
--  ,RolexIndicator STRING
--  ,PolicyPremium NUMERIC)
--PARTITION BY
--  DATE_TRUNC(AsOfDate, MONTH)
--AS
SELECT
  AsOfDate
  ,PolicyNumber
  ,ProductLine
  ,LOBCode
  ,PolicyType
  ,Country
  ,InforcePolicyPeriodID
  ,ASL
  ,COUNT(DISTINCT LocationNumber) AS LocationCount
  ,RolexIndicator
  ,SUM(LocationPremium) AS PolicyPremium
FROM `{project}.{dataset}.nonpromoted_CL_Location_MonthlyInforcePremium`
GROUP BY
  AsOfDate
  ,PolicyNumber
  ,ProductLine
  ,LOBCode
  ,PolicyType
  ,Country
  ,InforcePolicyPeriodID
  ,ASL
  ,RolexIndicator