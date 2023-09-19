--#StandardSQL
--CREATE OR REPLACE TABLE
--  `{project}.{dataset}.nonpromoted_PL_ItemLevel_MonthlyInforcePremium`
--  (AsOfDate DATE
--  ,ProductLine STRING
--  ,LOBCode STRING
--  ,PolicyNumber STRING
--  ,PolicyPeriodID INTEGER
--  ,ItemNumber NUMERIC
--  ,ItemPremium NUMERIC)
--PARTITION BY
--  DATE_TRUNC(AsOfDate, MONTH) 
--AS
SELECT *
FROM `{project}.{dataset}.nonpromoted_PJ_Item_MonthlyInforcePremium` AS PJ
UNION ALL
SELECT *
FROM `{project}.{dataset}.nonpromoted_PA_Article_MonthlyInforcePremium` AS PA
