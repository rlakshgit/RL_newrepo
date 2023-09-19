--CREATE OR REPLACE TABLE
--  `{project}.{dataset}.nonpromoted_PL_Policy_MonthlyInforcePremium`
--  (AsOfDate DATE
--  ,ProductLine STRING
--  ,LOBCode STRING
--  ,PolicyNumber STRING
--  ,PolicyPeriodID INTEGER
--  ,ItemCount INTEGER
--  ,PolicyPremium NUMERIC
--  ,InforcePremiumRange STRING)
--PARTITION BY
--  DATE_TRUNC(AsOfDate, MONTH) 
--AS
SELECT
	AsOfDate
	,ProductLine
	,LOBCode
	,PolicyNumber
	,PolicyPeriodID
	,COUNT(DISTINCT ItemNumber) AS ItemCount
	,SUM(ItemPremium) AS PolicyPremium
	,CASE
		WHEN SUM(ItemPremium) BETWEEN 0 AND 100 THEN '< $100'
		WHEN SUM(ItemPremium) BETWEEN 101 AND 200 THEN '$101-$200'
		WHEN SUM(ItemPremium) BETWEEN 201 AND 300 THEN '$201-$300'
		WHEN SUM(ItemPremium) BETWEEN 301 AND 500 THEN '$301-$500'
		WHEN SUM(ItemPremium) BETWEEN 501 AND 1000 THEN '$501-$1000'
		WHEN SUM(ItemPremium) > 1000 THEN '$1000+'
		ELSE 'unknown'
		END AS InforcePremiumRange

FROM `{project}.{dataset}.nonpromoted_PL_ItemLevel_MonthlyInforcePremium`
GROUP BY
	AsOfDate
	,ProductLine
	,LOBCode
	,PolicyNumber
	,PolicyPeriodID
