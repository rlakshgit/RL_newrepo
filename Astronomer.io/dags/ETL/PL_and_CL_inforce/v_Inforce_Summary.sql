CREATE or REPLACE VIEW {project}.{dataset}.v_PL_Itemlevel_summary
AS
with cte as
(
SELECT
EXTRACT(YEAR from InforceDate) YEAR
  ,QuarterNumber QTR
  ,Extract(MONTH from InforceDate) MONTH
  ,InforceDate
  ,PolicyNumber POLICY_NO
  ,InforceCountryCode AS COUNTRY_CODE
  ,InforceCountryDesc AS COUNTRY_DESC
  ,PrimaryInsured AS POLICY_STATE
  ,SUM(ItemLimit)																				AS ITEM_LIMIT
  ,COUNT(DISTINCT (CASE WHEN ItemNumber<> 0 THEN ItemNumber END))									AS ITEM_COUNT
  ,SUM(PremiumInforce)																			AS INFORCE_PREMIUM
  ,CASE
    WHEN SUM(PremiumInforce) BETWEEN 0 AND 100 THEN '< $100'
    WHEN SUM(PremiumInforce) BETWEEN 101 AND 200 THEN '$101-$200'
    WHEN SUM(PremiumInforce) BETWEEN 201 AND 300 THEN '$201-$300'
    WHEN SUM(PremiumInforce) BETWEEN 301 AND 500 THEN '$301-$500'
    WHEN SUM(PremiumInforce) BETWEEN 501 AND 1000 THEN '$501-$1,000'
    WHEN SUM(PremiumInforce) > 1000 THEN '$1,000 +'
    ELSE 'unknown'
    END																							AS INF_PREM_RNG
  ,CASE WHEN (PolicyEffectiveDate - PolicyOrigEffDate ) < 15 THEN 'NB' ELSE 'REN' END							AS NB_REN


FROM `{project}.{dataset}.pl_items_monthly_inforce` Where BusinessTypeDesc = 'Direct'

GROUP BY
  YEAR
  ,QuarterNumber
  ,MONTH
  ,InforceDate
  ,PrimaryInsured
  ,InforceCountryCode
  ,InforceCountryDesc
  ,PolicyNumber
  ,CASE WHEN (PolicyEffectiveDate - PolicyOrigEffDate) < 15 THEN 'NB' ELSE 'REN' END
  )
  SELECT
 YEAR
 ,QTR
 ,MONTH
 ,InforceDate
 ,COUNTRY_CODE
 ,COUNTRY_DESC
 ,POLICY_STATE
 ,NB_REN
 ,INF_PREM_RNG
 ,COUNT(DISTINCT POLICY_NO)																		AS PL_POLICY_COUNT
 ,SUM(ITEM_LIMIT)																				AS TOTAL_ITEM_LIMIT
 ,SUM(ITEM_COUNT)																				AS TOTAL_ITEM_COUNT
 ,SUM(INFORCE_PREMIUM)																			AS PL_INFORCE_PREMIUM
 ,CAST('{date}' as Date) as bq_load_date

FROM cte

GROUP BY
  YEAR
  ,QTR
  ,MONTH
  ,InforceDate
  ,COUNTRY_CODE
  ,COUNTRY_DESC
  ,POLICY_STATE
  ,NB_REN
  ,INF_PREM_RNG
  ORDER BY YEAR,QTR,MONTH,COUNTRY_CODE,COUNTRY_DESC,POLICY_STATE, NB_REN, INF_PREM_RNG


