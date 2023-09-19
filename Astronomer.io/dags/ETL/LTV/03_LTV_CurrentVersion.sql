
-- CREATE OR REPLACE TABLE `semi-managed-reporting.wip_sjalaparthi.wip_ltv_pp_p_current` AS 
SELECT 
  t.AccountNumber,
  t.PolicyNumber,
  t.LTV,
  1.0 AS LTV_model_version,
  t.PolicyProfile,
  1.0 AS PP_model_version,
  date('{date}')  AS currentversion_load_date
FROM
(
  SELECT DISTINCT
    CAST(AccountNumber AS STRING) AS AccountNumber,
    PolicyNumber
    ,Retention AS LTV
    ,PolicyProfile
    ,TermNumber
    ,ModelNumber
    ,RANK() OVER(PARTITION BY CAST(AccountNumber AS STRING)
                              ,PolicyNumber
                  ORDER BY    TermNumber DESC
                              ,ModelNumber DESC
                              ,allversion_load_date DESC
                              ,PeriodEffDate DESC
                              ,TransEffDate DESC
                              ,JobNumber DESC) AS PolRank
  FROM `{project}.{dataset}.{all_version_table}` 
)t
WHERE t.PolRank = 1