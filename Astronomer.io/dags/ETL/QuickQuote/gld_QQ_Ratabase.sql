SELECT
  REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'JOB_NUMBER'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS JobNumber
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'ACCOUNT_NUMBER'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS AccountNumber
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'DATE_OF_BIRTH'\), \('Value', '([0-9/]+)'\)") AS DateOfBirth
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'POLICY_EFF_DATE'\), \('Value', '([0-9/]+)'\)") AS PolicyEffectiveDate
  --,RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_STATE AS PolicyState
 -- ,REGEXP_EXTRACT(RatabaseProperties, r"\('LegalEntityRegion', '([a-zA-Z0-9_.+-]+)'\)") AS PolicyState
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('LegalEntityRegion', '([a-zA-Z0-9 _.+-]+)'\)") AS PolicyState
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'ARTICLE_TYPE'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemType
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'ARTICLE_SUB_TYPE'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemSubType
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'ARTICLE_GENDER'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemGender
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'LIMIT'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemValue
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'DEDUCTIBLE'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemDeductible
  ,REGEXP_EXTRACT(RatabaseProperties, r"\('Name', 'ANNUAL_PREM'\), \('Value', '([a-zA-Z0-9_.+-]+)'\)") AS ItemPremium
  ,RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES AS ItemCount
  ,RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_SCHED_VALUE AS PolicyValue
  ,SAFE_CAST(RatabaseProperties_Policy_Outputs_RatabaseField_POLICY_TOTAL_PREM AS float64) AS PolicyPremium
  ,RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_GRP AS CreditScore
  ,CAST('{date}' as DATE) as bq_load_date

FROM
  `{project}.{base_dataset}.t_request_pa`

GROUP BY
  RatabaseProperties
  ,RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_STATE
  ,RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES
  ,RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_SCHED_VALUE
  ,SAFE_CAST(RatabaseProperties_Policy_Outputs_RatabaseField_POLICY_TOTAL_PREM AS float64)
  ,RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_GRP



