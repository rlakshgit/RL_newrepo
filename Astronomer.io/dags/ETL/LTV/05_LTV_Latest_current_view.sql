CREATE OR REPLACE VIEW  `{project}.{dataset}.vw_model_ltv_pp_results`
AS

SELECT 

  AccountNumber,

  PolicyNumber,

  CAST(LTV AS int64) AS LTV,

  CAST(currentversion_load_date AS STRING) AS LTV_run_date,

  LTV_model_version,

  CAST(PolicyProfile AS int64) AS PP,

  CAST(currentversion_load_date AS STRING) AS PP_run_date,

  PP_model_version

FROM `{project}.{dataset}.t_ltv_currentversions` 

WHERE currentversion_load_date = (SELECT max(currentversion_load_date) FROM `{project}.{dataset}.t_ltv_currentversions` )