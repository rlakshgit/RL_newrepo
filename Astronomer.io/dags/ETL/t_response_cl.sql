with test as ( SELECT distinct * except(RatingStatus,	data_land_date,data_run_date,RatingStatus_Policy_LOBS)
,replace(RatingStatus_Policy_LOBS,'None', "null") as new_RatingStatus
FROM `{project}.{dataset}.t_response_cl` {filter} )

select new_RatingStatus
,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_LOSS_COST" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_LOSS_COST

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "WIND_HAIL_EXCL_DD_CREDIT" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as WIND_HAIL_EXCL_DD_CREDIT

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "WIND_HAIL_EXCL_CREDIT" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as WIND_HAIL_EXCL_CREDIT

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "PROP_LCM" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as PROP_LCM

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_PROP_RATE_NBR_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_PROP_RATE_NBR_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_CONSTR_REL_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_CONSTR_REL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_LIMIT_REL_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_LIMIT_REL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_PROT_FIRE_CLASS_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_PROT_FIRE_CLASS_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BLDG_CODE_EFF_GRADE_REL_FCTR") from  unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BLDG_CODE_EFF_GRADE_REL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_SPRINKLER_REL_FCTR") from  unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_SPRINKLER_REL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_PROT_SAFEGUARD_FCTR") from  unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_PROT_SAFEGUARD_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "WIND_LOSS_MITIGATION_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as WIND_LOSS_MITIGATION_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_BASE_RATE" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_BASE_RATE

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BLDG_BPP_WITHOUT_BI_EE_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BLDG_BPP_WITHOUT_BI_EE_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "INS_TO_VALUE_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as INS_TO_VALUE_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "REMOVE_INS_TO_VALUE_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as REMOVE_INS_TO_VALUE_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_LIMIT_TO_VALUE_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_LIMIT_TO_VALUE_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BRAND_LABEL_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BRAND_LABEL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "LIAB_LCM" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as LIAB_LCM

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "LIAB_CLASS_GROUP_REL_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as LIAB_CLASS_GROUP_REL_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "LIAB_INCR_LIMIT_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as LIAB_INCR_LIMIT_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "OPT_PD_LIAB_DED_FCTR" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as OPT_PD_LIAB_DED_FCTR

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "LIAB_RATE" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as LIAB_RATE

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "LIAB_BASE_PREM" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as LIAB_BASE_PREM

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "BPP_BASE_PREM" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_BASE_PREM

,(select (select json_extract_scalar(outputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Outputs.CalcRatabaseField'))as outputs where json_extract_scalar(outputs,'$.Name') = "TOTAL_BPP_BASE_PREM" ) from unnest(json_extract_array(new_RatingStatus,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as TOTAL_BPP_BASE_PREM


from test