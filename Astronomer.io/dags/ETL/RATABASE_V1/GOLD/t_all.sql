with test as (
SELECT distinct *except(data_land_date,	data_run_date,RATEREQUEST,RATEREQUEST_ANCHOR,RATEREQUEST_POLICY_POLINPUTS,RATEREQUEST_POLICY_LOBS)
,replace(RATEREQUEST_POLICY_LOBS,'None', "null") as new_lobs
,replace(RATEREQUEST_POLICY_POLINPUTS,'None', "null") as new_inputs
FROM `{project}.{dataset}.t_all` {filter}
)

select  * except(new_lobs,new_inputs)

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_LOSS_COST" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_LOSS_COST

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "WIND_HAIL_EXCL_DD_CREDIT" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as WIND_HAIL_EXCL_DD_CREDIT

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "WIND_HAIL_EXCL_CREDIT" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as WIND_HAIL_EXCL_CREDIT

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "PROP_LCM" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as PROP_LCM

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_PROP_RATE_NBR_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_PROP_RATE_NBR_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_CONSTR_REL_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_CONSTR_REL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_LIMIT_REL_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_LIMIT_REL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_PROT_FIRE_CLASS_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_PROT_FIRE_CLASS_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BLDG_CODE_EFF_GRADE_REL_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BLDG_CODE_EFF_GRADE_REL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_SPRINKLER_REL_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_SPRINKLER_REL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_PROT_SAFEGUARD_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_PROT_SAFEGUARD_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "WIND_LOSS_MITIGATION_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as WIND_LOSS_MITIGATION_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_BASE_RATE") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_BASE_RATE

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BLDG_BPP_WITHOUT_BI_EE_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BLDG_BPP_WITHOUT_BI_EE_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "INS_TO_VALUE_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as INS_TO_VALUE_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "REMOVE_INS_TO_VALUE_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as REMOVE_INS_TO_VALUE_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_LIMIT_TO_VALUE_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_LIMIT_TO_VALUE_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BRAND_LABEL_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BRAND_LABEL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "BPP_BASE_PREM") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as BPP_BASE_PREM

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "TOTAL_BPP_BASE_PREM") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as TOTAL_BPP_BASE_PREM

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMINPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "IRPM_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as IRPM_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "LIAB_LCM") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as LIAB_LCM

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "LIAB_CLASS_GROUP_REL_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as LIAB_CLASS_GROUP_REL_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "LIAB_INCR_LIMIT_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as LIAB_INCR_LIMIT_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "OPT_PD_LIAB_DED_FCTR") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as OPT_PD_LIAB_DED_FCTR

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "LIAB_RATE") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as LIAB_RATE

,(select (select json_extract_scalar(inputs,'$.text') from unnest(json_extract_array(lobs,'$.REGIONS.REGION.ITEMS.ITEM.ITEMOUTPUTS.FLD'))as inputs where json_extract_scalar(inputs,'$.NM') = "LIAB_BASE_PREM") from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LEGALENTITYPRODGROUP')= "JEWELERS BOP" )  as LIAB_BASE_PREM

,(select json_extract_scalar(inputarray,'$.text') from unnest(json_extract_array(new_inputs,'$.FLD')) as inputarray where json_extract_scalar(inputarray,'$.NM')= "Z_LIAB_EXP_BASE")  as Z_LIAB_EXP_BASE

from test
