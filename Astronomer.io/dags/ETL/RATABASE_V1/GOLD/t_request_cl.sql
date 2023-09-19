with test as (
select distinct *except(RatabaseProperties,RatabaseProperties_Policy_LOBS,RatabaseProperties_Policy_Inputs,data_land_date,data_run_date)
,replace(RatabaseProperties_Policy_LOBS,'None', "null") as new_lobs
,replace(RatabaseProperties_Policy_Inputs,'None', "null") as new_inputs
from `{project}.{dataset}.t_request_cl` {filter}
)

select *except(new_lobs,new_inputs)

,(select (select json_extract_scalar(inputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Inputs.CalcRatabaseField'))as inputs where json_extract_scalar(inputs,'$.Name') = "BPP_PROT_SAFEGUARD_FCTR" ) from unnest(json_extract_array(new_lobs,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_PROT_SAFEGUARD_FCTR

,(select (select json_extract_scalar(inputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.CalcRegion.Items.CalcItem.Inputs.CalcRatabaseField'))as inputs where json_extract_scalar(inputs,'$.Name') = "IRPM_FCTR" ) from unnest(json_extract_array(new_lobs,'$.CalcLOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as IRPM_Fctr

,(select json_extract_scalar(inputarray,'$.value') from unnest(json_extract_array(new_inputs,'$.CalcRatabaseField')) as inputarray where json_extract_scalar(inputarray,'$.name')= "Z_LIAB_EXP_BASE")  as Z_LIAB_EXP_BASE

from test