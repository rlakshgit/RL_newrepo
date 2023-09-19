with test as
(SELECT *except( data_land_date,data_run_date,Body, Body_RatingServiceRequest_RatabaseProperties_Policy_LOBS,Body_RatingServiceRequest_RatabaseProperties_Policy_Inputs)
,replace(Body_RatingServiceRequest_RatabaseProperties_Policy_LOBS,'None', "null") as new_lobs
,replace(Body_RatingServiceRequest_RatabaseProperties_Policy_Inputs,'None', "null") as new_inputs
FROM `{project}.{dataset}.t_soaprequest_cl` {filter}
)

select *except(new_lobs,new_inputs)

,(select (select json_extract_scalar(inputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.Region.Items.Item.Inputs.RatabaseField'))as inputs where json_extract_scalar(inputs,'$.Name') = "BPP_PROT_SAFEGUARD_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as BPP_PROT_SAFEGUARD_FCTR

,(select (select json_extract_scalar(inputs,'$.Value') from unnest(json_extract_array(lobs,'$.Regions.Region.Items.Item.Inputs.RatabaseField'))as inputs where json_extract_scalar(inputs,'$.Name') = "IRPM_FCTR" ) from unnest(json_extract_array(new_lobs,'$.LOB')) as lobs where json_extract_scalar(lobs, '$.LegalEntityProductGroup')= "Jewelers BOP" )  as IRPM_FCTR

,(select json_extract_scalar(inputarray,'$.value') from unnest(json_extract_array(new_inputs,'$.RatabaseField')) as inputarray where json_extract_scalar(inputarray,'$.name')= "Z_LIAB_EXP_BASE")  as Z_LIAB_EXP_BASE

from test