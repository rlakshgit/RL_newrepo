WITH
base as (SELECT
        JobNumber
        ,CAST(AccountNumber AS STRING) AccountNumber
        ,PolicyNumber
        ,LocationNumber
        ,PolicyInsuredContactFullName
        ,LocationAddress1
        ,LocationAddress2
        ,LocationCity
        ,LocationStateDesc
        ,LocationStateCode
        ,LocationPostalCode
        ,LocationCountryDesc


        FROM `semi-managed-reporting.core_insurance_cl.nonpromoted_cl_locations_bop`

        GROUP BY
        JobNumber
        ,AccountNumber
        ,PolicyNumber
        ,LocationNumber
        ,PolicyInsuredContactFullName
        ,LocationAddress1
        ,LocationAddress2
        ,LocationCity
        ,LocationStateDesc
        ,LocationStateCode
        ,LocationPostalCode
        ,LocationCountryDesc

        UNION DISTINCT

        SELECT
        JobNumber
        ,CAST(AccountNumber AS STRING) AccountNumber
        ,PolicyNumber
        ,LocationNumber
        ,PolicyInsuredContactFullName
        ,LocationAddress1
        ,LocationAddress2
        ,LocationCity
        ,LocationStateDesc
        ,LocationStateCode
        ,LocationPostalCode
        ,LocationCountryDesc

        FROM `semi-managed-reporting.core_insurance_cl.nonpromoted_cl_locations_im`
        GROUP BY
        JobNumber
        ,AccountNumber
        ,PolicyNumber
        ,LocationNumber
        ,PolicyInsuredContactFullName
        ,LocationAddress1
        ,LocationAddress2
        ,LocationCity
        ,LocationStateDesc
        ,LocationStateCode
        ,LocationPostalCode
        ,LocationCountryDesc
)

, intermediate as (SELECT DISTINCT
    cl.JobNumber
    , cl.AccountNumber
    , cl.PolicyNumber
    , cl.LocationNumber
    , cl.PolicyInsuredContactFullName
    , cl.LocationAddress1
    , cl.LocationAddress2
    , cl.LocationCity
    , cl.LocationStateDesc
    , cl.LocationPostalCode
    , cl.LocationCountryDesc
    , TO_BASE64(SHA256(ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationAddress2
        , cl.LocationCity
        , cl.LocationStateDesc
        , cl.LocationPostalCode
        , cl.LocationCountryDesc], ' '))) as GWPC_LocationKey
    , cl.PolicyInsuredContactFullName as input_name
    , ARRAY_TO_STRING( [
        cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], ' ') as input_address
    , ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], ' ') as PlaceID_Lookup
    , ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], '||') as ExperianBIN_Lookup
FROM base as cl)

,
current_jdp_gw AS (SELECT DISTINCT GWPC_LocationKey, JobNumber
     FROM `{project}.{dataset_jdp}.{table}`
     )
                                 
SELECT *, DATE('{{{{ ds }}}}') as date_created
FROM intermediate
WHERE GWPC_LocationKey not in (SELECT GWPC_LocationKey FROM current_jdp_gw)
OR JobNumber not in (SELECT JobNumber FROM current_jdp_gw)
		                 