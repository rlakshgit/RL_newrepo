with base as (
              SELECT *
              FROM `semi-managed-reporting.{source_dataset}.{source_table}`
              )
        
        ,
    intermediate as (
          SELECT DISTINCT _id
                  , Location_Id
                  , Name
                  , Location_AddressLine1
                  , Location_AddressLine2
                  , Location_Name
                  , Location_State
                  , Location_City
                  , Location_Zip
                   , TO_BASE64(SHA256(ARRAY_TO_STRING( [
                       Name
                    , Location_AddressLine1
                    , Location_State
                    , Location_City
                     , SPLIT(Location_Zip, '-')[OFFSET(0)]], ' '))
                                                        ) as Zing_LocationKey
                   , Name as input_name
                   , ARRAY_TO_STRING( [
                    Location_AddressLine1
                    , Location_AddressLine2
                    , Location_State
                    , Location_City
                     , SPLIT(Location_Zip, '-')[OFFSET(0)]], ' ') as input_address
                   , ARRAY_TO_STRING( [
                      Name
                    , Location_AddressLine1
                    , Location_State
                    , Location_City
                     , SPLIT(Location_Zip, '-')[OFFSET(0)]], ' ') as PlaceID_Lookup
                   , ARRAY_TO_STRING( [
                      Name
                    , Location_AddressLine1
                    , Location_State
                    , Location_City
                     , SPLIT(Location_Zip, '-')[OFFSET(0)]], '||') as ExperianBIN_Lookup
          FROM base WHERE rank = 1
          )

          ,
          current_jdp_zing AS (SELECT DISTINCT(Location_Id)
                   FROM `{project}.{dataset_jdp}.{table}`
                   )

select *, DATE('{{{{ ds }}}}') as date_created
from intermediate
WHERE Location_Id not in (SELECT * FROM current_jdp_zing)