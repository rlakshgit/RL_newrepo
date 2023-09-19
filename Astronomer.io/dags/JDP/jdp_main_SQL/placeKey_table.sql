WITH source AS (
                                   SELECT places_pid
                                          , places_name
                                          , places_address
                                          , places_types
                                          , places_json
                                          , places_update_time as update_time
                                          , if(is_place_of_type_establishment ="True", is_place_of_type_establishment,is_place_of_type_establishment_from_detailsAPI) as is_place_of_type_establishment
                                          , date_created
                                   FROM `{project}.{source_dataset}.places_stage_2`
                                   WHERE DATE(date_created) = DATE("{{{{ ds }}}}")
                                   AND places_pid != 'NOT_FOUND'
                                   AND places_name NOT IN ('ZERO_RESULTS', 'NON_SPECIFIC_LOCATION','POOR_NAME_MATCH', 'POOR_ADDRESS_MATCH')
                                   ),

                             target AS (
                                         SELECT MAX(placeKey) as placeKey
                                                , current_pid
                                                , MAX(previous_pid) as previous_pid
                                                , MAX(update_time) as update_time
                                         FROM `{project}.{source_dataset}.placeKey_table`
                                         WHERE current_pid != 'NOT_FOUND'
                                         AND places_name NOT IN ('ZERO_RESULTS', 'NON_SPECIFIC_LOCATION','POOR_NAME_MATCH', 'POOR_ADDRESS_MATCH')
                                         GROUP BY current_pid),

                             existing AS (
                                         SELECT DISTINCT placeKey
                                                , current_pid
                                                , previous_pid
                                         FROM source LEFT JOIN target
                                         ON source.places_pid = target.current_pid
                                         WHERE placeKey is not NULL
                                        )

                            SELECT IFNULL(placeKey, TO_BASE64(SHA256(ARRAY_TO_STRING([s.places_name, s.places_address], ' ')))) as placeKey
                                 , s.places_name
                                 , s.places_address
                                 , s.places_pid as current_pid
                                 , e.previous_pid as previous_pid
                                 , places_json
                                 , s.is_place_of_type_establishment
                                 , DATETIME(s.update_time) as update_time
                                 , DATE(date_created) as date_created
                            FROM source s LEFT JOIN existing e
                            ON current_pid = places_pid