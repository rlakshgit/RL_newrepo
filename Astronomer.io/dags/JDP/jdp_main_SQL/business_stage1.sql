WITH
                            current_placekeys as (
                            SELECT PlaceKey, places_name, places_address FROM (
                                    SELECT
                                        *
                                        ,RANK() OVER (PARTITION BY  PlaceKey
                                                      ORDER BY      update_time DESC, date_created DESC, current_pid DESC) AS PlaceKey_ranking
                                    FROM `{project}.{source_dataset}.placeKey_table`)
                                    WHERE PlaceKey_ranking = 1
                                    ),

                                stage as (
                                             SELECT {query_master_fields}
                                                   , PlaceID_Lookup
                                                   , ExperianBIN_Lookup
                                                   , input_name
                                                   , input_address
                                                   , places_name
                                                   , places_address
                                                   , PlaceKey
                                                   , BusinessKey
                                             FROM `{project}.{source_dataset}.places_stage_2`
--
                                   WHERE DATE(date_created) = DATE("{{{{ ds }}}}")
                                 )
--                                  SELECT * FROM stage
                                 ,

                                 master as (
                                             SELECT {query_master_fields}
                                                    , PlaceKey
                                                    , BusinessKey
                                             FROM `{project}.{source_dataset}.places_master`
                                              WHERE date_created = DATE("{{{{ ds }}}}")
                                             ),

                                 joined as (
                                             SELECT {query_stage_fields_labeled}
                                                      , stage.PlaceID_Lookup
                                                      , stage.ExperianBIN_Lookup
                                                      , stage.input_name
                                                      , stage.input_address
                                                      , cp.places_name
                                                      , cp.places_address
                                                      , master.PlaceKey
                                                      , master.BusinessKey
                                              FROM master
                                              LEFT JOIN stage
                                                    ON {query_join_fields}
                                              LEFT JOIN current_placekeys cp
                                                    on IFNULL(master.PlaceKey, 'NULL')=IFNULL(cp.PlaceKey, 'NULL')

                                             )
                            SELECT DISTINCT * FROM(

                                         SELECT {query_master_fields}
                                                , PlaceID_Lookup
                                                , ExperianBIN_Lookup
                                                , input_name
                                                , input_address
                                                , PlaceKey
                                                , BusinessKey
                                                , DATE("{{{{ ds }}}}")   as date_created
                                         FROM joined
                                         WHERE ExperianBIN_Lookup IS NOT NULL
                                         GROUP BY {query_master_fields}, PlaceID_Lookup, ExperianBIN_Lookup, input_name, input_address, PlaceKey, BusinessKey, date_created

                                         UNION DISTINCT

                                         SELECT DISTINCT {query_nullif_fields}
                                                , NULLIF(PlaceID_Lookup, PlaceID_Lookup)
                                                , ARRAY_TO_STRING( [
                                                                    places_name
                                                                    , places_address
                                                                   ], "||") as ExperianBIN_Lookup
                                                , places_name as input_name
                                                , places_address as input_address
                                                , PlaceKey
                                                , BusinessKey
                                                , DATE("{{{{ ds }}}}")   as date_created
                                         FROM joined
                                         WHERE PlaceKey IS NOT NULL
                                         GROUP BY {query_master_fields}, PlaceID_Lookup, ExperianBIN_Lookup, input_name, input_address, PlaceKey, BusinessKey, date_created
                                         )