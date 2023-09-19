WITH stage as (
                                           SELECT {query_master_fields}
                                                  , PlaceKey
                                                  , BusinessKey
                                                  , places_pid
                                           FROM `{project}.{source_dataset}.places_stage_2`
                                 WHERE DATE(date_created) = DATE("{{{{ ds }}}}")
                               ),

                          existing_placekey as (
                                           SELECT DISTINCT PlaceKey, current_pid
                                           FROM `{project}.{source_dataset}.placeKey_table`
                                          WHERE current_pid != 'NOT_FOUND'
                                           ),

                          -- this captures data that gets added directly to the placekey table (ie. GEM digger)
                          new_place_keys AS (
                                        SELECT DISTINCT {query_gem_digger_fields}
                                              , PlaceKey
                                              , '' BusinessKey
                                              , date_created
                                          FROM `{project}.{source_dataset}.placeKey_table` b
                                      WHERE NOT EXISTS
                                        (SELECT *
                                         FROM `{project}.{source_dataset}.places_master`  a
                                         WHERE a.PlaceKey = b.PlaceKey))


                          SELECT {query_master_fields}
                                  , PlaceKey
                                  , BusinessKey
                                  , DATE("{{{{ ds }}}}")  as date_created
                          FROM (
                             SELECT {query_stage_fields}
                                    , existing_placekey.PlaceKey
                                    , stage.BusinessKey
                             FROM stage LEFT JOIN existing_placekey
                             ON existing_placekey.current_pid = stage.places_pid


					         EXCEPT DISTINCT
						     SELECT {query_master_fields}
                                    , PlaceKey
                                    , BusinessKey
						     FROM `{project}.{source_dataset}.places_master`
                             WHERE date_created != DATE("{{{{ ds }}}}")
                           )

                        UNION DISTINCT

                 SELECT {query_nullif_fields}
                                  , PlaceKey
                                  , NULLIF(BusinessKey, '')
                                  , DATE("{{{{ ds }}}}")   date_created
                      FROM  new_place_keys