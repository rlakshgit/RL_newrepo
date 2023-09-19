WITH stage as (
                                    SELECT {query_stage_fields}
                                           , PlaceID_Lookup
                                           , ExperianBIN_Lookup
                                           , input_name
                                           , input_address
                                           , PlaceKey
                                           , BusinessKey
                                    FROM `{project}.{source_dataset}.business_stage_2`
    					            WHERE date_created = DATE("{{{{ ds }}}}")
    						        ),

                       master as (
                                    SELECT {query_master_fields}
                                           , PlaceKey
                                           , BusinessKey
                                    FROM `{project}.{source_dataset}.business_master`
    					            WHERE date_created = DATE("{{{{ ds }}}}")
                                    ),

                             business_joined as (
                                    SELECT DISTINCT {query_stage_fields_labeled}
                                           , stage.PlaceID_Lookup
                                           , stage.ExperianBIN_Lookup
                                           , stage.input_name
                                           , stage.input_address
                                           , master.PlaceKey
                                           , master.BusinessKey

                                    FROM master LEFT JOIN stage
                                    ON {query_join_fields}
                                    AND IFNULL(master.PlaceKey, 'NULL')=IFNULL(stage.PlaceKey, 'NULL')
                                    )

                                    SELECT {query_stage_fields}
                                           , PlaceID_Lookup
                                           , ExperianBIN_Lookup
                                           , input_name
                                           , input_address
                                           , PlaceKey
                                           , BusinessKey
                                           , DATE("{{{{ ds }}}}") as date_created
                                    FROM (
                                      SELECT {query_stage_fields}
                                             , PlaceID_Lookup
                                             , ExperianBIN_Lookup
                                             , input_name
                                             , input_address
                                             , PlaceKey
                                             , BusinessKey
                                      FROM business_joined

    					              EXCEPT DISTINCT
                                      SELECT {query_stage_fields}
                                             , PlaceID_Lookup
                                             , ExperianBIN_Lookup
                                             , input_name
                                             , input_address
                                             , PlaceKey
                                             , BusinessKey
                                      FROM `{project}.{source_dataset}.internal_stage`
                                      WHERE date_created != DATE("{{{{ ds }}}}")
                                    )