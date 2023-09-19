WITH businesskey as (
                             SELECT BusinessKey, experian_business_id
                             FROM `{project}.{source_dataset}.businessKey_table`
					         WHERE date_created = DATE("{{{{ ds }}}}")
                             ),

                             stage as (
                             SELECT {query_master_fields}
                                    , PlaceKey
                                    , BusinessKey
                                    , experian_business_id
                             FROM `{project}.{source_dataset}.business_stage_2`
					         WHERE DATE(date_created) = DATE("{{{{ ds }}}}")
                             GROUP BY {query_master_fields}, PlaceKey, BusinessKey, experian_business_id
                             )

                           SELECT {query_master_fields}
                                  , PlaceKey
                                  , BusinessKey
                                  , DATE("{{{{ ds }}}}") as date_created
                           FROM (
                             SELECT {query_stage_fields_labeled}
                                    , stage.PlaceKey
                                    , businesskey.BusinessKey
                             FROM stage LEFT JOIN businesskey
                             ON businesskey.experian_business_id = stage.experian_business_id

                           )