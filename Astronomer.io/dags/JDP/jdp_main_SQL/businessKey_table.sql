WITH source AS (
                               SELECT BusinessKey
                                      , experian_business_id
                                      , experian_name
                                      , experian_phone
                                      , experian_street
                                      , experian_city
                                      , experian_state
                                      , experian_zip
                                      , experian_update_time as update_time
                               FROM `{project}.{source_dataset}.business_stage_2`
                               WHERE DATE(date_created) = DATE("{{{{ ds }}}}")
                               AND experian_business_id > 0
                               EXCEPT DISTINCT
						       SELECT BusinessKey
                                     , experian_business_id
                                     , experian_name
                                     , experian_phone
                                     , experian_street
                                     , experian_city
                                     , experian_state
                                     , experian_zip
                                     , experian_update_time as update_time
						       FROM `{project}.{source_dataset}.businessKey_table`
                               WHERE date_created != DATE("{{{{ ds }}}}")
                              )


						      SELECT DISTINCT IFNULL(BusinessKey, TO_BASE64(SHA256(ARRAY_TO_STRING([s.experian_name, s.experian_street, s.experian_city, s.experian_state, s.experian_zip], ' ')))) as BusinessKey
							         , s.experian_name
                                     , s.experian_phone
	   						         , s.experian_street
                                     , s.experian_city
                                     , s.experian_state
                                     , s.experian_zip
							         , s.experian_business_id
							         , MAX(s.update_time) as experian_update_time
							         , DATE("{{{{ ds }}}}") as date_created
						      FROM source s
						      GROUP BY BusinessKey
							         , s.experian_name
                                     , s.experian_phone
	   						         , s.experian_street
                                     , s.experian_city
                                     , s.experian_state
                                     , s.experian_zip
							         , s.experian_business_id