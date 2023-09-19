WITH brick_info AS (
                                SELECT CAST(EXPERIAN_BUSINESS_ID as INT64) AS EXPERIAN_BUSINESS_ID
                                        , BUSINESS_NAME
                                        , ADDRESS
                                        , CITY
                                        , STATE
                                        , ZIP_CODE
                                        , ZIP_PLUS_4
                                        , COUNTY_NAME
                                        , GEO_CODE_LATITUDE
                                        , GEO_CODE_LONGITUDE
                                        , PHONE_NUMBER
                                        , EXECUTIVE_TITLE
                                        , EXECUTIVE_FIRST_NAME
                                        , EXECUTIVE_LAST_NAME
                                        , EXECUTIVE_COUNT
                                        , ESTABLISH_DATE
                                        , LATEST_REPORTED_DATE
                                        , YEARS_IN_FILE
                                        , YEAR_BUSINESS_STARTED
                                        , ESTIMATED_NUMBER_OF_EMPLOYEES
                                        , ESTIMATED_ANNUAL_SALES_SIGN
                                        , (CAST(ESTIMATED_ANNUAL_SALES_AMOUNT as INT64) * 1000) AS ESTIMATED_ANNUAL_SALES_AMOUNT
                                        , PRIMARY_NAICS_CODE
                                        , SECOND_NAICS_CODE
                                        , THIRD_NAICS_CODE
                                        , FOURTH_NAICS_CODE
                                        , LINKAGE_TYPE_CODE
                                        , IPV2_RISK_SCORE_CLASS
                                        , FINANCIAL_STABILITY_RISK_SCORE
                                        , FSR_RISK_CLASS
                                        , FSR_SCORE_FACTOR_1
                                        , FSR_SCORE_FACTOR_2
                                        , FSR_SCORE_FACTOR_3
                                        , FSR_SCORE_FACTOR_4
                                        , FSR_SCORE_CHANGE_SIGN
                                        , FSR_SCORE_CHANGE
                                        , PARENT_NAME
                                        , PARENT_CITY_PROVIDENCE
                                        , PARENT_STATE
                                        , ULTIMATE_PARENT_NAME
                                        , ULTIMATE_PARENT_CITY
                                        , ULTIMATE_PARENT_STATE
                                        , BANKRUPTCY_FILED
                                        , NUMBER_OF_DEROGATORY
                                        , INTELLISCORE_PLUS_V2
                                        
                                        FROM `{source_project}.references_experian.experian_brick_current`  WHERE CAST(EXPERIAN_BUSINESS_ID as INT64) IN (SELECT experian_business_id from `{source_project}.{source_dataset_jdp}.businessKey_table` GROUP BY experian_business_id)
                                        ),
                                        
                    businesskey_ranked AS (
                                    SELECT BusinessKey
									, experian_name
                                    , REPLACE(experian_phone, 'nan', 'None') as experian_phone
									, experian_street
									, experian_city
									, experian_state
									, experian_zip
									, experian_business_id
									, experian_update_time
									, date_created
									, RANK() OVER (PARTITION BY  BusinessKey
                                                            ORDER BY experian_update_time DESC) AS BusinessKey_ranking
                                    FROM `{source_project}.{source_dataset_jdp}.businessKey_table`
                                )
                                                            
                            Select distinct * FROM businesskey_ranked 
                            LEFT JOIN brick_info
                            USING(experian_business_id)
                            WHERE BusinessKey_ranking = 1
