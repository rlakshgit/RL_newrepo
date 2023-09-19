WITH gwpc_config_rankings AS (SELECT * FROM (
                                          SELECT
                                              *
                                              ,RANK() OVER (PARTITION BY  PolicyNumber
                                                            ORDER BY      EndDate DESC, JobNumber DESC) AS GWPC_config_ranking
--                                            FROM `semi-managed-reporting.core_insurance.promoted_policy_transactions`
                                           FROM `{source_project}.{transaction_dataset}.promoted_policy_transactions`
                                           WHERE PolicyNumber LIKE '55-%')
                                   WHERE GWPC_config_ranking = 1
                             ),



    cl_research_im_attr AS (SELECT risk_location_key , PolicyInsuredContactFullName , LocationTypeCode , LocationTypeDesc, ConfigEndDate, config_ranking  FROM (
                                          SELECT
                                               DISTINCT risk_location_key , PolicyInsuredContactFullName, LocationTypeCode , LocationTypeDesc , ConfigEndDate
                                              ,RANK() OVER (PARTITION BY  risk_location_key
                                                            ORDER BY      ConfigEndDate DESC, JobNumber DESC) AS config_ranking
--                                            FROM `semi-managed-reporting.data_products_t1_insurance_cl.nonpromoted_research_base_im`
                                           FROM `{source_project}.{research_dataset}.nonpromoted_research_base_im`
                                           )
                                   WHERE config_ranking = 1
                             ),

    joined AS (SELECT AccountNumber, a.PolicyNumber, JobNumber, risk_location_key, LocationNumber, GWPC_LocationKey,  input_name, input_address, TranType, a.PeriodEffDate, a.TransEffDate, BegDate, EndDate
                          , c.PolicyInsuredContactFullName
                          , c.LocationTypeCode
                          , c.LocationTypeDesc , FROM gwpc_config_rankings a
        LEFT JOIN `{source_project}.{source_dataset_jdp}.internal_source_GWPC` b
        USING(JobNumber)
        LEFT JOIN cl_research_im_attr c
        USING(risk_location_key)
        WHERE GWPC_LocationKey IS NOT NULL AND GWPC_LocationKey != ''
        ),

    location_rankings AS (SELECT * FROM
                                    (SELECT *, RANK() OVER (PARTITION BY  GWPC_LocationKey
                                                  ORDER BY EndDate DESC, JobNumber DESC) AS location_ranking FROM joined)
                                  WHERE location_ranking = 1)

     ,final AS (SELECT * EXCEPT(location_ranking), if((current_date() > EndDate), 'Prior_Insured', 'Current_Insured') as Insured_Status
                                FROM location_rankings
                                )

SELECt * FROM final