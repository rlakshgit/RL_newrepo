WITH gwpc_locs AS (
                                        SELECT a.AccountNumber
                                            , a.PolicyNumber
                                            , a.JobNumber
                                            , a.LocationNumber
                                            , GWPC_LocationKey
                                            , input_name as GW_Name
                                            , input_address as GW_FullAddress
                                            , TranType as MostRecentTransaction
                                            , PeriodEffDate
                                            , BegDate
                                            , EndDate
                                            , Insured_Status
                                            , IF((LOB_BOP = 'BOP') AND (LOB_IM IS NULL), 'BOP_ONLY', LOB_BOP) as LOB_BOP
                                            , b.* EXCEPT(JobNumber, PolicyNumber, AccountNumber, LocationNumber, LOB_BOP)
                                        FROM `{source_project}.{core_sales_cl_dataset}.promoted_recent_active_gwpc_locations` a
                                        LEFT JOIN `prod-edl.gld_JDP.t_JDP_gwpc` b
                                        ON a.JobNumber = b.JobNumber
                                        AND a.risk_location_key = b.risk_location_key
                                        AND IFNULL(a.LocationNumber, -99) = IFNULL(b.LocationNumber, -99) 
                                        WHERE b.JobNumber IS NOT NULL
                                        ),
                                        
                                     master as (WITH gwpc_ranked AS (
                                                    SELECT GWPC_LocationKey, PlaceKey, GWPC_AccountNumber, GWPC_PolicyNumber, DUNS, SF_LocationKey, BusinessKey, date_created ,RANK() OVER (PARTITION BY  GWPC_LocationKey
                                                                                              ORDER BY date_created DESC, GWPC_PolicyNumber) AS gwpc_lockey_ranking
                                                                  FROM `{source_project}.{source_dataset_jdp}.business_master` WHERE GWPC_LocationKey IS NOT NULL)
                                
                                                SELECT * EXCEPT(gwpc_lockey_ranking, PlaceKey), MAX(PlaceKey) as PlaceKey FROM gwpc_ranked WHERE gwpc_lockey_ranking = 1
                                                GROUP BY GWPC_AccountNumber, GWPC_PolicyNumber, GWPC_LocationKey, DUNS, SF_LocationKey, BusinessKey, date_created
                                                )
                                                
                                SELECT gwpc_locs.*
                                      , experian_business_id
                                      , ESTABLISH_DATE
                                      , YEARS_IN_FILE
                                      , YEAR_BUSINESS_STARTED
                                      , ESTIMATED_NUMBER_OF_EMPLOYEES
                                      , ESTIMATED_ANNUAL_SALES_SIGN
                                      , ESTIMATED_ANNUAL_SALES_AMOUNT
                                      , BANKRUPTCY_FILED
                                      , NUMBER_OF_DEROGATORY
                                      , FINANCIAL_STABILITY_RISK_SCORE
                                        , FSR_RISK_CLASS
                                        , FSR_SCORE_FACTOR_1
                                        , FSR_SCORE_FACTOR_2
                                        , FSR_SCORE_FACTOR_3
                                        , FSR_SCORE_FACTOR_4
                                        , FSR_SCORE_CHANGE_SIGN
                                        , FSR_SCORE_CHANGE
                                      , INTELLISCORE_PLUS_V2
                                      
                                FROM gwpc_locs
                                 LEFT JOIN master
                                    USING(GWPC_LocationKey)
                                LEFT JOIN `{source_project}.{core_sales_cl_dataset}.promoted_experian_businesses` experian
                                    USING(BusinessKey)