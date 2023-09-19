WITH

        sf_loc_rankings AS (SELECT SF_LocationKey ,SF_AccountNumber ,SalesforceID ,Name ,PrimaryAddress_street ,PrimaryAddress_city ,PrimaryAddress_state ,PrimaryAddress_postalCode, PrimaryAddress_country ,Primary_Location ,Location_Number, data_land_date
                                  FROM (
                                          SELECT
                                              *
                                              ,RANK() OVER (PARTITION BY  SF_LocationKey
                                                            ORDER BY      b.data_land_date  DESC) AS SF_loc_ranking
--                                            FROM `semi-managed-reporting.core_JDP.internal_source_SF` b
                                        FROM `{source_project}.{source_dataset_jdp}.internal_source_SF` b
                                           )
                                   WHERE SF_loc_ranking = 1
                             )

                ,
                sf_id_rankings AS (SELECT *
                                  FROM (
                                          SELECT
                                              *
                                              ,RANK() OVER (PARTITION BY  Id
                                                            ORDER BY      b.LastModifiedDate DESC) AS SF_id_ranking
                                           FROM `prod-edl.ref_salesforce.sf_account` b
                                           )
                                   WHERE SF_id_ranking = 1
                             )

--                 ,
--                 sf_trade_assoc_rankings AS (SELECT sf_trade_associations__c.Id AS TradeAssociationId, sf_account.Name As TradeAssociationName, sf_trade_associations__c.Name AS TANumber
--                                               ,sf_trade_associations__c.TA_Id__c ,sf_trade_associations__c.Active__c, Jeweler__c
--                                             FROM (
--                                                     SELECT
--                                                         *
--                                                         ,RANK() OVER (PARTITION BY  b.Jeweler__c
--                                                                       ORDER BY      b.LastModifiedDate DESC, b.Id DESC) AS SF_ta_ranking
--                                                      FROM `prod-edl.ref_salesforce.sf_trade_associations__c` b
--                                                      ) sf_trade_associations__c
--                                             INNER JOIN `prod-edl.ref_salesforce.sf_account` sf_account
--                                                 ON sf_account.id = sf_trade_associations__c.Trade_Association__c
--                                              WHERE SF_ta_ranking = 1
--                                        )

     ,final AS (
          SELECT DISTINCT

                 SF_LocationKey

                 ,a.SalesforceID

              ,sf_account.Policy_Number__c AS PolicyNumber

              ,sf_account.Agency_Name_and_Producer_Code__c

--               ,sf_trade_associations__c.Trade_Association__c AS TradeAssociationAccountId

              ,sf_account.Name AS JewelerAccountName

              ,sf_account.Type As JewelerAccountType

              ,PrimaryAddress_street ,PrimaryAddress_city ,PrimaryAddress_state ,PrimaryAddress_postalCode, PrimaryAddress_country ,Primary_Location ,Location_Number, a.data_land_date

--               ,c.TradeAssociationId

--               ,c.TradeAssociationName

              --,sf_account.BillingStreet

              --,sf_account.BillingCity

              --,sf_account.Billingstate

              --,sf_account.BillingCountry

              --,sf_account.Phone

--               ,c.TA_Id__c

--               ,c.Active__c

--               ,c.TANumber

              --,sf_trade_associations__c.LastModifiedDate

          FROM sf_loc_rankings a

              INNER JOIN sf_id_rankings sf_account

                  ON a.SalesforceID = sf_account.id

--                LEFT JOIN sf_trade_assoc_rankings c

--                   ON a.SalesforceID = c.Jeweler__c

--               INNER JOIN cte_TradeAssociations

--                   ON cte_TradeAssociations.TradeAssociationId = sf_trade_associations__c.Id

--           --WHERE sf_account.id = '0013600000SqsqQAAR'

--           --WHERE LOWER(sf_account.name) like '%american gem society%'

--           AND sf_trade_associations__c.TA_Id__c!='None'
                                )

SELECT * FROM final
