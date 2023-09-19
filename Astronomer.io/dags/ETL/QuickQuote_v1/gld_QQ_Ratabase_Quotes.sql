#StandardSQL
/*
    Each record represents a single quote.
    Join article details table for further information on the specific characteristics of the quote.
*/
SELECT
    request.QuoteServiceID
    ,request.RatabaseProperties_Policy_LOB_LegalEntityProductGroup AS ProductType
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_TRANSACTION_TYPE AS TransactionType
    ,response.RatingTimings_ServiceCompletedTimestamp AS QuoteUpdateTimestamp
    ,JSON_VALUE(request.RatabaseProperties_Policy_Extension, '$.RatabaseField[0].Value') AS AccountNumber
    ,JSON_VALUE(request.RatabaseProperties_Policy_Extension, '$.RatabaseField[1].Value') AS PolicyNumber
    ,JSON_VALUE(request.RatabaseProperties_Policy_Extension, '$.RatabaseField[2].Value') AS JobNumber
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_EFF_DATE AS PolicyEffectiveDate
    ,request.RatabaseProperties_Policy_LOB_Region_LegalEntityRegion AS PolicyState
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES AS ArticleCount
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_SCHED_VALUE AS PolicyValue
    ,JSON_VALUE(REPLACE(response.RatingStatus_Policy_Outputs, 'None', 'null'), '$.RatabaseField[2].Value') AS PolicyTotalPremium
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_GRP AS InsuranceScoreGroup
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_PROFILE AS InsuranceScoreProfile
    ,RANK() OVER (PARTITION BY   JSON_VALUE(request.RatabaseProperties_Policy_Extension, '$.RatabaseField[0].Value')
                                ,JSON_VALUE(request.RatabaseProperties_Policy_Extension, '$.RatabaseField[2].Value')
                  ORDER BY       response.RatingTimings_ServiceCompletedTimestamp) AS QuoteSequence
    --,DATE('{date}') AS bq_load_date

--FROM        `qa-edl.B_QA_ref_ratabase_new.t_request_pa` AS request
FROM        `{project}.{base_dataset}.t_request_pa` AS request
LEFT JOIN  -- `qa-edl.B_QA_ref_ratabase_new.t_response_pa` AS response
            `{project}.{base_dataset}.t_response_pa` AS response
        ON  request.QuoteServiceID = response.QuoteServiceID

GROUP BY
   request.QuoteServiceID
    ,request.RatabaseProperties_Policy_LOB_LegalEntityProductGroup
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_TRANSACTION_TYPE
    ,response.RatingTimings_ServiceCompletedTimestamp
    ,request.RatabaseProperties_Policy_Extension
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_EFF_DATE
    ,request.RatabaseProperties_Policy_LOB_Region_LegalEntityRegion
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_POLICY_SCHED_VALUE
    ,response.RatingStatus_Policy_Outputs
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_GRP
    ,request.RatabaseProperties_Policy_Inputs_RatabaseField_INS_SCORE_PROFILE
    