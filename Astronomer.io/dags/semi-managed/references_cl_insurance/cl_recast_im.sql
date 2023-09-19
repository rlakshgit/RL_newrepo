##### Initial Checks
-- SELECT 
--     PolicyNumber
--     ,TermNumber
--     ,LocationNumber
-- FROM `{project}.references_cl_insurance.cl_ref_model_us_im_V3` 
-- GROUP BY 
--     PolicyNumber
--     ,TermNumber
--     ,LocationNumber
-- HAVING COUNT(*) > 1;
WITH dt_risk AS 
(
    SELECT 
        t.PolicyNumber
        ,t.TermNumber
        ,t.Country
        ,ROUND(SUM(PremiumWritten * RiskGroup * RiskFlag)/SUM(PremiumWritten * RiskFlag)) AS RiskGroup
    FROM 
    (
        SELECT 
            cl_ref_risk_score_prem.PolicyNumber
            ,cl_ref_risk_score_prem.TermNumber
            ,cl_ref_risk_score_prem.LocationNumber
            ,cl_ref_risk_score_prem.Country
            ,cl_ref_risk_score_prem.PremiumWritten
            ,cl_model.RiskGroup
            ,CASE WHEN cl_model.RiskGroup IS NOT NULL THEN 1 ELSE 0 END AS RiskFlag
        
        FROM `{project}.{dataset}.cl_ref_risk_score_prem` AS cl_ref_risk_score_prem

            LEFT JOIN `{project}.{dataset}.cl_ref_model_us_im_V3` AS cl_model 
            ON cl_model.PolicyNumber = cl_ref_risk_score_prem.PolicyNumber
            AND cl_model.TermNumber = CAST(cl_ref_risk_score_prem.TermNumber AS int64)
            AND cl_model.LocationNumber = cl_ref_risk_score_prem.LocationNumber
    )t

    GROUP BY 
        t.PolicyNumber
        ,t.TermNumber
        ,t.Country
)
##### Check1: SELECT PolicyNumber,TermNumber FROM dt_risk GROUP BY PolicyNumber,TermNumber HAVING COUNT(*) > 1;
SELECT  
    f.PolicyNumber
    ,f.TermNumber
    ,f.ProductCode 
    ,f.Country
    ,CASE WHEN f.Country = 'CAN' THEN NULL ELSE RiskGroupModified END AS RiskGroup
    -- ,RiskGroup
FROM (
    SELECT 
        PolicyNumber
        ,TermNumber
        ,'IM' AS ProductCode
        ,Country
        -- ,ROW_NUMBER() OVER(PARTITION BY PolicyNumber ORDER BY TermNumber) AS RowNum
        -- ,SUM(CASE WHEN RiskGroup IS NOT NULL THEN 1 ELSE 0 END) OVER(PARTITION BY PolicyNumber ORDER BY TermNumber ROWS UNBOUNDED PRECEDING) AS cumSumRiskFlag
        ,RiskGroup
        ,LAST_VALUE(RiskGroup IGNORE NULLS) OVER(PARTITION BY PolicyNumber ORDER BY TermNumber ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RiskGroupModified
    FROM dt_risk 
)f
-- WHERE PolicyNumber = '55-000006'
-- ORDER BY TermNumber
