
SELECT DISTINCT
    enhanced_im.PolicyNumber
    ,CAST(enhanced_im.TermNumber_PolicyGroup AS INT64) AS TermNumber
    ,enhanced_im.LocationNumber
    ,CAST(LEFT(risk_score.RiskGroup,1) AS INT64) AS RiskGroup
   
FROM `{project}.data_products_t1_insurance_cl.nonpromoted_research_enhanced_im` AS enhanced_im
    
    INNER JOIN `{project}.references_lookup_insurance_cl.{risk_table}` AS risk_score
	-- INNER JOIN `dev-edl.references_lookup_insurance_cl.model-cl-im-risk-score-2022-11-28` AS risk_score
    ON risk_score.PolLocTermVersID= enhanced_im.PolLocTermVersID

WHERE enhanced_im.PolicyVersion  = 1
