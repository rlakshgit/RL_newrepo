CREATE or REPLACE VIEW {project}.{dataset}.pl_loss_ratio_inforce_policies
AS
SELECT
    InforceDate
    ,LOBCode
    ,COUNT(DISTINCT PolicyNumber) AS DistinctPolicies
    ,SUM(PremiumInforce) AS InforcePremium
FROM `{project}.{dataset}.pl_policy_monthly_inforce`
WHERE BusinessTypeDesc = 'Direct'
GROUP BY
    InforceDate
    ,LOBCode

