#StandardSQL
SELECT
    CAST(ACCOUNT_NUMBER AS string) AS ACCOUNT_NUMBER
    ,POLICY_NUMBER
    ,RECAST
    ,SUB_RECAST

FROM `{project}.{dataset}.recast_fy_2020`

UNION ALL

SELECT *
FROM (
    SELECT
        prior.ACCOUNT_NUMBER
        ,prior.POLICY_NUMBER
        ,prior.RECAST
        ,prior.SUB_RECAST

    FROM `semi-managed-reporting.core_insurance_pjpa.recast_fy_2020_before_2014` AS prior
    LEFT OUTER JOIN `{project}.{dataset}.recast_fy_2020` AS ytd
        ON prior.POLICY_NUMBER = ytd.POLICY_NUMBER
    WHERE ytd.POLICY_NUMBER IS NULL
)