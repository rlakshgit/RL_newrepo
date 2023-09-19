#StandardSQL
WITH base AS
(
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,CAST(ModelNumber AS int64) AS ModelNumber
        ,InsuredCountryCode
        ,TransactionTypeGroup
        ,TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.fact_transaction_count`
    WHERE ProductLine = 'CL'
    UNION ALL
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,InsuredCountryCode
        ,TransactionTypeGroup
        ,0 AS TransactionCount
        ,WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.fact_written_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,InsuredCountryCode
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.fact_earned_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,InsuredCountryCode
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,IncurredLossNetRecovery
        ,IncurredALAENetRecovery
    FROM `{project}.{dataset}.fact_incurred_loss`
    WHERE ProductLine = 'CL'
)
,actual AS 
(
    SELECT 
        a.AccountingDate
        ,a.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,partitionMap.PartitionName
        ,a.PlanYear
        ,SUM(a.TransactionCount) AS TransactionCount
        ,SUM(a.WrittenPremium) AS WrittenPremium
        ,SUM(a.EarnedPremium) AS EarnedPremium
        ,SUM(a.IncurredLossNetRecovery) AS IncurredLossNetRecovery
        ,SUM(a.IncurredALAENetRecovery) AS IncurredALAENetRecovery
    FROM 
    (
        SELECT 
            base.AccountingDate 
            ,base.TransactionTypeGroup
            ,base.ProductCode
            ,base.TransactionCount
            ,base.WrittenPremium
            ,base.EarnedPremium
            ,base.IncurredLossNetRecovery
            ,base.IncurredALAENetRecovery
            ,CASE WHEN base.ProductCode = 'IM' AND base.InsuredCountryCode != 'CAN' THEN polRisk.RiskGroup 
             END AS RiskGroup
            ,COALESCE(polRisk.PlanYear,CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)) AS PlanYear
        FROM base
            LEFT JOIN (SELECT * FROM `{project}.{dataset}.cl_riskgroup_im` WHERE PlanYear = CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)) AS polRisk
            ON polRisk.PolicyNumber = base.PolicyNumber
            AND polRisk.ProductCode = base.ProductCode
    )a
    
        LEFT JOIN `{project}.{dataset}.cl_partition_mapping` AS partitionMap
        ON partitionMap.ProductCode = a.ProductCode
        AND COALESCE(partitionMap.RiskGroup, -1) = COALESCE(a.RiskGroup, -1)
        AND partitionMap.PlanYear = a.PlanYear
    GROUP BY
        a.AccountingDate
        ,a.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,partitionMap.PartitionName
        ,a.PlanYear
)
,plan AS 
(
    SELECT 
        DATE(dailyPlan.AccountingDate) AS AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
        ,dailyPlan.PlanYear
        ,SUM(dailyPlan.Plan_TransactionCount) AS Plan_TransactionCount
        ,SUM(dailyPlan.Plan_WrittenPremium) AS Plan_WrittenPremium
    FROM `{project}.{dataset}.cl_plan_numbers_daily` AS dailyPlan
    WHERE dailyPlan.PlanYear = CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)
    GROUP BY
        dailyPlan.AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
        ,dailyPlan.PlanYear
)

SELECT 
    COALESCE(actual.PlanYear,plan.PlanYear ) AS PlanYear
    ,COALESCE(actual.AccountingDate,plan.AccountingDate ) AS AccountingDate
    ,COALESCE(actual.TransactionTypeGroup,plan.TransactionTypeGroup ) AS TransactionTypeGroup
    ,COALESCE(actual.PartitionCode,plan.PartitionCode ) AS PartitionCode
    ,COALESCE(actual.PartitionName,plan.PartitionName ) AS PartitionName
    ,COALESCE(actual.TransactionCount,0) AS TransactionCount
    ,ROUND(COALESCE(actual.WrittenPremium,0),2) AS WrittenPremium
    ,ROUND(COALESCE(actual.EarnedPremium,0),2) AS EarnedPremium
    ,ROUND(COALESCE(actual.IncurredLossNetRecovery,0),2) AS IncurredLossNetRecovery
    ,ROUND(COALESCE(actual.IncurredALAENetRecovery,0),2) AS IncurredALAENetRecovery
    ,ROUND(COALESCE(plan.Plan_TransactionCount,0),2) AS Plan_TransactionCount
    ,ROUND(COALESCE(plan.Plan_WrittenPremium,0),2) AS Plan_WrittenPremium
    ,PARSE_DATE('%Y%m%d', Cast({ddate} AS String)) AS DataQueryTillDate

FROM actual
    FULL JOIN plan 
    ON plan.AccountingDate = actual.AccountingDate
    AND plan.TransactionTypeGroup = actual.TransactionTypeGroup
    AND plan.PartitionCode = actual.PartitionCode
