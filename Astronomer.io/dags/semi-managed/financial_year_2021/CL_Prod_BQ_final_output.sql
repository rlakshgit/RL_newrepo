#StandardSQL
WITH base AS
(
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,TransactionTypeGroup
        ,TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `semi-managed-reporting.financial_year_2021.transaction_count`
    WHERE ProductLine = 'CL'
    UNION ALL
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,TransactionTypeGroup
        ,0 AS TransactionCount
        ,WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `semi-managed-reporting.financial_year_2021.written_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `semi-managed-reporting.financial_year_2021.earned_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,IncurredLossNetRecovery
        ,IncurredALAENetRecovery
    FROM `semi-managed-reporting.financial_year_2021.incurred_loss`
    WHERE ProductLine = 'CL'
)
,attr AS
(
    SELECT * FROM `semi-managed-reporting.financial_year_2021.cl_recast_fy_2021`
)
,actual AS 
(
    SELECT 
        base.AccountingDate
        ,base.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,attr.PremiumBin
        ,attr.Segment
        ,SUM(base.TransactionCount) AS TransactionCount
        ,SUM(base.WrittenPremium) AS WrittenPremium
        ,SUM(base.EarnedPremium) AS EarnedPremium
        ,SUM(base.IncurredLossNetRecovery) AS IncurredLossNetRecovery
        ,SUM(base.IncurredALAENetRecovery) AS IncurredALAENetRecovery
    FROM base
        LEFT JOIN attr 
        ON attr.PolicyNumber = base.PolicyNumber
        LEFT JOIN `semi-managed-reporting.financial_year_2021.cl_partition_mapping` AS partitionMap
        ON partitionMap.PremiumBin = attr.PremiumBin
        AND partitionMap.Segment = attr.Segment
    GROUP BY
        base.AccountingDate
        ,base.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,attr.PremiumBin
        ,attr.Segment
)
,plan AS 
(
    SELECT 
        DATE(dailyPlan.AccountingDate) AS AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,partitionMap.PremiumBin
        ,partitionMap.Segment
        ,SUM(dailyPlan.Plan_TransactionCount) AS Plan_TransactionCount
        ,SUM(dailyPlan.Plan_WrittenPremium) AS Plan_WrittenPremium
    FROM `semi-managed-reporting.financial_year_2021.cl_plan_numbers_daily` AS dailyPlan
        LEFT JOIN 
            (SELECT DISTINCT PremiumBin, Segment, PartitionCode
             FROM `semi-managed-reporting.financial_year_2021.cl_partition_mapping`) AS partitionMap
        ON partitionMap.PartitionCode = dailyPlan.PartitionCode
    GROUP BY
        dailyPlan.AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,partitionMap.PremiumBin
        ,partitionMap.Segment
)

SELECT 
    COALESCE(actual.AccountingDate,plan.AccountingDate ) AS AccountingDate
    ,COALESCE(actual.TransactionTypeGroup,plan.TransactionTypeGroup ) AS TransactionTypeGroup
    ,COALESCE(actual.PartitionCode,plan.PartitionCode ) AS PartitionCode
    ,COALESCE(actual.PremiumBin,plan.PremiumBin ) AS PremiumBin
    ,COALESCE(actual.Segment, plan.Segment) AS Segment
    ,COALESCE(actual.TransactionCount,0) AS TransactionCount
    ,COALESCE(actual.WrittenPremium,0) AS WrittenPremium
    ,COALESCE(actual.EarnedPremium,0) AS EarnedPremium
    ,COALESCE(actual.IncurredLossNetRecovery,0) AS IncurredLossNetRecovery
    ,COALESCE(actual.IncurredALAENetRecovery,0) AS IncurredALAENetRecovery
    ,COALESCE(plan.Plan_TransactionCount,0) AS Plan_TransactionCount
    ,COALESCE(plan.Plan_WrittenPremium,0) AS Plan_WrittenPremium
    ,DATE('{date}') as DataQueryTillDate

FROM actual
    FULL JOIN plan 
    ON plan.AccountingDate = actual.AccountingDate
    AND plan.TransactionTypeGroup = actual.TransactionTypeGroup
    AND plan.PartitionCode = actual.PartitionCode
