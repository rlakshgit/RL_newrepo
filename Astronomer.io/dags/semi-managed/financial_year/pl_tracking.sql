#StandardSQL

CREATE OR REPLACE TABLE `{project}.{dataset}.pl_tracking_daily` AS

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
    FROM `{project}.{dataset}.fact_transaction_count`
    WHERE ProductLine = 'PL'
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
    FROM `{project}.{dataset}.fact_written_premium`
    WHERE ProductLine = 'PL'
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
    FROM `{project}.{dataset}.fact_earned_premium`
    WHERE ProductLine = 'PL'
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
    FROM `{project}.{dataset}.fact_incurred_loss`
    WHERE ProductLine = 'PL'
)
,actual AS 
(
    SELECT 
        base.AccountingDate
        ,base.TransactionTypeGroup
        ,attr.PartitionCode
        ,attr.PartitionName
        ,attr.DistributionSource
        ,attr.DistributionChannel
        ,attr.Recast
        ,COALESCE(attr.PlanYear,CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)) AS PlanYear
        ,SUM(base.TransactionCount) AS TransactionCount
        ,SUM(base.WrittenPremium) AS WrittenPremium
        ,SUM(base.EarnedPremium) AS EarnedPremium
        ,SUM(base.IncurredLossNetRecovery) AS IncurredLossNetRecovery
        ,SUM(base.IncurredALAENetRecovery) AS IncurredALAENetRecovery

    FROM base
        LEFT JOIN `{project}.{dataset}.pl_recast_all` attr 
        ON attr.PolicyNumber = base.PolicyNumber
    WHERE EXTRACT(YEAR FROM base.AccountingDate) >= (CAST(LEFT(CAST({ddate} AS STRING),4) AS INT) -2)

    GROUP BY
        base.AccountingDate
        ,base.TransactionTypeGroup
        ,attr.PartitionCode
        ,attr.PartitionName
        ,attr.DistributionSource
        ,attr.DistributionChannel
        ,attr.Recast
        ,PlanYear
)
,plan AS 
(
    SELECT 
        DATE(dailyPlan.AccountingDate) AS AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
        ,partitionMap.DistributionSource
        ,partitionMap.DistributionChannel
        ,partitionMap.Recast
        ,dailyPlan.PlanYear
        ,SUM(dailyPlan.Plan_TransactionCount) AS Plan_TransactionCount
        ,SUM(dailyPlan.Plan_WrittenPremium) AS Plan_WrittenPremium

    FROM `{project}.{dataset}.pl_plan_numbers_daily` AS dailyPlan

        LEFT JOIN 
            (SELECT DISTINCT PlanYear,Recast,PartitionName,DistributionSource,DistributionChannel FROM `{project}.{dataset}.pl_partition_mapping`) AS partitionMap
        ON partitionMap.PartitionName = dailyPlan.PartitionName
        AND partitionMap.PlanYear = dailyPlan.PlanYear

    WHERE dailyPlan.PlanYear = CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)

    GROUP BY
        dailyPlan.AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
        ,partitionMap.DistributionSource
        ,partitionMap.DistributionChannel
        ,partitionMap.Recast
        ,dailyPlan.PlanYear
)

SELECT 
    COALESCE(actual.PlanYear,plan.PlanYear ) AS PlanYear
    ,COALESCE(actual.AccountingDate,plan.AccountingDate ) AS AccountingDate
    ,COALESCE(actual.TransactionTypeGroup,plan.TransactionTypeGroup ) AS TransactionTypeGroup
    ,COALESCE(actual.PartitionCode,plan.PartitionCode ) AS PartitionCode
    ,COALESCE(actual.PartitionName,plan.PartitionName ) AS PartitionName
    ,COALESCE(actual.DistributionSource,plan.DistributionSource ) AS DistributionSource
    ,COALESCE(actual.DistributionChannel,plan.DistributionChannel ) AS DistributionChannel
    ,COALESCE(actual.Recast,plan.Recast ) AS Recast
    ,COALESCE(actual.TransactionCount,0) AS TransactionCount
    ,COALESCE(actual.WrittenPremium,0) AS WrittenPremium
    ,COALESCE(actual.EarnedPremium,0) AS EarnedPremium
    ,COALESCE(actual.IncurredLossNetRecovery,0) AS IncurredLossNetRecovery
    ,COALESCE(actual.IncurredALAENetRecovery,0) AS IncurredALAENetRecovery
    ,COALESCE(plan.Plan_TransactionCount,0) AS Plan_TransactionCount
    ,COALESCE(plan.Plan_WrittenPremium,0) AS Plan_WrittenPremium
    ,PARSE_DATE('%Y%m%d', Cast({ddate} AS String)) as DataQueryTillDate

FROM actual
    FULL JOIN plan 
    ON plan.AccountingDate = actual.AccountingDate
    AND plan.TransactionTypeGroup = actual.TransactionTypeGroup
    AND plan.PartitionCode = actual.PartitionCode
    AND plan.PartitionName = actual.PartitionName
    AND plan.DistributionSource = actual.DistributionSource
    AND plan.DistributionChannel = actual.DistributionChannel
    AND plan.Recast = actual.Recast
    AND plan.PlanYear = actual.PlanYear
