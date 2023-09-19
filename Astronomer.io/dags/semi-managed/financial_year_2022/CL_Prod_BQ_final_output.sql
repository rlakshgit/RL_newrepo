#StandardSQL
WITH base AS
(
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,CAST(ModelNumber AS int64) AS ModelNumber
        ,CASE WHEN CAST(TermNumber AS INT64) = 1 THEN CAST(TermNumber AS INT64) - 0 ELSE CAST(TermNumber AS INT64) - 1 END AS PriorTerm
        ,InsuredCountryCode
        ,TransactionTypeGroup
        ,TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.transaction_count`
    WHERE ProductLine = 'CL'
    UNION ALL
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,CASE WHEN CAST(TermNumber AS INT64) = 1 THEN CAST(TermNumber AS INT64) - 0 ELSE CAST(TermNumber AS INT64) - 1 END AS PriorTerm
        ,InsuredCountryCode
        ,TransactionTypeGroup
        ,0 AS TransactionCount
        ,WrittenPremium
        ,0 AS EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.written_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,CASE WHEN CAST(TermNumber AS INT64) = 1 THEN CAST(TermNumber AS INT64) - 0 ELSE CAST(TermNumber AS INT64) - 1 END AS PriorTerm
        ,InsuredCountryCode
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,EarnedPremium
        ,0 AS IncurredLossNetRecovery
        ,0 AS IncurredALAENetRecovery
    FROM `{project}.{dataset}.earned_premium`
    WHERE ProductLine = 'CL'
    UNION ALL 
    SELECT 
        DATE(AccountingDate) AS AccountingDate
        ,PolicyNumber
        ,ProductCode
        ,TermNumber
        ,ModelNumber
        ,CASE WHEN CAST(TermNumber AS INT64) = 1 THEN CAST(TermNumber AS INT64) - 0 ELSE CAST(TermNumber AS INT64) - 1 END AS PriorTerm
        ,InsuredCountryCode
        ,'' AS TransactionTypeGroup
        ,0 AS TransactionCount
        ,0 AS WrittenPremium
        ,0 AS EarnedPremium
        ,IncurredLossNetRecovery
        ,IncurredALAENetRecovery
    FROM `{project}.{dataset}.incurred_loss`
    WHERE ProductLine = 'CL'
)
,actual AS 
(
    SELECT 
        a.AccountingDate
        ,a.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,partitionMap.PartitionName
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
            ,CASE WHEN base.ProductCode = 'IM' AND base.InsuredCountryCode != 'CAN' THEN 
                    CASE WHEN base.ModelNumber = 1 THEN priorRisk.RiskGroup ELSE termRisk.RiskGroup END
             END AS RiskGroup
        FROM base
            LEFT JOIN `{project}.{dataset}.cl_riskgroup_im_fy_2022` AS termRisk
            ON termRisk.PolicyNumber = base.PolicyNumber
            AND termRisk.TermNumber = base.TermNumber
            AND termRisk.ProductCode = base.ProductCode
            LEFT JOIN `{project}.{dataset}.cl_riskgroup_im_fy_2022` AS priorRisk
            ON priorRisk.PolicyNumber = base.PolicyNumber
            AND priorRisk.TermNumber = base.PriorTerm
            AND priorRisk.ProductCode = base.ProductCode
    )a
    
        LEFT JOIN `{project}.{dataset}.cl_partition_mapping` AS partitionMap
        ON partitionMap.ProductCode = a.ProductCode
        AND COALESCE(partitionMap.RiskGroup, -1) = COALESCE(a.RiskGroup, -1)
    GROUP BY
        a.AccountingDate
        ,a.TransactionTypeGroup
        ,partitionMap.PartitionCode
        ,partitionMap.PartitionName
)
,plan AS 
(
    SELECT 
        DATE(dailyPlan.AccountingDate) AS AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
        ,SUM(dailyPlan.Plan_TransactionCount) AS Plan_TransactionCount
        ,SUM(dailyPlan.Plan_WrittenPremium) AS Plan_WrittenPremium
    FROM `{project}.{dataset}.cl_plan_numbers_daily` AS dailyPlan
    GROUP BY
        dailyPlan.AccountingDate
        ,dailyPlan.TransactionTypeGroup
        ,dailyPlan.PartitionCode
        ,dailyPlan.PartitionName
)

SELECT 
    COALESCE(actual.AccountingDate,plan.AccountingDate ) AS AccountingDate
    ,COALESCE(actual.TransactionTypeGroup,plan.TransactionTypeGroup ) AS TransactionTypeGroup
    ,COALESCE(actual.PartitionCode,plan.PartitionCode ) AS PartitionCode
    ,COALESCE(actual.PartitionName,plan.PartitionName ) AS PartitionName
    ,COALESCE(actual.TransactionCount,0) AS TransactionCount
    ,Round(COALESCE(actual.WrittenPremium,0),2) AS WrittenPremium
    ,Round(COALESCE(actual.EarnedPremium,0),2) AS EarnedPremium
    ,Round(COALESCE(actual.IncurredLossNetRecovery,0),2) AS IncurredLossNetRecovery
    ,Round(COALESCE(actual.IncurredALAENetRecovery,0),2) AS IncurredALAENetRecovery
    ,Round(COALESCE(plan.Plan_TransactionCount,0),2) AS Plan_TransactionCount
    ,Round(COALESCE(plan.Plan_WrittenPremium,0),2) AS Plan_WrittenPremium
     ,DATE('{date}') as DataQueryTillDate

FROM actual
    FULL JOIN plan 
    ON plan.AccountingDate = actual.AccountingDate
    AND plan.TransactionTypeGroup = actual.TransactionTypeGroup
    AND plan.PartitionCode = actual.PartitionCode
