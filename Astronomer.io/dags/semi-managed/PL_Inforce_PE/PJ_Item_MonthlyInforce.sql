#StandardSQL
--CREATE OR REPLACE TABLE
--  `semi-managed-reporting.core_insurance_pjpa.nonpromoted_PJ_Item_MonthlyInforcePremium`
--  (AsOfDate DATE
--  ,ProductLine STRING
--  ,LOBCode STRING
--  ,PolicyNumber STRING
--  ,PolicyPeriodID INTEGER
--  ,ItemNumber NUMERIC
--  ,ItemPremium NUMERIC)
--PARTITION BY
--  DATE_TRUNC(AsOfDate, MONTH) 
--AS
WITH transactions AS (
    SELECT
        DATE(earned.AsOfDate) AS AsOfDate
        ,trxn.PolicyNumber
        ,earned.InforcePolicyPeriodID AS PolicyPeriodID
        ,COALESCE(item.ItemNumber, trxn.ItemNo, 1) AS ItemNumber
	    ,SUM(earned.InforcePremium) AS TransactionPremium

	FROM        `{project}.{dataset_ref_pe_pc}.pcrt_earn_summ` AS earned
    INNER JOIN  `{project}.{dataset_ref_pe_pc}.pcrt_trxn_summ` AS trxn
            ON  earned.ExtTrxnID = trxn.ID
            AND trxn.IsCeded = FALSE
            AND trxn.PolicyLineCode = 'PJ'
    INNER JOIN  `{project}.{dataset_ref_pc_current}.pc_policyperiod` AS pperiod
            ON  earned.InforcePolicyPeriodID = pperiod.ID
            AND DATE(pperiod._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    INNER JOIN  `{project}.{dataset_ref_pc_current}.pc_job` AS job
            ON  pperiod.JobID = job.ID
            AND job.Subtype <> 2
            AND DATE(job._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
	FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pcx_jewelryitem_jmic_pl` AS item
			ON	pperiod.ID = item.BranchID
			AND earned.InforcePLItemCovItemPublicID = item.PublicID
			AND item.IsItemInactive = FALSE
            AND DATE(item._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    WHERE   DATE(pperiod._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
	GROUP BY
        earned.AsOfDate
        ,trxn.PolicyNumber
        ,earned.InforcePolicyPeriodID
        ,item.ItemNumber
        ,trxn.ItemNo
        ,earned.InforcePremium
)

SELECT
    AsOfDate
    ,'PL' AS ProductLine
    ,'PJ' AS LOBCode
    ,PolicyNumber
    ,ItemNumber
    ,PolicyPeriodID
    ,SUM(TransactionPremium) AS ItemPremium
FROM        transactions

GROUP BY
    AsOfDate
    ,PolicyNumber
    ,ItemNumber
    ,PolicyPeriodID