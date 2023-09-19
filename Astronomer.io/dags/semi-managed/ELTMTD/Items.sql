WITH items AS (
SELECT
    DATE(trxn.TrxnWrittenDate) AS AccountingDate
    ,'PL' AS ProductLine
    ,CASE
        WHEN trxn.PolicyLineCode = 'JPA' THEN 'PA' ELSE trxn.PolicyLineCode END AS LOBCode
    ,CASE
        WHEN trxn.CompanyCode = 'JMCN' THEN 'CAN' ELSE 'USA' END AS Country
    ,trxn.LocStateCode AS ItemState
    ,trxn.PrimaryLocationStateCode AS InsuredState
    ,trxn.RatedStateCode
    ,trxn.AnnualStmntLine AS ASL
    ,trxn.PolicyNumber
    ,CASE
        WHEN t_job.NAME = 'Submission' THEN 'Submission'
        ELSE 'Non-Submission' END AS TransactionTypeGroup
    ,t_job.NAME AS TransactionType
    ,CASE
		WHEN trxn.PolicyLineCode = 'JPA' THEN COALESCE(article.ItemNumber, 0)
		ELSE COALESCE(item.ItemNumber, 0)
		END AS ItemNumber
    ,SUM(COALESCE(trxn.TrxnAmount, 0)) AS ItemWrittenPremium
    ,DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS DataQueryTillDate

FROM       `{project}.{dataset_ref_pe_pc}.pcrt_trxn_summ` AS trxn
LEFT JOIN  `{project}.{dataset_ref_pc_current}.pc_policyperiod` AS pperiod
        ON  trxn.BranchID = pperiod.ID
        AND DATE(pperiod._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
LEFT JOIN   `{project}.{dataset_ref_pc_current}.pc_job` AS job
        ON  pperiod.JobID = job.ID
        AND DATE(job._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
LEFT JOIN   `{project}.{dataset_ref_pc_current}.pctl_job` AS t_job
        ON  job.Subtype = t_job.ID
FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pcx_personalarticle_jm` AS article
	ON	trxn.JPAArticlePublicID = article.PublicID
        AND     trxn.JPAArticlePublicID IS NOT NULL
        AND DATE(article._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pcx_jewelryitem_jmic_pl` AS item
	ON	trxn.PLItemPublicID = item.PublicID
        AND     trxn.PLItemPublicID IS NOT NULL
        AND DATE(item._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

WHERE       trxn.PolicyLineCode IN ('PJ', 'JPA')
        AND (DATE(trxn.TrxnWrittenDate) >= '2016-01-01' AND DATE(trxn.TrxnWrittenDate) <= CURRENT_DATE())
        AND trxn.IsCeded = false
        AND trxn.Premium = true

GROUP BY
    DATE(trxn.TrxnWrittenDate)
    ,trxn.PolicyLineCode
    ,trxn.CompanyCode 
    ,trxn.LocStateCode
    ,trxn.PrimaryLocationStateCode
    ,trxn.RatedStateCode
    ,trxn.AnnualStmntLine 
    ,trxn.PolicyNumber
    ,t_job.NAME
    ,article.ItemNumber
    ,item.ItemNumber
)

SELECT
    items.AccountingDate 
    ,items.ProductLine 
    ,items.LOBCode
    ,items.Country
    ,items.ItemState
    ,items.InsuredState
    ,items.RatedStateCode
    ,items.ASL
    ,items.PolicyNumber
    ,mapping.Recast
    ,mapping.SubRecast
    ,mapping.PartitionCode
    ,mapping.PartitionName
    ,TransactionTypeGroup
    ,TransactionType
    ,ItemNumber
    ,SUM(ItemWrittenPremium) AS ItemWrittenPremium
    ,DataQueryTillDate
FROM items
LEFT JOIN   `semi-managed-reporting.{dataset_fy_2022}.wip_recast_FULL` AS recast
        ON  items.PolicyNumber = recast.PolicyNumber
LEFT JOIN   `semi-managed-reporting.{dataset_fy_2022}.pl_partition_mapping` AS mapping
        ON  recast.Recast = mapping.Recast
        AND recast.SubRecast = mapping.SubRecast

GROUP BY
    items.AccountingDate 
    ,items.ProductLine 
    ,items.LOBCode
    ,items.Country
    ,items.ItemState
    ,items.InsuredState
    ,items.RatedStateCode
    ,items.ASL
    ,items.PolicyNumber
    ,mapping.Recast
    ,mapping.SubRecast
    ,mapping.PartitionCode
    ,mapping.PartitionName
    ,TransactionTypeGroup
    ,TransactionType
    ,ItemNumber
    ,DataQueryTillDate