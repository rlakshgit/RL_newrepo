SELECT
    AccountingDate
    ,ProductLine
    ,LOBCode
    ,Country
    ,InsuredState
	,RatedStateCode
    --,PrimaryState
    ,ASL
    ,PolicyNumber
    ,Recast
    ,SubRecast
    ,PartitionCode
    ,PartitionName
    ,TransactionTypeGroup
    ,TransactionType
    ,COUNT(ItemNumber) AS ItemCount
    ,SUM(ItemWrittenPremium) AS PolicyWrittenPremium
    ,DataQueryTillDate

FROM        `{project}.{dataset}.nonpromoted_PL_WP_Items` AS items

GROUP BY
    AccountingDate
    ,ProductLine
    ,LOBCode
    ,Country
    ,InsuredState
	,RatedStateCode
    --,PrimaryState
    ,ASL
    ,PolicyNumber
    ,Recast
    ,SubRecast
    ,PartitionCode
    ,PartitionName
    ,TransactionTypeGroup
    ,TransactionType
    ,DataQueryTillDate
