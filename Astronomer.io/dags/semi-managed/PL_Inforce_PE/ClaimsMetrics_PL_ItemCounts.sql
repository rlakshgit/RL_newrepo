--CREATE OR REPLACE VIEW semi-managed-reporting.data_products_t2_claims.nonpromoted_ClaimsMetrics_PL_ItemCounts
--AS
SELECT
    inforce.AsOfDate
    ,`semi-managed-reporting.custom_functions.fn_GetDateKeyFromDate`(STRING(inforce.AsOfDate)) AS AsOfDateKey
    ,inforce.ProductLine
    ,inforce.LOBCode
    ,pperiod.NewRen
    ,pperiod.AgencyMasterCode
    ,pperiod.AgencyMasterName
    ,pperiod.AgencyCode
    ,pperiod.AgencyName
    ,pperiod.DW_SourceOfBusiness
    ,mapping.Recast
    ,mapping.SubRecast
    ,mapping.PartitionCode
    ,mapping.PartitionName
    ,mapping.DistributionSource
    ,mapping.DistributionChannel
    ,SUM(inforce.ItemCount) AS ItemCount
    ,SUM(inforce.PolicyPremium) AS InforcePremium 

FROM        `{bq_target_project}.{dataset}.nonpromoted_PL_Policy_MonthlyInforcePremium` AS inforce
LEFT JOIN   `semi-managed-reporting.core_insurance.VIEW_policyperiods` AS pperiod
        ON  inforce.PolicyPeriodID = pperiod.PolicyPeriodID
LEFT JOIN   `semi-managed-reporting.financial_year_2022.wip_recast_FULL` AS recast
        ON  inforce.PolicyNumber = recast.PolicyNumber
LEFT JOIN   `semi-managed-reporting.financial_year_2022.pl_partition_mapping` AS mapping
        ON  recast.Recast = mapping.Recast
        AND recast.SubRecast = mapping.SubRecast
        
GROUP BY
    inforce.AsOfDate
    ,inforce.ProductLine
    ,inforce.LOBCode
    ,pperiod.NewRen
    ,pperiod.AgencyMasterCode
    ,pperiod.AgencyMasterName
    ,pperiod.AgencyCode
    ,pperiod.AgencyName
    ,pperiod.DW_SourceOfBusiness
    ,mapping.Recast
    ,mapping.SubRecast
    ,mapping.PartitionCode
    ,mapping.PartitionName
    ,mapping.DistributionSource
    ,mapping.DistributionChannel
