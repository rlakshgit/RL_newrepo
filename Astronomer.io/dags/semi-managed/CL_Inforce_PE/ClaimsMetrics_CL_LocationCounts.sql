--CREATE OR REPLACE VIEW semi-managed-reporting.data_products_t2_claims.nonpromoted_ClaimsMetrics_CL_LocationCounts
--AS
SELECT
    inforce.AsOfDate
    ,`semi-managed-reporting.custom_functions.fn_GetDateKeyFromDate`(STRING(inforce.AsOfDate)) AS AsOfDateKey
    ,inforce.PolicyNumber
    ,CONCAT(inforce.PolicyNumber, "-", inforce.LocationNumber) AS PolicyLocation
    ,inforce.ProductLine
    ,inforce.LOBCode
    ,inforce.PolicyType
    ,inforce.ASL
    ,pperiod.NewRen
    ,pperiod.AccountSegment
    ,pperiod.AgencyMasterCode
    ,pperiod.AgencyMasterName
    ,pperiod.AgencyCode
    ,pperiod.AgencyName
    ,pperiod.DW_SourceOfBusiness
    ,recast.PremiumBin
    ,recast.Segment
    ,mapping.PartitionCode
    ,inforce.LocationNumber
    ,inforce.RolexIndicator
    ,inforce.LocationPremium
    ,CASE
        WHEN inforce.PolicyNumber = "55-013722" THEN 1
        ELSE 0
        END AS OceanCargoFlag

FROM        `{bq_target_project}.{dataset}.nonpromoted_CL_Location_MonthlyInforcePremium` AS inforce
INNER JOIN   `semi-managed-reporting.wip_tverner.wip_VIEW_policyperiods` AS pperiod
        ON  inforce.InforcePolicyPeriodID = pperiod.PolicyPeriodID
LEFT JOIN   `{bq_target_project}.{dataset_fy_2022}.cl_riskgroup_im_fy_2022` AS  recast
        ON  inforce.PolicyNumber = recast.PolicyNumber
LEFT JOIN   `semi-managed-reporting.financial_year_2022.cl_partition_mapping` AS mapping
        ON  recast.PremiumBin = mapping.PremiumBin
        AND recast.Segment = mapping.Segment
GROUP BY
    inforce.AsOfDate
    ,inforce.ProductLine
    ,inforce.LOBCode
    ,inforce.PolicyType
    ,inforce.PolicyNumber
    ,inforce.ASL
    ,pperiod.NewRen
    ,pperiod.AccountSegment
    ,pperiod.AgencyMasterCode
    ,pperiod.AgencyMasterName
    ,pperiod.AgencyCode
    ,pperiod.AgencyName
    ,pperiod.DW_SourceOfBusiness
    ,recast.PremiumBin
    ,recast.Segment
    ,mapping.PartitionCode
    ,inforce.LocationNumber
    ,inforce.RolexIndicator
    ,inforce.LocationPremium
