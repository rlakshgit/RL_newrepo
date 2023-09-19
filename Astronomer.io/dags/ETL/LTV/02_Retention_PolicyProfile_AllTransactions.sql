-- CREATE OR REPLACE TABLE `{project}.{dataset}.wip_ltv_pp_p` AS 

WITH policyLevel AS 
(
    SELECT  
        JobNumber, 
        COUNT(DISTINCT ItemNumber) AS ScheduleItemCount,
        SUM(ItemLimit) AS SchedulePolicyLimit

    FROM `{project}.{dataset}.t_ltv_source` AS attr

    WHERE   1 = 1
            AND attr.ItemRank = 1
            AND attr.IsInactive = 0
            AND attr.CoverageTypeCode = 'SCH'
            -- AND attr.InsuredCountry = 'USA'
            AND attr.ItemLimit > 0

    GROUP BY 
        JobNumber
)
,attr AS 
(
   SELECT 
        attr.*
        ,COALESCE(ScheduleItemCount,0) AS ScheduleItemCount
        ,COALESCE(SchedulePolicyLimit,0) AS SchedulePolicyLimit

    FROM `{project}.{dataset}.t_ltv_source` AS attr

        LEFT JOIN policyLevel 
        ON policyLevel.JobNumber = attr.JobNumber 
        -- AND policyLevel.bq_load_date = attr.bq_load_date
)
,config1 AS 
(
    SELECT 
        attr.AccountNumber,
        attr.PolicyNumber, 
        attr.JobNumber, 
        attr.PeriodEffDate,
        attr.TransEffDate,
        attr.AccountingDate,
        attr.TermNumber,
        attr.ModelNumber,
        attr.InsuredAge,
        attr.InsuredCountry, 
        attr.InsuredState,
        attr.ItemNumber, 
        attr.RiskType, 
        attr.CoverageTypeCode, 
        attr.IsInactive, 
        attr.ItemClass, 
        attr.ItemLimit,
        attr.ScheduleItemCount,
        attr.SchedulePolicyLimit,
        CASE    WHEN attr.RiskPostalCode IS NULL OR attr.RiskPostalCode = '?' THEN SUBSTR(attr.InsuredPostalCode,1,5) 
                ELSE SUBSTR(attr.RiskPostalCode,1,5) END AS Rating_Zip,
        ref_pp_agelabel.AgeLabel,
        ref_pp_valuelabel.RP_ItemValueLabel,
        ref_pp_cntlabel.ItemCountLabel,
        ref_pp_polvaluelabel.PolicyValueLabel,
        ref_ltv_polvaluelabel.LTV_PolicyValueLabel

    FROM attr

        LEFT JOIN `{project}.{dataset}.ref_pp_agelabel` AS ref_pp_agelabel
        ON attr.InsuredAge BETWEEN ref_pp_agelabel.LowerLimit AND ref_pp_agelabel.UpperLimit

        LEFT JOIN `{project}.{dataset}.ref_pp_valuelabel` AS ref_pp_valuelabel
        ON attr.ItemLimit BETWEEN ref_pp_valuelabel.LowerLimit AND ref_pp_valuelabel.UpperLimit

        LEFT JOIN `{project}.{dataset}.ref_pp_cntlabel` AS ref_pp_cntlabel
        ON attr.ScheduleItemCount BETWEEN ref_pp_cntlabel.LowerLimit AND ref_pp_cntlabel.UpperLimit

        LEFT JOIN `{project}.{dataset}.ref_pp_polvaluelabel` AS ref_pp_polvaluelabel
        ON attr.SchedulePolicyLimit BETWEEN ref_pp_polvaluelabel.LowerLimit AND ref_pp_polvaluelabel.UpperLimit

        LEFT JOIN `{project}.{dataset}.ref_ltv_polvaluelabel` AS ref_ltv_polvaluelabel
        ON attr.SchedulePolicyLimit BETWEEN ref_ltv_polvaluelabel.LowerLimit AND ref_ltv_polvaluelabel.UpperLimit

    WHERE   1 = 1
            AND attr.ItemRank = 1
            AND attr.IsInactive = 0
            AND attr.CoverageTypeCode = 'SCH'
            AND attr.InsuredCountry = 'USA'
            --AND attr.JobNumber = 'J_3843518'
)
,Config2 AS 
(
    SELECT
        Config1.*,
        -- LTV_AgeBeta, LTV_ItemCountBeta, LTV_CountValueBeta,
        -- RP_ItemFactor,RP_ItemValueFactor,RP_ItemCountFactor,RP_CntValFactor,RP_ItemAgeFactor,RP_TerritoryFactor,RP_RegionFactor,
        round(RP_ItemFactor * RP_ItemValueFactor * RP_ItemCountFactor * RP_CntValFactor * 
                RP_ItemAgeFactor * RP_TerritoryFactor * RP_RegionFactor,3) AS RiskProfileScore,
        round(exp(5.4505059537 + LTV_AgeBeta + LTV_ItemCountBeta + LTV_CountValueBeta),3) AS LTVScore
    FROM Config1
        ## Policy Profile
        #Territory
        LEFT JOIN `{project}.{dataset}.ref_territory_group` AS ref_territory_group
        ON ref_territory_group.ZIP_Code = Config1.Rating_Zip

        LEFT JOIN `{project}.{dataset}.ref_pp_territory` AS ref_pp_territory
        ON ref_pp_territory.RP_Territory = COALESCE(ref_territory_group.mb_terr_group,'03> 03')

        #Region
        LEFT JOIN `{project}.{dataset}.ref_pp_region` AS ref_pp_region
        ON ref_pp_region.State = Config1.InsuredState

        #Item Type
        LEFT JOIN `{project}.{dataset}.ref_pp_itemtype` AS ref_pp_itemtype
        ON ref_pp_itemtype.ItemClass = Config1.ItemClass

        #Item X ItemLimit
        LEFT JOIN `{project}.{dataset}.ref_pp_itemvalue` AS ref_pp_itemvalue
        ON ref_pp_itemvalue.RP_ItemGroup = ref_pp_itemtype.RP_ItemGroup
        AND ref_pp_itemvalue.RP_ItemValueLabel = Config1.RP_ItemValueLabel

        #ItemCount
        LEFT JOIN `{project}.{dataset}.ref_pp_itemcount` AS ref_pp_itemcount
        ON ref_pp_itemcount.RP_CountGroup = ref_pp_itemtype.RP_CountGroup
        AND ref_pp_itemcount.ItemCountLabel = Config1.ItemCountLabel

        #ItemCount x PolicyValue
        LEFT JOIN `{project}.{dataset}.ref_pp_cntvalue` AS ref_pp_cntvalue
        ON ref_pp_cntvalue.ItemCountGrp = ref_pp_itemcount.ItemCountGrp
        AND ref_pp_cntvalue.PolicyValueLabel = Config1.PolicyValueLabel

        #Item x Age
        LEFT JOIN `{project}.{dataset}.ref_pp_itemage` AS ref_pp_itemage
        ON ref_pp_itemage.RP_ItemGroup = ref_pp_itemtype.RP_ItemGroup
        AND ref_pp_itemage.AgeLabel = Config1.AgeLabel

        ## Retention
        #Age
        LEFT JOIN `{project}.{dataset}.ref_ltv_age` AS ref_ltv_age
        ON ref_ltv_age.AgeLabel = Config1.AgeLabel

        #ItemCount
        LEFT JOIN `{project}.{dataset}.ref_ltv_itemcount` AS ref_ltv_itemcount
        ON ref_ltv_itemcount.ItemCountLabel = Config1.ItemCountLabel

        #CountValue
        LEFT JOIN `{project}.{dataset}.ref_ltv_cntvalue` AS ref_ltv_cntvalue
        ON ref_ltv_cntvalue.ItemCountGrp = ref_pp_itemcount.ItemCountGrp
        AND ref_ltv_cntvalue.LTV_PolicyValueLabel = Config1.LTV_PolicyValueLabel
)
,pp_score AS 
(
    SELECT 
        JobNumber
        ,round(avg(RiskProfileScore),3) AS PolicyProfileScore
    FROM Config2
    GROUP BY 
        JobNumber
)
,final AS 
(
SELECT DISTINCT
    Config2.JobNumber,
    Config2.SchedulePolicyLimit,
    -- pp_score.PolicyProfileScore,
    ref_ltv_ltv.Retention,
    ref_ltv_ltv.RetentionGroup,
    ref_ltv_ltv.RetentionGroup1,
    ref_pp_policyprofile.PolicyProfile,
    ref_pp_policyprofile.Profile2 AS PolicyProfileGroup

FROM Config2

    LEFT JOIN pp_score
    ON pp_score.JobNumber = Config2.JobNumber
    -- AND pp_score.bq_load_date = Config2.bq_load_date

    LEFT JOIN `{project}.{dataset}.ref_ltv_ltv` AS ref_ltv_ltv
    ON Config2.LTVScore BETWEEN ref_ltv_ltv.LowerLimit AND ref_ltv_ltv.UpperLimit

    LEFT JOIN `{project}.{dataset}.ref_pp_policyprofile` AS ref_pp_policyprofile
    ON pp_score.PolicyProfileScore BETWEEN ref_pp_policyprofile.LowerLimit AND ref_pp_policyprofile.UpperLimit
)

SELECT 
    attr.*,
    CASE WHEN attr.InsuredCountry != 'USA' OR attr.SchedulePolicyLimit = 0 THEN 0 ELSE final.Retention END AS Retention,
    CASE WHEN attr.InsuredCountry != 'USA' OR attr.SchedulePolicyLimit = 0 THEN '00>0' ELSE final.RetentionGroup END AS RetentionGroup,
    CASE WHEN attr.InsuredCountry != 'USA' OR attr.SchedulePolicyLimit = 0 THEN '00 > 00' ELSE final.RetentionGroup1 END AS RetentionGroup1,
    CASE WHEN attr.InsuredCountry != 'USA' OR attr.SchedulePolicyLimit = 0 THEN 0 ELSE final.PolicyProfile END AS PolicyProfile,
    CASE WHEN attr.InsuredCountry != 'USA' OR attr.SchedulePolicyLimit = 0 THEN '00>0' ELSE final.PolicyProfileGroup END AS PolicyProfileGroup,
    date('{date}')  AS allversion_load_date   
FROM attr

    LEFT JOIN final
    ON final.JobNumber = attr.JobNumber

