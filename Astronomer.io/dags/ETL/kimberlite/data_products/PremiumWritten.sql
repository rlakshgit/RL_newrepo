/* Test / Validate

SELECT PolicyNumber, AccountSegment, JobNumber, CoverageCode, TransactionType, COUNT(*)
FROM `qa-edl.B_QA_ref_kimberlite.PremiumWritten`
GROUP BY PolicyNumber, AccountSegment, JobNumber, CoverageCode, TransactionType

SELECT AccountingYear, Product, AccountingQuarter,SUM(WrittenPremium)
FROM `qa-edl.B_QA_ref_kimberlite.PremiumWritten`
WHERE AccountingYear = 2021
GROUP BY AccountingYear, Product, AccountingQuarter
ORDER BY AccountingYear, Product, AccountingQuarter

SELECT Product, SUM(WrittenPremium)
FROM `qa-edl.B_QA_ref_kimberlite.PremiumWritten`
GROUP BY Product

---TransactionCounts
	select dp.JobType, COUNT(DISTINCT dp.PolicyNumber) polCount , SUM(PremiumWritten) prem
	FROM [DW_DDS].[bi_dds].[FactMonthlyPremiumWritten] pw
		--inner join [DW_DDS].[bi_dds].[DimRiskSegmentCore] core on pw.RiskSegmentKey = core.RiskSegmentKey
	  inner join [bi_dds].[DimLineOfBusiness] lob on pw.LineOfBusinessKey = lob.LineOfBusinessKey
	  inner join [bi_dds].dimPolicy dp on dp.PolicyKey = pw.PolicyKey
	  where datekey between 20190101 and 20191231
	  AND lob.LOBCode = 'PA'
	  group by dp.JobType

*/

/**********************************************************************************************************************************
 ***** Kimberlite - Curated Data Set ********
		PremiumWritten
**********************************************************************************************************************************/
/*
----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

    11/01/2021  DROBAK      Initial create (ProductAndRiskConfiguration table not ready yet)
    11/19/2021  DROBAK      Corrections to joins for coverage and product&risk config tables
    11/22/2021  DROBAK      FIX: for IM ProductAndRiskConfiguration was an INNER JOIN
    11/24/2021  DROBAK      FIX: remove date range portion of join to TransactionConfiguration
	11/29/2021	DROBAK		Switch AccountingDate from TransactionConfiguration to FinancialTransaction

----------------------------------------------------------------------------------------------------------------------------------
 *****  Unioned Sections  *****
----------------------------------------------------------------------------------------------------------------------------------
	BOP Direct
    IM Direct
    UMB Direct
    PA Direct
    PJ Direct
        Note: Ceded not inlcuded at this time though untested commented sections exist
-----------------------------------------------------------------------------------------------------------------------------------
*/	
/**********************************************************************************************************************************/
DELETE `{project}.{dp_dataset}.PremiumWritten` WHERE bq_load_date = DATE(DATE({partition_date}));

INSERT INTO `{project}.{dp_dataset}.PremiumWritten`


    (
        AccountingDate,
        AccountingYear,
        AccountingQuarter,
        AccountingMonth,
        BusinessUnit,
        Product,
        AccountSegment,
        JobNumber,
        PolicyNumber,
        ItemNumber,
        CoverageCode,
        TransactionType,
        TransactionCount,
        PolicyCoverageRank,
        WrittenPremium,
        bq_load_date
    )

    --CREATE OR REPLACE TABLE `{project}.{dp_dataset}.PremiumWritten`
    --AS
    ---------------------
    ---- BOP Direct ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'                                                                               AS BusinessUnit
        ,'BOP'                                                                                      AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
    --  ,0 As ItemNumber
        ,CLCoverageLevelAttributesBOP.CoverageCode                                                  AS CoverageCode
        --,'None' AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
                ELSE 1
            END																					    AS TransactionCount
    
    --    ,0 AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesBOP.CoverageCode
                        ORDER BY 
                                    ProductAndRiskConfiguration.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate    --Needed?
                        )												    						AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                     AS WrittenPremium
        ,DATE(CURRENT_DATE) AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionBOPDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE({partition_date})) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem

    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, CoverageCode, ProductCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) CLCoverageLevelAttributesBOP
        ON FinancialTransaction.BOPCoverageKey= CLCoverageLevelAttributesBOP.CoverageKey
        AND CLCoverageLevelAttributesBOP.ProductCode = 'BOP'
 
 -- Issue with this join 
    LEFT JOIN (SELECT RiskLocationBusinessOwnersKey, RiskBuildingKey, ItemNumber, TranType FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE({partition_date})) ProductAndRiskConfiguration
        ON FinancialTransaction.RiskLocationKey=ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey  
        AND FinancialTransaction.RiskBuildingKey=ProductAndRiskConfiguration.RiskBuildingKey
       
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY 
         FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,TransactionConfiguration.Segment
    --	,dPol.SourceOfBusiness
        ,TransactionConfiguration.JobNumber
        ,TransactionConfiguration.PolicyNumber
    	,ProductAndRiskConfiguration.ItemNumber
        ,CLCoverageLevelAttributesBOP.CoverageCode
        ,TransactionConfiguration.TranType
        ,ProductAndRiskConfiguration.TranType
        ,DATE(CURRENT_DATE)

/*
UNION ALL 

    ---- BOP Ceded ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'                                                                          AS BusinessUnit
        ,'BOP'                                                                                      AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    --	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        ,CLCoverageLevelAttributesBOP.CoverageCode                                                  AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesBOP.CoverageCode
                        ORDER BY 
                                    --ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium
        ,DATE(CURRENT_DATE) AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionBOPCeded` FinancialTransactionBOPCeded
    LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE("2021-11-10") )TransactionConfiguration
            --AND EXTRACT(YEAR FROM AccountingDate) >= (EXTRACT(YEAR FROM CURRENT_DATE())-3) AND DATE(AccountingDate) <= DATE("2021-11-10")) TransactionConfiguration
        ON FinancialTransactionBOPCeded.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransactionBOPCeded.SourceSystem = TransactionConfiguration.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON TransactionConfiguration.PolicyTransactionKey=ProductAndRiskConfiguration.PolicyTransactionKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, RiskBOPLocationKey, RiskBOPBuildingKey, CoverageCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE("2021-11-10")) CLCoverageLevelAttributesBOP
        ON TransactionConfiguration.PolicyTransactionKey=CLCoverageLevelAttributesBOP.PolicyTransactionKey
        AND ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey=CLCoverageLevelAttributesBOP.RiskBOPLocationKey
        AND ProductAndRiskConfiguration.RiskBuildingKey=CLCoverageLevelAttributesBOP.RiskBOPBuildingKey

    WHERE FinancialTransactionBOPCeded.bq_load_date = "2021-11-10"
    AND FinancialTransactionBOPCeded.TransactionPostedDate IS NOT NULL
    AND FinancialTransactionBOPCeded.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    AND FinancialTransactionBOPCeded.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
*/

UNION ALL 

    ---------------------
    ---- IM Direct ----
    SELECT 
        CAST(FinancialTransaction.TransactionWrittenDate AS DATE)						            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'                                                                               AS BusinessUnit
        ,'IM'                                                                                       AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        --,0 AS ItemNumber
        ,CLCoverageLevelAttributesIM.CoverageCode                                                   AS CoverageCode
        --,'None' AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
                ELSE 1
        END																						    AS TransactionCount
    
        --,0 AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesIM.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate --needed?
                        )												    						AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                     AS WrittenPremium
        ,DATE(CURRENT_DATE)                                                                         AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt  --, ProductAndRiskConfiguration.ItemNumber
    FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE({partition_date})) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem
        
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, CoverageCode, ProductCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) CLCoverageLevelAttributesIM
        ON FinancialTransaction.IMCoverageKey= CLCoverageLevelAttributesIM.CoverageKey
        AND CLCoverageLevelAttributesIM.ProductCode = 'IM'

    --Issue with Location keys due to cost join vs policyperiod join (some Location Public ID's not in Risk table)    
    LEFT JOIN (SELECT RiskLocationIMKey, ItemNumber, TranType FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE({partition_date})) ProductAndRiskConfiguration
        ON FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationIMKey

    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY 
         FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,TransactionConfiguration.Segment
    --	,dPol.SourceOfBusiness
        ,TransactionConfiguration.JobNumber
        ,TransactionConfiguration.PolicyNumber
    	,ProductAndRiskConfiguration.ItemNumber
        ,CLCoverageLevelAttributesIM.CoverageCode
        ,TransactionConfiguration.TranType
        ,ProductAndRiskConfiguration.TranType
        ,DATE(CURRENT_DATE)

/*
UNION ALL 

    ---------------------
    ---- IM Ceded ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'																				AS BusinessUnit
        ,'IM'																						AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        ,CLCoverageLevelAttributesIM.CoverageCode                                                   AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesIM.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionIMCeded` FinancialTransactionIMCeded
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE("2021-11-10")) TransactionConfiguration
        ON FinancialTransactionIMCeded.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransactionIMCeded.SourceSystem = TransactionConfiguration.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON FinancialTransaction.PolicyTransactionKey = ProductAndRiskConfiguration.PolicyTransactionKey
        AND FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationIMKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, RiskIMLocationKey, CoverageCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE("2021-11-10")) CLCoverageLevelAttributesIM
        ON TransactionConfiguration.PolicyTransactionKey=CLCoverageLevelAttributesIM.PolicyTransactionKey
        AND FinancialTransaction.CoverageKey = CLCoverageLevelAttributesIM.CoverageKey
        --AND ProductAndRiskConfiguration.RiskLocationIMKey=CLCoverageLevelAttributesIM.RiskIMLocationKey

    WHERE FinancialTransactionIMCeded.bq_load_date = "2021-11-10"
    AND FinancialTransactionIMCeded.TransactionPostedDate IS NOT NULL
    AND FinancialTransactionIMCeded.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    AND FinancialTransactionIMCeded.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
*/

UNION ALL 

    ---------------------
    ---- UMB Direct ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'                                                                               AS BusinessUnit
        ,'UMB'                                                                                      AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
    --    ,0 AS ItemNumber
        ,CLCoverageLevelAttributesUMB.CoverageCode                                                  AS CoverageCode
        --,'None' AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
    --    ,0 AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesUMB.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate --Needed?
                        )												    						AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                     AS WrittenPremium
        ,DATE(CURRENT_DATE)                                                                         AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionUMBDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE({partition_date})) TransactionConfiguration
            --AND EXTRACT(YEAR FROM AccountingDate) >= (EXTRACT(YEAR FROM CURRENT_DATE())-3)
            --AND DATE(AccountingDate) <= DATE({partition_date})) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem
    
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, CoverageCode, ProductCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) CLCoverageLevelAttributesUMB
        ON FinancialTransaction.UMBCoverageKey= CLCoverageLevelAttributesUMB.CoverageKey
        AND CLCoverageLevelAttributesUMB.ProductCode = 'UMB'
  
    --Issue with this join at Location
    LEFT JOIN (SELECT RiskLocationBusinessOwnersKey, ItemNumber, TranType FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE({partition_date})) ProductAndRiskConfiguration
        ON FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey

    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
        FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,TransactionConfiguration.Segment
    --	,dPol.SourceOfBusiness
        ,TransactionConfiguration.JobNumber
        ,TransactionConfiguration.PolicyNumber
    	,ProductAndRiskConfiguration.ItemNumber
        ,CLCoverageLevelAttributesUMB.CoverageCode
        ,TransactionConfiguration.TranType
        ,ProductAndRiskConfiguration.TranType
        ,DATE(CURRENT_DATE)
/*
UNION ALL 

    ---------------------
    ---- UMB Ceded ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'																				AS BusinessUnit
        ,'UMB'                                                                                      AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        ,CLCoverageLevelAttributesBOP.CoverageCode                                                  AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,CLCoverageLevelAttributesBOP.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionUMBDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE("2021-11-10")) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON FinancialTransaction.PolicyTransactionKey = ProductAndRiskConfiguration.PolicyTransactionKey
        AND FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, CoverageCode, ProductCode FROM `{project}.{dest_dataset}.CLCoverageLevelAttributes` WHERE bq_load_date=DATE("2021-10-03")) CLCoverageLevelAttributesUMB
        ON TransactionConfiguration.PolicyTransactionKey=CLCoverageLevelAttributesUMB.PolicyTransactionKey
        AND FinancialTransaction.UMBCoverageKey = CLCoverageLevelAttributesUMB.CoverageKey
        AND CLCoverageLevelAttributesBOP.ProductCode = 'UMB'

    WHERE FinancialTransaction.bq_load_date = "2021-11-10"
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
-- -- matched to Prod WP Dashboard for september 2021
*/

UNION ALL 

    ---------------------
    ---- PA Direct ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Personal'                                                                                 AS BusinessUnit
        ,'PA'                                                                                       AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
    --    ,0 AS ItemNumber
        ,PAJewelryCoverageLevelAttributes.CoverageCode                                              AS CoverageCode
        --,'None' AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE WHEN PAJewelryCoverageLevelAttributes.CoverageCode = 'JewelryItemCov_JM' THEN 
                CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                        WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
                        ELSE 1
                END
            ELSE 0
        END																						    AS TransactionCount
    
    --    ,0 AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,PAJewelryCoverageLevelAttributes.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate --needed?
                        )												    						AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                     AS WrittenPremium
        ,DATE(CURRENT_DATE)                                                                         AS bq_load_date


    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionPADirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE({partition_date})) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem
  
    LEFT JOIN (SELECT PAJewelryCoverageKey, PolicyTransactionKey, RiskPAJewelryKey, CoverageCode FROM `{project}.{dest_dataset}.PAJewelryCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) PAJewelryCoverageLevelAttributes
        ON FinancialTransaction.PAJewelryCoverageKey = PAJewelryCoverageLevelAttributes.PAJewelryCoverageKey

    LEFT JOIN (SELECT RiskPAJewelryKey, ItemNumber, TranType FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE({partition_date})) ProductAndRiskConfiguration
        ON FinancialTransaction.RiskPAJewelryKey=ProductAndRiskConfiguration.RiskPAJewelryKey
        
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeGroup IN ('ScheduledPremium', 'UnscheduledPremium')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
         FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,TransactionConfiguration.Segment
    --	,dPol.SourceOfBusiness
        ,TransactionConfiguration.JobNumber
        ,TransactionConfiguration.PolicyNumber
    	,ProductAndRiskConfiguration.ItemNumber
        ,PAJewelryCoverageLevelAttributes.CoverageCode
        ,TransactionConfiguration.TranType
        ,ProductAndRiskConfiguration.TranType
        ,DATE(CURRENT_DATE)

UNION ALL 

    ---------------------
    ---- PJ Direct ----
    SELECT 
        FinancialTransaction.TransactionWrittenDate										            AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Personal'                                                                                 AS BusinessUnit
        ,'PJ'                                                                                       AS Product
        ,TransactionConfiguration.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,TransactionConfiguration.JobNumber															AS JobNumber
        ,TransactionConfiguration.PolicyNumber														AS PolicyNumber							
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
    --    ,0 AS ItemNumber
        ,PJCoverageLevelAttributes.CoverageCode                                                     AS CoverageCode
        --,'None' AS CoverageCode
        ,TransactionConfiguration.TranType														    AS TransactionType
    
        ,CASE WHEN PJCoverageLevelAttributes.CoverageCode = 'JewelryItemCov_JMIC_PL' THEN 
                CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                        WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
                        ELSE 1
                END
            ELSE 0
        END																						    AS TransactionCount
    
    --    ,0 AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,TransactionConfiguration.JobNumber
                                    ,PJCoverageLevelAttributes.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate --needed?
                        )												    						AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                     AS WrittenPremium
        ,DATE(CURRENT_DATE)                                                                         AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionPJDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date=DATE({partition_date})) TransactionConfiguration
        ON FinancialTransaction.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = TransactionConfiguration.SourceSystem
    
    LEFT JOIN (SELECT PolicyTransactionKey, ItemCoverageKey, RiskJewelryItemKey, CoverageCode FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) PJCoverageLevelAttributes
        ON FinancialTransaction.ItemCoverageKey = PJCoverageLevelAttributes.ItemCoverageKey
        
    LEFT JOIN (SELECT RiskJewelryItemKey, ItemNumber, TranType FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE({partition_date})) ProductAndRiskConfiguration
        ON FinancialTransaction.RiskJewelryItemKey=ProductAndRiskConfiguration.RiskJewelryItemKey
       
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeGroup IN ('ScheduledPremium', 'UnscheduledPremium')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
         FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,TransactionConfiguration.Segment
    --	,dPol.SourceOfBusiness
        ,TransactionConfiguration.JobNumber
        ,TransactionConfiguration.PolicyNumber
    	,ProductAndRiskConfiguration.ItemNumber
        ,PJCoverageLevelAttributes.CoverageCode
        ,TransactionConfiguration.TranType
        ,ProductAndRiskConfiguration.TranType
        ,DATE(CURRENT_DATE)


/*
select max(bq_load_date) from `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
select max(bq_load_date) from `{project}.{dest_dataset}.CLCoverageLevelAttributes`

select max(bq_load_date) from `{project}.{dest_dataset}.FinancialTransactionUMBCeded`
select max(bq_load_date) from `{project}.{dest_dataset}.dar_FinancialTransactionUMBCeded`

select max(bq_load_date) from `{project}.{dest_dataset}.FinancialTransactionUMBDirect`
select max(bq_load_date) from `{project}.{dest_dataset}.dar_FinancialTransactionUMBDirect`

select max(bq_load_date) from `{project}.{dest_dataset}.FinancialTransactionBOPDirect`
select max(bq_load_date) from `{project}.{dest_dataset}.dar_FinancialTransactionBOPDirect`

select max(bq_load_date) from `{project}.{dest_dataset}.FinancialTransactionBOPCeded`
select max(bq_load_date) from `{project}.{dest_dataset}.dar_FinancialTransactionBOPCeded`

select max(bq_load_date)FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` 
select max(bq_load_date)FROM `{project}.{dest_dataset}.dar_FinancialTransactionIMDirect` 

select max(bq_load_date)FROM `{project}.{dest_dataset}.FinancialTransactionIMCeded` 
select max(bq_load_date)FROM `{project}.{dest_dataset}.dar_FinancialTransactionIMCeded` 

select max(bq_load_date)FROM `{project}.{dest_dataset}.FinancialTransactionPADirect`

select max(_PARTITIONTIME) from `qa-edl.B_QA_ref_pc_current.pcx_ilmtransaction_jmic`
select max(bq_load_date) from `{project}.{dest_dataset}.PAJewelryCoverageLevelAttributes`

select max(bq_load_date) from `{project}.{dest_dataset}.PJCoverageLevelAttributes`
select max(bq_load_date) from `{project}.{dest_dataset}.TransactionConfiguration`

select max(bq_load_date) FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration`
*/
