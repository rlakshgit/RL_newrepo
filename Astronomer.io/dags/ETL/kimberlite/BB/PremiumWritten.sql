-- tag: PremiumWritten - tag ends/
/*********************************************************************************************************************************
 ***** Kimberlite ******
		PremiumWritten.sql
**********************************************************************************************************************************/
/*
----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

    03/04/2022  DROBAK      Initial create in ref_kimberlite dataset
	05/09/2022	DROBAK		Ensure correct table name in ref_kimberlite dataaset: PremiumWritten
	06/01/2022	DROBAK		Add (per DW table): PolicyPeriodPublicID, PolicyTermPublicID; ProductType, ProductCode replaces Product
	08/16/2022  TVERNER     Add TermNumber and LocationCountry for production reports

	
	NOTE:					Do not add fields to PremiumWritten, instead join to TransactionConfiguration to get Exposure base output
----------------------------------------------------------------------------------------------------------------------------------
 *****  Unioned Sections  *****
----------------------------------------------------------------------------------------------------------------------------------
	BOP Direct
    IM Direct
    UMB Direct
    PA Direct
    PJ Direct
        Note: Ceded not included at this time though untested commented sections exist below
-----------------------------------------------------------------------------------------------------------------------------------

Notes on possible future fields:
		--CompanyKey,				--need to add but cannot until we get business unit table built in kimberlite
		--PolicyPeriodPublicID,		--? add PolicyTransaction.PolicyPeriodPublicID ?
		--PolicyTermPublicID,		--? would need to add PolicyTermPublicID to PolicyTransaction, and then to PremiumWritten
	
*/	
/**********************************************************************************************************************************/
   --CREATE OR REPLACE TABLE `{project}.{dest_dataset}.PremiumWritten`
    --AS
    ---------------------
    ---- BOP Direct ----
    INSERT INTO `{project}.{dest_dataset}.PremiumWritten`
   (
    SourceSystem
    ,FinancialTransactionKey 
    ,PolicyTransactionKey 
    ,RiskLocationKey 
    ,RiskBuildingKey 
    ,RiskStockKey 
    ,RiskPAJewelryKey 
    ,RiskJewelryItemKey 
    ,CoverageKey 
	,PolicyPeriodPublicID 
	,PolicyTermPublicID 
    ,AccountingDate 
    ,AccountingYear 
    ,AccountingQuarter 
    ,AccountingMonth 
    ,BusinessUnit 
    ,ProductCode 
	,ProductType 
    ,AccountSegment 
    ,JobNumber 
    ,PolicyNumber 
    ,PolicyExpirationDate 
    ,PolicyTransEffDate 
    ,ItemNumber 
    ,ModelNumber 
    ,CoverageCode 
    ,TransactionType 
    ,TransactionCount 
    ,PolicyCoverageRank 
    ,WrittenPremium 
	,TermNumber 
	,LocationCountry 
    ,bq_load_date 
)
    
    SELECT 
        PolicyTransaction.SourceSystem															AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,RiskLocationBusinessOwners.RiskLocationKey												AS RiskLocationKey
		,RiskBuilding.RiskBuildingKey															AS RiskBuildingKey
		,CAST(NULL AS BYTES)																	AS RiskStockKey
		,CAST(NULL AS BYTES)																	AS RiskPAJewelryKey
		,CAST(NULL AS BYTES)																	AS RiskJewelryItemKey
		,Coverage.BOPCoverageKey																AS CoverageKey
		,PolicyTransaction.PolicyPeriodPublicID													AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID													AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate											AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)							AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)						AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)						AS AccountingMonth
        ,'Commercial'																			AS BusinessUnit
		,CASE 
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'BOP'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
			WHEN FinancialTransaction.LineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
			WHEN FinancialTransaction.LineCode = 'PersonalArtclLine_JM' THEN 'PA'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine')
			THEN CASE
					WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
					WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
				END
			ELSE FinancialTransaction.LineCode
		END																						AS ProductCode
		,CASE
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
			WHEN FinancialTransaction.LineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
			ELSE FinancialTransaction.LineCode
		END																						AS ProductType
        ,PolicyTransaction.Segment																AS AccountSegment
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber															AS PolicyNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate															AS PolicyTransEffDate
    	,0																						AS ItemNumber
		,PolicyTransaction.ModelNumber															AS ModelNumber
        ,Coverage.CoverageCode																	AS CoverageCode
        ,PolicyTransaction.TranType																AS TransactionType
        ,CASE	WHEN PolicyTransaction.TranType = 'Issuance' THEN -1
                WHEN PolicyTransaction.TranType = 'Rewrite' THEN 0
                ELSE 1
            END																					AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    FinancialTransaction.TransactionWrittenDate
                        )												    					AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                 AS WrittenPremium
        ,PolicyTransaction.TermNumber                                                           AS TermNumber -- TVERNER: 2022-08-15
        ,RiskLocationBusinessOwners.LocationCountry                                             AS LocationCountry -- TVERNER: 2022-08-16
        ,CURRENT_DATE																			AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{core_dataset}.FinancialTransactionBOPDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date=DATE({partition_date})) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem

    LEFT JOIN (SELECT PolicyTransactionKey, BOPCoverageKey, CoverageCode FROM `{project}.{core_dataset}.CoverageBOP` WHERE bq_load_date=DATE({partition_date})) Coverage
        ON FinancialTransaction.BOPCoverageKey= Coverage.BOPCoverageKey
        --AND Coverage.ProductCode = 'BOP'
 
    LEFT JOIN (SELECT RiskLocationKey, PolicyTransactionKey, LocationCountry FROM `{project}.{core_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date=DATE({partition_date})) RiskLocationBusinessOwners
        ON FinancialTransaction.RiskLocationKey=RiskLocationBusinessOwners.RiskLocationKey
		AND FinancialTransaction.PolicyTransactionKey = RiskLocationBusinessOwners.PolicyTransactionKey
    LEFT JOIN (SELECT RiskBuildingKey, PolicyTransactionKey FROM `{project}.{core_dataset}.RiskBuilding` WHERE bq_load_date=DATE({partition_date})) RiskBuilding
        ON FinancialTransaction.RiskBuildingKey=RiskBuilding.RiskBuildingKey
		AND FinancialTransaction.PolicyTransactionKey = RiskBuilding.PolicyTransactionKey
       
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY 
        PolicyTransaction.SourceSystem
		,FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey
		,RiskLocationBusinessOwners.RiskLocationKey
		,RiskBuilding.RiskBuildingKey
		,Coverage.BOPCoverageKey
		,PolicyTransaction.PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID
        ,FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,PolicyTransaction.Segment
    --	,dPol.SourceOfBusiness
        ,PolicyTransaction.JobNumber
        ,PolicyTransaction.PolicyNumber
		,PolicyTransaction.PeriodEndDate
		,PolicyTransaction.TransEffDate
    	--,ProductAndRiskConfiguration.ItemNumber
        ,PolicyTransaction.TermNumber
		,PolicyTransaction.ModelNumber
        ,Coverage.CoverageCode
        ,RiskLocationBusinessOwners.LocationCountry
        ,PolicyTransaction.TranType
        ,FinancialTransaction.LineCode
        ,PolicyTransaction.OfferingCode
        ,CURRENT_DATE

/*
UNION ALL 

    ---- BOP Ceded ----
    SELECT 
        PolicyTransaction.SourceSystem														AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,PolicyTransaction.PolicyPeriodPublicID												AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID												AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										        AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'                                                                          AS BusinessUnit
        ,'BOP'                                                                                      AS Product
        ,PolicyTransaction.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber														AS PolicyNumber							
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
    --	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        ,CoverageIMBOP.CoverageCode                                                  AS CoverageCode
        ,PolicyTransaction.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,CoverageIMBOP.CoverageCode
                        ORDER BY 
                                    --ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium
        ,CURRENT_DATE AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionBOPCeded` FinancialTransactionBOPCeded
    LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date=DATE("2021-11-10") )PolicyTransaction
            --AND EXTRACT(YEAR FROM AccountingDate) >= (EXTRACT(YEAR FROM CURRENT_DATE())-3) AND DATE(AccountingDate) <= DATE("2021-11-10")) PolicyTransaction
        ON FinancialTransactionBOPCeded.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransactionBOPCeded.SourceSystem = PolicyTransaction.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON PolicyTransaction.PolicyTransactionKey=ProductAndRiskConfiguration.PolicyTransactionKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, RiskBOPLocationKey, RiskBOPBuildingKey, CoverageCode FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date=DATE("2021-11-10")) CoverageIMBOP
        ON PolicyTransaction.PolicyTransactionKey=CoverageIMBOP.PolicyTransactionKey
        AND ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey=CoverageIMBOP.RiskBOPLocationKey
        AND ProductAndRiskConfiguration.RiskBuildingKey=CoverageIMBOP.RiskBOPBuildingKey

    WHERE FinancialTransactionBOPCeded.bq_load_date = "2021-11-10"
    AND FinancialTransactionBOPCeded.TransactionPostedDate IS NOT NULL
    AND FinancialTransactionBOPCeded.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    AND FinancialTransactionBOPCeded.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
*/

UNION ALL 

    ---------------------
    ---- IM Direct ----
    SELECT 
        PolicyTransaction.SourceSystem															AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,RiskLocationIM.RiskLocationKey															AS RiskLocationKey
		,CAST(NULL AS BYTES)																	AS RiskBuildingKey
		,FinancialTransaction.RiskStockKey														AS RiskStockKey
		,CAST(NULL AS BYTES)																	AS RiskPAJewelryKey
		,CAST(NULL AS BYTES)																	AS RiskJewelryItemKey
		,Coverage.IMCoverageKey																	AS CoverageKey
		,PolicyTransaction.PolicyPeriodPublicID													AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID													AS PolicyTermPublicID
        ,CAST(FinancialTransaction.TransactionWrittenDate AS DATE)						        AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					        AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                      AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                    AS AccountingMonth
        ,'Commercial'                                                                           AS BusinessUnit
		,CASE 
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'BOP'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
			WHEN FinancialTransaction.LineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
			WHEN FinancialTransaction.LineCode = 'PersonalArtclLine_JM' THEN 'PA'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
			THEN CASE 
					WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
					WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
				END
			ELSE FinancialTransaction.LineCode
		END																						AS ProductCode
		,CASE
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
			WHEN FinancialTransaction.LineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
			ELSE FinancialTransaction.LineCode
		END																						AS ProductType
        ,PolicyTransaction.Segment																AS AccountSegment
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber															AS PolicyNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate															AS PolicyTransEffDate
    	,0																						AS ItemNumber
		,PolicyTransaction.ModelNumber															AS ModelNumber
        ,Coverage.CoverageCode																	AS CoverageCode
        ,PolicyTransaction.TranType																AS TransactionType    
        ,CASE	WHEN PolicyTransaction.TranType = 'Issuance' THEN -1
                WHEN PolicyTransaction.TranType = 'Rewrite' THEN 0
                ELSE 1
        END																						AS TransactionCount
       ,DENSE_RANK() OVER(PARTITION BY
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    FinancialTransaction.TransactionWrittenDate
                        )												    					AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                 AS WrittenPremium
        ,PolicyTransaction.TermNumber                                                           AS TermNumber --TVERNER: 2022-08-15
        ,RiskLocationIM.LocationCountry                                                         AS LocationCountry --TVERNER: 2022-08-16
        ,CURRENT_DATE																			AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt  --, ProductAndRiskConfiguration.ItemNumber
    FROM `{project}.{core_dataset}.FinancialTransactionIMDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date=DATE({partition_date})) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem
        
    LEFT JOIN (SELECT PolicyTransactionKey, IMCoverageKey, CoverageCode FROM `{project}.{core_dataset}.CoverageIM` WHERE bq_load_date=DATE({partition_date})) Coverage
        ON FinancialTransaction.IMCoverageKey= Coverage.IMCoverageKey
        --AND Coverage.ProductCode = 'IM'

    LEFT JOIN (SELECT RiskLocationKey, PolicyTransactionKey, LocationCountry FROM `{project}.{core_dataset}.RiskLocationIM` WHERE bq_load_date=DATE({partition_date})) RiskLocationIM
        ON FinancialTransaction.RiskLocationKey=RiskLocationIM.RiskLocationKey
		AND FinancialTransaction.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey

    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY 
        PolicyTransaction.SourceSystem
		,FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey
		,RiskLocationIM.RiskLocationKey
		,FinancialTransaction.RiskStockKey
		,Coverage.IMCoverageKey
		,PolicyTransaction.PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID
        ,FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,PolicyTransaction.Segment
    --	,dPol.SourceOfBusiness
        ,PolicyTransaction.JobNumber
        ,PolicyTransaction.PolicyNumber
		,PolicyTransaction.PeriodEndDate
		,PolicyTransaction.TransEffDate
        ,PolicyTransaction.TermNumber
		,PolicyTransaction.ModelNumber
        ,Coverage.CoverageCode
        ,RiskLocationIM.LocationCountry
        ,PolicyTransaction.TranType
        ,FinancialTransaction.LineCode
        ,PolicyTransaction.OfferingCode
        ,CURRENT_DATE

/*
UNION ALL 

    ---------------------
    ---- IM Ceded ----
    SELECT 
        PolicyTransaction.SourceSystem														AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,PolicyTransaction.PolicyPeriodPublicID												AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID												AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										        AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'																				AS BusinessUnit
        ,'IM'																						AS Product
        ,PolicyTransaction.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber														AS PolicyNumber							
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
        ,Coverage.CoverageCode                                                   AS CoverageCode
        ,PolicyTransaction.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionIMCeded` FinancialTransactionIMCeded
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date=DATE("2021-11-10")) PolicyTransaction
        ON FinancialTransactionIMCeded.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransactionIMCeded.SourceSystem = PolicyTransaction.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON FinancialTransaction.PolicyTransactionKey = ProductAndRiskConfiguration.PolicyTransactionKey
        AND FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationIMKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, RiskIMLocationKey, CoverageCode FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date=DATE("2021-11-10")) Coverage
        ON PolicyTransaction.PolicyTransactionKey=Coverage.PolicyTransactionKey
        AND FinancialTransaction.CoverageKey = Coverage.CoverageKey
        --AND ProductAndRiskConfiguration.RiskLocationIMKey=Coverage.RiskIMLocationKey

    WHERE FinancialTransactionIMCeded.bq_load_date = "2021-11-10"
    AND FinancialTransactionIMCeded.TransactionPostedDate IS NOT NULL
    AND FinancialTransactionIMCeded.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    AND FinancialTransactionIMCeded.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
*/

UNION ALL 

    ---------------------
    ---- UMB Direct ----
    SELECT 
        PolicyTransaction.SourceSystem															AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,RiskLocationBusinessOwners.RiskLocationKey												AS RiskLocationKey
		,CAST(NULL AS BYTES)																	AS RiskBuildingKey
		,CAST(NULL AS BYTES)																	AS RiskStockKey
		,CAST(NULL AS BYTES)																	AS RiskPAJewelryKey
		,CAST(NULL AS BYTES)																	AS RiskJewelryItemKey
		,Coverage.UMBCoverageKey																AS CoverageKey
		,PolicyTransaction.PolicyPeriodPublicID													AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID													AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										    AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					        AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                      AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                    AS AccountingMonth
        ,'Commercial'                                                                           AS BusinessUnit
		,CASE 
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'BOP'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
			WHEN FinancialTransaction.LineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
			WHEN FinancialTransaction.LineCode = 'PersonalArtclLine_JM' THEN 'PA'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
			THEN CASE 
					WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
					WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
				END
			ELSE FinancialTransaction.LineCode
		END																						AS ProductCode
		,CASE
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
			WHEN FinancialTransaction.LineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
			ELSE FinancialTransaction.LineCode
		END																						AS ProductType
        ,PolicyTransaction.Segment																AS AccountSegment
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber															AS PolicyNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate															AS PolicyTransEffDate
    	,0																						AS ItemNumber
		,PolicyTransaction.ModelNumber															AS ModelNumber
        ,Coverage.CoverageCode																	AS CoverageCode
        ,PolicyTransaction.TranType																AS TransactionType
        ,CASE	WHEN PolicyTransaction.TranType = 'Issuance' THEN -1
                WHEN PolicyTransaction.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				    AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    FinancialTransaction.TransactionWrittenDate
                        )												    					AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                 AS WrittenPremium
        ,PolicyTransaction.TermNumber                                                           AS TermNumber --TVERNER: 2022-08-15
        ,RiskLocationBusinessOwners.LocationCountry                                             AS LocationCountry --TVERNER: 2022-08-16
        ,CURRENT_DATE																			AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{core_dataset}.FinancialTransactionUMBDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date=DATE({partition_date})) PolicyTransaction
            --AND EXTRACT(YEAR FROM AccountingDate) >= (EXTRACT(YEAR FROM CURRENT_DATE())-3)
            --AND DATE(AccountingDate) <= DATE({partition_date})) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem
    
    LEFT JOIN (SELECT PolicyTransactionKey, UMBCoverageKey, CoverageCode FROM `{project}.{core_dataset}.CoverageUMB` WHERE bq_load_date=DATE({partition_date})) Coverage
        ON FinancialTransaction.UMBCoverageKey= Coverage.UMBCoverageKey
        --AND CoverageIMUMB.ProductCode = 'UMB'
  
	LEFT JOIN (SELECT RiskLocationKey, PolicyTransactionKey, LocationCountry FROM `{project}.{core_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date=DATE({partition_date})) RiskLocationBusinessOwners
        ON FinancialTransaction.RiskLocationKey=RiskLocationBusinessOwners.RiskLocationKey
		AND FinancialTransaction.PolicyTransactionKey = RiskLocationBusinessOwners.PolicyTransactionKey

    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeSubGroup IN ('PREMIUM', 'TERROR','MIN_PREMIUM_LIAB_ADJ', 'MIN_PREMIUM_PROP_ADJ', 'PREMIUM_ADJ', 'MIN_PREMIUM_ADJ')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
        PolicyTransaction.SourceSystem
		,FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey
		,RiskLocationBusinessOwners.RiskLocationKey
		,Coverage.UMBCoverageKey
		,PolicyTransaction.PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID
        ,FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,PolicyTransaction.Segment
    --	,dPol.SourceOfBusiness
        ,PolicyTransaction.JobNumber
        ,PolicyTransaction.PolicyNumber
		,PolicyTransaction.PeriodEndDate
		,PolicyTransaction.TransEffDate
        ,PolicyTransaction.TermNumber
		,PolicyTransaction.ModelNumber
        ,Coverage.CoverageCode
        ,RiskLocationBusinessOwners.LocationCountry
        ,PolicyTransaction.TranType
        ,FinancialTransaction.LineCode
        ,PolicyTransaction.OfferingCode
        ,CURRENT_DATE
/*
UNION ALL 

    ---------------------
    ---- UMB Ceded ----
    SELECT 
        PolicyTransaction.SourceSystem														AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,PolicyTransaction.PolicyPeriodPublicID												AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID												AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										        AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					            AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                          AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                        AS AccountingMonth
        ,'Commercial'																				AS BusinessUnit
        ,'UMB'                                                                                      AS Product
        ,PolicyTransaction.Segment															AS AccountSegment
    --	,dPol.SourceOfBusiness																		AS SourceOfBusiness
        ,PolicyTransaction.JobNumber															AS JobNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate														AS PolicyTransEffDate
    	,ProductAndRiskConfiguration.ItemNumber														AS ItemNumber
    --  ,0 As ItemNumber
		,PolicyTransaction.ModelNumber														AS ModelNumber
        ,CoverageIMBOP.CoverageCode                                                  AS CoverageCode
        ,PolicyTransaction.TranType														    AS TransactionType
    
        ,CASE	WHEN ProductAndRiskConfiguration.TranType = 'Issuance' THEN -1 
                WHEN ProductAndRiskConfiguration.TranType = 'Rewrite' THEN 0
         ELSE 1
         END   																				        AS TransactionCount
    
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,CoverageIMBOP.CoverageCode
                        ORDER BY
                                    ProductAndRiskConfiguration.ItemNumber
                                    FinancialTransaction.TransactionWrittenDate --temporary order by
                        )												    						AS PolicyCoverageRank
        ,TransactionAmount                                                                          AS WrittenPremium

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{dest_dataset}.FinancialTransactionUMBDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date=DATE("2021-11-10")) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem
    
    INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date=DATE("2021-11-10")) ProductAndRiskConfiguration
        ON FinancialTransaction.PolicyTransactionKey = ProductAndRiskConfiguration.PolicyTransactionKey
        AND FinancialTransaction.RiskLocationKey = ProductAndRiskConfiguration.RiskLocationBusinessOwnersKey
    
    LEFT JOIN (SELECT PolicyTransactionKey, CoverageKey, CoverageCode, ProductCode FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date=DATE("2021-10-03")) CoverageIMUMB
        ON PolicyTransaction.PolicyTransactionKey=CoverageIMUMB.PolicyTransactionKey
        AND FinancialTransaction.UMBCoverageKey = CoverageIMUMB.CoverageKey
        AND CoverageIMBOP.ProductCode = 'UMB'

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
        PolicyTransaction.SourceSystem															AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,CAST(NULL AS BYTES)																	AS RiskLocationKey
		,CAST(NULL AS BYTES)																	AS RiskBuildingKey
		,CAST(NULL AS BYTES)																	AS RiskStockKey
		,RiskPAJewelry.RiskPAJewelryKey															AS RiskPAJewelryKey
		,CAST(NULL AS BYTES)																	AS RiskJewelryItemKey
		,Coverage.PAJewelryCoverageKey															AS CoverageKey
		,PolicyTransaction.PolicyPeriodPublicID													AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID													AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										    AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					        AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                      AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                    AS AccountingMonth
        ,'Personal'                                                                             AS BusinessUnit
		,CASE 
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'BOP'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
			WHEN FinancialTransaction.LineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
			WHEN FinancialTransaction.LineCode = 'PersonalArtclLine_JM' THEN 'PA'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
			THEN CASE 
					WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
					WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
				END
			ELSE FinancialTransaction.LineCode
		END																						AS ProductCode
		,CASE
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
			WHEN FinancialTransaction.LineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
			ELSE FinancialTransaction.LineCode
		END																						AS ProductType
        ,PolicyTransaction.Segment																AS AccountSegment
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber															AS PolicyNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate															AS PolicyTransEffDate
    	,RiskPAJewelry.JewelryArticleNumber														AS ItemNumber
		,PolicyTransaction.ModelNumber															AS ModelNumber
        ,Coverage.CoverageCode								                                    AS CoverageCode
        ,PolicyTransaction.TranType																AS TransactionType
        ,CASE WHEN Coverage.CoverageCode = 'JewelryItemCov_JM' THEN
                CASE	WHEN PolicyTransaction.TranType = 'Issuance' THEN -1
                        WHEN PolicyTransaction.TranType = 'Rewrite' THEN 0
                        ELSE 1
                END
            ELSE 0
        END																						AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    RiskPAJewelry.JewelryArticleNumber
                                    ,FinancialTransaction.TransactionWrittenDate
                        )												    					AS PolicyCoverageRank
        ,SUM(TransactionAmount)                                                                 AS WrittenPremium
        ,PolicyTransaction.TermNumber                                                           AS TermNumber --TVERNER: 2022-08-15
        ,CAST(NULL AS STRING)                                                                   AS LocationCountry --TVERNER: 2022-08-16
        ,CURRENT_DATE																			AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{core_dataset}.FinancialTransactionPADirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date=DATE({partition_date})) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem
  
    LEFT JOIN (SELECT PAJewelryCoverageKey, PolicyTransactionKey, RiskPAJewelryKey, CoverageCode FROM `{project}.{core_dataset}.CoveragePAJewelry` WHERE bq_load_date=DATE({partition_date})) Coverage
        ON FinancialTransaction.PAJewelryCoverageKey = Coverage.PAJewelryCoverageKey

    LEFT JOIN (SELECT RiskPAJewelryKey, PolicyTransactionKey, JewelryArticleNumber FROM `{project}.{core_dataset}.RiskPAJewelry` WHERE bq_load_date=DATE({partition_date})) RiskPAJewelry
        ON FinancialTransaction.RiskPAJewelryKey=RiskPAJewelry.RiskPAJewelryKey
		AND FinancialTransaction.PolicyTransactionKey = RiskPAJewelry.PolicyTransactionKey
        
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeGroup IN ('ScheduledPremium', 'UnscheduledPremium')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
        PolicyTransaction.SourceSystem
		,FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey
		,RiskPAJewelry.RiskPAJewelryKey
		,Coverage.PAJewelryCoverageKey
		,PolicyTransaction.PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID
        ,FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,PolicyTransaction.Segment
    --	,dPol.SourceOfBusiness
        ,PolicyTransaction.JobNumber
        ,PolicyTransaction.PolicyNumber
		,PolicyTransaction.PeriodEndDate
		,PolicyTransaction.TransEffDate
    	,RiskPAJewelry.JewelryArticleNumber
        ,PolicyTransaction.TermNumber
		,PolicyTransaction.ModelNumber
        ,Coverage.CoverageCode
        ,PolicyTransaction.TranType
        ,FinancialTransaction.LineCode
        ,PolicyTransaction.OfferingCode
        ,CURRENT_DATE

UNION ALL 

    ---------------------
    ---- PJ Direct ----
    SELECT 
        PolicyTransaction.SourceSystem															AS SourceSystem
		,FinancialTransactionKey																AS FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey													AS PolicyTransactionKey
		,CAST(NULL AS BYTES)																	AS RiskLocationKey
		,CAST(NULL AS BYTES)																	AS RiskBuildingKey
		,CAST(NULL AS BYTES)																	AS RiskStockKey
		,CAST(NULL AS BYTES)																	AS RiskPAJewelryKey
		,RiskJewelryItem.RiskJewelryItemKey														AS RiskJewelryItemKey
		,Coverage.ItemCoverageKey																AS CoverageKey
		,PolicyTransaction.PolicyPeriodPublicID													AS PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID													AS PolicyTermPublicID
		,FinancialTransaction.TransactionWrittenDate										    AS AccountingDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)					        AS AccountingYear
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)                      AS AccountingQuarter
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)	                    AS AccountingMonth
        ,'Personal'                                                                             AS BusinessUnit
		,CASE 
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'BOP'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
			WHEN FinancialTransaction.LineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
			WHEN FinancialTransaction.LineCode = 'PersonalArtclLine_JM' THEN 'PA'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
			THEN CASE 
					WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
					WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
				END
			ELSE FinancialTransaction.LineCode
		END																						AS ProductCode
		,CASE
			WHEN FinancialTransaction.LineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
			WHEN FinancialTransaction.LineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
			WHEN FinancialTransaction.LineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
			WHEN FinancialTransaction.LineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
			ELSE FinancialTransaction.LineCode
		END																						AS ProductType
        ,PolicyTransaction.Segment																AS AccountSegment
        ,PolicyTransaction.JobNumber															AS JobNumber
        ,PolicyTransaction.PolicyNumber															AS PolicyNumber
		,PolicyTransaction.PeriodEndDate														AS PolicyExpirationDate
		,PolicyTransaction.TransEffDate															AS PolicyTransEffDate
    	,RiskJewelryItem.ItemNumber																AS ItemNumber
		,PolicyTransaction.ModelNumber															AS ModelNumber
        ,Coverage.CoverageCode					                                                AS CoverageCode
        ,PolicyTransaction.TranType																AS TransactionType
        ,CASE WHEN Coverage.CoverageCode = 'JewelryItemCov_JMIC_PL' THEN
                CASE	WHEN PolicyTransaction.TranType = 'Issuance' THEN -1
                        WHEN PolicyTransaction.TranType = 'Rewrite' THEN 0
                        ELSE 1
                END
            ELSE 0
        END																						AS TransactionCount
        ,DENSE_RANK() OVER(PARTITION BY 
                                    FinancialTransaction.TransactionWrittenDate
                                    ,PolicyTransaction.JobNumber
                                    ,Coverage.CoverageCode
                        ORDER BY
                                    RiskJewelryItem.ItemNumber
                                    ,FinancialTransaction.TransactionWrittenDate --needed?
                        )												    					AS PolicyCoverageRank
        ,SUM(TransactionAmount)																	AS WrittenPremium
        ,PolicyTransaction.TermNumber                                                           AS TermNumber --TVERNER: 2022-08-15
        ,CAST(NULL AS STRING)                                                                   AS LocationCountry --TVERNER: 2022-08-16
        ,CURRENT_DATE																			AS bq_load_date

    --SELECT SUM(TransactionAmount) As SumTransAmt
    FROM `{project}.{core_dataset}.FinancialTransactionPJDirect` FinancialTransaction
    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date=DATE({partition_date})) PolicyTransaction
        ON FinancialTransaction.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
        AND FinancialTransaction.SourceSystem = PolicyTransaction.SourceSystem
    
    LEFT JOIN (SELECT PolicyTransactionKey, ItemCoverageKey, RiskJewelryItemKey, CoverageCode FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` WHERE bq_load_date=DATE({partition_date})) Coverage
        ON FinancialTransaction.ItemCoverageKey = Coverage.ItemCoverageKey
        
    LEFT JOIN (SELECT RiskJewelryItemKey, PolicyTransactionKey, ItemNumber FROM `{project}.{core_dataset}.RiskJewelryItem` WHERE bq_load_date=DATE({partition_date})) RiskJewelryItem
        ON FinancialTransaction.RiskJewelryItemKey=RiskJewelryItem.RiskJewelryItemKey
		AND FinancialTransaction.PolicyTransactionKey = RiskJewelryItem.PolicyTransactionKey
       
    WHERE FinancialTransaction.bq_load_date = DATE({partition_date})
    AND FinancialTransaction.TransactionPostedDate IS NOT NULL
    AND FinancialTransaction.ChargeGroup IN ('ScheduledPremium', 'UnscheduledPremium')
    --AND FinancialTransaction.TransactionWrittenDate BETWEEN "2021-09-01" AND "2021-09-30"
    GROUP BY
        PolicyTransaction.SourceSystem
		,FinancialTransactionKey
		,PolicyTransaction.PolicyTransactionKey
		,RiskJewelryItem.RiskJewelryItemKey
		,Coverage.ItemCoverageKey
		,PolicyTransaction.PolicyPeriodPublicID
		,PolicyTransaction.PolicyTermPublicID
        ,FinancialTransaction.TransactionWrittenDate
        ,EXTRACT(YEAR FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(QUARTER FROM FinancialTransaction.TransactionWrittenDate)
        ,EXTRACT(MONTH FROM FinancialTransaction.TransactionWrittenDate)
        ,PolicyTransaction.Segment
    --	,dPol.SourceOfBusiness
        ,PolicyTransaction.JobNumber
        ,PolicyTransaction.PolicyNumber
		,PolicyTransaction.PeriodEndDate
		,PolicyTransaction.TransEffDate
    	,RiskJewelryItem.ItemNumber
        ,PolicyTransaction.TermNumber
		,PolicyTransaction.ModelNumber
        ,Coverage.CoverageCode
        ,PolicyTransaction.TranType
        ,FinancialTransaction.LineCode
        ,PolicyTransaction.OfferingCode
        ,CURRENT_DATE

/*
select max(bq_load_date) from `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes`
select max(bq_load_date) from `{project}.{dest_dataset}.CoverageIM`

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
select max(bq_load_date) from `{project}.{dest_dataset}.PolicyTransaction`

select max(bq_load_date) FROM `{project}.{dest_dataset}.ProductAndRiskConfiguration`
*/
