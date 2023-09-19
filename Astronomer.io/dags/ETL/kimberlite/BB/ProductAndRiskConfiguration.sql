-- tag: ProductAndRiskConfiguration - tag ends/
/* Building Blocks Extract Query
    ProductAndRiskConfiguration.sql
		BigQuery Converted
	
	----------------------------------------------------------------------------------------------------------------------
    -- CHANGE LOG --
        04/22/2021  DROBAK      Add filters for Fixed Rank Fields = 1
		08/26/2021	DROBAK		Add RiskStockIM fields
		09/14/2021	DROBAK		Added DATE(DATE({partition_date})) as bq_load_date to SELECT
		11/02/2021	SLJ			Modified query to build from Transaction Configuration and changed the joins as to eliminate cross join between buildings
		12/08/2021	DROBAK		Replace InsuranceProductCode with PolicyLineCode
		05/23/2022	DROBAK		Add ProductCode, ProductType; Remove PolicyLineCode and InsuranceProductCode
		05/31/2022	DROBAK		Fix source for OfferingCode; Added ELSE clause to ProductCode and ProductType CASE; 
								Made bq_load_date placeholder look consistent to other code
		09/16/2022	DROBAK		Added fields: IsPrimaryBuildingLocation, IsPrimaryLocation
		02/14/2023	DROBAK		Convert NULL ItemNumber and JewelryArticleNumber to INTEGER

	--------------------------------------------------------------------------------------------------------------------
	-------------------------------------------------------------
	--    PolicyTransaction (one to)
	--    PolicyTransactionProduct (many)
	-------------------------------------------------------------
*/
--DELETE `{project}.{dest_dataset}.ProductAndRiskConfiguration` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.ProductAndRiskConfiguration` 
	(  
 		SourceSystem
		,PolicyTransactionKey
		,RiskJewelryItemKey
		,RiskPAJewelryKey
		,RiskLocationBusinessOwnersKey
		,RiskBuildingKey
		,RiskLocationIMKey
		,RiskStockKey
		,AccountNumber
		,Segment
		,BusinessUnit
		,PolicyNumber
		,LegacyPolicyNumber
		,PeriodEffDate
		,PeriodEndDate
		,JobNumber
		,TranType
		,TermNumber
		,ModelNumber
		,TransEffDate
		,JobCloseDate
		,WrittenDate
		,CancelEffDate
		,AccountingDate
		,TranCYBegDate
		,TranCYEndDate
		,TranCYMultiplier
		,TranPYBegDate
		,TranPYEndDate
		,ProductCode
		,ProductType
		,ItemNumber
		,JewelryArticleNumber
		,IsInactive
		,LocationNumber
		,IsPrimaryLocation
		,BuildingNumber
		,IsPrimaryBuildingLocation
		,bq_load_date
	)

SELECT 
    TransactionConfiguration.SourceSystem                                           AS SourceSystem
    ,TransactionConfiguration.PolicyTransactionKey									AS PolicyTransactionKey
    ,RiskJewelryItem.RiskJewelryItemKey												AS RiskJewelryItemKey
    ,NULL																			AS RiskPAJewelryKey
    ,NULL																			AS RiskLocationBusinessOwnersKey
    ,NULL																			AS RiskBuildingKey
    ,NULL																			AS RiskLocationIMKey
    ,NULL																			AS RiskStockKey

    ,TransactionConfiguration.AccountNumber											AS AccountNumber
    ,TransactionConfiguration.Segment												AS Segment
    ,TransactionConfiguration.BusinessUnit											AS BusinessUnit
    ,TransactionConfiguration.PolicyNumber											AS PolicyNumber
    ,TransactionConfiguration.LegacyPolicyNumber									AS LegacyPolicyNumber
    ,TransactionConfiguration.PeriodEffDate											AS PeriodEffDate
    ,TransactionConfiguration.PeriodEndDate											AS PeriodEndDate
    ,TransactionConfiguration.JobNumber												AS JobNumber
    ,TransactionConfiguration.TranType												AS TranType
    ,TransactionConfiguration.TermNumber											AS TermNumber
    ,TransactionConfiguration.ModelNumber											AS ModelNumber
    ,TransactionConfiguration.TransEffDate											AS TransEffDate
    ,TransactionConfiguration.JobCloseDate											AS JobCloseDate
    ,TransactionConfiguration.WrittenDate											AS WrittenDate
    ,TransactionConfiguration.CancelEffDate											AS CancelEffDate
    ,TransactionConfiguration.AccountingDate										AS AccountingDate
    ,TransactionConfiguration.CalendarYearBeginDate                                 AS TranCYBegDate
    ,TransactionConfiguration.CalendarYearEndDate                                   AS TranCYEndDate
    ,TransactionConfiguration.CalendarYearMultiplier                                AS TranCYMultiplier
    ,TransactionConfiguration.PolicyYearBeginDate                                   AS TranPYBegDate
    ,TransactionConfiguration.PolicyYearEndDate                                     AS TranPYEndDate
	,CASE 
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'BOP'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM' THEN 'PA'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
		THEN CASE 
				WHEN PolicyTransactionProduct.OfferingCode IN ('JB', 'JBP') THEN 'JB'
				WHEN PolicyTransactionProduct.OfferingCode IN ('JS', 'JSP') THEN 'JS'
			END
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductCode
	,CASE
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductType
    ,RiskJewelryItem.ItemNumber														AS ItemNumber
    ,CAST(NULL AS INTEGER)															AS JewelryArticleNumber
    ,RiskJewelryItem.IsItemInactive                                                 AS IsInactive
    ,NULL													                        AS LocationNumber
	,NULL																			AS IsPrimaryLocation
    ,NULL																			AS BuildingNumber
	,NULL																			AS IsPrimaryBuildingLocation
    ,DATE('{date}')																	AS bq_load_date	

FROM `{project}.{dest_dataset}.TransactionConfiguration` AS TransactionConfiguration

    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
    ON PolicyTransactionProduct.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
    AND PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL'

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskJewelryItem` WHERE bq_load_date = DATE({partition_date}) AND FixedItemRank = 1 AND IsTransactionSliceEffective = 1) AS RiskJewelryItem 
    ON RiskJewelryItem.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey

WHERE   1 = 1
	AND TransactionConfiguration.bq_load_date = DATE({partition_date})

##### ** JPALine
UNION ALL
SELECT  
    TransactionConfiguration.SourceSystem                                           AS SourceSystem
    ,TransactionConfiguration.PolicyTransactionKey									AS PolicyTransactionKey
    ,NULL																			AS RiskJewelryItemKey
    ,RiskPAJewelry.RiskPAJewelryKey													AS RiskPAJewelryKey
    ,NULL																			AS RiskLocationBusinessOwnersKey
    ,NULL																			AS RiskBuildingKey
    ,NULL																			AS RiskLocationIMKey
    ,NULL																			AS RiskStockKey

    ,TransactionConfiguration.AccountNumber											AS AccountNumber
    ,TransactionConfiguration.Segment												AS Segment
    ,TransactionConfiguration.BusinessUnit											AS BusinessUnit
    ,TransactionConfiguration.PolicyNumber											AS PolicyNumber
    ,TransactionConfiguration.LegacyPolicyNumber									AS LegacyPolicyNumber
    ,TransactionConfiguration.PeriodEffDate											AS PeriodEffDate
    ,TransactionConfiguration.PeriodEndDate											AS PeriodEndDate
    ,TransactionConfiguration.JobNumber												AS JobNumber
    ,TransactionConfiguration.TranType												AS TranType
    ,TransactionConfiguration.TermNumber											AS TermNumber
    ,TransactionConfiguration.ModelNumber											AS ModelNumber
    ,TransactionConfiguration.TransEffDate											AS TransEffDate
    ,TransactionConfiguration.JobCloseDate											AS JobCloseDate
    ,TransactionConfiguration.WrittenDate											AS WrittenDate
    ,TransactionConfiguration.CancelEffDate											AS CancelEffDate
    ,TransactionConfiguration.AccountingDate										AS AccountingDate
    ,TransactionConfiguration.CalendarYearBeginDate                                 AS TranCYBegDate
    ,TransactionConfiguration.CalendarYearEndDate                                   AS TranCYEndDate
    ,TransactionConfiguration.CalendarYearMultiplier                                AS TranCYMultiplier
    ,TransactionConfiguration.PolicyYearBeginDate                                   AS TranPYBegDate
    ,TransactionConfiguration.PolicyYearEndDate                                     AS TranPYEndDate
	,CASE 
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'BOP'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM' THEN 'PA'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
		THEN CASE 
				WHEN PolicyTransactionProduct.OfferingCode IN ('JB', 'JBP') THEN 'JB'
				WHEN PolicyTransactionProduct.OfferingCode IN ('JS', 'JSP') THEN 'JS'
			END
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductCode
	,CASE
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductType
    ,CAST(NULL AS INTEGER)															AS ItemNumber
    ,RiskPAJewelry.JewelryArticleNumber												AS JewelryArticleNumber
    ,RiskPAJewelry.IsInactive                                                       AS IsInactive
    ,NULL													                        AS LocationNumber
	,NULL																			AS IsPrimaryLocation
    ,NULL																			AS BuildingNumber
	,NULL																			AS IsPrimaryBuildingLocation
    ,DATE('{date}')																	AS bq_load_date	

FROM `{project}.{dest_dataset}.TransactionConfiguration` AS TransactionConfiguration

    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
    ON PolicyTransactionProduct.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
    AND PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM'

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskPAJewelry` WHERE bq_load_date = DATE({partition_date}) AND FixedArticleRank = 1 AND IsTransactionSliceEffective = 1) AS RiskPAJewelry 
    ON RiskPAJewelry.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey

WHERE   1 = 1
	AND TransactionConfiguration.bq_load_date = DATE({partition_date})

##### ** BOPLine
UNION ALL
SELECT 
    TransactionConfiguration.SourceSystem                                           AS SourceSystem
    ,TransactionConfiguration.PolicyTransactionKey									AS PolicyTransactionKey
    ,NULL																			AS RiskJewelryItemKey
    ,NULL																			AS RiskPAJewelryKey
    ,RiskLocationBusinessOwners.RiskLocationKey										AS RiskLocationBusinessOwnersKey
    ,RiskBuilding.RiskBuildingKey													AS RiskBuildingKey
    ,NULL																			AS RiskLocationIMKey
    ,NULL																			AS RiskStockKey

    ,TransactionConfiguration.AccountNumber											AS AccountNumber
    ,TransactionConfiguration.Segment												AS Segment
    ,TransactionConfiguration.BusinessUnit											AS BusinessUnit
    ,TransactionConfiguration.PolicyNumber											AS PolicyNumber
    ,TransactionConfiguration.LegacyPolicyNumber									AS LegacyPolicyNumber
    ,TransactionConfiguration.PeriodEffDate											AS PeriodEffDate
    ,TransactionConfiguration.PeriodEndDate											AS PeriodEndDate
    ,TransactionConfiguration.JobNumber												AS JobNumber
    ,TransactionConfiguration.TranType												AS TranType
    ,TransactionConfiguration.TermNumber											AS TermNumber
    ,TransactionConfiguration.ModelNumber											AS ModelNumber
    ,TransactionConfiguration.TransEffDate											AS TransEffDate
    ,TransactionConfiguration.JobCloseDate											AS JobCloseDate
    ,TransactionConfiguration.WrittenDate											AS WrittenDate
    ,TransactionConfiguration.CancelEffDate											AS CancelEffDate
    ,TransactionConfiguration.AccountingDate										AS AccountingDate
    ,TransactionConfiguration.CalendarYearBeginDate                                 AS TranCYBegDate
    ,TransactionConfiguration.CalendarYearEndDate                                   AS TranCYEndDate
    ,TransactionConfiguration.CalendarYearMultiplier                                AS TranCYMultiplier
    ,TransactionConfiguration.PolicyYearBeginDate                                   AS TranPYBegDate
    ,TransactionConfiguration.PolicyYearEndDate                                     AS TranPYEndDate
	,CASE 
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'BOP'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM' THEN 'PA'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
		THEN CASE 
				WHEN PolicyTransactionProduct.OfferingCode IN ('JB', 'JBP') THEN 'JB'
				WHEN PolicyTransactionProduct.OfferingCode IN ('JS', 'JSP') THEN 'JS'
			END
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductCode
	,CASE
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductType
    ,CAST(NULL AS INTEGER)															AS ItemNumber
    ,CAST(NULL AS INTEGER)															AS JewelryArticleNumber
    ,NULL                                                                           AS IsInactive
    ,RiskLocationBusinessOwners.LocationNumber										AS LocationNumber
	,RiskLocationBusinessOwners.IsPrimaryLocation									AS IsPrimaryLocation
    ,RiskBuilding.BuildingNumber													AS BuildingNumber
	,RiskBuilding.IsPrimaryBuildingLocation											AS IsPrimaryBuildingLocation
    ,DATE('{date}')																	AS bq_load_date	

FROM `{project}.{dest_dataset}.TransactionConfiguration` AS TransactionConfiguration

    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
    ON PolicyTransactionProduct.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
    AND PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine'

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskLocationBusinessOwners` WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1) AS RiskLocationBusinessOwners 
    ON RiskLocationBusinessOwners.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskBuilding` WHERE bq_load_date = DATE({partition_date}) AND FixedBuildingRank = 1 AND IsTransactionSliceEffective = 1) AS RiskBuilding 
    ON RiskBuilding.PolicyTransactionKey = RiskLocationBusinessOwners.PolicyTransactionKey
    AND RiskBuilding.RiskLocationKey = RiskLocationBusinessOwners.RiskLocationKey

WHERE   1 = 1
	AND TransactionConfiguration.bq_load_date = DATE({partition_date})

##### ** ILMLine
UNION ALL
SELECT 
    TransactionConfiguration.SourceSystem                                           AS SourceSystem
    ,TransactionConfiguration.PolicyTransactionKey									AS PolicyTransactionKey
    ,NULL																			AS RiskJewelryItemKey
    ,NULL																			AS RiskPAJewelryKey
    ,NULL																			AS RiskLocationBusinessOwnersKey
    ,NULL																			AS RiskBuildingKey
    ,RiskLocationIM.RiskLocationKey													AS RiskLocationIMKey
    ,RiskStockIM.RiskStockKey														AS RiskStockKey

    ,TransactionConfiguration.AccountNumber											AS AccountNumber
    ,TransactionConfiguration.Segment												AS Segment
    ,TransactionConfiguration.BusinessUnit											AS BusinessUnit
    ,TransactionConfiguration.PolicyNumber											AS PolicyNumber
    ,TransactionConfiguration.LegacyPolicyNumber									AS LegacyPolicyNumber
    ,TransactionConfiguration.PeriodEffDate											AS PeriodEffDate
    ,TransactionConfiguration.PeriodEndDate											AS PeriodEndDate
    ,TransactionConfiguration.JobNumber												AS JobNumber
    ,TransactionConfiguration.TranType												AS TranType
    ,TransactionConfiguration.TermNumber											AS TermNumber
    ,TransactionConfiguration.ModelNumber											AS ModelNumber
    ,TransactionConfiguration.TransEffDate											AS TransEffDate
    ,TransactionConfiguration.JobCloseDate											AS JobCloseDate
    ,TransactionConfiguration.WrittenDate											AS WrittenDate
    ,TransactionConfiguration.CancelEffDate											AS CancelEffDate
    ,TransactionConfiguration.AccountingDate										AS AccountingDate
    ,TransactionConfiguration.CalendarYearBeginDate                                 AS TranCYBegDate
    ,TransactionConfiguration.CalendarYearEndDate                                   AS TranCYEndDate
    ,TransactionConfiguration.CalendarYearMultiplier                                AS TranCYMultiplier
    ,TransactionConfiguration.PolicyYearBeginDate                                   AS TranPYBegDate
    ,TransactionConfiguration.PolicyYearEndDate                                     AS TranPYEndDate
	,CASE 
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'BOP'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM' THEN 'PA'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
		THEN CASE 
				WHEN PolicyTransactionProduct.OfferingCode IN ('JB', 'JBP') THEN 'JB'
				WHEN PolicyTransactionProduct.OfferingCode IN ('JS', 'JSP') THEN 'JS'
			END
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductCode
	,CASE
        WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
        WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
        ELSE PolicyTransactionProduct.PolicyLineCode
    END AS ProductType
    ,CAST(NULL AS INTEGER)															AS ItemNumber
    ,CAST(NULL AS INTEGER)															AS JewelryArticleNumber
    ,NULL                                                                           AS IsInactive
    ,RiskLocationIM.LocationNumber													AS LocationNumber
	,RiskLocationIM.IsPrimaryLocation												AS IsPrimaryLocation
    ,NULL																			AS BuildingNumber
	,NULL																			AS IsPrimaryBuildingLocation
    ,DATE('{date}')																	AS bq_load_date	

FROM `{project}.{dest_dataset}.TransactionConfiguration` AS TransactionConfiguration

    INNER JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
    ON PolicyTransactionProduct.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey
    AND PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine')

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1) AS RiskLocationIM 
    ON RiskLocationIM.PolicyTransactionKey = TransactionConfiguration.PolicyTransactionKey

    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date}) AND FixedStockRank = 1 AND IsTransactionSliceEffective = 1) AS RiskStockIM 
    ON RiskStockIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
    AND RiskStockIM.RiskLocationKey = RiskLocationIM.RiskLocationKey

WHERE   1 = 1
	AND TransactionConfiguration.bq_load_date = DATE({partition_date})
