#Excelion2021!#StandardSQL
WITH singles AS (
SELECT
    QuoteServiceID
    ,JSON_QUERY_ARRAY(
                REPLACE(RatabaseProperties_Policy_LOB_Region_Coverage, 'None', 'null'),
                '$') AS Article
    ,JSON_QUERY_ARRAY(
                REPLACE(RatabaseProperties_Policy_LOB_Region_Item, 'None', 'null'),
                '$.Inputs.RatabaseField') AS WearerDetails

FROM `{project}.{base_dataset}.t_request_pa`

WHERE RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES = "1"
),

multiples AS (
    SELECT
    QuoteServiceID
    ,JSON_QUERY_ARRAY(
                REPLACE(RatabaseProperties_Policy_LOB_Region_Coverage, 'None', 'null'),
                '$') AS Article
    ,JSON_QUERY_ARRAY(
                REPLACE(RatabaseProperties_Policy_LOB_Region_Item, 'None', 'null'),
                '$.Inputs.RatabaseField') AS WearerDetails

FROM `{project}.{base_dataset}.t_request_pa`

WHERE RatabaseProperties_Policy_Inputs_RatabaseField_NBR_POLICY_SCHED_ARTICLES <> "1"
),

articles AS (
    SELECT
        QuoteServiceID
        ,JSON_QUERY_ARRAY(article, '$.Inputs.RatabaseField') AS Article
        ,JSON_VALUE(WearerDetails[OFFSET(0)], '$.Value') AS ConvictionFelony
        ,JSON_VALUE(WearerDetails[OFFSET(1)], '$.Value') AS ConvictionMisdmnr
        ,JSON_VALUE(WearerDetails[OFFSET(2)], '$.Value') AS DateOfBirth
        ,JSON_VALUE(WearerDetails[OFFSET(3)], '$.Value') AS Occupation
        ,JSON_VALUE(WearerDetails[OFFSET(4)], '$.Value') AS RecordType
        ,JSON_VALUE(WearerDetails[OFFSET(5)], '$.Value') AS WearerID
        ,JSON_VALUE(WearerDetails[OFFSET(6)], '$.Value') AS AlarmType
        ,JSON_VALUE(WearerDetails[OFFSET(7)], '$.Value') AS LocationID
        ,JSON_VALUE(WearerDetails[OFFSET(8)], '$.Value') AS TerritoryCode
        ,JSON_VALUE(WearerDetails[OFFSET(9)], '$.Value') AS NbrCovsOnLoc
        
    FROM singles,
    UNNEST(Article) AS article
    
    UNION ALL
    
    SELECT
        QuoteServiceID
        ,JSON_QUERY_ARRAY(article, '$.Inputs.RatabaseField') AS Article
        ,JSON_VALUE(WearerDetails[OFFSET(0)], '$.Value') AS ConvictionFelony
        ,JSON_VALUE(WearerDetails[OFFSET(1)], '$.Value') AS ConvictionMisdmnr
        ,JSON_VALUE(WearerDetails[OFFSET(2)], '$.Value') AS DateOfBirth
        ,JSON_VALUE(WearerDetails[OFFSET(3)], '$.Value') AS Occupation
        ,JSON_VALUE(WearerDetails[OFFSET(4)], '$.Value') AS RecordType
        ,JSON_VALUE(WearerDetails[OFFSET(5)], '$.Value') AS WearerID
        ,JSON_VALUE(WearerDetails[OFFSET(6)], '$.Value') AS AlarmType
        ,JSON_VALUE(WearerDetails[OFFSET(7)], '$.Value') AS LocationID
        ,JSON_VALUE(WearerDetails[OFFSET(8)], '$.Value') AS TerritoryCode
        ,JSON_VALUE(WearerDetails[OFFSET(9)], '$.Value') AS NbrCovsOnLoc

    FROM multiples,
    UNNEST(Article) AS article
)

SELECT
    QuoteServiceID
    ,JSON_VALUE(Article[OFFSET(0)], '$.Value') AS ArticleID
    ,JSON_VALUE(Article[OFFSET(1)], '$.Value') AS CoverageID
    ,JSON_VALUE(Article[OFFSET(2)], '$.Value') AS AppraisalDate
    ,JSON_VALUE(Article[OFFSET(3)], '$.Value') AS AppraisalReceived
    ,JSON_VALUE(Article[OFFSET(4)], '$.Value') AS AppraisalRequested
    ,JSON_VALUE(Article[OFFSET(5)], '$.Value') AS AffinityLevel
    ,JSON_VALUE(Article[OFFSET(6)], '$.Value') AS ArticleManufactureYear
    ,JSON_VALUE(Article[OFFSET(7)], '$.Value') AS ArticleDamage
    ,JSON_VALUE(Article[OFFSET(8)], '$.Value') AS ArticleGender
    ,JSON_VALUE(Article[OFFSET(9)], '$.Value') AS ArticleNumber
    ,JSON_VALUE(Article[OFFSET(10)], '$.Value') AS ArticleStored
    ,JSON_VALUE(Article[OFFSET(12)], '$.Value') AS ArticleType
    ,JSON_VALUE(Article[OFFSET(11)], '$.Value') AS ArticleSubtype
    ,JSON_VALUE(Article[OFFSET(13)], '$.Value') AS DaysOutOfVault
    ,JSON_VALUE(Article[OFFSET(14)], '$.Value') AS DistributionSource
    ,JSON_VALUE(Article[OFFSET(15)], '$.Value') AS GemCertification
    ,JSON_VALUE(Article[OFFSET(16)], '$.Value') AS GemType
    ,JSON_VALUE(Article[OFFSET(17)], '$.Value') AS DiscGradingRpt
    ,JSON_VALUE(Article[OFFSET(18)], '$.Value') AS IRPM
    ,JSON_VALUE(Article[OFFSET(19)], '$.Value') AS JMCarePlan
    ,JSON_VALUE(Article[OFFSET(20)], '$.Value') AS ValuationType
    ,JSON_VALUE(Article[OFFSET(21)], '$.Value') AS CovTypeCode
    ,JSON_VALUE(Article[OFFSET(22)], '$.Value') AS ArticleDeductible
    ,JSON_VALUE(Article[OFFSET(23)], '$.Value') AS ArticleLimit
    ,ConvictionFelony
    ,ConvictionMisdmnr
    ,DateOfBirth
    ,Occupation
    ,RecordType
    ,WearerID
    ,AlarmType
    ,LocationID
    ,TerritoryCode
    ,NbrCovsOnLoc 
    
FROM articles
