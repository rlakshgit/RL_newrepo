/***Building Block***
	BBDQ_PJCoverageLevelAttributes_TransCovCode.sql
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/01/2021	DROBAK		Init
---------------------------------------------------------------------------------------------------
*/
--each PolicyTransactionKey & CoverageTypeCode have one record

	SELECT 'Non SCH Transaction has one CoverageTypeCode' AS UnitTest
			, PolicyTransactionKey
			, CoverageTypeCode
			, JobNumber
			, COUNT(CoverageTypeCode) AS KeyCount
			, DATE('{date}') AS bq_load_date			
	--FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` 
	--WHERE bq_load_date = "2021-06-16"
	FROM (SELECT * FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageTypeCode != 'SCH'
	GROUP BY PolicyTransactionKey, CoverageTypeCode, JobNumber
	HAVING COUNT(CoverageTypeCode) > 1

	
/*	Review exceptions output from above

	SELECT 
			ItemCoverageKey, PolicyTransactionKey, RiskJewelryItemKey
			, CoverageTypeCode
			, CoverageNumber
			, JobNumber
			, PolicyNumber
			, CoverageCode 
            ,CoverageTypeCode
    FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` 
	WHERE bq_load_date = "2021-06-13"
	--AND CoverageTypeCode = 'SCH'
	--AND PolicyTransactionKey = FROM_BASE64('1FuXfa9TOR5fB3G+MzrbXm0QAjva+I54+WHsKbiY6hE=')
	AND PolicyTransactionKey = FROM_BASE64('yOtvdE6LIvLXwj81oNAle4O5pkq+X2YYRafCBgyVkqo=')
*/