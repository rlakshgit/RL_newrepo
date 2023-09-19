/***Building Block***
	BBDQ_PJCoverageLevelAttributes_SCHTransCovCode.sql
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/01/2021	DROBAK		Init
---------------------------------------------------------------------------------------------------
*/
--Where CoverageTypeCode = 'SCH' every CoverageTypeCode & CoverageNumber has one record
	SELECT  'Each Trans has only one SCH type record' AS UnitTest
			, PolicyTransactionKey
			, CoverageTypeCode
			, CoverageNumber
			, JobNumber
			, COUNT(CoverageNumber) AS KeyCount
			, DATE('{date}') AS bq_load_date			
	--FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` 
	--WHERE bq_load_date = "2021-06-13"
	FROM (SELECT * FROM `{project}.{dest_dataset}.PJCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageTypeCode = 'SCH'
	GROUP BY PolicyTransactionKey, CoverageTypeCode, CoverageNumber, JobNumber
	HAVING COUNT(CoverageNumber) > 1


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