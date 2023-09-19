/***Building Block***
	BBDQ_PAJewelryCoverageLevelAttributes_TransCovCode.sql
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/07/2021	DROBAK		Init
---------------------------------------------------------------------------------------------------
*/
--each PolicyTransactionKey & CoverageTypeCode have one record

	SELECT 'Non SCH Transaction has one CoverageTypeCode' AS UnitTest
			, PolicyTransactionKey
			, CoverageTypeCode
			, JobNumber
			, COUNT(CoverageTypeCode) AS KeyCount
			, DATE('{date}') AS bq_load_date
	--FROM `{project}.{dest_dataset}.PACoverageLevelAttributes` 
	--WHERE bq_load_date = "2021-06-16"
	FROM (SELECT * FROM `{project}.{dest_dataset}.PAJewelryCoverageLevelAttributes` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageTypeCode != 'SCH'
	GROUP BY PolicyTransactionKey, CoverageTypeCode, JobNumber
	HAVING COUNT(CoverageTypeCode) > 1

/*	Review exceptions output from above

   	SELECT 
			PAJewelryCoverageKey, PolicyTransactionKey, RiskPAJewelryKey
			, CoverageTypeCode
			, CoverageNumber
			, JobNumber
			, PolicyNumber
			, CoverageCode 
            ,CoverageTypeCode
    FROM `{project}.{dest_dataset}.PACoverageLevelAttributes` 
	WHERE bq_load_date = "2021-06-20"
	--AND CoverageTypeCode = 'SCH'
	AND PolicyTransactionKey = FROM_BASE64('/koB99WVyU+/xV+gvI8hW/h83mSGm4HaB1Su4CAJ+JE=')
*/