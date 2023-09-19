/**** CoverageIM_MissingRisks.sql ********
 
 *****  Change History  *****

	08/26/2021	DROBAK		Add IsTransactionSliceEffective
	01/10/2022	SLJ			Add jobnumber, add Logic split between location and stock id
	02/09/2022	DROBAK		Add Where clause for new DQ source_record_exceptions table
	01/04/2023	DROBAK		Add PolicyPeriodStatus exclusion for Draft, Withdrawn, Expired, New, NotTaken
*/
SELECT 
	'MISSING RISKS' AS UnitTest
	,PolicyNumber
	,JobNumber
	,CoverageLevel
	,CoveragePublicID
	,RiskLocationKey
	,RiskStockKey
	,IsTransactionSliceEffective
	,PolicyPeriodStatus
	,DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) AS CoverageIM
WHERE	RiskLocationKey IS NULL 
	AND IsTransactionSliceEffective != 0
	AND PolicyPeriodStatus NOT IN (1, 3, 5, 6, 11)	--Exclude: Draft, Withdrawn, Expired, New, NotTaken
    AND CoverageIM.JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='CoverageIM' AND DQTest='MISSING RISKS' AND KeyCol1Name='JobNumber')

UNION ALL

SELECT 
	'MISSING RISKS' AS UnitTest
	,PolicyNumber
	,JobNumber
	,CoverageLevel
	,CoveragePublicID
	,RiskLocationKey
	,RiskStockKey
	,IsTransactionSliceEffective
	,PolicyPeriodStatus
	,DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) AS CoverageIM
WHERE	RiskStockKey IS NULL
	AND CoverageLevel like '%stock%'
	AND IsTransactionSliceEffective != 0
	AND CoverageIM.JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='CoverageIM' AND DQTest='MISSING RISKS' AND KeyCol1Name='JobNumber')
