-- tag: RiskStockIM_DupesOverall - tag ends/
/**** RiskStockIM_DupesOverall.sql ********
 
 *****  Change History  *****

	06/17/2021	DROBAK		INIT
	02/09/2022	DROBAK		And Where clause for new DQ source_record_exceptions table
	
*/
----------------------------------
--unit test w/ Dupe records
	SELECT 
		'DUPE by ID' as UnitTest
		,RiskStockIM.StockPublicID
		,CAST(NULL AS STRING) as LocationPublicID	
		,RiskStockIM.JobNumber 
		,RiskStockIM.LocationNumber
		,CAST(NULL AS INT64) as FixedStockRank
		,COUNT(1) as NumRecords
		, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date})) AS dt
	WHERE 1 = 1
		AND RiskStockIM.JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='RiskStockIM' AND DQTest='DUPE by ID' AND KeyCol1Name='JobNumber')
	GROUP BY 
		RiskStockIM.StockPublicID
		,RiskStockIM.JobNumber 
		,RiskStockIM.LocationNumber
	HAVING COUNT(1)>1

UNION ALL

--unit test w/ Dupe records
	SELECT 
		'DUPE by Tran-Loc' as UnitTest
		,CAST(NULL AS STRING) as StockPublicID
		,LocationPublicID
		,RiskStockIM.JobNumber 
		,RiskStockIM.LocationNumber
		,RiskStockIM.FixedStockRank
		,COUNT(1) as NumRecords
		, DATE('{date}') AS bq_load_date 	
	FROM (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` WHERE bq_load_date = DATE({partition_date})) AS dt
	WHERE 1 = 1
		AND IsTransactionSliceEffective = 1
		AND FixedStockRank = 1
		AND RiskStockIM.JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='RiskStockIM' AND DQTest='DUPE by Tran-Loc' AND KeyCol1Name='JobNumber')
	GROUP BY 
		RiskStockIM.LocationPublicID
		,RiskStockIM.JobNumber 
		,RiskStockIM.LocationNumber
		,RiskStockIM.FixedStockRank
	HAVING COUNT(1)>1	