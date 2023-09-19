	/***Building Block***
	BBDQ_CLRiskInlandMarineAttributes_LocnStock.sql
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/01/2021	DROBAK		Init
	07/08/2021	DROBAK		Added TransactionStatus & split into a UNION
	04/08/2022	DROBAK		Added Filters for IsTransacionSlice* fields to avoid false positive errors
	04/13/2022	DROBAK		Add Where clause for new DQ source_record_exceptions table

---------------------------------------------------------------------------------------------------
*/
	--RiskStockIM  = Every location has only one stock
	SELECT	'IM Location with > 1 Stock' AS UnitTest
			, JobNumber
			, LocationNumber
			, COUNT(LocationNumber) AS KeyCount
	--FROM `qa-edl.B_QA_ref_kimberlite.CLRiskInLandMarineAttributes`
	--WHERE bq_load_date = "2021-06-16"
	FROM (SELECT * FROM `{project}.{dest_dataset}.CLRiskInLandMarineAttributes` WHERE bq_load_date = DATE({partition_date}))
	WHERE TransactionStatus = 'Bound'
		AND IsTransactionSliceEffStock = 1
		AND IsTransactionSliceEffLocn = 1
		AND JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='CLRiskInLandMarineAttributes' AND DQTest='IM Location with > 1 Stock' AND KeyCol1Name='JobNumber')
	GROUP BY JobNumber, LocationNumber
	HAVING COUNT(LocationNumber) > 1

	UNION ALL

	SELECT 'IM Location with > 1 Stock' AS UnitTest
            , JobNumber
			, LocationNumber
			, COUNT(LocationNumber) AS KeyCount
	FROM (SELECT * FROM `{project}.{dest_dataset}.CLRiskInLandMarineAttributes` WHERE bq_load_date = DATE({partition_date}))
	WHERE TransactionStatus != 'Bound'
		AND IsTransactionSliceEffStock = 1
		AND IsTransactionSliceEffLocn = 1
		AND JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='CLRiskInLandMarineAttributes' AND DQTest='IM Location with > 1 Stock' AND KeyCol1Name='JobNumber')
    GROUP BY PolicyTransactionKey, JobNumber, LocationNumber
    HAVING COUNT(LocationNumber) > 1
