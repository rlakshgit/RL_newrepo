/***Building Block***
	BBDQ_CLRiskInlandMarineAttributes_TransxLocn.sql
---------------------------------------------------------------------------------------------------

	*****  Change History  *****

	07/01/2021	DROBAK		Init
	07/08/2021	DROBAK		Added JobNumber
---------------------------------------------------------------------------------------------------
*/
--RiskLocationIM = each PolicyTransactionKey & LocationNumber have one record

	SELECT 'Transaction has one LocationNumber' AS UnitTest
			, PolicyTransactionKey
			, JobNumber
			, LocationNumber
			, COUNT(LocationNumber) AS KeyCount
			, DATE('{date}') AS bq_load_date			
	--FROM `qa-edl.B_QA_ref_kimberlite.CLRiskInLandMarineAttributes` 
	--WHERE bq_load_date = "2021-06-16"
	FROM (SELECT * FROM `{project}.{dest_dataset}.CLRiskInLandMarineAttributes` WHERE bq_load_date = DATE({partition_date}))
	--WHERE bq_load_date = DATE({partition_date})
	GROUP BY PolicyTransactionKey, LocationNumber, JobNumber
	HAVING COUNT(LocationNumber) > 1
