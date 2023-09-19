-- tag: RiskLocationIM_MissingRisks - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Checks
	RiskLocationIM_MissingRisks.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/01/2021	SLJ			INIT
	03/01/2023	DROBAK		Added logic to source_record_exceptions
	
	-------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
SELECT 'MISSING RISKS'	AS UnitTest
	,  LocationPublicID
	,  RiskLocationKey
	,  JobNumber
	,  DATE('{date}')	AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM` WHERE bq_load_date = DATE({partition_date})) AS RiskLocationIM
WHERE 1=1
AND JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='RiskLocationIM' AND DQTest='MISSING RISKS' AND KeyCol1Name='JobNumber')
AND RiskLocationKey IS NULL  --all item coverage should be tied to a risk 