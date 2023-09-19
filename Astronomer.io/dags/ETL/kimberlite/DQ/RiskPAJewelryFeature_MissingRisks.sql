/**** RiskPAJewelryFeature_MissingRisks.sql ********
 
 *****  Change History  *****

	06/28/2021	SLJ			Init
	02/09/2022	DROBAK		And Where clause for new DQ source_record_exceptions table
-------------------------------------------------------------------------------------------------------------
*/
--Missing Article Jewelery Key
SELECT 
	'Missing Article Key'		AS UnitTest
	,PAJewelryFeaturePublicID
	,JobNumber
	,DATE('{date}')				AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskPAJewelryFeature` WHERE bq_load_date = DATE({partition_date})) RiskPAJewelryFeature
WHERE RiskPAJewelryFeature.RiskPAJewelryKey IS NULL
AND	pc_job.JobNumber NOT IN (	SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` 
								WHERE TableName='RiskPAJewelryFeature' AND DQTest='Missing Article Key' AND KeyCol1Name='JobNumber')
