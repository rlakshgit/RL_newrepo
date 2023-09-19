/**** RiskJewelryItemFeature_MissingRisks.sql ********

 *****  Change History  *****

	06/01/2021	SLJ		Init
	02/02/2022	SLJ		Use Feature Key in place of Feature Detail Key
*/
--MISSING Risks In Extract
SELECT
	'MISSING RISKS' as UnitTest
	,RiskItemFeatureKey
	,JobNumber
	,ItemFeaturePublicID
	,DATE('{date}') AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskJewelryItemFeature` WHERE bq_load_date = DATE({partition_date}))
WHERE RiskItemFeatureKey IS NULL  --all item coverage should be tied to a risk 
--AND Jobnumber=@jobnumber 