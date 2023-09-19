/****** RiskJewelryItemFeature_DupesOverall.sql *********************
 
 *****  Change History  *****

	06/01/2021	SLJ			Init
	02/02/2022	SLJ/DROBAK	Rewrite as Key changed
	04/26/2022	DROBAK		Added  IsTransactionSliceEffective = 1
	05/27/2022	DROBAK		Added logic for source_record_exceptions

**********************************************************************/
--DUPES In Extract
SELECT 
    'DUPES'					AS UnitTest
    ,ItemFeaturePublicID	AS ItemFeaturePublicID
    ,COUNT(*)				AS NumRecords
     ,DATE('{date}')		AS bq_load_date 
FROM `{project}.{dest_dataset}.RiskJewelryItemFeature` AS RiskJewelryItemFeature
WHERE bq_load_date = DATE({partition_date})
  AND IsTransactionSliceEffective = 1
  AND RiskJewelryItemFeature.JobNumber NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions` WHERE TableName='RiskJewelryItemFeature' AND DQTest='DUPES' AND KeyCol1Name='JobNumber')
GROUP BY 
    ItemFeaturePublicID
HAVING COUNT(*) > 1 --dupe check

/* 
SELECT 
    'DUPES'					AS UnitTest
    ,ItemFeaturePublicID	AS ItemFeaturePublicID
    ,COUNT(*)				AS NumRecords
 --    ,DATE('{date}')		AS bq_load_date 
FROM `qa-edl.B_QA_ref_kimberlite.RiskJewelryItemFeature` RiskJewelryItemFeature
WHERE bq_load_date = DATE("2022-05-08")
  AND IsTransactionSliceEffective = 1
  AND RiskJewelryItemFeature.JobNumber NOT IN (SELECT KeyCol1 FROM `qa-edl.B_QA_ref_kimberlite_DQ.source_record_exceptions` WHERE TableName='RiskJewelryItemFeature' AND DQTest='DUPES' AND KeyCol1Name='JobNumber')
GROUP BY 
    ItemFeaturePublicID
HAVING COUNT(*) > 1 --dupe check
--64
*/
/*	Troubleshooting query

	SELECT 
		'DUPES'					AS UnitTest
		,F.ItemFeaturePublicID	AS ItemFeaturePublicID
		,COUNT(*)				AS NumRecords
		-- ,DATE('{date}')		AS bq_load_date 
		 ,F2.RiskJewelryItemKey
		 ,F2.JewelryItemNumber
		 ,P.TransEffDate
		 ,R.PolicyNumber
		 ,R.JobNumber
		 ,P.TransactionStatus
	FROM (SELECT * FROM `prod-edl.ref_kimberlite_DQ.RiskJewelryItemFeature_DupesOverall`  WHERE bq_load_date = DATE("2022-05-15")) AS F
	INNER JOIN (SELECT * FROM `prod-edl.ref_kimberlite.RiskJewelryItemFeature`  WHERE bq_load_date = DATE("2022-05-15")) AS F2
		ON F.ItemFeaturePublicID = F2.ItemFeaturePublicID
	INNER JOIN (SELECT * FROM `prod-edl.ref_kimberlite.RiskJewelryItem` WHERE bq_load_date = "2022-05-15") AS R
		ON F2.ItemPublicID = R.ItemPublicID
	INNER JOIN (SELECT * FROM `prod-edl.ref_kimberlite.PolicyTransaction` WHERE bq_load_date = "2022-05-15") AS P    
		ON F2.PolicyTransactionKey = P.PolicyTransactionKey
	WHERE F2.IsTransactionSliceEffective = 1
	--and R.ItemPublicID = 'pc:23195376'
	GROUP BY 
		F.ItemFeaturePublicID
		,F2.RiskJewelryItemKey
		,F2.JewelryItemNumber
		,P.TransEffDate
		,R.PolicyNumber
		,R.JobNumber
		,P.TransactionStatus
	HAVING COUNT(*) > 1 --dupe check
	ORDER BY P.TransEffDate DESC, ItemFeaturePublicID
*/