/**********************************************************************************************
	Kimberlite - DQ Checks
	CoverageJewelryItem_DupesOverall
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	12/22/2021	SLJ		Modified and added logic for check duplicates by fixedid

	-------------------------------------------------------------------------------------------
*/
SELECT 
	'DUPES BY KEY' as UnitTest
	,PJCov.ItemCoverageKey
	,PJCov.JobNumber
	,PJCov.CoverageLevel
	,PJCov.CoveragePublicID
	,PJCovSumm.NumRecords
	,DATE('{date}') AS bq_load_date

FROM 
(
	SELECT 
		ItemCoverageKey
		,count(*) AS NumRecords
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) 
	GROUP BY
		ItemCoverageKey
	HAVING COUNT(*) > 1
)PJCovSumm

	INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) AS PJCov
	ON PJCov.ItemCoverageKey = PJCovSumm.ItemCoverageKey

WHERE	1 = 1
		--AND JobNumber = ISNULL(@jobnumber, JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
	
UNION ALL

SELECT 
	'DUPES BY COVERAGE FixedID' as UnitTest
	,PJCov.ItemCoverageKey
	,PJCov.JobNumber
	,PJCov.CoverageLevel
	,PJCov.CoveragePublicID
	,PJCovSumm.NumRecords
	,DATE('{date}') AS bq_load_date

FROM 
(
	SELECT 
		PolicyPeriodPublicID
		,CoverageFixedID
		,CoverageLevel
		,count(*) AS NumRecords
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) 
	WHERE	1 = 1
			AND IsTransactionSliceEffective = 1
			AND FixedCoverageInBranchRank = 1
	GROUP BY
		PolicyPeriodPublicID
		,CoverageFixedID
		,CoverageLevel
	HAVING COUNT(*) > 1
)PJCovSumm

	INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageJewelryItem` WHERE bq_load_date = DATE({partition_date})) AS PJCov
	ON PJCov.PolicyPeriodPublicID = PJCovSumm.PolicyPeriodPublicID
	AND PJCov.CoverageFixedID = PJCovSumm.CoverageFixedID
	AND PJCov.CoverageLevel = PJCovSumm.CoverageLevel