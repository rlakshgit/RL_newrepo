/***** Dupes In Extract *****/
SELECT * FROM (
	SELECT 
		'DUPES BY KEY' as UnitTest
		,PAJewelryCoverageKey
		,JobNumber
		,CoverageLevel
		,CoveragePublicID
		,count(*) OVER(PARTITION BY PAJewelryCoverageKey) AS CoverageRowCount
		,DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoveragePAJewelry` WHERE bq_load_date = DATE({partition_date}))  
	WHERE	1 = 1
			--AND JobNumber = ISNULL(@jobnumber, JobNumber)
			--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
	
	UNION ALL

	SELECT 
		'DUPES BY COVERAGE NUM' as UnitTest
		,PAJewelryCoverageKey
		,JobNumber
		,CoverageLevel
		,CoveragePublicID
		,COUNT(*) OVER(PARTITION BY PolicyPeriodPublicID,CoverageNumber,CoverageLevel) AS CoverageRowCount
		,DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoveragePAJewelry` WHERE bq_load_date = DATE({partition_date})) 
	WHERE	1 = 1
			AND IsTransactionSliceEffective = 1
			AND FixedCoverageInBranchRank = 1
			--AND JobNumber = ISNULL(@jobnumber, JobNumber)
			--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
)t
WHERE CoverageRowCount > 1