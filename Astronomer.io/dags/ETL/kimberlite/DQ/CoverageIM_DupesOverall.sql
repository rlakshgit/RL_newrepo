
SELECT 
	'DUPES BY KEY' as UnitTest
	,IMCov.IMCoverageKey
	,IMCov.JobNumber
	,IMCov.CoverageLevel
	,IMCov.CoveragePublicID
	,IMCovSumm.NumRecords
	,DATE('{date}') AS bq_load_date

FROM 
(
	SELECT 
		IMCoverageKey
		,count(*) AS NumRecords
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) 
	GROUP BY
		IMCoverageKey
	HAVING COUNT(*) > 1
)IMCovSumm

	INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) AS IMCov
	ON IMCov.IMCoverageKey = IMCovSumm.IMCoverageKey

--WHERE	1 = 1
		--AND JobNumber = ISNULL(@jobnumber, JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
	
UNION ALL

SELECT 
	'DUPES BY COVERAGE FixedID' as UnitTest
	,IMCov.IMCoverageKey
	,IMCov.JobNumber
	,IMCov.CoverageLevel
	,IMCov.CoveragePublicID
	,IMCovSumm.NumRecords
	,DATE('{date}') AS bq_load_date

FROM 
(
	SELECT 
		PolicyPeriodPublicID
		,CoverageFixedID
		,CoverageLevel
		,count(*) AS NumRecords
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) 
	WHERE	1 = 1
			AND IsTransactionSliceEffective = 1
			AND FixedCoverageInBranchRank = 1
	GROUP BY
		PolicyPeriodPublicID
		,CoverageFixedID
		,CoverageLevel
	HAVING COUNT(*) > 1
)IMCovSumm

	INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date})) AS IMCov
	ON IMCov.PolicyPeriodPublicID = IMCovSumm.PolicyPeriodPublicID
	AND IMCov.CoverageFixedID = IMCovSumm.CoverageFixedID
	AND IMCov.CoverageLevel = IMCovSumm.CoverageLevel

--WHERE	1 = 1
		--AND JobNumber = ISNULL(@jobnumber, JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)