/**** CoverageIM_MissingByX.sql ********
 
 *****  Change History  *****

	08/25/2021	DROBAK		Improve EffDate Logic (make = to BOP code)
*/

--Missing Values[Line]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'Line'								AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'Line' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)

UNION ALL

--Missing Values[SubLine]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'SubLine'							AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'SubLine' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)

UNION ALL

--Missing Values[Location]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'Location'							AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'Location' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)

UNION ALL

--Missing Values[sub-location]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'SubLoc'							AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'SubLoc' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)

UNION ALL

--Missing Values[stock]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'Stock'							AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'Stock' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)

UNION ALL

--Missing Values[substock]
SELECT 
	'MISSING'							AS UnitTest
	,JobNumber							AS JobNumber
	,'SubStock'							AS CoverageLevel
	,cov.PublicID						AS CoveragePublicID
	,cov.FixedID						AS CoverageFixedID
	,cov.BranchID						AS CoverageBranchID
	,pc_policyperiod.EditEffectiveDate	AS EditEffectiveDate
	,cov.EffectiveDate					AS CoverageEffectiveDate
	,cov.ExpirationDate					AS CoverageExpirationDate
	,cov.PatternCode					AS CoveragePatternCode
	,cov.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,cov.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,cov.FinalPersistedTempFromDt_JMIC	AS CoverageFinalPersistedTempFromDt
	,cov.FinalPersistedTempToDt_JMIC	AS CoverageFinalPersistedTempToDt
	,DATE('{date}')						AS bq_load_date	

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS cov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = cov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
	ON pcx_ilmcost_jmic.ILMLineCov = cov.FixedID 
	AND pcx_ilmcost_jmic.BranchID = pc_policyperiod.ID
WHERE	cov.PublicID NOT IN (SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageIM` WHERE CoverageLevel = 'SubStock' AND bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
				AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND Jobnumber = ISNULL(@jobnumber,JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)