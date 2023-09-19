--Missing Values[Line]
SELECT 
	'MISSING'													AS UnitTest
	,pc_job.JobNumber											AS JobNumber
	,'UnScheduledCov'											AS CoverageLevel
	,pcx_personalartcllinecov_jm.PublicID						AS CoveragePublicID
	,pcx_personalartcllinecov_jm.FixedID						AS CoverageFixedID
	,pc_policyperiod.PolicyNumber								AS PolicyNumber
	,pcx_personalartcllinecov_jm.FinalPersistedLimit_JMIC		AS CoverageFinalPersistedLimit
	,pcx_personalartcllinecov_jm.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,pc_policyperiod.EditEffectiveDate							AS EditEffectiveDate
	,pcx_personalartcllinecov_jm.EffectiveDate					AS CoverageExpirationDate
	,pcx_personalartcllinecov_jm.ExpirationDate					AS CoverageEffectiveDate
	,DATE('{date}')												AS bq_load_date

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalartcllinecov_jm
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = pcx_personalartcllinecov_jm.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID

WHERE	pcx_personalartcllinecov_jm.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoveragePAJewelry` where CoverageCode != 'JewelryItemCov_JM' and bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND JobNumber = ISNULL(@jobnumber, JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
UNION ALL
--Missing Values[Article]
SELECT 
	'MISSING'													AS UnitTest
	,pc_job.JobNumber											AS JobNumber
	,'ScheduledCov'												AS CoverageLevel
	,pcx_personalarticlecov_jm.PublicID							AS CoveragePublicID
	,pcx_personalarticlecov_jm.FixedID							AS CoverageFixedID
	,pc_policyperiod.PolicyNumber								AS PolicyNumber
	,pcx_personalarticlecov_jm.FinalPersistedLimit_JMIC			AS CoverageFinalPersistedLimit
	,pcx_personalarticlecov_jm.FinalPersistedDeductible_JMIC	AS CoverageFinalPersistedDeductible
	,pc_policyperiod.EditEffectiveDate							AS EditEffectiveDate
	,pcx_personalarticlecov_jm.EffectiveDate					AS CoverageExpirationDate
	,pcx_personalarticlecov_jm.ExpirationDate					AS CoverageEffectiveDate
	,DATE('{date}')												AS bq_load_date

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticlecov_jm
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
	ON pc_policyperiod.id = pcx_personalarticlecov_jm.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
	ON pc_job.id = pc_policyperiod.JobID

WHERE	pcx_personalarticlecov_jm.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoveragePAJewelry` where CoverageCode = 'JewelryItemCov_JM' and bq_load_date = DATE({partition_date}))
		AND(pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd))
		--AND JobNumber = ISNULL(@jobnumber, JobNumber)
		--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)