/**********************************************************************************************
	Kimberlite - DQ Checks
	CoverageJewelryItem_MissingByX.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/01/2021	DROBAK		Updated EditEffectiveDate logic
	12/22/2021	SLJ			Modified tests and filter for coverage code
	02/09/2022	DROBAK		And Where clause for new DQ source_record_exceptions table
	-------------------------------------------------------------------------------------------
*/
/***** Missing Records *****/
--Missing Values[Line]
SELECT
	'MISSING'													AS UnitTest
	,pc_policyperiod.PolicyNumber								AS PolicyNumber
	,pc_job.JobNumber											AS JobNumber
	,'UnScheduledCov'											AS CoverageLevel
	,pcx_jmpersonallinecov.PublicID								AS CoveragePublicID
	,pcx_jmpersonallinecov.FixedID								AS CoverageFixedID
	,pcx_jmpersonallinecov.FinalPersistedLimit_JMIC				AS CoverageFinalPersistedLimit
	,pcx_jmpersonallinecov.FinalPersistedDeductible_JMIC		AS CoverageFinalPersistedDeductible
	,pc_policyperiod.EditEffectiveDate							AS EditEffectiveDate
	,pcx_jmpersonallinecov.EffectiveDate						AS CoverageExpirationDate
	,pcx_jmpersonallinecov.ExpirationDate						AS CoverageEffectiveDate
	,DATE('{date}')												AS bq_load_date

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jmpersonallinecov` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmpersonallinecov
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
		ON pc_policyperiod.id = pcx_jmpersonallinecov.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.id = pc_policyperiod.JobID

WHERE	pcx_jmpersonallinecov.PublicID NOT IN ( SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageJewelryItem`
												WHERE CoverageCode != 'JewelryItemCov_JMIC_PL' AND bq_load_date = DATE({partition_date}) )
AND	(	pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.PeriodStart)
		AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd)
	)
AND		pc_job.JobNumber NOT IN (	SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions`
									WHERE TableName='CoverageJewelryItem' AND DQTest='MISSING' AND KeyCol1Name='JobNumber')

UNION ALL

--Missing Values[Item]
SELECT
	'MISSING'													AS UnitTest
	,pc_policyperiod.PolicyNumber								AS PolicyNumber
	,pc_job.JobNumber											AS JobNumber
	,'ScheduledCov'												AS CoverageLevel
	,pcx_jwryitemcov_jmic_pl.PublicID							AS CoveragePublicID
	,pcx_jwryitemcov_jmic_pl.FixedID							AS CoverageFixedID
	,pcx_jwryitemcov_jmic_pl.FinalPersistedLimit_JMIC			AS CoverageFinalPersistedLimit
	,pcx_jwryitemcov_jmic_pl.FinalPersistedDeductible_JMIC		AS CoverageFinalPersistedDeductible
	,pc_policyperiod.EditEffectiveDate							AS EditEffectiveDate
	,pcx_jwryitemcov_jmic_pl.EffectiveDate						AS CoverageExpirationDate
	,pcx_jwryitemcov_jmic_pl.ExpirationDate						AS CoverageEffectiveDate
	,DATE('{date}')												AS bq_load_date

FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jwryitemcov_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jwryitemcov_jmic_pl
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
		ON pc_policyperiod.id = pcx_jwryitemcov_jmic_pl.BranchID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.id = pc_policyperiod.JobID

WHERE	pcx_jwryitemcov_jmic_pl.PublicID NOT IN (	SELECT CoveragePublicID FROM `{project}.{dest_dataset}.CoverageJewelryItem`
													WHERE CoverageCode = 'JewelryItemCov_JMIC_PL' AND bq_load_date = DATE({partition_date}) )
AND	(	pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
		AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd)
	)
AND		pc_job.JobNumber NOT IN (	SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions`
									WHERE TableName='CoverageJewelryItem' AND DQTest='MISSING' AND KeyCol1Name='JobNumber')
