--Missing Values[Umbrella]
SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'Line' as CoverageLevel
					, cov.PublicID 
					, cov.FixedID 
					, cov.BranchID 
					, pc_policyperiod.EditEffectiveDate
					, cov.EffectiveDate 
					, cov.ExpirationDate
					, cov.PatternCode 
					, cov.FinalPersistedLimit_JMIC 
					, cov.FinalPersistedDeductible_JMIC 
					, DATE('{date}') AS bq_load_date						
from (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME={partition_date}) cov
inner join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
inner join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
--inner join (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_umbcost_jmic on pcx_umbcost_jmic.UmbrellaLineCov=cov.FixedID AND pcx_umbcost_jmic.BranchID=pc_policyperiod.ID
where cov.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoverageUMB` where bq_load_date=DATE({partition_date}))
--and cov.EffectiveDate > EditEffectiveDate
	AND(pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))