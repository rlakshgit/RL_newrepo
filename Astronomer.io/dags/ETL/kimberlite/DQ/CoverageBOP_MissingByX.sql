/**** CoverageBOP_MissingByX.sql ********
 
 *****  Change History  *****
 -------------------------------------------------------------------------------------------------------------------------------------
	06/29/2021	DROBAK		Add CoveragePublicID; align column names; add EffDate Logic
	12/22/2021	DROBAK		Added joins to pc_policyline and pctl_policyline (too many false positives otherwise)

 -------------------------------------------------------------------------------------------------------------------------------------
*/

	--Missing Values[Line]
	SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'Line' as CoverageLevel
					, pc_policyperiod.policynumber as PolicyNumber
					, cov.PublicID AS CoveragePublicID
					, cov.effectivedate as CoverageEffectiveDate
					, cov.ExpirationDate as CoverageExpirationDate
					, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME={partition_date}) cov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME={partition_date}) pc_policyline
			ON pc_policyperiod.ID = pc_policyline.BranchID
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	WHERE cov.PublicID NOT IN (select CoveragePublicID from `{project}.{dest_dataset}.CoverageBOP` where CoverageLevel='Line' and bq_load_date=DATE({partition_date}))
		AND cov.FixedID IN (select BusinessOwnersCov from `{project}.{pc_dataset}.pc_bopcost` where BusinessOwnersCov is not null and _PARTITIONTIME={partition_date})
		AND (pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))
		
UNION ALL
	--Missing Values[SubLine]
	SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'SubLine' as CoverageLevel
					, pc_policyperiod.policynumber as PolicyNumber
					, cov.PublicID AS CoveragePublicID
					, cov.effectivedate as CoverageEffectiveDate
					, cov.ExpirationDate as CoverageExpirationDate
					, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME={partition_date}) cov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubline_jmic` WHERE _PARTITIONTIME={partition_date}) pcx_bopsubline_jmic
			ON	pcx_bopsubline_jmic.BranchID = pc_policyperiod.ID
			AND	pcx_bopsubline_jmic.FixedID = cov.BOPSubLine
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_bopsubline_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pcx_bopsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME={partition_date}) pc_policyline
			ON pc_policyperiod.ID = pc_policyline.BranchID
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	WHERE cov.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoverageBOP` where CoverageLevel='SubLine' and bq_load_date=DATE({partition_date}))
		AND cov.FixedID in (select BOPSubLineCov from `{project}.{pc_dataset}.pc_bopcost` where BOPSubLineCov is not null and _PARTITIONTIME={partition_date})
		AND (pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))

UNION ALL 
		
	--Missing Values[Location]
	SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'Location' as CoverageLevel
					, pc_policyperiod.policynumber as PolicyNumber
					, cov.PublicID AS CoveragePublicID
					, cov.effectivedate as CoverageEffectiveDate
					, cov.ExpirationDate as CoverageExpirationDate
					, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME={partition_date}) cov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME={partition_date}) pc_policyline
			ON pc_policyperiod.ID = pc_policyline.BranchID
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	WHERE cov.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoverageBOP` where CoverageLevel='Location' and bq_load_date=DATE({partition_date}))
		AND cov.FixedID in (select BOPLocationCov from `{project}.{pc_dataset}.pc_bopcost` where BOPLocationCov is not null and _PARTITIONTIME={partition_date})
		AND (pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))

UNION ALL
		
	--Missing Values[sub-location]
	SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'SubLoc' as CoverageLevel
					, pc_policyperiod.policynumber as PolicyNumber
					, cov.PublicID AS CoveragePublicID
					, cov.effectivedate as CoverageEffectiveDate
					, cov.ExpirationDate as CoverageExpirationDate
					, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME={partition_date}) cov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME={partition_date}) pc_policyline
			ON pc_policyperiod.ID = pc_policyline.BranchID
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	WHERE cov.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoverageBOP` where CoverageLevel='SubLoc' and bq_load_date=DATE({partition_date}))
		AND cov.FixedID in (select BOPSubLocCov from `{project}.{pc_dataset}.pc_bopcost` where BOPSubLocCov is not null and _PARTITIONTIME={partition_date})
		AND (pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))

UNION ALL
		
	--Missing Values[building]
	SELECT 'MISSING' as UnitTest
					, JobNumber
					, 'Building' as CoverageLevel
					, pc_policyperiod.policynumber as PolicyNumber
					, cov.PublicID AS CoveragePublicID
					, cov.effectivedate as CoverageEffectiveDate
					, cov.ExpirationDate as CoverageExpirationDate
					, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME={partition_date}) cov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pc_policyperiod on pc_policyperiod.id = cov.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME={partition_date}) pc_policyline
			ON pc_policyperiod.ID = pc_policyline.BranchID
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(cov.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'
	WHERE cov.PublicID not in (select CoveragePublicID from `{project}.{dest_dataset}.CoverageBOP` where CoverageLevel='Building' and bq_load_date=DATE({partition_date}))
		AND cov.FixedID in (select BOPBuildingCov from `{project}.{pc_dataset}.pc_bopcost` where BOPBuildingCov is not null and _PARTITIONTIME={partition_date})
		--and pc_policyperiod.EditEffectiveDate < cov.ExpirationDate
		AND (pc_policyperiod.EditEffectiveDate >= COALESCE(cov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(cov.ExpirationDate,pc_policyperiod.PeriodEnd))

