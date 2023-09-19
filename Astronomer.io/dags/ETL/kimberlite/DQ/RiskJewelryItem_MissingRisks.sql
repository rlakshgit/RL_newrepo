--unit test w/ Missing Records
select 
	'MISSING' as UnitTest
	, i.ItemNumber
	, pp.PolicyNumber
	, j.JobNumber 
    , DATE('{date}') AS bq_load_date		
from 
	(SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` where _PARTITIONTIME={partition_date}) i
	inner join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` where _PARTITIONTIME={partition_date}) pp
		on pp.id = i.BranchID	
	left join (SELECT * FROM `{project}.{dest_dataset}.RiskJewelryItem` where bq_load_date=DATE({partition_date})) e
		on e.ItemPublicID=i.PublicID
	left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` where _PARTITIONTIME={partition_date}) j
		on j.ID = pp.JobID
where 1=1
	AND e.ItemPublicID is null
	AND j.JobNumber is not null	
	AND(	pp.EditEffectiveDate >= COALESCE(i.EffectiveDate,pp.PeriodStart)
			AND pp.EditEffectiveDate <  COALESCE(i.ExpirationDate,pp.PeriodEnd) 
		)	
	--and j.JobNumber='4282790'
	--and pp.policynumber ='24-035290'
	--and j.JobNumber='8408118'
	--AND pc_policyperiod.policynumber =@policynumber	--Testing	
