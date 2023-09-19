--unit test w/ Dupe records
select 
	'DUPE' as UnitTest
	, pp.PolicyNumber
	, j.JobNumber 
	, e.ItemPublicID
	, e.ItemNumber
	,Count(1) as NumRecords
    , DATE('{date}') AS bq_load_date	
from 
	(SELECT * FROM  `{project}.{dest_dataset}.RiskJewelryItem` WHERE bq_load_date=DATE({partition_date})) e
	left outer join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME={partition_date}) pp
		on pp.Publicid = e.PolicyPeriodPublicID
	left outer join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME={partition_date}) j
		on j.ID = pp.JobID
where 1=1
	--and j.JobNumber='4282790'
	--and pp.policynumber ='24-035290'
group by 
	e.ItemPublicID,pp.PolicyNumber,j.JobNumber,e.ItemNumber
having count(1)>1