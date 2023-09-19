--MISSING Risks In Extract
SELECT 'MISSING RISKS' as UnitTest
						, CoverageLevel
						, CoveragePublicID
						, RiskLocationKey
						, DATE('{date}') AS bq_load_date	
from (SELECT * FROM `{project}.{dest_dataset}.CoverageUMB` WHERE bq_load_date=DATE({partition_date})) AS CoverageUMB
inner join (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod 
	on pc_policyperiod.PublicID = CoverageUMB.PolicyPeriodPublicID
WHERE CoverageUMB.RiskLocationKey is null  --all umb coverage should be tied to a risk (primary location)
and CoverageUMB.PolicyPeriodFixedID IS NOT NULL
and pc_policyperiod.Status = 3 --'Bound'
