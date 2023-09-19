SELECT 'DUPES' as UnitTest, UMBCoverageKey, CoverageLevel, CoveragePublicID, count(*) AS NumRecords, DATE('{date}') AS bq_load_date
from (SELECT * FROM `{project}.{dest_dataset}.CoverageUMB` WHERE bq_load_date=DATE({partition_date}))
--where Jobnumber=ISNULL(@jobnumber,JobNumber)
--and PolicyNumber = ISNULL(@policynumber, PolicyNumber)
group by UMBCoverageKey, CoverageLevel, CoveragePublicID
having count(*)>1 --dupe check