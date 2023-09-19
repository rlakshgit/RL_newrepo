
			SELECT 'DUPES' as UnitTest,BOPCoverageKey, CoverageLevel, CoveragePublicID, count(*) AS NumRecords, DATE('{date}') AS bq_load_date 
			from (SELECT * FROM `{project}.{dest_dataset}.CoverageBOP` WHERE bq_load_date=DATE({partition_date}))
			--where Policynumber=@policynumber
			--and CoverageRank=1
			group by BOPCoverageKey, CoverageLevel, CoveragePublicID
			having count(*)>1 --dupe check