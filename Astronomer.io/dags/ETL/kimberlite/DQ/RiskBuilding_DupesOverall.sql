
	--DUPES In Extract
		SELECT 'DUPES OVERALL' as UnitTest, RiskBuildingKey, BuildingPublicID, count(*) as NumRecords, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.RiskBuilding` WHERE bq_load_date = DATE({partition_date}))
		--where Policynumber=ISNULL(@policynumber, PolicyNumber)
		--and CoverageRank=1
		group by RiskBuildingKey, BuildingPublicID
		having count(*)>1 --dupe check
