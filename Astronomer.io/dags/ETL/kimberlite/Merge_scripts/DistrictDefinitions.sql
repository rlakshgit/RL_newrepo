--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [DistrictName] + ''',' +  ISNULL('''' + [ZipCode] + '''','NULL') + ',' +  ISNULL('''' + CAST([pasRiskUnitNumber] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([gwRiskUnitNumber] AS VARCHAR(10)) + '''','NULL') + '),'
--	FROM [bief_src].[DistrictDefinitions]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.DistrictDefinitions`
(
	ID  INT64, 
	DistrictName STRING, 
	ZipCode STRING, 
	pasRiskUnitNumber STRING, 
	gwRiskUnitNumber STRING
);

MERGE INTO `{project}.{dest_dataset}.DistrictDefinitions` AS Target 
USING (
		SELECT 1 as ID, 'NYC Jlrs District ' as DistrictName,'10036' as ZipCode,NULL as pasRiskUnitNumber,NULL as gwRiskUnitNumber UNION ALL
		SELECT 2, 'NYC Jlrs District ',NULL,'20366',NULL UNION ALL
		SELECT 3, 'NYC Jlrs District ',NULL,'301',NULL UNION ALL
		SELECT 4, 'NYC Jlrs District ',NULL,'20792',NULL UNION ALL
		SELECT 5, 'NYC Jlrs District ',NULL,'148666','92' UNION ALL
		SELECT 6, 'NYC Jlrs District ',NULL,'756',NULL UNION ALL
		SELECT 7, 'NYC Jlrs District ',NULL,'89','72' UNION ALL
		SELECT 8, 'Chicago Jlrs District','60602',NULL,NULL UNION ALL
		SELECT 9, 'Chicago Jlrs District','60603',NULL,NULL UNION ALL
		SELECT 10,'LA Jlrs District','90013',NULL,NULL UNION ALL
		SELECT 11,'LA Jlrs District','90014',NULL,NULL UNION ALL
		SELECT 12,'LA Jlrs District','90017',NULL,NULL
	) 
AS Source --([ID],[DistrictName], [ZipCode], [pasRiskUnitNumber], [gwRiskUnitNumber]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET DistrictName = Source.DistrictName
	,ZipCode = Source.ZipCode
	,pasRiskUnitNumber = Source.pasRiskUnitNumber
	,gwRiskUnitNumber = Source.gwRiskUnitNumber
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,DistrictName, ZipCode, pasRiskUnitNumber, gwRiskUnitNumber) 
	VALUES (ID,DistrictName, ZipCode, pasRiskUnitNumber, gwRiskUnitNumber) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;