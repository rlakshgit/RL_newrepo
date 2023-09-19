--Populate [bief_src].[DOLStates] Lookup Table, which contains states that are subject to Defense Outside Limit (DOL)

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.DOLStates`
(
	GeographyStateCode STRING
);

-- Reference Data for DOL States
MERGE INTO `{project}.{dest_dataset}.DOLStates` AS Target 
USING (
	SELECT 'AR' as GeographyStateCode UNION ALL
	SELECT 'LA' UNION ALL
	SELECT 'ME' UNION ALL
	SELECT 'MO' UNION ALL
	SELECT 'NM' UNION ALL
	SELECT 'VT' UNION ALL
	SELECT 'WY'
	) 
AS Source --(GeographyStateCode) 
ON Target.GeographyStateCode = Source.GeographyStateCode 
--WHEN MATCHED THEN --Do Nothing
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (GeographyStateCode) 
	VALUES (GeographyStateCode) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;