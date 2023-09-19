--Populate bief_src.BusinessType Lookup Table
--SELECT '(' + CAST(ID AS VARCHAR(5)) + '''' + BusinessTypeDesc + ''',''' + BusinessSubTypeDesc + '''),' 
--	FROM DW_SOURCE.bief_src.BusinessType

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BusinessType`
(
	ID  INT64, 
	BusinessTypeDesc  STRING, 
	BusinessSubTypeDesc STRING
);

MERGE INTO `{project}.{dest_dataset}.BusinessType` AS Target 
USING ( 
		SELECT 1 as ID, 'Direct' as BusinessTypeDesc,'Policy' as BusinessSubTypeDesc UNION ALL
		SELECT 2, 'Ceded','Policy' UNION ALL
		SELECT 3, 'Ceded','Risk Unit' UNION ALL
		SELECT 4, 'Ceded','Trade Show' 
	) 
AS Source
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET BusinessTypeDesc = Source.BusinessTypeDesc
	,BusinessSubTypeDesc = Source.BusinessSubTypeDesc
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,BusinessTypeDesc,BusinessSubTypeDesc) 
	VALUES (ID,BusinessTypeDesc,BusinessSubTypeDesc) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;