--SELECT '(' + ISNULL('''' + [LegacyItemClassCode] + '''','NULL') + ',' +  ISNULL('''' + [LegacyDescription] + '''','NULL') + ',' +  ISNULL('''' + [gwItemClassCode] + '''','NULL') + ',' + ISNULL('''' + [gwDescription] + '''','NULL') + '),'
--	FROM [bief_src].[ItemClassCrosswalk] ORDER BY [ID]
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ItemClassCrosswalk`
(
	ID  INT64, 
	LegacyItemClassCode STRING, 
	LegacyDescription STRING, 
	gwItemClassCode STRING, 
	gwDescription STRING
);

MERGE INTO `{project}.{dest_dataset}.ItemClassCrosswalk` AS Target 
USING (
		SELECT 1 AS ID, 'BR' as LegacyItemClassCode,'Bracelet' as LegacyDescription,'Brac' as gwItemClassCode,'Bracelet' as gwDescription UNION ALL
		SELECT 2, 'CH','Chain','Chain','Chain' UNION ALL
		SELECT 3, 'CO','Combination (1 valu for 2 pcs)','Combination','Combination' UNION ALL
		SELECT 4, 'P','Pendant','Combination','Combination' UNION ALL
		SELECT 5, 'ER','Engagement/Wedding Ring Set','EngRg','Engagement Ring' UNION ALL
		SELECT 6, 'ERG','Earrings','pairearrings','Pair Earrings' UNION ALL
		SELECT 7, 'GR','Gent\'s Ring','GentsRg','Gents Ring' UNION ALL
		SELECT 8, 'GW','Gent\'s Watch','GentsWatch','Gents Watch' UNION ALL
		SELECT 9, 'GWR','Gent\'s Watch Rolex','GentsWatch','Gents Watch' UNION ALL
		SELECT 10,'JE','Jewelery','other','Jewelery/Other' UNION ALL
		SELECT 11,'O','Other (Hair, Belt, Keys, etc)','other','Jewelery/Other' UNION ALL
		SELECT 12,'LR','Ladies Ring','LadiesRg','Ladies Ring' UNION ALL
		SELECT 13,'FR','Fashion / Cluster/ Dinner Ring','LadiesRg','Ladies Ring' UNION ALL
		SELECT 14,'LW','Ladies Watch','LadiesWatch','Ladies Watch' UNION ALL
		SELECT 15,'LWR','Ladies Watch Rolex','LadiesWatch','Ladies Watch' UNION ALL
		SELECT 16,'LS','Loose Stones','LooseStone','Loose Stone' UNION ALL
		SELECT 17,'N','Necklace','Necklace','Necklace'
	) 
AS Source --([ID], [LegacyItemClassCode], [LegacyDescription], [gwItemClassCode], [gwDescription]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET LegacyItemClassCode = Source.LegacyItemClassCode
	,LegacyDescription = Source.LegacyDescription
	,gwItemClassCode = Source.gwItemClassCode
	,gwDescription = Source.gwDescription
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID, LegacyItemClassCode, LegacyDescription, gwItemClassCode, gwDescription) 
	VALUES (ID, LegacyItemClassCode, LegacyDescription, gwItemClassCode, gwDescription) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;