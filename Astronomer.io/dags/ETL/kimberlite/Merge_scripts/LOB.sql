--Populate [bief_src].[LOB] Lookup Table
--SELECT '(' + CAST([ID] AS VARCHAR(5)) + ',''' + [System] + ''',''' + [SystemCode] + ''',' + ISNULL('''' + [SystemOfferingCode] + '''','NULL') + ',''' + [SystemLOB] + ''',''' + [SystemProductLineCode] + ''',''' + [ProductLineDescription] + ''',''' + [LOB] + ''',''' + [LOB Description] + '''),'
--  FROM [bief_src].[LOB]
--	ORDER BY ID

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.LOB`
(
	ID INT64,
	System STRING, 
	SystemCode STRING, 
	SystemOfferingCode STRING, 
	SystemLOB STRING, 
	SystemProductLineCode STRING, 
	ProductLineDescription STRING, 
	LOB STRING, 
	LOBDescription STRING
);

MERGE INTO `{project}.{dest_dataset}.LOB` AS Target 
USING (
		SELECT 1 as ID, 'PCA' as System,'20' as SystemCode,NULL as SystemOfferingCode,'Commercial Property' as SystemLOB,'CL' as SystemProductLineCode,'Commercial' as ProductLineDescription,'CPP' as LOB,'Combination Package Policy' as LOBDescription UNION ALL
		SELECT 2, 'PCA','28',NULL,'Jewelers Standard','CL','Commercial','JS','Jewelers Standard' UNION ALL
		SELECT 3, 'PCA','36',NULL,'General Liability','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 4, 'PCA','47',NULL,'Umbrella','CL','Commercial','UMB','Umbrella' UNION ALL
		SELECT 5, 'PCA','52',NULL,'Crime','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 6, 'PCA','60',NULL,'Glass and Sign','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 7, 'PCA','68',NULL,'Personal Jewelry','PL','Personal','PJ','Personal Jewelry' UNION ALL
		SELECT 8, 'PCA','70',NULL,'Commercial Inland Ma','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 9, 'PCA','75',NULL,'Businessowners','CL','Commercial','BOP','Businessowners' UNION ALL
		SELECT 10,'PCA','76',NULL,'Businessowners','CL','Commercial','BOP','Businessowners' UNION ALL
		SELECT 11,'PCA','79',NULL,'Mail Shipment','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 12,'PCA','80',NULL,'Jewelers Block','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 13,'PCA','81',NULL,'Jewelers Block','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 14,'PCA','90',NULL,'Centurion','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 15,'PCA','91',NULL,'Weather Event','CL','Commercial','WPE','Weather Event' UNION ALL
		SELECT 16,'PAS','BOP',NULL,'Businessowners','CL','Commercial','BOP','Businessowners' UNION ALL
		SELECT 17,'PAS','CPP',NULL,'Combination Package ','CL','Commercial','CPP','Combination Package Policy' UNION ALL
		SELECT 18,'PAS','JB',NULL,'Jewelers Block','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 19,'PAS','JS',NULL,'Jewelers Standard','CL','Commercial','JS','Jewelers Standard' UNION ALL
		SELECT 20,'PAS','PJ',NULL,'Personal Jewelry','PL','Personal','PJ','Personal Jewelry' UNION ALL
		SELECT 21,'PAS','UMB',NULL,'Umbrella','CL','Commercial','UMB','Umbrella' UNION ALL
		SELECT 22,'GW','PersonalJewelryLine_JMIC_PL',NULL,'Personal Jewelry','PL','Personal','PJ','Personal Jewelry' UNION ALL
		SELECT 23,'GW','BusinessOwnersLine',NULL,'Businessowners','CL','Commercial','BOP','Businessowners' UNION ALL
		SELECT 24,'GW','UmbrellaLine_JMIC',NULL,'Umbrella','CL','Commercial','UMB','Umbrella' UNION ALL
		SELECT 25,'GW','ILMLine_JMIC','JS','Jewelers Standard','CL','Commercial','JS','Jewelers Standard' UNION ALL
		SELECT 26,'GW','JMIC_PL',NULL,'Personal Jewelry','PL','Personal','PJ','Personal Jewelry' UNION ALL
		SELECT 27,'GW','JMIC_CL',NULL,'?','CL','Commercial','XCL','UnknownCommercial' UNION ALL
		SELECT 28,'GW','ILMLine_JMIC','JSP','Jewelers Standard','CL','Commercial','JS','Jewelers Standard' UNION ALL
		SELECT 29,'GW','ILMLine_JMIC','JB','Jewelers Block','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 30,'GW','ILMLine_JMIC','JBP','Jewelers Block','CL','Commercial','JB','Jewelers Block' UNION ALL
		SELECT 31,'GW','ILMLine_JMIC',NULL,'Jewelers Standard','CL','Commercial','JS','Jewelers Standard' UNION ALL
		SELECT 32,'GW','PersonalArtclLine_JM','JPAStandard_JM','Personal Articles','PL','Personal','PA','Personal Articles' UNION ALL
		SELECT 33,'GW','PersonalArtclLine_JM',NULL,'Personal Articles','PL','Personal','PA','Personal Articles' UNION ALL
		SELECT 34,'GW','JPAPersonalArticles',NULL,'Personal Articles','PL','Personal','PA','Personal Articles' UNION ALL
		SELECT 35,'GW','JMICPJLine',NULL,'Personal Jewelry','PL','Personal','PJ','Personal Jewelry'
	) 
AS Source --([ID], [System], [SystemCode], [SystemOfferingCode], [SystemLOB], [SystemProductLineCode], [ProductLineDescription], [LOB], [LOBDescription]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET System = Source.System
	,SystemCode = Source.SystemCode
	,SystemOfferingCode = Source.SystemOfferingCode
	,SystemLOB = Source.SystemLOB
	,SystemProductLineCode = Source.SystemProductLineCode
	,ProductLineDescription = Source.ProductLineDescription
	,LOB = Source.LOB
	,LOBDescription = Source.LOBDescription
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,System, SystemCode, SystemOfferingCode, SystemLOB, SystemProductLineCode, ProductLineDescription, LOB, LOBDescription) 
	VALUES (ID,System, SystemCode, SystemOfferingCode, SystemLOB, SystemProductLineCode, ProductLineDescription, LOB, LOBDescription) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;