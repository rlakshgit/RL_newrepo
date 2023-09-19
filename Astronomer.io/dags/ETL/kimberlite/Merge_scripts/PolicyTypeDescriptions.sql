--SELECT '(''' + [SourceSystem] + ''',''' + [SourcePolicyTypeCode] + ''',''' + [ConformedPolicyTypeCode] + ''',''' + [ConformedPolicyTypeDescription] + '''),'
--	FROM [bi_lookup].[PolicyTypeDescriptions] ORDER BY [SourceSystem], [SourcePolicyTypeCode]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.PolicyTypeDescriptions`
(
	SourceSystem STRING, 
	SourcePolicyTypeCode STRING, 
	ConformedPolicyTypeCode STRING, 
	ConformedPolicyTypeDescription STRING	
);
MERGE INTO `{project}.{dest_dataset}.PolicyTypeDescriptions` AS Target 
USING (
		SELECT 'GW' as SourceSystem,'BOP' as SourcePolicyTypeCode,'BOP' as ConformedPolicyTypeCode,'Businessowners' as ConformedPolicyTypeDescription UNION ALL
		SELECT 'GW','JB','JB','Jewelers Block' UNION ALL
		SELECT 'GW','JBP','JBP','Jewelers Pak' UNION ALL
		SELECT 'GW','JMIC_CL','CL','Commercial' UNION ALL
		SELECT 'GW','JMIC_PL','PJ','Personal  Jewelry' UNION ALL
		SELECT 'GW','JPAPersonalArticles','JPAS','JPA Personal Articles' UNION ALL
		SELECT 'GW','JPAStandard_JM','JPAS','JPA Standard' UNION ALL
		SELECT 'GW','JS','JS','Jewelers Standard' UNION ALL
		SELECT 'GW','JSP','JSP','Jewelers Standard Pak' UNION ALL
		SELECT 'GW','SEL','SEL','BOP Select' UNION ALL
		SELECT 'PAS','CPP','CPP','Combination Package Policy' UNION ALL
		SELECT 'PAS','CRF','CRF','Craftsman' UNION ALL
		SELECT 'PAS','JB','JB','Jewelers Block' UNION ALL
		SELECT 'PAS','JBP','JBP','Jewelers Pak' UNION ALL
		SELECT 'PAS','JS','JS','Jewelers Standard' UNION ALL
		SELECT 'PAS','JSP','JSP','Jewelers Standard Pak' UNION ALL
		SELECT 'PAS','MJ','MJ','Manufacturing Jewelers Block' UNION ALL
		SELECT 'PAS','MJP','MJP','Manufacturing Jewelers Pak' UNION ALL
		SELECT 'PAS','PJ','PJ','Personal  Jewelry' UNION ALL
		SELECT 'PAS','SBP','BOP','Businessowners' UNION ALL
		SELECT 'PCA','JB','JB','Jewelers Block' UNION ALL
		SELECT 'PCA','JBP','JBP','Jewelers Pak' UNION ALL
		SELECT 'PCA','SBP','BOP','Businessowners'
	) 
AS Source --(SourceSystem, SourcePolicyTypeCode, ConformedPolicyTypeCode, ConformedPolicyTypeDescription) 
ON Target.SourceSystem = Source.SourceSystem
	AND Target.SourcePolicyTypeCode = Source.SourcePolicyTypeCode
WHEN MATCHED THEN UPDATE
SET ConformedPolicyTypeCode = Source.ConformedPolicyTypeCode
	,ConformedPolicyTypeDescription = Source.ConformedPolicyTypeDescription
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (SourceSystem, SourcePolicyTypeCode, ConformedPolicyTypeCode, ConformedPolicyTypeDescription) 
	VALUES (SourceSystem, SourcePolicyTypeCode, ConformedPolicyTypeCode, ConformedPolicyTypeDescription) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;