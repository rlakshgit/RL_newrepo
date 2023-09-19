--SELECT '(''' + [LOB] + ''',''' + [PolicyNumber] + ''',' + ISNULL('''' + [Notes] + '''','NULL') + '),'
--	FROM [bi_lookup].[UniquePolicies]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.UniquePolicies`
(
	LOB  STRING, 
	PolicyNumber STRING, 
	Notes STRING
);

MERGE INTO `{project}.{dest_dataset}.UniquePolicies` AS Target 
USING (
	SELECT 'BOP' as LOB,'1004052' as PolicyNumber,'Tourneau' as Notes UNION ALL
	SELECT 'BOP','910908','Tail Policy EPLI' UNION ALL
	SELECT 'JB','1004248','Centurion Policy - for Tradeshows' UNION ALL
	SELECT 'JB','172331','Tourneau'
	) 
AS Source --([LOB], [PolicyNumber], [Notes]) 
ON Target.LOB = Source.LOB
	AND Target.PolicyNumber = Source.PolicyNumber
WHEN MATCHED THEN UPDATE
SET Notes = Source.Notes
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (LOB, PolicyNumber, Notes) 
	VALUES (LOB, PolicyNumber, Notes) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;