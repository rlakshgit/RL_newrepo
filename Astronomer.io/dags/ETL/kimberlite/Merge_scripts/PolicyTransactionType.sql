--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [SourceSystem] + ''',''' + [SourcePolicyTransactionCode] + ''',''' + [SourcePolicyTransactionDesc] + ''',' + ISNULL('''' + [ConformedPolicyTransactionCode] + '''','NULL') + ',''' + [ConformedPolicyTransactionDesc] + '''),'
--	FROM [bief_src].[PolicyTransactionType]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.PolicyTransactionType`
(
	ID INT64,
	SourceSystem STRING, 
	SourcePolicyTransactionCode STRING, 
	SourcePolicyTransactionDesc STRING, 
	ConformedPolicyTransactionCode STRING, 
	ConformedPolicyTransactionDesc STRING
);

MERGE INTO `{project}.{dest_dataset}.PolicyTransactionType` AS Target 
USING (
		SELECT 1 as ID, 'GW' as SourceSystem,'Audit' as SourcePolicyTransactionCode,'Audit' as SourcePolicyTransactionDesc,'AU' as ConformedPolicyTransactionCode,'Audit' as ConformedPolicyTransactionDesc UNION ALL
		SELECT 2, 'GW','Cancellation','Cancellation','CN','Cancellation' UNION ALL
		SELECT 3, 'GW','Issuance','Issuance','IS','Issuance' UNION ALL
		SELECT 4, 'GW','PolicyChange','Policy Change','PC','Policy Change' UNION ALL
		SELECT 5, 'GW','Reinstatement','Reinstatement','RE','Reinstatement' UNION ALL
		SELECT 6, 'GW','Renewal','Renewal','RN','Renewal' UNION ALL
		SELECT 7, 'GW','Rewrite','Rewrite','RW','Rewrite' UNION ALL
		SELECT 8, 'GW','RewriteNewAccount','Rewrite New Account','RW','Rewrite' UNION ALL
		SELECT 9, 'GW','Submission','Submission','SU','Submission' UNION ALL
		SELECT 10,'PAS','NB','New Business','SU','Submission' UNION ALL
		SELECT 11,'PAS','RE','Reinstatement','RE','Reinstatement' UNION ALL
		SELECT 12,'PAS','ER','Extended Reporting Period Endorsement','ER','Extended Reporting Period Endorsement' UNION ALL
		SELECT 13,'PAS','CN','Cancellation','CN','Cancellation' UNION ALL
		SELECT 14,'PAS','RN','Renewal','RN','Renewal' UNION ALL
		SELECT 15,'PAS','RW','Rewrite','RW','Rewrite' UNION ALL
		SELECT 16,'PAS','EN','Endorsement','PC','Policy Change' UNION ALL
		SELECT 17,'PCA','6','Cancellation                                 ','CN','Cancellation' UNION ALL
		SELECT 18,'PCA','1','Correction SELECT change without a dec page)       ','PC','Policy Change' UNION ALL
		SELECT 19,'PCA','2','Change SELECT endorsement)                         ','PC','Policy Change' UNION ALL
		SELECT 20,'PCA','0','Reinstatement                                ','RE','Reinstatement' UNION ALL
		SELECT 21,'PCA','4','New Business                                 ','SU','Submission' UNION ALL
		SELECT 22,'PCA','80','Renew or Do Not Renew                        ','RN','Renewal' UNION ALL
		SELECT 23,'PCA','8','Renew or Do Not Renew                        ','RN','Renewal' UNION ALL
		SELECT 24,'PCA','36','Automatic Underwriting Activity','36','Automatic Underwriting Activity' UNION ALL
		SELECT 25,'PCA','34','Automatic Underwriting Activity','34','Automatic Underwriting Activity' UNION ALL
		SELECT 26,'PCA','35','Automatic Underwriting Activity','35','Automatic Underwriting Activity' UNION ALL
		SELECT 27,'PCA','32','Automatic Underwriting Activity','32','Automatic Underwriting Activity' UNION ALL
		SELECT 28,'PCA','31','Automatic Underwriting Activity','31','Automatic Underwriting Activity' UNION ALL
		SELECT 29,'PCA','33','Automatic Underwriting Activity','33','Automatic Underwriting Activity' UNION ALL
		SELECT 30,'PCA','43','Automatic Underwriting Activity','43','Automatic Underwriting Activity' UNION ALL
		SELECT 31,'PCA','40','Automatic Underwriting Activity','40','Automatic Underwriting Activity' UNION ALL
		SELECT 32,'GW','Job','Job',NULL,'Job'
	) 
AS Source --([ID],[SourceSystem], [SourcePolicyTransactionCode], [SourcePolicyTransactionDesc], [ConformedPolicyTransactionCode], [ConformedPolicyTransactionDesc]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET SourceSystem = Source.SourceSystem
	,SourcePolicyTransactionCode = Source.SourcePolicyTransactionCode
	,SourcePolicyTransactionDesc = Source.SourcePolicyTransactionDesc
	,ConformedPolicyTransactionCode = Source.ConformedPolicyTransactionCode
	,ConformedPolicyTransactionDesc = Source.ConformedPolicyTransactionDesc
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,SourceSystem, SourcePolicyTransactionCode, SourcePolicyTransactionDesc, ConformedPolicyTransactionCode, ConformedPolicyTransactionDesc) 
	VALUES (ID,SourceSystem, SourcePolicyTransactionCode, SourcePolicyTransactionDesc, ConformedPolicyTransactionCode, ConformedPolicyTransactionDesc) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;