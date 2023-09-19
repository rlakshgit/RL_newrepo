CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ASL`
(
	ID  INT64, 
	pasASLIdentity  STRING, 
	pcaAnnualStatement STRING, 
	gwASLSSID STRING, 
	AnnualStatementLine STRING, 
	ASLDescription  STRING
);

MERGE INTO `{project}.{dest_dataset}.ASL` AS Target 
USING ( SELECT 1 as ID, '1' as pasASLIdentity,'090' as pcaAnnualStatement,'90' as gwASLSSID,'090' as AnnualStatementLine,'Inland Marine' as ASLDescription UNION ALL
		SELECT 2, '2','051','51','051','CMP Property' UNION ALL
		SELECT 3, '3','052','52','052','CMP Liability' UNION ALL
		SELECT 4, '4','080','80','080','Ocean Marine'
	) 
Source --(ID, pasASLIdentity, pcaAnnualStatement, gwASLSSID, AnnualStatementLine, ASLDescription) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET pasASLIdentity = Source.pasASLIdentity
	,pcaAnnualStatement = Source.pcaAnnualStatement
	,gwASLSSID = Source.gwASLSSID
	,AnnualStatementLine = Source.AnnualStatementLine
	,ASLDescription = Source.ASLDescription
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID, pasASLIdentity, pcaAnnualStatement, gwASLSSID, AnnualStatementLine, ASLDescription) 
	VALUES (ID, pasASLIdentity, pcaAnnualStatement, gwASLSSID, AnnualStatementLine, ASLDescription) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;