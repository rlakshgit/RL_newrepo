--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [ProductLine] + ''',' +  ISNULL('''' + [GWClosedClaimOutcome] + '''','NULL') + ',' +  ISNULL('''' + [PASCWPCode] + '''','NULL') + ',' + ISNULL('''' + [PASCWPDescription] + '''','NULL') + ',' +  ISNULL('''' + [ConformedClosedClaimOutcome] + '''','NULL') + '),'
--	FROM [bief_src].[ClosedClaimOutcome]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ClosedClaimOutcome`
(
	ID INT64,
	ProductLine STRING, 
	GWClosedClaimOutcome STRING, 
	PASCWPCode STRING, 
	PASCWPDescription STRING, 
	ConformedClosedClaimOutcome STRING
);

MERGE INTO `{project}.{dest_dataset}.ClosedClaimOutcome` AS Target 
USING (
		SELECT 1 as ID, 'PL' as ProductLine,'Claim denied by Jewelers Mutual' as GWClosedClaimOutcome,'CD' as PASCWPCode,'Claim denied by Jewelers Mutual' as PASCWPDescription,'Claim denied by Jewelers Mutual' as ConformedClosedClaimOutcome UNION ALL
		SELECT 2, 'PL','Claim under the deductible','UND','Claim Under the Deductible','Claim under the deductible' UNION ALL
		SELECT 3, 'PL','Completed',NULL,NULL,'Completed' UNION ALL
		SELECT 4, 'PL','Duplicate',NULL,NULL,'Duplicate' UNION ALL
		SELECT 5, 'PL','Fraud',NULL,NULL,'Fraud' UNION ALL
		SELECT 6, 'PL','Insured did not pursue','DNP','Insured did not pursue','Insured did not pursue' UNION ALL
		SELECT 7, 'PL','Insured withdrew','WDR','Insured withdrew','Insured withdrew' UNION ALL
		SELECT 8, 'PL','Jeweler did not submit invoice','JNI','Jeweler did not submit invoice','Jeweler did not submit invoice' UNION ALL
		SELECT 9, 'PL','Jeweler repaired at no charge','JNC','Jeweler repaired at no charge','Jeweler repaired at no charge' UNION ALL
		SELECT 10,'PL','Miscellaneous','MIS','Miscellaneous','Miscellaneous' UNION ALL
		SELECT 11,'PL','Mistake',NULL,NULL,'Mistake' UNION ALL
		SELECT 12,'PL','No coverage','NC','No coverage','No coverage' UNION ALL
		SELECT 13,'PL','Payments Complete',NULL,NULL,'Payments Complete' UNION ALL
		SELECT 14,'PL','Recovery','REC','Recovery','Recovery' UNION ALL
		SELECT 15,'PL','Rescission',NULL,NULL,'Rescission' UNION ALL
		SELECT 16,'PL','Settlement','SET','Settlement','Settlement' UNION ALL
		SELECT 17,'PL',NULL,NULL,'Closed without payment','Closed without payment SELECT PAS)' UNION ALL
		SELECT 18,'CL',NULL,NULL,'No response from insd for supporting doc','Completed' UNION ALL
		SELECT 19,'CL',NULL,NULL,'Claim denied by Jewelers Mutual','Insured did not pursue' UNION ALL
		SELECT 20,'CL',NULL,NULL,'Claim paid by 3rd part SELECT i.e. landlord)','Completed' UNION ALL
		SELECT 21,'CL',NULL,NULL,'Damages under the policy deductible','Claim Under deductible' UNION ALL
		SELECT 22,'CL',NULL,NULL,'Insd requested the claim be withdrawn','Insured did not pursue' UNION ALL
		SELECT 23,'CL',NULL,NULL,'No response from clmt/attny for supp doc','Completed'
	) 
AS Source --([ID],[ProductLine], [GWClosedClaimOutcome], [PASCWPCode], [PASCWPDescription], [ConformedClosedClaimOutcome]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET ProductLine = Source.ProductLine
	,GWClosedClaimOutcome = Source.GWClosedClaimOutcome
	,PASCWPCode = Source.PASCWPCode
	,PASCWPDescription = Source.PASCWPDescription
	,ConformedClosedClaimOutcome = Source.ConformedClosedClaimOutcome
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,ProductLine, GWClosedClaimOutcome, PASCWPCode, PASCWPDescription, ConformedClosedClaimOutcome) 
	VALUES (ID,ProductLine, GWClosedClaimOutcome, PASCWPCode, PASCWPDescription, ConformedClosedClaimOutcome) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;