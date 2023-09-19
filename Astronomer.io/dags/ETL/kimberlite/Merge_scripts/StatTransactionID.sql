--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [ProductLine] + ''',''' + [PolicyTransactionType] + ''',' + ISNULL('''' + [CancellationReason] + '''','NULL') + ',' + ISNULL('''' + [ISOCancellationReason] + '''','NULL') + ',' + ISNULL('''' + [ISOTransactionID] + '''','NULL') + ',' + ISNULL('''' + [NISSCode] + '''','NULL') + ',' + CAST([IsActive] AS CHAR(1)) + '),'
--	FROM [bief_src].[StatTransactionId] ORDER BY [ID]


CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.StatTransactionId`
(
	ID INT64,
	ProductLine STRING, 
	PolicyTransactionType STRING, 
	CancellationReason STRING, 
	ISOCancellationReason STRING, 
	ISOTransactionID STRING, 
	NISSCode STRING, 
	IsActive INT64	
);

MERGE INTO `{project}.{dest_dataset}.StatTransactionId` AS Target 
USING (
		SELECT 1 as ID, 'PL' as ProductLine,'Cancellation' as PolicyTransactionType,'Add to Homeowners - Convenience' as CancellationReason,'Insured Option'as ISOCancellationReason,'11' as ISOTransactionID,'n/a' as NISSCode,0 as IsActive UNION ALL
		SELECT 2, 'PL','Cancellation','Add to Homeowners - Price','Insured Option','11','n/a',0 UNION ALL
		SELECT 3, 'PL','Cancellation','Cancellation of underlying insurance','Company Option','13','n/a',0 UNION ALL
		SELECT 4, 'PL','Cancellation','Combine Policies','Company Option','13','n/a',1 UNION ALL
		SELECT 5, 'PL','Cancellation','Condemned/unsafe','Company Option','13','n/a',0 UNION ALL
		SELECT 6, 'PL','Cancellation','Courtesy Flat Cancel','Company Option','13','n/a',1 UNION ALL
		SELECT 7, 'PL','Cancellation','Credit and Security Exposure','Company Option','13','n/a',1 UNION ALL
		SELECT 8, 'PL','Cancellation','Credit and Travel Exposure','Company Option','13','n/a',1 UNION ALL
		SELECT 9, 'PL','Cancellation','Criminal conduct by the insured','Company Option','13','n/a',0 UNION ALL
		SELECT 10,'PL','Cancellation','Criminal record','Company Option','13','n/a',1 UNION ALL
		SELECT 11,'PL','Cancellation','Divorce/Break in Relationship','Company Option','13','n/a',1 UNION ALL
		SELECT 12,'PL','Cancellation','Does not meet program or program requirements','Company Option','13','n/a',0 UNION ALL
		SELECT 13,'PL','Cancellation','Does not meet program/product requirements','Company Option','13','n/a',0 UNION ALL
		SELECT 14,'PL','Cancellation','Failure to comply with safety recommendations','Company Option','13','n/a',0 UNION ALL
		SELECT 15,'PL','Cancellation','Failure to comply with terms and conditions','Company Option','13','n/a',0 UNION ALL
		SELECT 16,'PL','Cancellation','Failure to cooperate','Company Option','13','n/a',0 UNION ALL
		SELECT 17,'PL','Cancellation','Fraud','Company Option','13','n/a',0 UNION ALL
		SELECT 18,'PL','Cancellation','Insured Deceased','Insured Option','11','n/a',1 UNION ALL
		SELECT 19,'PL','Cancellation','Insured Request - Coverage Not Needed','Insured Option','11','n/a',1 UNION ALL
		SELECT 20,'PL','Cancellation','Insured Request - Homeowners','Insured Option','11','n/a',1 UNION ALL
		SELECT 21,'PL','Cancellation','Insured Request - Price','Insured Option','11','n/a',1 UNION ALL
		SELECT 22,'PL','Cancellation','Insured Request - Item Returned/Sold','Insured Option','11','n/a',1 UNION ALL
		SELECT 23,'PL','Cancellation','Insured\'s request - (Finance co. nonpay)','Insured Option','11','n/a',1 UNION ALL
		SELECT 24,'PL','Cancellation','Insured\'s request - N.O.C','Insured Option','11','n/a',0 UNION ALL
		SELECT 25,'PL','Cancellation','Loss history','Company Option','13','n/a',1 UNION ALL
		SELECT 26,'PL','Cancellation','Loss of Article','Company Option','13','n/a',1 UNION ALL
		SELECT 27,'PL','Cancellation','Loss of reinsurance','Company Option','13','n/a',0 UNION ALL
		SELECT 28,'PL','Cancellation','Material Change in Risk','Company Option','13','n/a',1 UNION ALL
		SELECT 29,'PL','Cancellation','Misrepresentation','Company Option','13','n/a',1 UNION ALL
		SELECT 30,'PL','Cancellation','Moved Out of Country','Insured Option','11','n/a',1 UNION ALL
		SELECT 31,'PL','Cancellation','No employees/operations','Company Option','13','n/a',0 UNION ALL
		SELECT 32,'PL','Cancellation','No longer eligible for group or program','Company Option','13','n/a',0 UNION ALL
		SELECT 33,'PL','Cancellation','No Reason Given','Insured Option','11','n/a',1 UNION ALL
		SELECT 34,'PL','Cancellation','Non disclosure of losses or underwriting information','Company Option','13','n/a',0 UNION ALL
		SELECT 35,'PL','Cancellation','Non-report of payroll or failure to cooperate','Company Option','13','n/a',0 UNION ALL
		SELECT 36,'PL','Cancellation','Operations characteristics','Company Option','13','n/a',0 UNION ALL
		SELECT 37,'PL','Cancellation','Other','Insured Option','11','n/a',1 UNION ALL
		SELECT 38,'PL','Cancellation','Out of business/sold','Company Option','13','n/a',0 UNION ALL
		SELECT 39,'PL','Cancellation','Participation in wrap-up complete','Company Option','13','n/a',0 UNION ALL
		SELECT 40,'PL','Cancellation','Payment history','Company Option','13','n/a',0 UNION ALL
		SELECT 41,'PL','Cancellation','Payment not received','Non-Payment','12','n/a',1 UNION ALL
		SELECT 42,'PL','Cancellation','Policy Not Taken','Insured Option','11','n/a',1 UNION ALL
		SELECT 43,'PL','Cancellation','Policy not-taken','Insured Option','11','n/a',0 UNION ALL
		SELECT 44,'PL','Cancellation','Policy rewritten','Company Option','13','n/a',0 UNION ALL
		SELECT 45,'PL','Cancellation','Policy Rewritten','Company Option','13','n/a',1 UNION ALL
		SELECT 46,'PL','Cancellation','Policy rewritten (mid-term)','Company Option','13','n/a',0 UNION ALL
		SELECT 47,'PL','Cancellation','Products characteristics','Company Option','13','n/a',0 UNION ALL
		SELECT 48,'PL','Cancellation','Requested coverage/limit not available','Company Option','13','n/a',0 UNION ALL
		SELECT 49,'PL','Cancellation','Requested coverages/limits not available','Company Option','13','n/a',0 UNION ALL
		SELECT 50,'PL','Cancellation','Required information not provided','Company Option','13','n/a',0 UNION ALL
		SELECT 51,'PL','Cancellation','Required information was not provided','Company Option','13','n/a',1 UNION ALL
		SELECT 52,'PL','Cancellation','Substantial change in risk or increase in hazard','Company Option','13','n/a',0 UNION ALL
		SELECT 53,'PL','Cancellation','Suspension or revocation of license or permits','Company Option','13','n/a',0 UNION ALL
		SELECT 54,'PL','Cancellation','Theft of Article','Company Option','13','n/a',1 UNION ALL
		SELECT 55,'PL','Cancellation','Underwriting reasons','Company Option','13','n/a',0 UNION ALL
		SELECT 56,'PL','Cancellation','Vacant; below occupancy limit','Company Option','13','n/a',0 UNION ALL
		SELECT 57,'PL','Cancellation','Violation of health, safety, fire, or codes','Company Option','13','n/a',0 UNION ALL
		SELECT 58,'PL','Submission',NULL,NULL,'18','n/a',1 UNION ALL
		SELECT 59,'PL','Renewal',NULL,NULL,'19','n/a',1 UNION ALL
		SELECT 60,'PL','Rewrite',NULL,NULL,'19','n/a',1 UNION ALL
		SELECT 61,'PL','Reinstatement',NULL,NULL,'17','n/a',1 UNION ALL
		SELECT 62,'PL','Policy Change',NULL,NULL,'15','n/a',1 UNION ALL
		SELECT 63,'PL','Issuance',NULL,NULL,NULL,'n/a',1 UNION ALL
		SELECT 64,'PL','Audit',NULL,NULL,NULL,'n/a',1 UNION ALL
		SELECT 65,'PL','Automatic Underwriting Activity',NULL,NULL,NULL,'n/a',1 UNION ALL
		SELECT 66,'PL','Extended Reporting Period Endorsement',NULL,NULL,NULL,'n/a',1 UNION ALL
		SELECT 67,'CL','Renewal',NULL,NULL,'19','n/a',1 UNION ALL
		SELECT 68,'CL','Rewrite',NULL,NULL,'19','n/a',1 UNION ALL
		SELECT 69,'CL','Submission',NULL,NULL,'18','n/a',1 UNION ALL
		SELECT 70,'CL','Policy Change',NULL,NULL,'15','n/a',1 UNION ALL
		SELECT 71,'CL','Reinstatement',NULL,NULL,'17','n/a',1 UNION ALL
		SELECT 72,'CL','Cancellation','Agency/Agent No Longer Represents JM','Insured','11','N/A',1 UNION ALL
		SELECT 73,'CL','Cancellation','Agency/Agent Request','Insured','11','n/a',1 UNION ALL
		SELECT 74,'CL','Cancellation','Change in Policy Term','Insured','11','n/a',1 UNION ALL
		SELECT 75,'CL','Cancellation','Change in Policy Type','Insured','11','n/a',1 UNION ALL
		SELECT 76,'CL','Cancellation','Competition - Alarm Requirement','Insured','11','n/a',1 UNION ALL
		SELECT 77,'CL','Cancellation','Competition-In-Safe Requirement','Insured','11','n/a',1 UNION ALL
		SELECT 78,'CL','Cancellation','Competition-Other','Insured','11','n/a',1 UNION ALL
		SELECT 79,'CL','Cancellation','Competition-Price','Insured','11','n/a',1 UNION ALL
		SELECT 80,'CL','Cancellation','Competition - Product / Coverage','Insured','11','n/a',1 UNION ALL
		SELECT 81,'CL','Cancellation','Competition-Safe Requirement','Insured','11','n/a',1 UNION ALL
		SELECT 82,'CL','Cancellation','Competition - Self Insure','Insured','11','n/a',1 UNION ALL
		SELECT 83,'CL','Cancellation','Courtesy-Flat Cancel','Insured','11','n/a',1 UNION ALL
		SELECT 84,'CL','Cancellation','Dissatisfaction - Agency/Agent Service','Insured','11','n/a',1 UNION ALL
		SELECT 85,'CL','Cancellation','Dissatisfaction - Claim Service','Insured','11','n/a',1 UNION ALL
		SELECT 86,'CL','Cancellation','Dissatisfaction - JM Service','Insured','11','n/a',1 UNION ALL
		SELECT 87,'CL','Cancellation','Jeweler Deceased','Insured','11','n/a',1 UNION ALL
		SELECT 88,'CL','Cancellation','Known but not listed','Insured','11','n/a',1 UNION ALL
		SELECT 89,'CL','Cancellation','No Reason Given ','Insured','11','n/a',1 UNION ALL
		SELECT 90,'CL','Cancellation','Policy not taken','Insured','11','n/a',1 UNION ALL
		SELECT 91,'CL','Cancellation','Sold-New Owner Eligible/JM','Insured','11','n/a',1 UNION ALL
		SELECT 92,'CL','Cancellation','Sold-New Owner Not Eligible','Insured','11','n/a',1 UNION ALL
		SELECT 93,'CL','Cancellation','Store Closed','Insured','11','n/a',1 UNION ALL
		SELECT 94,'CL','Cancellation','Courtesy Flat Cancel','Carrier','13','n/a',1 UNION ALL
		SELECT 95,'CL','Cancellation','Forms Not Returned','Carrier','13','n/a',1 UNION ALL
		SELECT 96,'CL','Cancellation','Known But Not Listed','Carrier','13','n/a',1 UNION ALL
		SELECT 97,'CL','Cancellation','Material Change in Risk','Carrier','13','n/a',1 UNION ALL
		SELECT 98,'CL','Cancellation','Misrepresentation On Application','Carrier','13','n/a',1 UNION ALL
		SELECT 99,'CL','Cancellation','Required information not provided','Carrier','13','n/a',1 UNION ALL
		SELECT 100,'CL','Cancellation','Unacceptable Risk','Carrier','13','n/a',1 UNION ALL
		SELECT 101,'CL','Cancellation','Underwriting-Alarm Requirement','Carrier','13','n/a',1 UNION ALL
		SELECT 102,'CL','Cancellation','Underwriting-Loss History','Carrier','13','n/a',1 UNION ALL
		SELECT 103,'CL','Cancellation','Underwriting-Safe Requirement','Carrier','13','n/a',1 UNION ALL
		SELECT 104,'CL','Cancellation','Underwriting-Unwilling to Comply','Carrier','13','n/a',1 UNION ALL
		SELECT 105,'CL','Cancellation','Unfavorable Report','Carrier','13','n/a',1 UNION ALL
		SELECT 106,'CL','Cancellation','Non Payment','Non Payment','12','n/a',1 UNION ALL
		SELECT 107,'CL','Audit',NULL,NULL,NULL,NULL,1 UNION ALL
		SELECT 108,'CL','Issuance',NULL,NULL,NULL,NULL,1 UNION ALL
		SELECT 109,'CL','Automatic Underwriting Activity',NULL,NULL,NULL,NULL,1 UNION ALL
		SELECT 110,'CL','Extended Reporting Period Endorsement',NULL,NULL,NULL,NULL,1 UNION ALL
		SELECT 111,'CL','Cancellation','Payment Not Received','Non Payment','12','n/a',1 UNION ALL
		SELECT 112,'CL','Cancellation','Insured Request - Price','Insured','11','n/a',1 UNION ALL
		SELECT 113,'CL','Cancellation','Courtesy Flat Cancel for CL','Insured','11','n/a',1 UNION ALL
		SELECT 114,'CL','Cancellation','No Reason Given','Insured','11','n/a',1 UNION ALL
		SELECT 115,'PL','Cancellation','Other-Carrier','Insured Option','11','n/a',1 UNION ALL
		SELECT 116,'PL','Cancellation','Credit and Other','Company Option','13','n/a',1 UNION ALL
		SELECT 117,'CL','Cancellation','Loss History','Carrier','13','n/a',1 UNION ALL
		SELECT 118,'CL','Cancellation','Underwriting - In Safe Requirement','Carrier','13','n/a',1 UNION ALL
		SELECT 119,'CL','Cancellation','Conditional Extension - Forms not Returned','Carrier','13','n/a',1 UNION ALL
		SELECT 120,'CL','Cancellation','Other','Insured','11','n/a',1 UNION ALL
		SELECT 121,'CL','Cancellation','Misrepresentation','Insured','11','n/a',1 UNION ALL
		SELECT 122,'CL','Cancellation','Insured Deceased','Insured','11','n/a',1 UNION ALL
		SELECT 123,'CL','Cancellation','Known But Not Listed-Co','Carrier','13','n/a',1 UNION ALL
		SELECT 124,'PL','Cancellation','Insured Request - Price Not Competitive','Insured','11','n/a',1 UNION ALL
		SELECT 125,'PL','Cancellation','Insured Request - Took Coverage Elsewhere','Insured','11','n/a',1 UNION ALL
		SELECT 126,'PL','Cancellation','Item Value Adjustment','Insured','11','n/a',1
	) 
AS Source --([ID],[ProductLine], [PolicyTransactionType], [CancellationReason], [ISOCancellationReason], [ISOTransactionID], [NISSCode], [IsActive]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET ProductLine = Source.ProductLine
	,PolicyTransactionType = Source.PolicyTransactionType
	,CancellationReason = Source.CancellationReason
	,ISOCancellationReason = Source.ISOCancellationReason
	,ISOTransactionID = Source.ISOTransactionID
	,NISSCode = Source.NISSCode
	,IsActive = Source.IsActive
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,ProductLine, PolicyTransactionType, CancellationReason, ISOCancellationReason, ISOTransactionID, NISSCode, IsActive) 
	VALUES (ID,ProductLine, PolicyTransactionType, CancellationReason, ISOCancellationReason, ISOTransactionID, NISSCode, IsActive) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;