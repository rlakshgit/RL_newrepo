--SELECT '(' + CAST([LookupKey] AS VARCHAR(5)) + ',''' + [LookupType] + ''',''' + [LookupCode] + ''',''' + [LookupDesc] + ''',' + ISNULL('''' + [LookupDesc1] + '''','NULL') + ',' + ISNULL('''' + [LookupDesc2] + '''','NULL') + ',' + ISNULL('''' + [LookupDesc3] + '''','NULL') + ',' + ISNULL('''' + [LookupDesc4] + '''','NULL') + ',' + ISNULL('''' + [LookupDesc5] + '''','NULL') + ',' + ISNULL('''' + [LookupDesc6] + '''','NULL') + ',' + ISNULL(CAST([LookupSeq] AS VARCHAR(10)),'NULL') + ',' +  + CAST([LookupIsActive] AS VARCHAR(10)) + ',''' + CAST([ModifiedDate] AS VARCHAR(50)) + ''',''' + CAST([StartDate] AS VARCHAR(50)) + ''',''' + CAST([EndDate] AS VARCHAR(50)) + '''),'
--  FROM [bi_lookup].[Lookup] ORDER BY [LookupKey]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.Lookup`
(	
	LookupKey INT64, 
	LookupType STRING, 
	LookupCode STRING, 
	LookupDesc STRING, 
	LookupDesc1 STRING, 
	LookupDesc2 STRING, 
	LookupDesc3 STRING, 
	LookupDesc4 STRING, 
	LookupDesc5 STRING, 
	LookupDesc6 STRING, 
	LookupSeq INT64, 
	LookupIsActive INT64, 
	ModifiedDate STRING, 
	StartDate STRING, 
	EndDate STRING
);

--SET IDENTITY_INSERT `{project}.{dest_dataset}.Lookup` ON

MERGE INTO `{project}.{dest_dataset}.Lookup` AS Target 
USING (
		SELECT 1 as LookupKey,'PCA TRANCD' as LookupType,'01' as LookupCode,'Reserve' as LookupDesc,'Open' as LookupDesc1,'Loss' as LookupDesc2,'R' as LookupDesc3,cast(NULL as string) as LookupDesc4,cast(NULL as string) as LookupDesc5,cast(NULL as string) as LookupDesc6,1 as LookupSeq,1 as LookupIsActive,'2011-03-18 00:00:00.0000000' as ModifiedDate,'1900-01-01 00:00:00.0000000' as StartDate,'8900-12-31 00:00:00.0000000' as EndDate UNION ALL
		SELECT 2,'PCA TRANCD','11','Reserve','ReOpen','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 3,'PCA TRANCD','21','Reserve','Change','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 4,'PCA TRANCD','30','Reserve','Close','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 5,'PCA TRANCD','31','Reserve','Close','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 6,'PCA TRANCD','02','Reserve','Open','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 7,'PCA TRANCD','12','Reserve','ReOpen','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 8,'PCA TRANCD','22','Reserve','Change','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 9,'PCA TRANCD','32','Reserve','Close','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 10,'PCA TRANCD','03','Reserve','Open','Salvage','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 11,'PCA TRANCD','13','Reserve','ReOpen','Salvage','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 12,'PCA TRANCD','23','Reserve','Change','Salvage','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 13,'PCA TRANCD','33','Reserve','Close','Salvage','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 14,'PCA TRANCD','04','Reserve','Open','Subrogation','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 15,'PCA TRANCD','14','Reserve','ReOpen','Subrogation','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 16,'PCA TRANCD','24','Reserve','Change','Subrogation','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 17,'PCA TRANCD','34','Reserve','Close','Subrogation','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 18,'PCA TRANCD','05','Reserve','Open','Contrib','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 19,'PCA TRANCD','15','Reserve','ReOpen','Contrib','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 20,'PCA TRANCD','25','Reserve','Change','Contrib','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 21,'PCA TRANCD','35','Reserve','Close','Contrib','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 22,'PCA TRANCD','41','Payment','Partial','Loss','P',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 23,'PCA TRANCD','51','Payment','Final','Loss','P',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 24,'PCA TRANCD','61','Payment','After Close','Loss','P',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 25,'PCA TRANCD','42','Payment','Partial','Expense','P','DCC','Medical',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 26,'PCA TRANCD','43','Payment','Partial','Expense','P','DCC','Expert',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 27,'PCA TRANCD','44','Payment','Partial','Expense','P','DCC','Court Reporter',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 28,'PCA TRANCD','45','Payment','Partial','Expense','P','DCC','Legal',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 29,'PCA TRANCD','46','Payment','Partial','Expense','P','AO','Adjuster',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 30,'PCA TRANCD','47','Payment','Partial','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 31,'PCA TRANCD','48','Payment','Partial','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 32,'PCA TRANCD','52','Payment','Final','Expense','P','DCC','Medical',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 33,'PCA TRANCD','53','Payment','Final','Expense','P','DCC','Expert',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 34,'PCA TRANCD','54','Payment','Final','Expense','P','DCC','Court Reporter',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 35,'PCA TRANCD','55','Payment','Final','Expense','P','DCC','Legal',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 36,'PCA TRANCD','56','Payment','Final','Expense','P','AO','Adjuster',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 37,'PCA TRANCD','57','Payment','Final','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 38,'PCA TRANCD','58','Payment','Final','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 39,'PCA TRANCD','62','Payment','After Close','Expense','P','DCC','Medical',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 40,'PCA TRANCD','63','Payment','After Close','Expense','P','DCC','Expert',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 41,'PCA TRANCD','64','Payment','After Close','Expense','P','DCC','Court Reporter',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 42,'PCA TRANCD','65','Payment','After Close','Expense','P','DCC','Legal',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 43,'PCA TRANCD','66','Payment','After Close','Expense','P','AO','Adjuster',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 44,'PCA TRANCD','67','Payment','After Close','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 45,'PCA TRANCD','68','Payment','After Close','Expense','P','DCC','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 46,'PCA TRANCD','71','Recovery','Partial','Salvage','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 47,'PCA TRANCD','81','Recovery','Final','Salvage','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 48,'PCA TRANCD','91','Recovery','Partial','Salvage','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 49,'PCA TRANCD','72','Recovery','Partial','Subrogation','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 50,'PCA TRANCD','82','Recovery','Final','Subrogation','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 51,'PCA TRANCD','92','Recovery','Partial','Subrogation','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 52,'PCA TRANCD','73','Recovery','Partial','Contrib','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 53,'PCA TRANCD','83','Recovery','Final','Contrib','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 54,'PCA TRANCD','93','Recovery','Partial','Contrib','E',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 55,'PCA TRANCD','06','Ceded Reserve','Open','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 56,'PCA TRANCD','16','Ceded Reserve','ReOpen','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 57,'PCA TRANCD','26','Ceded Reserve','Change','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 58,'PCA TRANCD','36','Ceded Reserve','Close','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 59,'PCA TRANCD','07','Reinsurance','Open','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 60,'PCA TRANCD','17','Reinsurance','ReOpen','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 61,'PCA TRANCD','27','Reinsurance','Change','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 62,'PCA TRANCD','37','Reinsurance','Close','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 63,'PCA TRANCD','96','Reinsurance','Partial','Recievable','R','SAL',cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 64,'PCA TRANCD','97','Reinsurance','Final','Recievable','R','SUB',cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 65,'PCA TRANCD','98','Reinsurance','Final','Recievable','R','CONTRIB',cast(NULL as string),cast(NULL as string),1,0,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 66,'PCA TRANCD','74','Ceded Payment','Partial','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 67,'PCA TRANCD','84','Ceded Payment','Final','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 68,'PCA TRANCD','94','Ceded Payment','After Close','Loss','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 69,'PCA TRANCD','75','Ceded Payment','Partial','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 70,'PCA TRANCD','85','Ceded Payment','Final','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 71,'PCA TRANCD','95','Ceded Payment','After Close','Expense','R',cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 72,'PAS PAYMENTTYPE','ADJ','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Adjuster',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 73,'PAS PAYMENTTYPE','LGL','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Legal',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 74,'PAS PAYMENTTYPE','MSC','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Misc',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 75,'PAS PAYMENTTYPE','POL','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Police Report',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 76,'PAS PAYMENTTYPE','IND','Indemnity',cast(NULL as string),cast(NULL as string),cast(NULL as string),'Indemnity','Indemnity',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 77,'PAS PAYMENTTYPE','CRT','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Court Reporter',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 78,'PAS PAYMENTTYPE','EXW','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','Expert',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 79,'PAS PAYMENTTYPE','CPA','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'LAE','CPA',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 80,'PAS PAYMENTTYPE','MED','Expense',cast(NULL as string),cast(NULL as string),cast(NULL as string),'AO','Medical',cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 81,'PAS RECOVERYTYPE','DEFAULT','Recovery','Subrogation','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 82,'PAS RECOVERYTYPE','DED','Recovery','Deductible Recovery','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 83,'PAS RECOVERYTYPE','OVP','Recovery','Overpayment','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 84,'PAS RECOVERYTYPE','REI','Recovery','Reinsurance Recovery','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 85,'PAS RECOVERYTYPE','RES','Recovery','Restitution','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 86,'PAS RECOVERYTYPE','SAL','Recovery','Salvage','Salvage',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 87,'PAS RECOVERYTYPE','SBR','Recovery','Subrogation','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 88,'PAS RECOVERYTYPE','TPR','Recovery','Third Party Recovery (Other)','Subrogation',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-03-18 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 89,'SOURCE SYSTEM','GW-PC','Guidewire Policy Center','Guidewire','GW',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 90,'SOURCE SYSTEM','GW-BC','Guidewire Policy Center','Guidewire','GW',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 91,'SOURCE SYSTEM','GW-CC','Guidewire Claim Center','Guidewire','GW',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 92,'SOURCE SYSTEM','GW-AB','Guidewire Contact Center','Guidewire','GW',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 93,'SOURCE SYSTEM','PAS','Infinity','Legacy','PAS',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 94,'SOURCE SYSTEM','PCA','AS/400','Legacy','PCA',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),1,1,'2011-09-14 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 95,'PCA POLICY STATUS','P','Purge','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 96,'PCA POLICY STATUS','2','Triggered Renewal Activity','Active',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 97,'PCA POLICY STATUS','4','Underwriting Automatic Activity','Active',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 98,'PAS POLICY STATUS','DR','Declined Rewrite','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 99,'PAS POLICY STATUS','PE','Pending Endorsement Request','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 100,'PAS POLICY STATUS','V','Voided Transaction','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 101,'PAS POLICY STATUS','RE','Pending Reinstatement Request','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 102,'PAS POLICY STATUS','DA','Declined New Business Application','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 103,'PAS POLICY STATUS','PR','Pending Rewrite','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 104,'PAS POLICY STATUS','PC','Pending Renewal Application','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 105,'PAS POLICY STATUS','IN','In-force Policy','Active',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 106,'PAS POLICY STATUS','PA','Pending New Business Application','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 107,'PAS POLICY STATUS','EX','Expired Policy','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 108,'PAS POLICY STATUS','IT','In-force Tail Coverage','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 109,'PAS POLICY STATUS','DE','Declined Endorsement Request','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 110,'PAS POLICY STATUS','DT','Decline Trail','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 111,'PAS POLICY STATUS','CN','Canceled Policy','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 112,'PAS POLICY STATUS','DC','Declined Renewal Application','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 113,'PAS POLICY STATUS','HS','Policy History','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000' UNION ALL
		SELECT 114,'PAS POLICY STATUS','PT','Pending Extended Reporting (Tail) Endorsement','Inactive',cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as string),cast(NULL as INT64),1,'2011-10-11 00:00:00.0000000','1900-01-01 00:00:00.0000000','8900-12-31 00:00:00.0000000'
	) 
AS Source --([LookupKey], [LookupType], [LookupCode], [LookupDesc], [LookupDesc1], [LookupDesc2], [LookupDesc3], [LookupDesc4], [LookupDesc5], [LookupDesc6], [LookupSeq], [LookupIsActive], [ModifiedDate], [StartDate], [EndDate]) 
ON Target.LookupKey = Source.LookupKey 
WHEN MATCHED THEN UPDATE
SET LookupType = Source.LookupType
	,LookupCode = Source.LookupCode
	,LookupDesc = Source.LookupDesc
	,LookupDesc1 = Source.LookupDesc1
	,LookupDesc2 = Source.LookupDesc2
	,LookupDesc3 = Source.LookupDesc3
	,LookupDesc4 = Source.LookupDesc4
	,LookupDesc5 = Source.LookupDesc5
	,LookupDesc6 = Source.LookupDesc6
	,LookupSeq = Source.LookupSeq
	,LookupIsActive = Source.LookupIsActive
	,ModifiedDate = Source.ModifiedDate
	,StartDate = Source.StartDate
	,EndDate = Source.EndDate
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (LookupKey, LookupType, LookupCode, LookupDesc, LookupDesc1, LookupDesc2, LookupDesc3, LookupDesc4, LookupDesc5, LookupDesc6, LookupSeq, LookupIsActive, ModifiedDate, StartDate, EndDate) 
	VALUES (LookupKey, LookupType, LookupCode, LookupDesc, LookupDesc1, LookupDesc2, LookupDesc3, LookupDesc4, LookupDesc5, LookupDesc6, LookupSeq, LookupIsActive, ModifiedDate, StartDate, EndDate) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;

--SET IDENTITY_INSERT [bi_lookup].[Lookup] OFF