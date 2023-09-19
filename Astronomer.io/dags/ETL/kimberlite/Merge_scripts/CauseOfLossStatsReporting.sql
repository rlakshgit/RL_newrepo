--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [ProductLine] + ''',''' + [ConformedCauseOfLossDesc] + ''',' + ISNULL('''' + CAST([NISS_IM_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([ISO_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([NISS_GL_UMB_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([ISO_GL_UMB_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([NISS_BOP_PROP_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([NISS_BOP_TE_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([NISS_BOP_LIAB_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' +  ISNULL('''' + CAST([ISO_BOP_PROP_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([ISO_BOP_TE_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST( [ISO_BOP_LIAB_StatCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + CAST([IsActive] AS VARCHAR(10)) + '''','NULL') + '),'
--	FROM [bief_src].[CauseOfLossStatsReporting] ORDER BY [ID]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CauseOfLossStatsReporting`
(
	ID INT64,
	ProductLine STRING,
	 ConformedCauseOfLossDesc STRING,
	 NISS_IM_StatCode STRING,
	 ISO_StatCode STRING,
	 NISS_GL_UMB_StatCode STRING,
	 ISO_GL_UMB_StatCode STRING,
	 NISS_BOP_PROP_StatCode STRING,
	 NISS_BOP_TE_StatCode STRING,
	 NISS_BOP_LIAB_StatCode STRING,
	 ISO_BOP_PROP_StatCode STRING,
	 ISO_BOP_TE_StatCode STRING,
	 ISO_BOP_LIAB_StatCode STRING,
	 IsActive STRING	
);

MERGE INTO `{project}.{dest_dataset}.CauseOfLossStatsReporting` AS Target 
USING (
		SELECT 1 AS ID, 'CL' AS ProductLine, 'Crash of airplane' as ConformedCauseOfLossDesc,'4' AS NISS_IM_StatCode,'19' AS ISO_StatCode,'90' AS NISS_GL_UMB_StatCode,'90' AS ISO_GL_UMB_StatCode,'29' AS NISS_BOP_PROP_StatCode,'39' AS NISS_BOP_TE_StatCode,'49' AS NISS_BOP_LIAB_StatCode,'19' AS ISO_BOP_PROP_StatCode,'49' AS ISO_BOP_TE_StatCode,'25' AS ISO_BOP_LIAB_StatCode,'1' AS IsActive UNION ALL
		SELECT 2, 'CL','All other acts of terrorism','A','15','95','90','55','56','95','15','45','95','1' UNION ALL
		SELECT 3, 'CL','All other burglary-closed','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 4, 'CL','All Other Loss Not Defined','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 5, 'CL','Armed Robbery - open','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 6, 'CL','Armed Robbery - W/O Surveillance','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 7, 'CL','Armed robbery from auto','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 8, 'CL','Attended Auto','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 9, 'CL','Breakage','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 10,'CL','Burglary','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 11,'CL','Burglary from Auto','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 12,'CL','Burglary-Safe-on premise','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 13,'CL','Certified Acts of Terrorism','A','15','95','95','55','56','95','15','45','95','1' UNION ALL
		SELECT 14,'CL','Chemical Spills','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 15,'CL','Collapse other than Sinkhole','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 16,'CL','Collision including upset/ overturn','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 17,'CL','Compd Alrm-safe entered non UL clsd - on premise','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 18,'CL','Compd Alrm-safe entered UL apprvd clsd','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 19,'CL','Compd Alrm-safe not entrd non UL clsd - on premise','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 20,'CL','Compd Alrm-safe not entrd UL apprvd clsd','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 21,'CL','Custody of others','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 22,'CL','Custody of others-in transit','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 23,'CL','Customer Property Switch','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 24,'CL','Damaged while being worked on-off prem','9','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 25,'CL','Disappearance-off premises','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 26,'CL','Disappearance-on premises/open','5','07','90','90','29','39','49','07','37','21','1' UNION ALL
		SELECT 27,'CL','Discrimination','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 28,'CL','Earthquake','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 29,'CL','Employee Dishonesty','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 30,'CL','Entd prem not smsh win/dr non UL clsd','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 31,'CL','Entd prem not smsh win/dr UL apprvd clsd','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 32,'CL','EPL - Personal Injury','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 33,'CL','Equipment Breakdown','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 34,'CL','Explosion','7','19','90','90','29','39','49','03','33','25','1' UNION ALL
		SELECT 35,'CL','Fire','1','01','90','90','21','31','49','01','31','21','1' UNION ALL
		SELECT 36,'CL','Fire - From Woodburning Stove','1','01','90','90','21','31','49','01','31','21','1' UNION ALL
		SELECT 37,'CL','Flood','8','08','90','90','26','36','49','08','38','25','1' UNION ALL
		SELECT 38,'CL','Fraudulent Check/Credit Card Transaction','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 39,'CL','Freezing','9','10','90','90','29','39','49','10','40','25','1' UNION ALL
		SELECT 40,'CL','Glass Breakage','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 41,'CL','Grab & Run - open','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 42,'CL','Grab & Run - W/O Surveillance','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 43,'CL','Hail','2','02','90','90','22','32','49','02','32','25','1' UNION ALL
		SELECT 44,'CL','Harassment','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 45,'CL','Liab related to escaped liquid fuel','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 46,'CL','Liab related to lead poisoning','9','19','90','90','29','39','41','19','49','21','1' UNION ALL
		SELECT 47,'CL','Liability-Advertising Injury','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 48,'CL','Liability-All Other','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 49,'CL','Liability-Appraisal','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 50,'CL','Liability-Bodily Injury','9','19','16','16','29','39','41','19','49','21','1' UNION ALL
		SELECT 51,'CL','Liability-Dog Bite','9','19','90','90','29','39','41','19','49','21','1' UNION ALL
		SELECT 52,'CL','Liability-Fire Legal','9','19','90','90','29','39','41','19','49','21','1' UNION ALL
		SELECT 53,'CL','Liability-Mold','9','19','90','90','29','39','41','19','49','21','1' UNION ALL
		SELECT 54,'CL','Liability-Personal Injury','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 55,'CL','Liability-Piercing','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 56,'CL','Liability-Products','9','19','11','11','29','39','42','19','49','25','1' UNION ALL
		SELECT 57,'CL','Liability-Property Damage','9','19','26','26','29','39','41','19','49','21','1' UNION ALL
		SELECT 58,'CL','Lightning','1','01','90','90','21','31','49','01','31','25','1' UNION ALL
		SELECT 59,'CL','Loss at Trade Show-In Jewelers Custody','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 60,'CL','Loss at Trade Show-Merch in safe keeping','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 61,'CL','Loss of Merch-kept at home','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 62,'CL','Loss of Merch-while off premises','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 63,'CL','Medical Payments','9','19','20','20','29','39','45','19','49','25','1' UNION ALL
		SELECT 64,'CL','Mold-Property','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 65,'CL','Money','9','19','90','90','27','37','49','19','49','25','1' UNION ALL
		SELECT 66,'CL','Package Damaged','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 67,'CL','Power Surge-including Brownouts','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 68,'CL','Property Removal','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 69,'CL','Ring Switch - W/O Surveillance','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 70,'CL','Riot and civil commotion','3','04','90','90','23','33','49','04','34','25','1' UNION ALL
		SELECT 71,'CL','Sexual Harassment','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 72,'CL','Sinkhole Collapse','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 73,'CL','Slsman Loss - Not Involving Unattended','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 74,'CL','Smash Grab & Run-Open','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 75,'CL','Smash Grab & Run-prot win/doors closed','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 76,'CL','Smash Grab & Run-unprot wndws/doors clsd','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 77,'CL','Smoke','1','01','90','90','21','31','49','01','31','21','1' UNION ALL
		SELECT 78,'CL','Smoke - From Woodburning Stove','1','01','90','90','21','31','49','01','31','21','1' UNION ALL
		SELECT 79,'CL','Sneak Theft/Shoplift - W/O Surveillance','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 80,'CL','Sneak Theft/Shoplifting - open','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 81,'CL','Sprinkler Leakage','8','08','90','90','26','36','49','06','36','25','1' UNION ALL
		SELECT 82,'CL','Switch-Goods - open','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 83,'CL','Terrorism BI Cert Acts NBC-UMB/BOP Liab','A','15','95','82','55','56','95','15','45','95','1' UNION ALL
		SELECT 84,'CL','Terrorism BI Cert Acts-UMB/BOP Liab','A','15','95','95','55','56','95','15','45','95','1' UNION ALL
		SELECT 85,'CL','Terrorism BI NBC not Cert-UMB/BOP Liab','A','15','95','90','55','56','95','15','45','95','1' UNION ALL
		SELECT 86,'CL','Terrorism BI no NBC not Cert-UMB/BOP Lia','A','15','95','90','55','56','95','15','45','95','1' UNION ALL
		SELECT 87,'CL','Terrorism Certified Acts -JS JB BOP Prop','A','15','95','95','55','56','95','15','45','95','1' UNION ALL
		SELECT 88,'CL','Terrorism PD Cert Acts NBC-UMB/BOP Liab','A','15','95','85','55','56','95','15','45','95','1' UNION ALL
		SELECT 89,'CL','Terrorism PD Cert Acts-UMB/BOP Liab','A','15','95','95','55','56','95','15','45','95','1' UNION ALL
		SELECT 90,'CL','Terrorism PD NBC not Cert-UMB/BOP Liab','A','15','95','90','55','56','95','15','45','95','1' UNION ALL
		SELECT 91,'CL','Terrorism PD no NBC not Cert-UMB/BP Liab','A','15','95','90','55','56','95','15','45','95','1' UNION ALL
		SELECT 92,'CL','Theft SELECT Burglary/Robbery UNION ALL - Off Premises','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 93,'CL','Theft from auto SELECT Craftsman UNION ALL','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 94,'CL','Thrown Away','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 95,'CL','Time Element - Extra Expense','9','19','90','90','29','39','49','19','49','25','0' UNION ALL
		SELECT 96,'CL','Time Element - Loss Of Earnings','9','19','90','90','29','39','49','19','49','25','0' UNION ALL
		SELECT 97,'CL','Transit -G4S','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 98,'CL','Transit-Airborne','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 99,'CL','Transit-All Others','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 100,'CL','Transit-Brinks','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 101,'CL','Transit-Express Mail','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 102,'CL','Transit-FCRM','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 103,'CL','Transit-FedEx','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 104,'CL','Transit-FedEx DVX','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 105,'CL','Transit -Mail Shipments','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 106,'CL','Transit-Malca Amit','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 107,'CL','Transit-Messenger/Trucking Service','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 108,'CL','Transit-Parcel Pro','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 109,'CL','Transit-Transguardian','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 110,'CL','Transit-U.P.S.','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 111,'CL','Travel - Armed Robbery','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 112,'CL','Unattended Auto','5','07','90','90','29','39','49','07','37','25','1' UNION ALL
		SELECT 113,'CL','Vandalism','3','04','90','90','23','33','49','05','35','25','1' UNION ALL
		SELECT 114,'CL','Motor vehicle','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 115,'CL','Volcanic Action','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 116,'CL','War, Miltry Act & Terrorism-on prem/open','A','15','95','95','55','56','95','15','45','95','1' UNION ALL
		SELECT 117,'CL','Water Damage','8','08','90','90','24','34','49','08','38','25','1' UNION ALL
		SELECT 118,'CL','Weight of Ice, Snow or Sleet','7','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 119,'CL','Window Smash While Open SELECT protected UNION ALL','5','07','90','90','29','39','49','07','37','21','1' UNION ALL
		SELECT 120,'CL','Window Smash While Open SELECT unprotected UNION ALL','5','07','90','90','29','39','49','07','37','21','1' UNION ALL
		SELECT 121,'CL','Wind','2','02','90','90','22','32','49','02','32','25','1' UNION ALL
		SELECT 122,'CL','Workmanship','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 123,'CL','Wrongful Termination','9','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 124,'PL','Accidental Loss  - No Safe or Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 125,'PL','Accidental Loss - Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 126,'PL','Accidental Loss - Safe','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 127,'PL','Accidental Loss - Safe or Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 128,'PL','Accidental Loss - Safety Box','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 129,'PL','Armed Robbery - Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 130,'PL','Armed Robbery - No Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 131,'PL','Armed Robbery - Safe','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 132,'PL','Armed Robbery - Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 133,'PL','Armed Robbery - Safety Box','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 134,'PL','Burglary - Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 135,'PL','Burglary - No Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 136,'PL','Burglary - Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 137,'PL','Burglary - Safe','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 138,'PL','Burglary - Safety Box','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 139,'PL','Fire - Alarm','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 140,'PL','Fire - No Safe or Alarm','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 141,'PL','Fire - Safe','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 142,'PL','Fire - Safe or Alarm','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 143,'PL','Fire - Safety Box','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 144,'PL','Miscellaneous - Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 145,'PL','Miscellaneous - No Safe or Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 146,'PL','Miscellaneous - Safe','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 147,'PL','Miscellaneous - Safe or Alarm','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 148,'PL','Miscellaneous - Safety Box','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 149,'PL','Mold - Property related','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 150,'PL','Mysterious Disappearance - Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 151,'PL','Mysterious Disappearance - No Safe/Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 152,'PL','Mysterious Disappearance - Safe','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 153,'PL','Mysterious Disappearance - Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 154,'PL','Mysterious Disappearance - Safety Box','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 155,'PL','Theft - Alarm only','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 156,'PL','Theft - No Safe or Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 157,'PL','Theft - Safe and Alarm','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 158,'PL','Theft - Safe only','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 159,'PL','Theft - Safety Box','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 160,'PL','Accidental loss','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 161,'PL','Accidental loss - partial','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 162,'PL','Crash of airplane','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 163,'PL','Animal','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 164,'PL','Armed robbery','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 165,'PL','Burglary','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 166,'PL','Chipped stone','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 167,'PL','Credit card','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 168,'PL','Damage while being worked on','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 169,'PL','Earthquake','7','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 170,'PL','Explosion','1','03',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 171,'PL','Fire','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 172,'PL','Flood','8','08',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 173,'PL','Fraudulent Check/Credit Card Transaction','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 174,'PL','Hail','2','02',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 175,'PL','Lightning','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 176,'PL','Loss of a stone','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 177,'PL','Malicious mischief and vandalism','3','05',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 178,'PL','Miscellaneous causes','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 179,'PL','Mysterious disappearance','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 180,'PL','Physical damage','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 181,'PL','Riot and civil commotion','3','04',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 182,'PL','Shipping','4','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 183,'PL','Smoke','1','01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 184,'PL','Theft','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 185,'PL','Motor vehicle','4','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 186,'PL','Volcanic action','7','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 187,'PL','Water damage','8','08',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 188,'PL','Wind','2','02',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 189,'PL','Wear and Tear','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 190,'CL','Cyber Liability','9','25','90','90','29','39','59','19','49','25','1' UNION ALL
		SELECT 191,'PL','Chipped Stone - Center','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 192,'PL','Chipped Stone - Side','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 193,'PL','Loss of Stone - Center','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 194,'PL','Loss of Stone - Side','5','07',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 195,'PL','Wear and Tear- Bent, broken, worn prongs','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 196,'PL','Wear and Tear- Broken clasps, bracelets, or chains','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 197,'PL','Wear and Tear- Broken earring backs or posts','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 198,'PL','Wear and Tear- Broken or stretched pearl strands','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 199,'PL','Wear and Tear- Thinning or cracked ring shanks','9','19',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1' UNION ALL
		SELECT 200,'CL','Cyber Crime','9','25','90','90','29','39','59','19','49','25','1' UNION ALL
		SELECT 201,'CL','Cyber Extortion','9','25','90','90','29','39','59','19','49','25','1' UNION ALL
		SELECT 202,'CL','Cyber Terrorism','9','25','90','90','29','39','59','19','49','25','1' UNION ALL
		SELECT 203,'CL','Burglary-on premise','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 204,'CL','Armed Robbery','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 205,'CL','Armed Robbery - Open - Rolex','5','07','90','90','28','38','49','07','37','21','1' UNION ALL
		SELECT 206,'CL','Smash, Grab and Run - Open - Rolex','5','07','90','90','28','38','49','07','37','25','1' UNION ALL
		SELECT 207,'CL','Transit - Fed Ex - 1Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 208,'CL','Transit - Fed Ex - 2Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 209,'CL','Transit - Fed Ex - 3Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 210,'CL','Transit - Fed Ex - Ground','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 211,'CL','Transit - Fed Ex - Other','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 212,'CL','Transit - Partial - Damage','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 213,'CL','Transit - Partial - Missing Part','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 214,'CL','Transit - Partial - Repair','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 215,'CL','Transit - Total Loss - Damaged','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 216,'CL','Transit - Total Loss - Empty','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 217,'CL','Transit - Total Loss - Not Delivered','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 218,'CL','Transit - UPS - 1Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 219,'CL','Transit - UPS - 2Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 220,'CL','Transit - UPS - 3Day','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 221,'CL','Transit - UPS - Ground','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 222,'CL','Transit - UPS - Other','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 223,'CL','Transit - USPS - 1st Class','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 224,'CL','Transit - USPS - Express Mail','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 225,'CL','Transit - USPS - FCRM','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 226,'CL','Transit - USPS - Other','4','19','90','90','29','39','49','19','49','25','1' UNION ALL
		SELECT 227,'CL','Transit - USPS - Priority Mail','4','19','90','90','29','39','49','19','49','25','1' 
	) 
AS Source 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET ProductLine = Source.ProductLine
	,ConformedCauseOfLossDesc = Source.ConformedCauseOfLossDesc
	,NISS_IM_StatCode = Source.NISS_IM_StatCode
	,ISO_StatCode = Source.ISO_StatCode
	,NISS_GL_UMB_StatCode = Source.NISS_GL_UMB_StatCode
	,ISO_GL_UMB_StatCode = Source.ISO_GL_UMB_StatCode
	,NISS_BOP_PROP_StatCode = Source.NISS_BOP_PROP_StatCode
	,NISS_BOP_TE_StatCode = Source.NISS_BOP_TE_StatCode
	,NISS_BOP_LIAB_StatCode = Source.NISS_BOP_LIAB_StatCode
	,ISO_BOP_PROP_StatCode = Source.ISO_BOP_PROP_StatCode
	,ISO_BOP_TE_StatCode = Source.ISO_BOP_TE_StatCode
	,ISO_BOP_LIAB_StatCode = Source.ISO_BOP_LIAB_StatCode
	,IsActive = Source.IsActive
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,ProductLine, ConformedCauseOfLossDesc, NISS_IM_StatCode, ISO_StatCode, NISS_GL_UMB_StatCode, ISO_GL_UMB_StatCode, NISS_BOP_PROP_StatCode, NISS_BOP_TE_StatCode, NISS_BOP_LIAB_StatCode, ISO_BOP_PROP_StatCode, ISO_BOP_TE_StatCode, ISO_BOP_LIAB_StatCode, IsActive) 
	VALUES (ID,ProductLine, ConformedCauseOfLossDesc, NISS_IM_StatCode, ISO_StatCode, NISS_GL_UMB_StatCode, ISO_GL_UMB_StatCode, NISS_BOP_PROP_StatCode, NISS_BOP_TE_StatCode, NISS_BOP_LIAB_StatCode, ISO_BOP_PROP_StatCode, ISO_BOP_TE_StatCode, ISO_BOP_LIAB_StatCode, IsActive) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;
