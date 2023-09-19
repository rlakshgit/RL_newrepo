--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [GeographyStateCode] + ''',''' + [GeographyStateDesc] + ''',''' + [GeographyCountryCode] + ''',''' + [GeographyCountryDesc] + ''',''' + [GeographyRegion] + '''),'
--	FROM [bief_src].[Geography]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.Geography`
(
	ID  INT64, 
	GeographyStateCode STRING, 
	GeographyStateDesc STRING, 
	GeographyCountryCode STRING, 
	GeographyCountryDesc STRING, 
	GeographyRegion STRING
);

MERGE INTO `{project}.{dest_dataset}.Geography` AS Target 
USING (
		SELECT 1 as ID, 'AB' as GeographyStateCode,'Alberta' as GeographyStateDesc,'CAN' as GeographyCountryCode,'Canada' as GeographyCountryDesc,'West/Central' as GeographyRegion UNION ALL
		SELECT 2, 'BC','British Columbia','CAN','Canada','West/Central' UNION ALL
		SELECT 3, 'MB','Manitoba','CAN','Canada','West/Central' UNION ALL
		SELECT 4, 'NB','New Brunswick','CAN','Canada','West/Central' UNION ALL
		SELECT 5, 'NL','Newfoundland and Labrador','CAN','Canada','West/Central' UNION ALL
		SELECT 6, 'NS','Nova Scotia','CAN','Canada','West/Central' UNION ALL
		SELECT 7, 'NT','Northwest Territories','CAN','Canada','West/Central' UNION ALL
		SELECT 8, 'NU','Nunavut','CAN','Canada','West/Central' UNION ALL
		SELECT 9, 'ON','Ontario','CAN','Canada','West/Central' UNION ALL
		SELECT 10,'PE','Prince Edward Island','CAN','Canada','West/Central' UNION ALL
		SELECT 11,'QC','Québec','CAN','Canada','West/Central' UNION ALL
		SELECT 12,'SK','Saskatchewan','CAN','Canada','West/Central' UNION ALL
		SELECT 13,'YT','Yukon Territory','CAN','Canada','West/Central' UNION ALL
		SELECT 14,'AA','Armed Forces Americas','USA','United States','South' UNION ALL
		SELECT 15,'AE','Armed Forces Europe','USA','United States','South' UNION ALL
		SELECT 16,'AK','Alaska','USA','United States','West/Central' UNION ALL
		SELECT 17,'AL','Alabama','USA','United States','South' UNION ALL
		SELECT 18,'AP','Armed Forces Pacific','USA','United States','South' UNION ALL
		SELECT 19,'AR','Arkansas','USA','United States','South' UNION ALL
		SELECT 20,'AS','American Samoa','USA','United States','South' UNION ALL
		SELECT 21,'AZ','Arizona','USA','United States','West/Central' UNION ALL
		SELECT 22,'CA','California','USA','United States','West/Central' UNION ALL
		SELECT 23,'CO','Colorado','USA','United States','West/Central' UNION ALL
		SELECT 24,'CT','Connecticut','USA','United States','East' UNION ALL
		SELECT 25,'DC','District of Columbia','USA','United States','South' UNION ALL
		SELECT 26,'DE','Delaware','USA','United States','South' UNION ALL
		SELECT 27,'FL','Florida','USA','United States','South' UNION ALL
		SELECT 28,'FM','Federated States of Micronesia','USA','United States','South' UNION ALL
		SELECT 29,'GA','Georgia','USA','United States','South' UNION ALL
		SELECT 30,'GU','Guam','USA','United States','South' UNION ALL
		SELECT 31,'HI','Hawaii','USA','United States','West/Central' UNION ALL
		SELECT 32,'IA','Iowa','USA','United States','West/Central' UNION ALL
		SELECT 33,'ID','Idaho','USA','United States','West/Central' UNION ALL
		SELECT 34,'IL','Illinois','USA','United States','West/Central' UNION ALL
		SELECT 35,'IN','Indiana','USA','United States','East' UNION ALL
		SELECT 36,'KS','Kansas','USA','United States','South' UNION ALL
		SELECT 37,'KY','Kentucky','USA','United States','South' UNION ALL
		SELECT 38,'LA','Louisiana','USA','United States','South' UNION ALL
		SELECT 39,'MA','Massachusetts','USA','United States','East' UNION ALL
		SELECT 40,'MD','Maryland','USA','United States','South' UNION ALL
		SELECT 41,'ME','Maine','USA','United States','East' UNION ALL
		SELECT 42,'MH','Marshall Islands','USA','United States','South' UNION ALL
		SELECT 43,'MI','Michigan','USA','United States','East' UNION ALL
		SELECT 44,'MN','Minnesota','USA','United States','West/Central' UNION ALL
		SELECT 45,'MO','Missouri','USA','United States','South' UNION ALL
		SELECT 46,'MP','Northern Mariana Islands','USA','United States','South' UNION ALL
		SELECT 47,'MS','Mississippi','USA','United States','South' UNION ALL
		SELECT 48,'MT','Montana','USA','United States','West/Central' UNION ALL
		SELECT 49,'NC','North Carolina','USA','United States','South' UNION ALL
		SELECT 50,'ND','North Dakota','USA','United States','West/Central' UNION ALL
		SELECT 51,'NE','Nebraska','USA','United States','West/Central' UNION ALL
		SELECT 52,'NH','New Hampshire','USA','United States','East' UNION ALL
		SELECT 53,'NJ','New Jersey','USA','United States','East' UNION ALL
		SELECT 54,'NM','New Mexico','USA','United States','West/Central' UNION ALL
		SELECT 55,'NV','Nevada','USA','United States','West/Central' UNION ALL
		SELECT 56,'NY','New York','USA','United States','East' UNION ALL
		SELECT 57,'OH','Ohio','USA','United States','East' UNION ALL
		SELECT 58,'OK','Oklahoma','USA','United States','South' UNION ALL
		SELECT 59,'OR','Oregon','USA','United States','West/Central' UNION ALL
		SELECT 60,'PA','Pennsylvania','USA','United States','East' UNION ALL
		SELECT 61,'PR','Puerto Rico','USA','United States','South' UNION ALL
		SELECT 62,'PW','Palau','USA','United States','South' UNION ALL
		SELECT 63,'RI','Rhode Island','USA','United States','East' UNION ALL
		SELECT 64,'SC','South Carolina','USA','United States','South' UNION ALL
		SELECT 65,'SD','South Dakota','USA','United States','West/Central' UNION ALL
		SELECT 66,'TN','Tennessee','USA','United States','South' UNION ALL
		SELECT 67,'TX','Texas','USA','United States','South' UNION ALL
		SELECT 68,'UM','U.S. Minor Outlying Islands','USA','United States','South' UNION ALL
		SELECT 69,'UT','Utah','USA','United States','West/Central' UNION ALL
		SELECT 70,'VA','Virginia','USA','United States','South' UNION ALL
		SELECT 71,'VI','U.S. Virgin Islands','USA','United States','South' UNION ALL
		SELECT 72,'VT','Vermont','USA','United States','East' UNION ALL
		SELECT 73,'WA','Washington','USA','United States','West/Central' UNION ALL
		SELECT 74,'WI','Wisconsin','USA','United States','West/Central' UNION ALL
		SELECT 75,'WV','West Virginia','USA','United States','East' UNION ALL
		SELECT 76,'WY','Wyoming','USA','United States','West/Central'
	) 
AS Source --([ID],[GeographyStateCode], [GeographyStateDesc], [GeographyCountryCode], [GeographyCountryDesc], [GeographyRegion]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET GeographyStateCode = Source.GeographyStateCode
	,GeographyStateDesc = Source.GeographyStateDesc
	,GeographyCountryCode = Source.GeographyCountryCode
	,GeographyCountryDesc = Source.GeographyCountryDesc
	,GeographyRegion = Source.GeographyRegion
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,GeographyStateCode, GeographyStateDesc, GeographyCountryCode, GeographyCountryDesc, GeographyRegion) 
	VALUES (ID,GeographyStateCode, GeographyStateDesc, GeographyCountryCode, GeographyCountryDesc, GeographyRegion) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;