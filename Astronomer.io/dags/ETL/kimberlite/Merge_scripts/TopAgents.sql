--Populate [Incentive].[t_TopAgents] Lookup Table, which contains the current list of Top Agencies for the PL Agency Incentive program

-- added None in Property
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.TopAgents`
(
	AgencyMasterCode STRING,
	AgencyCode STRING,
	AgencyMasterName STRING,
	State STRING,
	Owner STRING
);

MERGE INTO `{project}.{dest_dataset}.TopAgents` AS Target 
USING (
     SELECT '200' as AgencyMasterCode,'200' as AgencyCode,'JM Insurance Services LLC' as AgencyMasterName,'Wisconsin' as State,'AKret' as Owner UNION ALL
     SELECT 'CAI','CAI','CAI Insurance Agency Inc','Ohio','AKret' UNION ALL
     SELECT '405','405','Hyde & Associates, Inc','Virginia','mlang' UNION ALL
     SELECT '638','638','Corporate 4 Insurance Agency Inc','Minnesota','croth' UNION ALL
     SELECT 'A05','A05','Judy Carter & Associates LLC','Alabama','KPete' UNION ALL
     SELECT 'B50','B50','JM Insurance Services LLC','Wisconsin','KPete' UNION ALL
     SELECT 'C04','C04','Mountain States Ins Marketing','Colorado','croth' UNION ALL
     SELECT 'C51','C51','Smith Brothers','Connecticut','AKret' UNION ALL
     SELECT 'CJB','CJB','CJB Insurance Services','Ontario','KPete' UNION ALL
     SELECT 'D01','D01','Riemer Insurance Group','Florida','mlang' UNION ALL
     SELECT 'D10','D10','Assured Partners of Florida dba Dawson of Florida','Florida','AKret' UNION ALL
     SELECT 'D52','D52','Southern Jewelry Insurance','Georgia','KPete' UNION ALL
     SELECT 'F50','F50','Associated Agencies Inc','Illinois','croth' UNION ALL
     SELECT 'F51','F51','Lubin-Bergman Organization Inc','Illinois','croth' UNION ALL
     SELECT 'G51','G51','The Clippinger Insurance Agency','Kansas','mlang' UNION ALL
     SELECT 'H51','H51','Clockwork Insurance Services Inc','Louisiana','KPete' UNION ALL
     SELECT 'I50','I50','Hub International Mid Atlantic Inc','Maryland','mlang' UNION ALL
     SELECT 'INV','INV','Invessa Assurances & Services Financiers','Quebec','KPete' UNION ALL
     SELECT 'J51','J51','Crosby & Henry Inc','Michigan','AKret' UNION ALL
     SELECT 'JLT','JLT','JLT Specialty USA','California','KPete' UNION ALL
     SELECT 'M01','M01','All American Insurance Inc','Nebraska','croth' UNION ALL
     SELECT 'M50','M50','M L Cutler & Company Inc','New Jersey','AKret' UNION ALL
     SELECT 'M80','M80','Child-Genovese Insurance Agency Inc.','Massachusetts','AKret' UNION ALL
     SELECT 'MAC','MAC','MAC Insurance & Financial Ltd','Alberta','KPete' UNION ALL
     SELECT 'MLK','MLK','L Melkonian Insurance Brokers Inc','Quebec','KPete' UNION ALL
     SELECT 'P02','P02','The Insurance Marketplace','New York','AKret' UNION ALL
     SELECT 'P05','P05','MJM Global Insurance Brokerage Group','Florida','AKret' UNION ALL
     SELECT 'P13','P13','The Signature B & B Companies','New York','AKret' UNION ALL
     SELECT 'P17','P17','Michael Zelichov Insurance Services Inc.','California','KPete' UNION ALL
     SELECT 'P19','P19','Cargil Insurance Corp dba AIB','California','KPete' UNION ALL
     SELECT 'Q50','Q50','Alfred J Davis Company','Oregon','croth' UNION ALL
     SELECT 'Q52','Q52','Hagan Hamilton Insurance','Oregon','croth' UNION ALL
     SELECT 'R01','R01','Jewelers Ins Services Inc','Pennsylvania','AKret' UNION ALL
     SELECT 'R53','R53','Butler & Messier Inc','Rhode Island','AKret' UNION ALL
     SELECT 'SOP','SOP','Soplex, Inc','Quebec','KPete' UNION ALL
     SELECT 'T01','T01','JM Insurance Services LLC','Texas','KPete' UNION ALL
     SELECT 'T50','T50','Benchmark Insurance Agency Inc','Utah','croth' UNION ALL
     SELECT 'U09','U09','Insurance Specialists of VA, Inc','Virginia','mlang'
)

AS Source --([AgencyMasterCode],[AgencyCode],[AgencyMasterName],[State],[Owner])
	ON Target.AgencyMasterCode = Source.AgencyMasterCode AND Target.AgencyCode = Source.AgencyCode 

WHEN MATCHED THEN 
UPDATE SET AgencyMasterCode = Source.AgencyMasterCode
	,AgencyCode = Source.AgencyCode
	,AgencyMasterName = Source.AgencyMasterName
	,State = Source.State
	,Owner = Source.Owner
	
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (AgencyMasterCode,AgencyCode,AgencyMasterName,State,Owner)
	VALUES (AgencyMasterCode,AgencyCode,AgencyMasterName,State,Owner)

WHEN NOT MATCHED BY SOURCE THEN 
	DELETE; 