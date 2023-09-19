-- tag: t_lkup_StrategicPartners - tag ends/
/**** Kimberlite - Reference and Lookup Tables ********
		t_lkup_StrategicPartners.sql 

 *****  Change History  *****

	05/17/2023	DROBAK		Initial List added

-------------------------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------------------------
--- Create Master-Reference Table ---
-------------------------------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS `{project}.{dest_dataset}.t_lkup_StrategicPartners`
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_lkup_StrategicPartners`
(
    ID					INT64
	,SourceSystem		STRING
    ,AgencyMasterCode	STRING
    ,AgencyMasterName   STRING
    ,Sequence			INT64
    ,IsActive			BOOLEAN
    ,DateCreated        DATE
	,DateModified		DATE
	,bq_load_date		DATE
)
  OPTIONS(
  description="Contains list of Strategic Partners at JM"
);
MERGE INTO `{project}.{dest_dataset}.t_lkup_StrategicPartners` AS Target 
USING (
SELECT 1 AS ID, 'GW' AS SourceSystem, 'Z100' AS AgencyMasterCode,'GEICO Insurance Agency Inc' AS AgencyMasterName,1 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2019-12-19') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 2 AS ID, 'GW' AS SourceSystem, 'KL001' AS AgencyMasterCode,'Kraft Lake Insurance Agency Inc' AS AgencyMasterName,2 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2019-12-19') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 3 AS ID, 'GW' AS SourceSystem, 'HE001' AS AgencyMasterCode,'Liberty Mutual Group Inc' AS AgencyMasterName,3 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2019-12-19') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 4 AS ID, 'GW' AS SourceSystem, 'U10' AS AgencyMasterCode,'McGriff Insurance Services Inc' AS AgencyMasterName,4 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2019-12-19') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 5 AS ID, 'GW' AS SourceSystem, 'Z400' AS AgencyMasterCode,'IIAA AGENCY ADMINISTRATIVE SERVICES' AS AgencyMasterName,5 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2021-10-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 6 AS ID, 'GW' AS SourceSystem, 'TWFG' AS AgencyMasterCode,'TWFG Insurance Services Inc' AS AgencyMasterName,7 AS Sequence, true AS IsActive, DATE('2019-12-19') AS DateCreated, DATE('2019-12-19') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 7 AS ID, 'GW' AS SourceSystem, 'AIB001' AS AgencyMasterCode,'Atlas Insurance Brokers, LLC' AS AgencyMasterName,8 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 8 AS ID, 'GW' AS SourceSystem, 'BKS001' AS AgencyMasterCode,'Baldwin Krystyn Sherman, LLC' AS AgencyMasterName,9 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 9 AS ID, 'GW' AS SourceSystem, 'INS001' AS AgencyMasterCode,'Banc Insurance Agency, Inc.' AS AgencyMasterName,10 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 10 AS ID, 'GW' AS SourceSystem, 'GH001' AS AgencyMasterCode,'Goosehead Insurance' AS AgencyMasterName,11 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 11 AS ID, 'GW' AS SourceSystem, 'BRP001' AS AgencyMasterCode,'Guided Insurance Solutions, LLC' AS AgencyMasterName,12 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 12 AS ID, 'GW' AS SourceSystem, 'ISA001' AS AgencyMasterCode,'Ivantage Select Agency, Inc' AS AgencyMasterName,13 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 13 AS ID, 'GW' AS SourceSystem, 'NBS' AS AgencyMasterCode,'NBS Insurance Agency Inc' AS AgencyMasterName,14 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 14 AS ID, 'GW' AS SourceSystem, 'RC001' AS AgencyMasterCode,'Orchid Insurance' AS AgencyMasterName,15 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 15 AS ID, 'GW' AS SourceSystem, 'PGI001' AS AgencyMasterCode,'Premier Group Insurance' AS AgencyMasterName,16 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 16 AS ID, 'GW' AS SourceSystem, 'YNG001' AS AgencyMasterCode,'Young Alfred' AS AgencyMasterName,17 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 17 AS ID, 'GW' AS SourceSystem, 'MAT001' AS AgencyMasterCode,'Matic Insurance Services' AS AgencyMasterName,18 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 18 AS ID, 'GW' AS SourceSystem, 'Z41' AS AgencyMasterCode,'We Insure Group' AS AgencyMasterName,19 AS Sequence, true AS IsActive, DATE('2021-10-12') AS DateCreated, DATE('2021-10-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 19 AS ID, 'GW' AS SourceSystem, 'AAS001' AS AgencyMasterCode,'Agents Alliance Services' AS AgencyMasterName,20 AS Sequence, true AS IsActive, DATE('2021-10-26') AS DateCreated, DATE('2021-10-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 20 AS ID, 'GW' AS SourceSystem, 'B99' AS AgencyMasterCode,'BUSINESS OWNERS LIABILITY TEAM' AS AgencyMasterName,21 AS Sequence, true AS IsActive, DATE('2021-10-26') AS DateCreated, DATE('2021-10-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 21 AS ID, 'GW' AS SourceSystem, 'USAA001' AS AgencyMasterCode,'USAA' AS AgencyMasterName,22 AS Sequence, true AS IsActive, DATE('2021-11-18') AS DateCreated, DATE('2021-11-18') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 22 AS ID, 'GW' AS SourceSystem, 'AOH001' AS AgencyMasterCode,'AAA of Ohio' AS AgencyMasterName,23 AS Sequence, true AS IsActive, DATE('2021-11-23') AS DateCreated, DATE('2021-11-23') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 23 AS ID, 'GW' AS SourceSystem, 'IMA001' AS AgencyMasterCode,'IMA Select' AS AgencyMasterName,24 AS Sequence, true AS IsActive, DATE('2022-1-6') AS DateCreated, DATE('2022-1-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 24 AS ID, 'GW' AS SourceSystem, 'BIG001' AS AgencyMasterCode,'Bankers Insurance, LLC' AS AgencyMasterName,25 AS Sequence, true AS IsActive, DATE('2022-1-6') AS DateCreated, DATE('2022-1-6') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 25 AS ID, 'GW' AS SourceSystem, 'EIG001' AS AgencyMasterCode,'Elite Insurance Group, Inc' AS AgencyMasterName,26 AS Sequence, true AS IsActive, DATE('2022-1-6') AS DateCreated, DATE('2022-1-6') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 26 AS ID, 'GW' AS SourceSystem, 'WSI001' AS AgencyMasterCode,'Westfield Insurance' AS AgencyMasterName,27 AS Sequence, true AS IsActive, DATE('2022-1-26') AS DateCreated, DATE('2022-1-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 27 AS ID, 'GW' AS SourceSystem, 'HOM001' AS AgencyMasterCode,'Homesite Insurance Agency' AS AgencyMasterName,28 AS Sequence, true AS IsActive, DATE('2022-1-26') AS DateCreated, DATE('2022-1-26') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 28 AS ID, 'GW' AS SourceSystem, 'MFB001' AS AgencyMasterCode,'COMMUNITY SERVICE ACCEPTANCE COMPANY' AS AgencyMasterName,29 AS Sequence, true AS IsActive, DATE('2022-4-13') AS DateCreated, DATE('2022-4-13') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 29 AS ID, 'GW' AS SourceSystem, 'JEB001' AS AgencyMasterCode,'HULL & COMPANY, LLC' AS AgencyMasterName,30 AS Sequence, true AS IsActive, DATE('2022-4-13') AS DateCreated, DATE('2022-4-13') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 30 AS ID, 'GW' AS SourceSystem, 'WIA001' AS AgencyMasterCode,'WORLD INS ASSOC LLC' AS AgencyMasterName,31 AS Sequence, true AS IsActive, DATE('2022-5-24') AS DateCreated, DATE('2022-5-24') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 31 AS ID, 'GW' AS SourceSystem, 'INM001' AS AgencyMasterCode,'INSURAMATCH, LLC' AS AgencyMasterName,32 AS Sequence, true AS IsActive, DATE('2022-5-24') AS DateCreated, DATE('2022-5-24') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 32 AS ID, 'GW' AS SourceSystem, 'MIPS001' AS AgencyMasterCode,'POLISEEK AIS INSURANCE SOLUTIONS, INC.' AS AgencyMasterName,33 AS Sequence, true AS IsActive, DATE('2022-5-24') AS DateCreated, DATE('2022-5-24') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 33 AS ID, 'GW' AS SourceSystem, 'ANE001' AS AgencyMasterCode,'AAA NORTHEAST INSURANCE AGENCY, INC.' AS AgencyMasterName,34 AS Sequence, true AS IsActive, DATE('2022-5-24') AS DateCreated, DATE('2022-5-24') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 34 AS ID, 'GW' AS SourceSystem, 'GRI001' AS AgencyMasterCode,'GUARANTEED RATE INS' AS AgencyMasterName,35 AS Sequence, true AS IsActive, DATE('2022-7-6') AS DateCreated, DATE('2022-7-6') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 35 AS ID, 'GW' AS SourceSystem, 'CI001' AS AgencyMasterCode,'WOODROW W. CROSS AGENCY, INC' AS AgencyMasterName,36 AS Sequence, true AS IsActive, DATE('2022-7-12') AS DateCreated, DATE('2022-7-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 36 AS ID, 'GW' AS SourceSystem, 'MIAIS001' AS AgencyMasterCode,'AUTO INSURANCE SPECIALISTS, LLC' AS AgencyMasterName,37 AS Sequence, true AS IsActive, DATE('2022-7-12') AS DateCreated, DATE('2022-7-12') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 37 AS ID, 'GW' AS SourceSystem, 'P13' AS AgencyMasterCode,'ACRISURE LLC' AS AgencyMasterName,38 AS Sequence, true AS IsActive, DATE('2022-11-8') AS DateCreated, DATE('2022-11-8') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 38 AS ID, 'GW' AS SourceSystem, 'HNE001' AS AgencyMasterCode,'HUB INTERNATIONAL NORTHEAST LIMITED' AS AgencyMasterName,39 AS Sequence, true AS IsActive, DATE('2022-11-28') AS DateCreated, DATE('2022-11-28') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 39 AS ID, 'GW' AS SourceSystem, 'CAF001' AS AgencyMasterCode,'AMERICAN FAMILY CONNECT INSURANCE AGENCY INC' AS AgencyMasterName,40 AS Sequence, true AS IsActive, DATE('2023-1-10') AS DateCreated, DATE('2023-1-10') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 40 AS ID, 'GW' AS SourceSystem, 'TGA001' AS AgencyMasterCode,'TWFG GENERAL AGENCY, LLC' AS AgencyMasterName,41 AS Sequence, true AS IsActive, DATE('2023-3-23') AS DateCreated, DATE('2023-3-23') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 41 AS ID, 'GW' AS SourceSystem, 'FCS001' AS AgencyMasterCode,'FIRST CONNECT INSURANCE SERVICES, LLC' AS AgencyMasterName,42 AS Sequence, true AS IsActive, DATE('2023-3-30') AS DateCreated, DATE('2023-3-30') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 42 AS ID, 'GW' AS SourceSystem, 'ACA001' AS AgencyMasterCode,'ACRISURE OF CALIFORNIA, LLC' AS AgencyMasterName,43 AS Sequence, true AS IsActive, DATE('2023-3-30') AS DateCreated, DATE('2023-3-30') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 43 AS ID, 'GW' AS SourceSystem, 'MAD001' AS AgencyMasterCode,'MADISON INSURANCE ASSOCIATION LLC' AS AgencyMasterName,44 AS Sequence, true AS IsActive, DATE('2023-3-30') AS DateCreated, DATE('2023-3-30') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 44 AS ID, 'GW' AS SourceSystem, 'QUC001' AS AgencyMasterCode,'QUICKINSURED COM LLC' AS AgencyMasterName,45 AS Sequence, true AS IsActive, DATE('2023-3-30') AS DateCreated, DATE('2023-3-30') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 45 AS ID, 'GW' AS SourceSystem, 'EHB001' AS AgencyMasterCode,'HOMEBODY INSURANCE AGENCY, LLC' AS AgencyMasterName,46 AS Sequence, true AS IsActive, DATE('2023-5-9') AS DateCreated, DATE('2023-5-9') AS DateModified, CURRENT_DATE() AS bq_load_date UNION ALL
SELECT 46 AS ID, 'GW' AS SourceSystem, 'GFC001' AS AgencyMasterCode,'GREATFLORIDA INSURANCE HOLDINGS CORP' AS AgencyMasterName,47 AS Sequence, true AS IsActive, DATE('2023-4-20') AS DateCreated, DATE('2023-4-20') AS DateModified, CURRENT_DATE() AS bq_load_date
) AS Source
ON Target.ID = Source.ID

WHEN MATCHED THEN UPDATE
SET SourceSystem = Source.SourceSystem
	,AgencyMasterCode = Source.AgencyMasterCode
	,AgencyMasterName = Source.AgencyMasterName
	,Sequence = Source.Sequence
	,IsActive = Source.IsActive
	,DateCreated = Source.DateCreated
	,DateModified = Source.DateModified
	,bq_load_date = Source.bq_load_date

WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID, SourceSystem, AgencyMasterCode, AgencyMasterName, Sequence, IsActive, DateCreated, DateModified, bq_load_date) 
	VALUES (ID, SourceSystem, AgencyMasterCode, AgencyMasterName, Sequence, IsActive, DateCreated, DateModified, bq_load_date) 

WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;
