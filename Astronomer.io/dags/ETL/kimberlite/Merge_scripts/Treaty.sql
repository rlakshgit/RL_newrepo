--Populate [bi_lookup].[Treaty] Lookup Table

--SELECT '(''' + [TreatyID] + ''',' + ISNULL('''' + [ReinsurerName] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerDomicileCity] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerDomicileState] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerDomicileCountry] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerOrganization] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerAddress] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerCity] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerState] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerCounty] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerCountry] + '''','NULL') + ',' + ISNULL('''' + [ReinsurerZipcode] + '''','NULL') + ',' + ISNULL('''' + [TreatyContractType] + '''','NULL') + ',' + ISNULL('''' + [TreatyShortDesc] + '''','NULL') + ',' + ISNULL('''' + [TreatyDesc2] + '''','NULL') + ',' + ISNULL('''' + CAST([TreatyEffDate] AS VARCHAR(50)) + '''','NULL') + ',' + ISNULL('''' + CAST([TreatyExpDate] AS VARCHAR(50)) + '''','NULL') + '),'
--	FROM [bi_lookup].[Treaty] 
--	ORDER BY TreatyID

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.Treaty`
(	
	TreatyID STRING, 
	ReinsurerName STRING, 
	ReinsurerDomicileCity STRING, 
	ReinsurerDomicileState STRING, 
	ReinsurerDomicileCountry STRING, 
	ReinsurerOrganization STRING, 
	ReinsurerAddress STRING, 
	ReinsurerCity STRING, 
	ReinsurerState STRING, 
	ReinsurerCounty STRING, 
	ReinsurerCountry STRING, 
	ReinsurerZipcode STRING, 
	TreatyContractType STRING, 
	TreatyShortDesc STRING, 
	TreatyDesc2 STRING, 
	TreatyEffDate STRING, 
	TreatyExpDate STRING
);

MERGE INTO  `{project}.{dest_dataset}.Treaty` AS Target 
USING (
		SELECT '4' as TreatyId,'MRB SURPLUS TREATY       ' as ReinsurerName,'Not Available' as ReinsurerDomicileCity,'Not Available' as ReinsurerDomicileState,'USA' as ReinsurerDomicileCountry,'Not Available' as ReinsurerOrganization,'Not Available' as ReinsurerAddress,'Not Available' as ReinsurerCity,'Not Available' as ReinsurerState,'Not Available' as ReinsurerCounty,'USA' as ReinsurerCountry,'Not Available' as ReinsurerZipcode,'Not Available' as TreatyContractType,'MRB SURPLUS TREATY' as TreatyShortDesc,'Not Available' as TreatyDesc2,'1980-01-01 00:00:00.000000' as TreatyEffDate,'8900-12-31 00:00:00.000000' as TreatyExpDate UNION ALL
		SELECT 'AA','EMPLOYERS RE FAC         ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','EMPLOYERS RE FAC  EXCESS      ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AB','KENTUCKY MINE SUB        ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','KENTUCKY MINE SUB        ','Not Available','1984-07-15 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AC','AMERICAN REINSURANCE     ','Princeton','NJ','USA','Not Available','Not Available','Princeton','NJ','Not Available','USA','08543-5241','Not Available','AMERICAN REINSURANCE  CAS FAC Treaty   ','Not Available','2000-02-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AD','HARTFORD STEAM BOILER    ','Hartford','CT','USA','Not Available',NULL,'Hartford','CT','Not Available','USA','06102-0524','Not Available','HARTFORD STEAM BOILER  MECH BRK   ','Not Available','2000-07-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AE','GERLING GLOBAL REINSURANC','New York','NY','USA','Not Available','110 William Street','New York','NY','Not Available','USA','10038-3901','Not Available','GERLING GLOBAL FACULTATIVE XS','Not Available','2001-05-29 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AF','RMC2 LLC                 ','Buffalo Grove','IL','USA','Not Available','600 N Buffalo Grove Road','Buffalo Grove','IL','Not Available','USA','60089','Not Available','RMC2 LLC FACULTATIVE XS               ','Not Available','2002-05-31 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AG','EVEREST REINSURANCE COMPA','Liberty Corner','NJ','USA','Not Available','477 Martinsville Road','Liberty Corner','NJ','Not Available','USA','07938-0830','Not Available','EVEREST FACULTATIVE XS','Not Available','2002-09-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AH','JLT RE SOLUTIONS, INC.   ','Bloomington','MN','USA','Not Available','8000 Norman Center Drive, Suite 620','Bloomington','MN','Not Available','USA','55437','Not Available','JLT RE TREATY MULTI LINE XS','Not Available','2002-10-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AI','JLT RE SOLUTIONS, INC.   ','Bloomington','MN','USA','Not Available','8000 Norman Center Drive, Suite 620','Bloomington','MN','Not Available','USA','55437','Not Available','JLT RE TREATY CATASTROPHE','Not Available','2002-10-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AJ','JLT RE SOLUTIONS, INC.   ','Bloomington','MN','USA','Not Available','8000 Norman Center Drive, Suite 620','Bloomington','MN','Not Available','USA','55437','Not Available','JLT RE FACULTATIVE EXCESS','Not Available','2003-10-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'AK','JLT RE SOLUTIONS, INC.   ','Bloomington','MN','USA','Not Available','8000 Norman Center Drive, Suite 620','Bloomington','MN','Not Available','USA','55437','Not Available','JLT RE FACULTATIVE EXCESS','Not Available','2004-04-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'B','WJ LEHRKE 2MD SURPLUS PROPERTY','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','WJ LEHRKE 2MD SURPLUS PROPERTY','Not Available','1981-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'C ','ILLINOIS MINE SUB        ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','ILLINOIS MINE SUB    ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'D ','GEN RE UMBRELLA          ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','GEN RE UMBRELLA          ','Not Available','1994-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'F ','AMERICAN RE-INSURANCE    ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AMERICAN RE-INSURANCE    ','Not Available','1995-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'H ','W J LEHRKE CASUALTY EXCES','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','W J LEHRKE CASUALTY EXCES','Not Available','1982-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'N','WJ LEHRKE 1ST SURPLUS II','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','WJ LEHRKE 1ST SURPLUS II','Not Available','1984-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'P','AMERICAN RE CASUALTY EXCESS','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AMERICAN RE CASUALTY EXCESS','Not Available','1984-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'Q ','AM RE AUTO FAC EXCESS    ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AM RE AUTO FAC EXCESS    ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'S ','GENERAL RE FAC EXCESS    ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','GENERAL RE FAC EXCESS    ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'T ','AM RE PROPERTY SURPLUS   ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AM RE PROPERTY SURPLUS   ','Not Available','1986-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'U ','AM RE UMBRELLA           ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AM RE UMBRELLA           ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'V ','WJ LEHRKE FAC EXCESS     ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','WJ LEHRKE FAC EXCESS     ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'W','AMERICAN RE PROPERTY EXCESS','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AMERICAN RE PROPERTY EXCESS','Not Available','1988-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'X ','AM RE MULTI EXCESS       ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AM RE MULTI EXCESS       ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'Y ','WEST VIRGINIA MINE SUB   ','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','WEST VIRGINIA MINE SUB   ','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000' UNION ALL
		SELECT 'Z ','AM RE NON-AUTO FAC EXCESS','Not Available','Not Available','USA','Not Available','Not Available','Not Available','Not Available','Not Available','USA','Not Available','Not Available','AMERICAN RE FAC EXCESS','Not Available','1992-01-01 00:00:00.000000','8900-12-31 00:00:00.000000'
	) 
AS Source-- ([TreatyID], [ReinsurerName], [ReinsurerDomicileCity], [ReinsurerDomicileState], [ReinsurerDomicileCountry], [ReinsurerOrganization], [ReinsurerAddress], [ReinsurerCity], [ReinsurerState], [ReinsurerCounty], [ReinsurerCountry], [ReinsurerZipcode], [TreatyContractType], [TreatyShortDesc], [TreatyDesc2], [TreatyEffDate], [TreatyExpDate]) 
ON Target.TreatyID = Source.TreatyID 
WHEN MATCHED THEN UPDATE
SET ReinsurerName = Source.ReinsurerName
	,ReinsurerDomicileCity = Source.ReinsurerDomicileCity
	,ReinsurerDomicileState = Source.ReinsurerDomicileState
	,ReinsurerDomicileCountry = Source.ReinsurerDomicileCountry
	,ReinsurerOrganization = Source.ReinsurerOrganization
	,ReinsurerAddress = Source.ReinsurerAddress
	,ReinsurerCity = Source.ReinsurerCity
	,ReinsurerState = Source.ReinsurerState
	,ReinsurerCounty = Source.ReinsurerCounty
	,ReinsurerCountry = Source.ReinsurerCountry
	,ReinsurerZipcode = Source.ReinsurerZipcode
	,TreatyContractType = Source.TreatyContractType
	,TreatyShortDesc = Source.TreatyShortDesc
	,TreatyDesc2 = Source.TreatyDesc2
	,TreatyEffDate = Source.TreatyEffDate
	,TreatyExpDate = Source.TreatyExpDate
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (TreatyID, ReinsurerName, ReinsurerDomicileCity, ReinsurerDomicileState, ReinsurerDomicileCountry, ReinsurerOrganization, ReinsurerAddress, ReinsurerCity, ReinsurerState, ReinsurerCounty, ReinsurerCountry, ReinsurerZipcode, TreatyContractType, TreatyShortDesc, TreatyDesc2, TreatyEffDate, TreatyExpDate) 
	VALUES (TreatyID, ReinsurerName, ReinsurerDomicileCity, ReinsurerDomicileState, ReinsurerDomicileCountry, ReinsurerOrganization, ReinsurerAddress, ReinsurerCity, ReinsurerState, ReinsurerCounty, ReinsurerCountry, ReinsurerZipcode, TreatyContractType, TreatyShortDesc, TreatyDesc2, TreatyEffDate, TreatyExpDate) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;