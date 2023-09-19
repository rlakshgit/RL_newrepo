--SELECT '(''' + [CoverageTypeCode] + ''',''' + [ConformedCoverageTypeCode] + ''',''' + [ConformedCoverageTypeDesc] + ''',' +ISNULL('''' + [ExhibitCoverageGroup] + '''','NULL') + ',' + ISNULL('''' + [ReinsuranceCoverageGroup] + '''','NULL') + ',''' + [SubjectToIRPM] + ''',''' + [SubjectToExperienceCredit] + ''',' + ISNULL('''' + [PeakOrTemp] + '''','NULL') + ',' + ISNULL('''' + [HSB_BPP_Group] + '''','NULL') + ',' + ISNULL('''' + [HSB_BIE_Group] + '''','NULL') + ',' + ISNULL('''' + [HSB_BLD_Group] + '''','NULL') + ',' + ISNULL('''' + [ISO_Stat_Code] + '''','NULL') + ',' + ISNULL('''' + [NISS_Stat_Code] + '''','NULL') + '),'
--	FROM [bief_src].[CoverageType] ORDER BY [CoverageTypeCode]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageType`
(
	CoverageTypeCode STRING, 
	ConformedCoverageTypeCode STRING, 
	ConformedCoverageTypeDesc STRING, 
	ExhibitCoverageGroup STRING, 
	ReinsuranceCoverageGroup STRING, 
	SubjectToIRPM STRING, 
	SubjectToExperienceCredit STRING, 
	PeakOrTemp STRING, 
	HSB_BPP_Group STRING, 
	HSB_BIE_Group STRING, 
	HSB_BLD_Group STRING, 
	ISO_Stat_Code STRING, 
	NISS_Stat_Code STRING 
);

MERGE INTO `{project}.{dest_dataset}.CoverageType` AS Target 
USING (
		SELECT '0' as CoverageTypeCode,'0' as ConformedCoverageTypeCode,'No Coverage Enhancement Form' as ConformedCoverageTypeDesc,'AllOther' as ExhibitCoverageGroup,'?' as ReinsuranceCoverageGroup,'N' as SubjectToIRPM,'N' as SubjectToExperienceCredit,'None' as PeakOrTemp,NULL as HSB_BPP_Group,NULL as HSB_BIE_Group,NULL as HSB_BLD_Group,'N/A' as ISO_Stat_Code,'N/A' as NISS_Stat_Code UNION ALL
		SELECT '051A','051A','Conversion Premium Adjustment - Property','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '052A','052A','Conversion Premium Adjustment - Liability',NULL,NULL,'N','N','None',NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '100','100','Single Limit Liability (BI,PD & Med)','?','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '101','101A','Umbrella Liability','?','Umbrella Liability','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '101a','101A','Umbrella Liability','?','Umbrella Liability','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '110','110A','Uninsured and Underinsured Motorist Coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '110A','110A','Uninsured and Underinsured Motorist Coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '120','120','Property Damage','?','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '130','130A','Per Person Medical Expense','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '130A','130A','Per Person Medical Expense','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '130X','130X','Medical Expense Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '132','132A','Products and Complete Operations','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '132A','132A','Products and Complete Operations','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '132X','132X','Exclusion - Products Complete Operations Hazard','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '133X','133X','Exclusion - Designated Work','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '135X','135X','Opt 1 Excl Yr2000 Computer/Elec problem','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '140A','140A','Designated Locations General Aggregate Limit','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '141A','141A','Designated Construction Project(s) General Aggregate Limit','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '142X','142X','Comprehensive Business Liability Exclusions - designated premises','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '143A','143A','Limitation of Coverage to Designated Premises or Project','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '150A','150A','Waiver of Transfer Rights of Recovery Against Others to Us','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '154','154','Liability only Businessowners','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '159','159','Property Only Businessowners','AllOther','?','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '160X','160X','Intercompany Produce Suits Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '161X','161X','Cross Liab Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '162X','162X','Punitive. Exemp, Vindict Dmgs Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '163X','163X','Asbestos Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '166X','166X','Damage  to Work performed by Sub-Contractors Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '168X','168X','Exclusion - Designated Products','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '170A','170A','Attorney Fees','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '181','181A','Personal and Advertising Injury - Specified Limit','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '181A','181A','Personal and Advertising Injury - Specified Limit','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '181B','181B','Personal and Advertising Injury - Coverage Amendment','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '181X','181X','Exclusion - Personal and Advertising Injury','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '182','858B','Broadened Coverage for Damage to Premises Rented to You','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '182X','182X','Silica and Silica Dust Excl','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '183X','183X','Volunteer Workers Excl','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '184X','184X','Total Pollution Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '184Y','184Y','Total Pollution Exclusion with a Building Heating Equipment and a Hostile Fire Exception','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '200A','200A','Scheduled - Jewelry',NULL,NULL,'N','N','None',NULL,NULL,NULL,NULL,NULL UNION ALL
		SELECT '220A','220A','Preventative Maintenance - Jewelry',NULL,NULL,'N','N','None',NULL,NULL,NULL,NULL,NULL UNION ALL
		SELECT '240A','240A','Unscheduled - Jewelry',NULL,NULL,'N','N','None',NULL,NULL,NULL,NULL,NULL UNION ALL
		SELECT '400','400A','Stock - Fire','RateStock','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '400A','400A','Stock - Fire','RateStock','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '401','401A','Stock-AOP and/or Exclude Stock for Sale From Theft','RateStockLimit','Stock','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '401A','401A','Stock-AOP and/or Exclude Stock for Sale From Theft','RateStockLimit','Stock','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '401B','401B','Stock Blanket','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '402','402A','Stock Peak','OtherStockPrem','?','Y','Y','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '402A','402A','Stock Peak','OtherStockPrem','?','Y','Y','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '402B','402B','Stock Peak Blanket','?','?','N','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '403','402A','Stock Peak','OtherStockPrem','?','Y','Y','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '404','402A','Stock Peak','OtherStockPrem','?','Y','Y','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '405','405','Customer Goods (Claims Only)','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '406','406','Memorandum/Consignment Items (Claims Only)','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '407','407A','Stock-Out of Safe','OtherStockPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '407A','407A','Stock-Out of Safe','OtherStockPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '408','408A','Stock Disappearance','OtherStockPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '408A','408A','Stock Disappearance','OtherStockPrem','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '409','409A','Scrap Gold for Refining','OtherStockPrem','Property','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '409A','409A','Scrap Gold for Refining','OtherStockPrem','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '410','410A','Money & Securities','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '410A','411A','Money & Securities - Peak Season','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '411','411A','Money & Securities - Peak Season','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '411A','411A','Money & Securities - Peak Season','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '412','411A','Money & Securities - Peak Season','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '413','411A','Money & Securities - Peak Season','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '414','414A','Tenants Improvements','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '414A','414A','Tenants Improvements','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '415','415A','Furniture, Fixtures, Safes, Modular Vault','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '415A','415A','Furniture, Fixtures, Safes, Modular Vault','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '416','416A','Patterns, Molds, Models and Dies','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '416A','416A','Patterns, Molds, Models and Dies','AllOther','Property','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '418A','418A','Loss Prevention Credit','AllOther','?','N','N','None',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '419','419A','Loss Free Renewal Credit','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '419A','419A','Loss Free Renewal Credit','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '419B','419B','Loss Free Renewal Credit -variable loss cost - manual prem','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '420','420A','Agreed Adjustment Amount','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '420A','420A','Agreed Adjustment Amount','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '421','421A','Show Windows Open to Business','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '421A','421A','Show Windows Open to Business','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '422','422','Window-In All Windows Open Un-Prot','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '423','423A','Show Windows Closed to Business - Protected','AllOther','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '423A','423A','Show Windows Closed to Business - Protected','AllOther','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '424','424A','Show Windows Closed to Business - Unprotected','AllOther','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '424A','424A','Show Windows Closed to Business - Unprotected','AllOther','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '425','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '426','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '427','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '428','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '429','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '430','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '431','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '432','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '433','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '434','421A','Show Windows Open to Business','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '435','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '436','423A','Show Windows Closed to Business - Protected','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '440','607A','USPS Registered Mail','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '441','441A','Armored Car Services','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '441A','441A','Armored Car Services','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '441B','441B','Armored Car Extension','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '442','442A','Specified Carriers','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '442A','442A','Specified Carriers','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '443','442A','Specified Carriers','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '444','442A','Specified Carriers','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '445','442A','Specified Carriers','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '445A','445A','Open Ocean Cargo','AllOther','?','N','N','None',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '446','442A','Specified Carriers','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '447','442A','Specified Carriers','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '448','448A','Unspecified Carriers','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '448A','448A','Unspecified Carriers','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '450','450A','Mail Shipments','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '450A','450A','Mail Shipments','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '455','455A','Professional Accounting Service Expense','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '455A','455A','Professional Accounting Service Expense','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '459','459A','Inland Marine - All Other','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '459A','459A','Inland Marine - All Other','AllOther','?','N','N','None',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '460','460A','Property in Safe/Vault','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '460A','460A','Property in Safe/Vault','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '461','461A','Goods in Custody of Jewelry Dealers','CustJlrDealPrem','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '461A','461A','Goods in Custody of Jewelry Dealers','CustJlrDealPrem','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '461B','461B','Custody of Jewelry Dealers Extension','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '463','463A','Maximum Travel Aggregate','TravelPrem','Maximum Travel Aggregate','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '463A','463A','Maximum Travel Aggregate','TravelPrem','Maximum Travel Aggregate','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '464','464A','Included Travel','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '464A','464A','Included Travel','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '465','465A','Principals, Officers and Employees','TravelRatePrem','Principals, Officers, and Employees','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '465A','465A','Principals, Officers and Employees','TravelRatePrem','Principals, Officers, and Employees','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '466','466A','Commissioned Salespersons','TravelRatePrem','Commissioned Salespersons','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '466A','466A','Commissioned Salespersons','TravelRatePrem','Commissioned Salespersons','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '467','467A','Property Otherwise Away','OthAway','Property Otherwise Away','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '467A','467A','Property Otherwise Away','OthAway','Property Otherwise Away','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '467B','467B','Otherwise Away Extension','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '468','468','Wind/Hail Deductible','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '469','469Y','Wind/Hail Exclusion','?','?','N','N',NULL,NULL,NULL,NULL,'09090','19' UNION ALL
		SELECT '469X','469X','Wind/Hail Exclusion - Direct Damage','?','?','N','N',NULL,NULL,NULL,NULL,'09090','19' UNION ALL
		SELECT '469Y','469Y','Wind/Hail Exclusion','?','?','N','N',NULL,NULL,NULL,NULL,'09090','19' UNION ALL
		SELECT '470','470A','Cost Plus - many various descriptions apply','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '470A','470A','Cost Plus - many various descriptions apply','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '470B','470B','Cost Plus - manual - many various descriptions apply','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '471','471A','Off Premise Showcases/Show Windows','AllOther','?','Y','Y',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '471A','471A','Off Premise Showcases/Show Windows','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '472','472A','Earthquake','AllOther','Earthquake (IM & BOP)','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '472A','472A','Earthquake','AllOther','Earthquake (IM & BOP)','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '472B','472B','Earthquake and Volcanic Eruption - Sublimit','?','Volcanic Eruption Sublimit','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '472C','472C','Sprinkler Leakage - Earthquake Extension - Building','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '472D','472D','Sprinkler Leakage - Earthquake Extension - Business Personal Property','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473','473A','Flood - Property/Building','AllOther','Flood Building and/or Stock (BOP-IM)','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473A','473A','Flood - Property/Building','AllOther','Flood Building (BOP)','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473B','473B','Flood - Business Personal Property','?','Flood BPP (BOP)','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473C','473C','Flood - All - Blanket','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473D','473D','Flood - Building - Blanket','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '473E','473E','Flood - Business Personal Property - Blanket','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '474','474A','Unattended Vehicle','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '474A','474A','Unattended Vehicle','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '474B','474B','Unattended Vehicle Extension','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '475','475A','Territory Extension','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '475A','475A','Territory Extension','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '476','476A','Shipment Extension','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '476A','476A','Shipment Extension','Shipments','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '477','477A','Earthquake with Manual Premium','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '477A','477A','Earthquake with Manual Premium','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '480','480A','Temp End - Stock','OtherStockPrem','Temporary Endorsement-Stock','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '480A','480A','Temp End - Stock','OtherStockPrem','Temporary Endorsement-Stock','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '481','481A','Temp End - Shipments (Inside Territory)','Shipments','Temp End - Shipments (Inside Territory)','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '481A','481A','Temp End - Shipments (Inside Territory)','Shipments','Temp End - Shipments (Inside Territory)','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '482','482A','Temp End - Shipments (Outside Territory)','Shipments','Temp End - Shipments (Outside Territory)','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '482A','482A','Temp End - Shipments (Outside Territory)','Shipments','Temp End - Shipments (Outside Territory)','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '483','483A','Temp End - Custody of Jewelry Dealers','CustJlrDealPrem','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '483A','483A','Temp End - Custody of Jewelry Dealers','CustJlrDealPrem','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '484','484A','Temp End - Off Premises Travel','TravelPrem','Temp End - Off Premise Travel','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '484A','484A','Temp End - Off Premises Travel','TravelPrem','Temp End - Off Premise Travel','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '485','485A','Temp End - Otherwise Away','OthAway','Temp End - Otherwise Away','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '485A','485A','Temp End - Otherwise Away','OthAway','Temp End - Otherwise Away','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '486','486A','Temporary Endorsement-Shows (Approved)','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '486A','486A','Temporary Endorsement-Shows (Approved)','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '487','487A','Temp Endorsement-Shows (Not Approved)','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '487A','487A','Temp Endorsement-Shows (Not Approved)','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '488','488A','Special Event Off Premises','Shows','Special Event Off Premise','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '488A','488A','Special Event Off Premises','Shows','Special Event Off Premise','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '489','489A','Temp End - All Other','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '489A','489A','Temp End - All Other','Shows','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '490','490A','Temp End-Going Out of Business Sale','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '490A','490A','Temp End-Going Out of Business Sale','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '491','491','Temporary Enodrsement','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '492','492','Temporary Endorsement','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '493','493','Temporary Endorsement','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '498','498A','Temp End - All Other','AllOther','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '498A','498A','Temp End - All Other','AllOther','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '499','499A','Temp End-Shows - All Other','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '499A','499A','Temp End-Shows - All Other','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '500','500','Building','?','?','N','N',NULL,NULL,NULL,'BLD','N/A','N/A' UNION ALL
		SELECT '509','509','Buildings-All Other','?','?','N','N',NULL,NULL,NULL,'BLD','N/A','N/A' UNION ALL
		SELECT '510','510','Furniture and Fixtures','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '511','511','Contents-Tenant\'S Improvements','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '512','512','Tools and Machinery','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '513','513','Contents-Patterns, Molds, Models And Dies','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '514','514','Contents-Giftware','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '515','515','Contents-Other Non-Jewelry Stock For Sale','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '516','516','Contents-Jewelry Stock For Sale $100 Or Less','?','?','N','N',NULL,'BPP',NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '519','519','Contents-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '530','530','Earnings','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '539','539','Earnings-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '540','540','Income','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '549','549','Income-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '550','550','Rents','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '559','559','Rents-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '560','560','Extra Expense','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '569','569','Extra Expense-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '570','570','Contingent Earnings','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '579','579','Contingent Earnings-All Other','?','?','N','N',NULL,NULL,'BIE',NULL,'N/A','N/A' UNION ALL
		SELECT '590','590','All Other','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '601','728A','Package Option - Gold (location)','BPP',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '601A','601A','Condominium Commercial Unit-Owners Optional Coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '602','602A','Primary Coverage for Personal Property of Others Not in the Jewelry Business','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '602A','602A','Primary Coverage for Personal Property of Others Not in the Jewelry Business','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '603','603A','Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '603A','603A','Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '603B','603B','Personal Property Off Premises Extension','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '603C','603C','Personal Property Off Premises Extension - Show or Exhibition','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '604','604A','Outdoor Property','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '604A','604A','Outdoor Property','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '605','605A','Equipment Breakdown - Tenant','AllOther','Equipment Breakdown','N','N',NULL,NULL,NULL,NULL,'09099','45' UNION ALL
		SELECT '605A','605A','Equipment Breakdown - Tenant','AllOther','Equipment Breakdown','N','N',NULL,NULL,NULL,NULL,'09099','45' UNION ALL
		SELECT '605B','605B','Equipment Breakdown - Owner','?','Equipment Breakdown','N','N',NULL,NULL,NULL,NULL,'09099','45' UNION ALL
		SELECT '605C','605C','Equipment Breakdown','AllOther','Equipment Breakdown','N','N','None',NULL,NULL,NULL,'09099','45' UNION ALL
		SELECT '606','827A','Utility Services - Direct Damage - Building','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','41' UNION ALL
		SELECT '607','607A','USPS Registered Mail','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '607A','607A','USPS Registered Mail','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '608','607A','USPS Registered Mail','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '609','609A','Treated Gemstones Liability+D6','Liability','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '609A','609A','Treated Gemstones Liability+D6','Liability','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '610','827B','Utility Services - Direct Damage - Business Personal Property','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','41' UNION ALL
		SELECT '611','958A','Utility Services - Time Element','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '615','605A','Equipment Breakdown - Tenant','AllOther','Equipment Breakdown','N','N',NULL,NULL,NULL,NULL,'09099','45' UNION ALL
		SELECT '630A','630A','Vacancy Permit','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '634A','634A','Dishonesty Cond Amend Shared Prem','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','5' UNION ALL
		SELECT '635B','635B','Jewelry Exclusion Endorsement with Business Income and Extra Expense Theft Limitation','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '635X','635X','Jewelry Exclusion Endorsement with Business Income and Extra Expense Theft Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '636A','636A','Manufactured Stock Valuation','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '637A','637A','Leasehold Interest - Tenants Lease Interest','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '637B','637B','Leasehold Interest - Improvements and Betterments','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '637C','637C','Leasehold Interest - Bonus Payments','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '637D','637D','Leasehold Interest - Prepaid Rent','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '639A','639A','Temporary Additional Named Insured','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '650','650A','USPS Express Mail','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '650A','650A','USPS Express Mail','Shipments','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '651','650A','USPS Express Mail','?','?','Y','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '652','652A','Tradeshow - While At','Shows','Tradeshows While At','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '652A','652A','Tradeshow - While At','Shows','Tradeshows While At','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '652B','652B','Tradeshow Blanket','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '653','653A','Tradeshow - To/From/While At','Shows','Tradeshows To/From/While At','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '653A','653A','Tradeshow - To/From/While At','Shows','Tradeshows To/From/While At','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '654','654A','Tradeshow - To/From','Shows','Tradeshows To/From','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '654A','654A','Tradeshow - To/From','Shows','Tradeshows To/From','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '655','655A','Shows-All Other','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '655A','655A','Shows-All Other','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '656','656A','Workmanship','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '656A','656A','Workmanship','AllOther','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '657','657A','To & From the Post Office','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '657A','657A','To & From the Post Office','TravelPrem','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '659','659A','Property all other endorsements','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '659A','659A','Propertu all other endorsements','AllOther','?','N','N','None',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '670','670','Wearing Extension','?','?','N','N','Temp',NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '699','699A','Temporary Endorsement - All Others, Shipment Coverage, Show Coverage, or Travel Coverage','AllOther','?','N','N','Temp',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '699A','699A','Temporary Endorsement - All Others, Shipment Coverage, Show Coverage, or Travel Coverage','AllOther','?','N','N','Temp',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '699B','699B','Temporary Endorsement - Additional Named Insured (Liability Coverage Only)','AllOther','?','N','N','Temp',NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '720','860A','Glass and Glass Expenses','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '721','861A','Signs (outdoor) - On Premises','?','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '725A','725A','Package Option - Silver (location)','?',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '726A','726A','Package Option - Platinum (location)',NULL,NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '728A','728A','Package Option - Gold (location)','BPP',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '740','740','Inland Marine Personal','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '741','741','Inland Marine Commercial','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '800','800A','Appraisal Equipment Including Master Stones','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '800A','800A','Appraisal Equipment Including Master Stones','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '803','803A','Newly Aquired or Constructed Property - Business Personal Property','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '803A','803A','Newly Aquired or Constructed Property - Business Personal Property','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '806','806A','Newly Aquired or Constructed Property - Building','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '806A','806A','Newly Aquired or Constructed Property - Building','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '809','809A','Fragile Articles Amendment','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '809A','809A','Fragile Articles Amendment','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '812','812A','Personal Effects','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '812A','812A','Personal Effects','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '813','604A','Outdoor Property','AllOther','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '815','815A','Signs Coverage (stationary) - Off Premises','AllOther','?','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '815A','815A','Signs Coverage (stationary) - Off Premises','AllOther','?','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '817','817A','Unspecified Personal Property Off Premises - Unattended Vehicle Coverage','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '817A','817A','Unspecified Personal Property Off Premises - Unattended Vehicle Coverage','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '818','818A','Property Temporarily Outside the Coverage Territory','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '818A','818A','Property Temporarily Outside the Coverage Territory','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '824','603A','Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '827','827A','Utility Services - Direct Damage - Building','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','41' UNION ALL
		SELECT '827A','827A','Utility Services - Direct Damage - Building','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','41' UNION ALL
		SELECT '827B','827B','Utility Services - Direct Damage - Business Personal Property','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','41' UNION ALL
		SELECT '840A','840A','Data Compromise - Response Expense Only','?','Data Compromise','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '840B','840B','Data Compromise - Response Expense and Defense Liability','?','Data Compromise','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '841','871G','Business Personal Property (Pawn) - Business Personal Property of Others Owned or Pledged','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '842','871F','Business Personal Property (Pawn) - Business Personal Property Obtained through Manufacturers and/or Wholesalers','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '843','871G','Business Personal Property (Pawn) - Business Personal Property of Others Owned or Pledged','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '845A','845A','Cyber Liability','?','Cyber Liability','Y','N',NULL,NULL,NULL,NULL,'90000','85' UNION ALL
		SELECT '849A','849A','Fur Limitation Amendment','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '850','850A','Building Coverage','Building','Property','Y','N',NULL,NULL,NULL,'BLD','01099','21' UNION ALL
		SELECT '850A','850A','Building Coverage','Building','Property','Y','N',NULL,NULL,NULL,'BLD','01099','21' UNION ALL
		SELECT '851','851A','Business Personal Property Seasonal Increase','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '851A','851A','Business Personal Property Seasonal Increase','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '852','871C','Business Personal Property (Scheduled) - Non-Jewelry Stock for Sale','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '852A','852A','Builders Risk Completed Value coverage - Newly constructed Building','?','Property','Y','N',NULL,NULL,NULL,'BLD','09099','19' UNION ALL
		SELECT '852B','852B','Builders Risk Completed Value coverage - Building Renovation','?','Property','Y','N',NULL,NULL,NULL,'BLD','09099','19' UNION ALL
		SELECT '853','871C','Business Personal Property (Scheduled) - Non-Jewelry Stock for Sale','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '854','854A','Liability and Medical Expense','Liability','Liability','Y','N',NULL,NULL,NULL,NULL,'10000','24' UNION ALL
		SELECT '854A','854A','Liability and Medical Expense','Liability','Liability','Y','N',NULL,NULL,NULL,NULL,'10000','24' UNION ALL
		SELECT '854B','854B','Business Liability Coverage - Amendment of Liability and Medical Expense Limits of Insurance','?','?','Y','N',NULL,NULL,NULL,NULL,'10000','24' UNION ALL
		SELECT '855','877A','Jewelry Limitation Amendment','BPP','Property','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '856','856A','Patterns, Dies, Molds, Models & Forms Amend','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '856A','856A','Patterns, Dies, Molds, Models & Forms Amend','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '857','871B','Business Personal Property (Scheduled) - Furniture & Fixtures, Improvements, Tools','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '858','858B','Broadened Coverage for Damage to Premises Rented to You','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '858B','858B','Broadened Coverage for Damage to Premises Rented to You','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '859','859A','Appraisal Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '859A','859A','Appraisal Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '860','860A','Glass and Glass Expenses','AllOther',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '860A','860A','Glass and Glass Expenses','AllOther',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '861','861A','Signs (outdoor) - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '861A','861A','Signs (outdoor) - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '862','862A','Employee Dishonesty','AllOther','Employee Dishonesty','Y','N',NULL,NULL,NULL,NULL,'09099','5' UNION ALL
		SELECT '862A','862A','Employee Dishonesty','AllOther','Employee Dishonesty','Y','N',NULL,NULL,NULL,NULL,'09099','5' UNION ALL
		SELECT '862B','862B','Employee Profit Sharing or Pension Plan (ERISA)','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','5' UNION ALL
		SELECT '862C','862C','Theft of Client\'s Property','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','5' UNION ALL
		SELECT '863','863','Silverware Pewter (Pak)','BPP','?','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '863A','863A','Computer Fraud and Funds Transfer Fraud','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','44' UNION ALL
		SELECT '864','864','Clock, Cases (Pak)','BPP','?','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '864A','864A','Loss of Rental Value - Landlord as Designated Payee','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','24' UNION ALL
		SELECT '865','861A','Signs (outdoor) - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','4' UNION ALL
		SELECT '866A','866A','Electronic Commerce (E-Commerce)','?','E-Commerce','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '867A','867A','Electronic Data','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','17' UNION ALL
		SELECT '868A','868A','Interruption of Computer Operations','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '869','869','Lessor\'s Risk Liability','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '870','870A','Hired Auto & Non-Owned Auto Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','13' UNION ALL
		SELECT '870A','870A','Hired Auto & Non-Owned Auto Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','13' UNION ALL
		SELECT '871','871A','Business Personal Property (Unscheduled)','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871A','871A','Business Personal Property (Unscheduled)','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871B','871B','Business Personal Property (Scheduled) - Furniture & Fixtures, Improvements, Tools','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871C','871C','Business Personal Property (Scheduled) - Non-Jewelry Stock for Sale','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871D','871D','Business Personal Property (Scheduled) - Jewelry Stock for Sale','?','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871E','871E','Business Personal Property (Pawn) - Business Personal Property Obtained through Manufacturers and/or Wholesalers','?','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871F','871F','Business Personal Property (Pawn) - Business Personal Property Obtained through Pawn Operations','?','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871G','871G','Business Personal Property (Pawn) - Business Personal Property of Others Owned or Pledged','?','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871H','871H','Business Personal Property (Patterns, Dies, Molds, Models, Forms)','?','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871I','871I','Business Personal Property (Pawn) – Obtained through Pawn Operations – Fire Arms','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871J','871J','Business Personal Property (Pawn) – Obtained through Pawn Operations – Other than Fire Arms','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871K','871K','Business Personal Property (Pawn) – Pledged – Fire Arms','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871L','871L','Business Personal Property (Pawn) – Pledged – Other than Fire Arms','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '871X','871X','Business Personal Property - Theft Exclusion','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '873','873','Oral Contractual Liability','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '874','874A','Business Income and Extra Expense Coverage (Actual Loss Sustained)','AllOther','Property','Y','N',NULL,NULL,'BIE',NULL,'00909','22' UNION ALL
		SELECT '874A','874A','Business Income and Extra Expense Coverage (Actual Loss Sustained)','AllOther','Property','Y','N',NULL,NULL,'BIE',NULL,'00909','22' UNION ALL
		SELECT '874B','874B','Business Income Changes - Dollar Limit for Business Income and Extra Expense Coverage','?','Property','Y','N',NULL,NULL,'BIE',NULL,'00909','22' UNION ALL
		SELECT '874C','874C','Business Income Changes - Time Period','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'00909','22' UNION ALL
		SELECT '874D','874D','Business Income and Extra Expense Revised Period of Indemnity','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'00909','22' UNION ALL
		SELECT '874E','874E','Business Income Changes - Beginning of the Period of Restoration for Wind and Hail Damage','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'00909','22' UNION ALL
		SELECT '874F','874F','Business Income Changes - Period of Restoration and Stated Limit for Wind and Hail','?','?','Y','N',NULL,NULL,'BIE',NULL,'00909','22' UNION ALL
		SELECT '874G','874G','Monthly Limit of Indemnity for Business Income and Extra Expense Coverage','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'00909','22' UNION ALL
		SELECT '874H','874H','Business Income and Extra Expense - Civil Authority Mileage Limit','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'00909','22' UNION ALL
		SELECT '875','812A','Personal Effects','BPP','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '876','877A','Jewelry Limitation Amendment','BPP','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '877','871D','Business Personal Property (Scheduled) - Jewelry Stock for Sale','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '877A','877A','Jewelry Limitation Amendment','BPP','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '877B','877B','Removal of Jewelry Limitation','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '878','602A','Primary Coverage for Personal Property of Others Not in the Jewelry Business','BPP','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '880','871B','Business Personal Property (Scheduled) - Furniture & Fixtures, Improvements, Tools','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'02099','22' UNION ALL
		SELECT '881','877A','Jewelry Limitation Amendment','BPP','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '890','890','Policy Fee','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '892','892','Policy Fee','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '895','871X','Business Personal Property - Theft Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '900','900A','Forgery or Alteration','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '900A','900A','Forgery or Alteration','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '902','902A','Money Orders and Counterfeit Money','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '902A','902A','Money Orders and Counterfeit Money','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '905','905','Burglary And Theft','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','08' UNION ALL
		SELECT '906','906A','Money and Securities - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','16' UNION ALL
		SELECT '906A','906A','Money and Securities - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','16' UNION ALL
		SELECT '906B','906B','Money and Securities - Season Increase - On Premises','?',NULL,'Y','N','Peak',NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '907','907A','Money and Securities - Off Premises','AllOther',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '907A','907A','Money and Securities - Off Premises','AllOther',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '907B','907B','Money and Securities - Season Increase - Off Premises','?',NULL,'Y','N','Peak',NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '908','906B','Money and Securities - Season Increase - On Premises','BPP','AllOther','Y','N','Peak',NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '909','907B','Money and Securities - Season Increase - Off Premises','AllOther',NULL,'Y','N','Peak',NULL,NULL,NULL,'09099','16' UNION ALL
		SELECT '910','910A','Stop Gap - Employers Liability+D265','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '910A','910A','Stop Gap - Employers Liability+D265','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '910B','910B','Stop Gap - Employers Liability  0-5 employees','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '911','911','Stop Gap-Bodily Injury by Disease','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '912','912','Stop Gap-Bodily Injury by Accident','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '917A','917A','Punitive Dmgs related to Cert Terr Excl','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '919X','919X','NBC & Radiological Excl','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '919Y','919Y','NBC and Terr Excl','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '920','920','Terrorism-Accept','AllOther',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '920A','920A','Cap on Losses from Certified Acts of Terrorism - Building/Property','?',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '920B','920B','Cap on Losses from Certified Acts of Terrorism - Business Personal Property','?',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '920C','920C','Cap on Losses from Certified Acts of Terrorism - Liability','?',NULL,'N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '921','921','Terrorism-Reject','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '921A','921A','Exclusion of Certified Acts of Terrorism - Building','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '921B','921B','Exclusion of Certified Acts of Terrorism - Business Personal Property','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '921C','921C','Exclusion of Certified Acts of Terrorism - Liability','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '922','922','Non-certified acts of terrorism excluded','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '923','923','Non-certified acts of terrorism Not excl','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '924','924','Loss Of Income - With A Limit','?','?','N','N',NULL,NULL,NULL,NULL,'00909','19' UNION ALL
		SELECT '925','925A','Package Option - Silver (policy)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '925A','925A','Package Option - Silver (policy)','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '926','926A','Package Option - Platinum (policy)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '926A','926A','Package Option - Platinum (policy)','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '927','927','Coverage Enhancement-Form W','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '928','928A','Package Option - Gold (policy)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '928A','928A','Package Option - Gold (policy)','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '930','930A','Inventory, Appraisal and Busines Income Accounting Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '930A','930A','Inventory, Appraisal and Busines Income Accounting Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '931A','931A','Discretionary Payroll Expense','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '932','932A','Debris Removal Amendment','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '932A','932A','Debris Removal Amendment','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '933A','933A','Identity Fraud Expense','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '934','934A','Fire Dept Service Charge','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '934A','934A','Fire Dept Service Charge','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '935','930A','Inventory, Appraisal and Busines Income Accounting Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '936','936A','Fire Extinguisher Systems Recharge Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '936A','936A','Fire Extinguisher Systems Recharge Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '937','937A','Pollutant Clean Up and Removal Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '937A','937A','Pollutant Clean Up and Removal Expense','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '938','938A','Lock Devices Replacement','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '938A','938A','Lock Devices Replacement','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '939','939A','Arson or Fraud Reward','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '939A','939A','Arson or Fraud Reward','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '940','940A','Business Income - Contract Penalty','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '940A','940A','Business Income - Contract Penalty','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '943','943A','Business Income - Dependent Properties','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '943A','943A','Business Income - Dependent Properties','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '943B','943B','Extra Expense - Dependent Properties','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '944A','944A','Business Income - Ordinary Payroll','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','22' UNION ALL
		SELECT '946','946A','Business Income and Extra Expense - Newly Acquired or Constructed Property','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '946A','946A','Business Income and Extra Expense - Newly Acquired or Constructed Property','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '947A','947A','Business Income - Extended Period of Indemnity','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '949','952A','Business Personal Property - Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '950A','950A','Blanket Building and Business Personal Property','?','?','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '950B','950B','Blanket Building','?','?','Y','N',NULL,NULL,NULL,'BLD','09099','19' UNION ALL
		SELECT '950C','950C','Blanket Business Personal Property','?','?','Y','N',NULL,'BPP',NULL,NULL,'09099','19' UNION ALL
		SELECT '952','952A','Business Personal Property - Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '952A','952A','Business Personal Property - Unspecified Personal Property Off Premises','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '953','953','Endorsement - Personal Liability','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '954','954A','Business Income - Pollutant Clean-up and Removal','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '954A','954A','Business Income - Pollutant Clean-up and Removal','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '956','818A','Property Temporarily Outside the Coverage Territory','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '956A','956A','Sinkhole coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '958','958A','Utility Services - Time Element','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '958A','958A','Utility Services - Time Element','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '959','959','Liability all other endorsements','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '960','960','Computer Hardware/Software','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','22' UNION ALL
		SELECT '961','961A','Accounts Receivables - On Premises','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','10' UNION ALL
		SELECT '961A','961A','Accounts Receivables - On Premises','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','10' UNION ALL
		SELECT '961B','961B','Accounts Receivables - Off Premises','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','10' UNION ALL
		SELECT '962','962A','Limited Piercing Liability (Above neck)','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '962A','962A','Limited Piercing Liability (Above neck)','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '963','963','Broad Form Vendors','Liability',NULL,'Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '964','964A','Fine Arts - Unscheduled','BPP','Property','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '964A','964A','Fine Arts - Unscheduled','BPP','Property','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '964B','964B','Fine Arts - Scheduled','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '965','965A','Valuable Papers and Records - On Premises','BPP','Property','Y','N',NULL,NULL,NULL,NULL,'09099','11' UNION ALL
		SELECT '965A','965A','Valuable Papers and Records - On Premises','BPP','Property','Y','N',NULL,'BPP',NULL,NULL,'09099','11' UNION ALL
		SELECT '965B','965B','Valuable Papers and Records - Off Premises','?',NULL,'Y','N',NULL,NULL,NULL,NULL,'09099','11' UNION ALL
		SELECT '966','966B','Mine Subsidence','AllOther','Mine Subsidence','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '966B','966B','Mine Subsidence','AllOther','Mine Subsidence','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT '967','967A','Employee Benefits Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '967A','967A','Employee Benefits Liability','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '967B','967B','Employee Benefits Liability Supplemental Extended Reporting Period','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968','968A','Additional Named Insured - Liability Coverage Only','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968A','968A','Additional Named Insured - Liability Coverage Only','Liability','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968B','968B','Additional Insured - Controlling Interest','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968C','968C','Additional Insured - State or Political Subdivisions - Permits Relating to Premises','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968D','968D','Additional Insured - Townhouse Associations','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968E','968E','Additional Insured - Mortgagee, Assignee, or Receiver','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968F','968F','Additional Insured - Owner or Other Interests From Whom Land Has Been Leased','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968G','968G','Additional Insured - Co-Owner of Insured Premises','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968H','968H','Additional Insured - Engineers, Architects, or Surveyors','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968I','968I','Additional Insured - Lessor of Leased Equipment','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968J','968J','Additional Insured - Vendor','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968K','968K','Additional Insured - Designated Person or Organization','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968L','968L','Additional Insured - Engineers, Architects, or Surveyors Not Engaged by the Named Insured','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968M','968M','Additional Insured - Owners, Lessees or Contractors - Scheduled Person or Organization','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968N','968N','Additional Insured - Owners, Lessees or Contractors - With Additional Insured Requirement in Construction Contract','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968O','968O','Additional Insured - Owners, Lessees or Contractors - Completed Operations','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968P','968P','Additional Insured - Grantor of Franchise Endorsement','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968Q','968Q','Additional Insured - Primary and Non-Contributing','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968R','968R','Additional Insured - Managers or Lessors of Premises','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968S','968S','Amendment of Insured Contract Definition','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '968T','968T','Additional Insured - State or Political Subdivisions - Permits','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '968U','968U','Additional Insured - Building Owners','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '969','969','Increased Costs-Ordinance or Law','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969A','969A','Increased Cost of Construction','AllOther',NULL,'N','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969B','969B','Ordinance or Law - Other than Earthquake - Coverage 1 - Coverage for Loss in the Value of the Undamaged Portion of the Building','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969C','969C','Ordinance or Law - Other than Earthquake - Combination Limit of Demolition Costs and Increased Cost of Construction','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969D','969D','Ordinance or Law - Earthquake - Coverage 1 - Coverage for Loss in the Value of the Undamaged Portion of the Building','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969E','969E','Ordinance or Law - Earthquake - Combination Limit of Demolition Costs and Increased Cost of Construction','?','Property','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969F','969F','Ordinance or Law - Other than Earthquake - Business Income and Extra Expense Coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '969G','969G','Ordinance or Law - Earthquake - Business Income and Extra Expense Coverage','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','40' UNION ALL
		SELECT '970','860A','Glass and Glass Expenses','?','?','N','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '971','860A','Glass and Glass Expenses','?','?','N','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '972','860A','Glass and Glass Expenses','?','?','N','N',NULL,NULL,NULL,NULL,'09099','3' UNION ALL
		SELECT '973','973A','Liability for Hazards of Lead','?','?','N','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '973A','973A','Liability for Hazards of Lead','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '973X','973X','Lead Liability Exclusion','?','?','Y','N',NULL,NULL,NULL,NULL,'90000','23' UNION ALL
		SELECT '974','974A','Water Backup and Sump Overflow (2008)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '974A','974A','Water Backup and Sump Overflow (2008)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '975','975','Rain','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '975A','975A','Changes - Limited Fungi','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '976','976','Snow','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '977','977','Temperature','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '977A','977A','Green Upgrades - Building','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '977B','977B','Green Upgrades - Business Personal Property','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '977C','977C','Green Upgrades - Extension of the Period of Restoration','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '977D','977D','Green Upgrades - Related Expense','?','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '978','978','Wind','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '978B','978B','Windstorm Loss mitigation Factors','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '978C','978C','NY Windstorm Protective Devices','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '979','979','Precipitation','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '979A','979A','Windstorm or Hail Loss to Roof - AVC','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '980','980','Automated Deductible','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '981','981','Aggregate/Catastrophe Treaty Reinsurance','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '982','982A','Accidental Spilling of Chemicals coverage','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '982A','982A','Accidental Spilling of Chemicals coverage','AllOther','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '984','974A','Water Backup and Sump Overflow (2008)','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '984A','984A','Newly Acquired Organizations - Businessonwers Liability','AllOther','?','Y','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '987','987A','Employment Related Practices Liability','Liability','EPLI','N','N',NULL,NULL,NULL,NULL,'90000','56' UNION ALL
		SELECT '987A','987A','Employment Related Practices Liability','Liability','EPLI','N','N',NULL,NULL,NULL,NULL,'90000','56' UNION ALL
		SELECT '987B','987B','Employment Related Practices Liability Supplemental Extended Reporting Period','Liability','EPLI','N','N',NULL,NULL,NULL,NULL,'90000','56' UNION ALL
		SELECT '987X','987X','Exclusion - Employment Related Practices Liability','?','?','N','N',NULL,NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT '988','988','EPLI - Underwritten','Liability','EPLI','N','N',NULL,NULL,NULL,NULL,'90000','56' UNION ALL
		SELECT '989','989','EPLI - Independent Contractors','Liability','EPLI','N','N',NULL,NULL,NULL,NULL,'90000','56' UNION ALL
		SELECT '990','990','Policy Fee','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '991','991','Municipal Taxes','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '992','992','Dividends','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '993','993','Other Taxes','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '994','994','Tax/Surcharge','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '995','995','Service Charge','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '997','997','Countersignature Fee','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT '999','999','Miscellaneous Coverages','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT 'BLDA','BLDA','Building - Automatic Increase','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT 'BPPA','BPPA','Business Personal Property - Automatic Increase','?','?','N','N',NULL,NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT 'MPA','MPA','Minimum Premium Adjustment Inland Marine','?','?','N','N',NULL,NULL,NULL,NULL,'NA','NA' UNION ALL
		SELECT 'MPAL','MPAL','Minimum Premium Adjustment Liability',NULL,NULL,'N','N','None',NULL,NULL,NULL,'90000','19' UNION ALL
		SELECT 'MPAP','MPAP','Minimum Premium Adjustment for Property',NULL,NULL,'N','N','None',NULL,NULL,NULL,'09099','19' UNION ALL
		SELECT 'SCH','SCH','Scheduled','?','Jewelry Property','N','N',NULL,NULL,NULL,NULL,'N/A','N/A' UNION ALL
		SELECT 'UNS','UNS','Unscheduled','?','?','N','N',NULL,NULL,NULL,NULL,'N/A','N/A'
	) 
AS Source --([CoverageTypeCode], [ConformedCoverageTypeCode], [ConformedCoverageTypeDesc], [ExhibitCoverageGroup], [ReinsuranceCoverageGroup], [SubjectToIRPM], [SubjectToExperienceCredit], [PeakOrTemp], [HSB_BPP_Group], [HSB_BIE_Group], [HSB_BLD_Group], [ISO_Stat_Code], [NISS_Stat_Code]) 
ON Target.CoverageTypeCode = Source.CoverageTypeCode
WHEN MATCHED THEN UPDATE
SET ConformedCoverageTypeCode = Source.ConformedCoverageTypeCode
	,ConformedCoverageTypeDesc = Source.ConformedCoverageTypeDesc
	,ExhibitCoverageGroup = Source.ExhibitCoverageGroup
	,ReinsuranceCoverageGroup = Source.ReinsuranceCoverageGroup
	,SubjectToIRPM = Source.SubjectToIRPM
	,SubjectToExperienceCredit = Source.SubjectToExperienceCredit
	,PeakOrTemp = Source.PeakOrTemp
	,HSB_BPP_Group = Source.HSB_BPP_Group
	,HSB_BIE_Group = Source.HSB_BIE_Group
	,HSB_BLD_Group = Source.HSB_BLD_Group
	,ISO_Stat_Code = Source.ISO_Stat_Code
	,NISS_Stat_Code = Source.NISS_Stat_Code
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (CoverageTypeCode, ConformedCoverageTypeCode, ConformedCoverageTypeDesc, ExhibitCoverageGroup, ReinsuranceCoverageGroup, SubjectToIRPM, SubjectToExperienceCredit, PeakOrTemp, HSB_BPP_Group, HSB_BIE_Group, HSB_BLD_Group, ISO_Stat_Code, NISS_Stat_Code) 
	VALUES (CoverageTypeCode, ConformedCoverageTypeCode, ConformedCoverageTypeDesc, ExhibitCoverageGroup, ReinsuranceCoverageGroup, SubjectToIRPM, SubjectToExperienceCredit, PeakOrTemp, HSB_BPP_Group, HSB_BIE_Group, HSB_BLD_Group, ISO_Stat_Code, NISS_Stat_Code) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;