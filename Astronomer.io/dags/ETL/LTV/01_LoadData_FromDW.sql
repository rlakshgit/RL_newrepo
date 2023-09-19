--DECLARE @EndDateKey AS INT = 20220131

;WITH Orig AS 
(
	SELECT
		dPol.ConformedPolicyNumber	
		,MIN(dPol.TransactionEffectiveDate)			AS PolicyOrigOrigDate
		,CONVERT (date,'{date}')                    AS bq_load_date


	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

	WHERE	1 = 1
			AND dPol.PolicyVersion <> 0
			AND CASE	WHEN dPol.SourceSystem = 'GW' AND dPol.PolicyStatus = 'Bound' THEN 1 
						WHEN dPol.SourceSystem = 'PAS' AND dPol.PolicyIssueDate<> -1 AND dPol.PolicyStatus NOT IN ('V','IT','DE','DA','DC') THEN 1 
						ELSE 0 END = 1
			AND dPol.AccountSegment = 'Personal Lines'
			AND dPol.PolicyType = 'PJ'
	GROUP BY 
		dPol.ConformedPolicyNumber
)
,MarketingAcct AS
(
	SELECT DISTINCT 
		AccountNumber
	FROM DW_INT.MarketingEmail.MarketingContact
)
SELECT 
	AccountNumber
	,PolicyNumber
	,JobNumber
	,TermNumber
	,ModelNumber
	,PeriodEffDate
	,TransEffDate
	,JobCloseDate
	,AccountingDate
	,PolicyYear
	,PolicyMonth
	,SourceOfBusiness
	,InsuredAge
	,InsuredGender
	,InsuredState
	,InsuredPostalCode
	,InsuredCountry
	,ItemNumber
	,RiskType
	,CoverageTypeCode
	,IsInactive
	,ItemClass
	,ItemLimit
	,RiskState
	,RiskCountry 
	,RiskPostalCode
	,WearerAge
	,DENSE_RANK() OVER(PARTITION BY JobNumber
									,ItemNumber
						ORDER BY	ItemLimit DESC) AS ItemRank

FROM 
(
SELECT DISTINCT
	dAccount.AccountNumber																					AS AccountNumber
	,dPol.ConformedPolicyNumber																				AS PolicyNumber
	,dPol.JobNumber																							AS JobNumber
	--,'J_' + dPol.JobNumber																					AS JobNumber
	,dPol.PolicyGroup																						AS TermNumber
	,dPol.PolicyVersion																						AS ModelNumber
	,dPol.PolicyEffectiveDate																				AS PeriodEffDate
	,dPol.TransactionEffectiveDate																			AS TransEffDate
	,dPol.JobCloseDate																						AS JobCloseDate
	,dPol.PolicyAccountingDateKey																			AS AccountingDate
	,YEAR(DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))							AS PolicyYear
	,MONTH(DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))							AS PolicyMonth
	,dPol.SourceOfBusiness																					AS SourceOfBusiness
	,COALESCE(DATEDIFF(YEAR
						,dPCont.BirthDate
						,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))
			  ,-1)																							AS InsuredAge
	,CASE	WHEN dPCont.Gender = '1' THEN 'FEMALE'
			WHEN dPCont.Gender = '2' THEN 'MALE'
			ELSE 'Unknown' 
	 END																									AS InsuredGender
	,COALESCE(dPGeo.GeographyStateCode,'Unknown') 															AS InsuredState
	,COALESCE(dPCont.PrimaryAddressPostalCode,dPCont.MailingAddressPostalCode)								AS InsuredPostalCode
	,COALESCE(dPGeo.GeographyCountryCode,'Unknown') 														AS InsuredCountry
	
	,dRSI.ItemNumber																						AS ItemNumber
	,dRSC.RiskType																							AS RiskType
	,dCType.ConformedCoverageTypeCode																		AS CoverageTypeCode
	,CASE	WHEN dRSC.RiskType = 'LOB' AND COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0) <= 0 THEN 1
			ELSE dRSC.IsInactive
	 END																									AS IsInactive
	,dIC.ItemClassDescription																				AS ItemClass
	,COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0)														AS ItemLimit
	,dRGeo.GeographyStateCode																				AS RiskState
	,dRGeo.GeographyCountryCode																				AS RiskCountry 
	,dRSI.ItemPostalCode																					AS RiskPostalCode
	,COALESCE(DATEDIFF(YEAR
				,wearerCont.BirthDate
				,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))
			  ,-1)																							AS WearerAge
	,DENSE_RANK() OVER(PARTITION BY dPol.JobNumber
                       ORDER BY		CASE WHEN dPol.PolicyStatus = 'Bound' THEN 1 ELSE 0 END DESC
									,CAST(dPol.PolicyVersion AS INT) 
									,dPol.JobCloseDate DESC
									,dPol.PolicyWrittenDate DESC
									,dPol.JobSubmissionDate DESC
									,CASE 
										WHEN dPol.PolicyStatus = 'Bound' THEN 0
										WHEN dPol.PolicyStatus = 'Renewing' THEN 1 
										WHEN dPol.PolicyStatus = 'Quoted' THEN 2
										WHEN dPol.PolicyStatus = 'Draft' THEN 3
										WHEN dPol.PolicyStatus = 'NonRenewed' THEN 4
										WHEN dPol.PolicyStatus = 'Declined' THEN 5
										WHEN dPol.PolicyStatus = 'NotTaken' THEN 6
										WHEN dPol.PolicyStatus = 'Expired' THEN 7
										WHEN dPol.PolicyStatus = 'Withdrawn' THEN 8
										ELSE 99
									 END ASC	
									,dPol.PolicyKey desc)													AS JobRank

FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAccount
	ON dAccount.AccountKey = dPol.AccountKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dPCont
	ON dPCont.ContactKey = dPol.PolicyInsuredContactKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dPGeo
	ON dPGeo.GeographyKey = dPCont.PrimaryAddressGeographyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentCore AS dRSC
	ON dRSC.PolicyKey = dPol.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentJewelryItem AS dRSI
	ON dRSI.RiskSegmentKey = dRSC.RiskSegmentJewelryItemKey
	--AND dRSC.RiskType = 'ITEM'

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimItemClass AS dIC
	ON dIC.ItemClassKey = dRSI.ItemClassKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverage AS dCov 
	ON dCov.RiskSegmentKey = dRSC.RiskSegmentKey
	AND CoverageKey <> - 1

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverageType AS dCType
	ON dCType.CoverageTypeKey = dCov.CoverageTypeKey
	--AND dCType.ConformedCoverageTypeCode = 'SCH'

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dRGeo
	ON dRGeo.GeographyKey = dRSI.ItemGeographyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS wearerCont
	ON wearerCont.ContactKey = dRSI.ItemLocatedWithContactKey

	INNER JOIN DW_DDS_CURRENT.bi_keymap.KeymapDimPolicy KDP
	ON dPol.PolicyKey = KDP.PolicyKey	

	INNER JOIN Orig 
	ON Orig.ConformedPolicyNumber = dPol.ConformedPolicyNumber

	INNER JOIN MarketingAcct
	ON MarketingAcct.AccountNumber = dAccount.AccountNumber
	
WHERE	1=1 
		AND dPol.AccountSegment = 'Personal Lines'
		AND dPol.PolicyType = 'PJ'
		AND dPol.JobNumber IS NOT NULL
		--AND dAccount.AccountNumber = '3000289307'
		
)t
WHERE JobRank = 1
AND COALESCE(t.JobCloseDate,t.TransEffDate) >= {StartDate}
AND COALESCE(t.JobCloseDate,t.TransEffDate) < {EndDate}
