
SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(fMPE.DateKey)					AS AccountingDate
	,dPol.ConformedPolicyNumber													AS PolicyNumber	
	,dLOB.LOBProductLineCode													AS ProductLine
	,CASE
		WHEN dPol.ConformedPolicyNumber = '55-013722' THEN 'Cargo'
		WHEN dLOB.LOBCode = 'JB' THEN 'IM'
		WHEN dLOB.LOBCode = 'JS' THEN 'IM'
		WHEN dLOB.LOBCode = 'BOP' THEN 'CMP'
		WHEN dLOB.LOBCode = 'UMB' THEN 'CMP'
		ELSE dLOB.LOBCode
	 END																		AS ProductCode
	,CAST(dPol.PolicyGroup as INT)												AS TermNumber
	,dPol.PolicyVersion															AS ModelNumber
	,dGeo.GeographyCountryCode													AS InsuredCountryCode
	,SUM(fMPE.PremiumEarned)													AS EarnedPremium
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate

FROM DW_DDS_CURRENT.bi_dds.FactMonthlyPremiumEarned AS fMPE

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBusType
	ON dBusType.BusinessTypeKey = fMPE.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fMPE.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fMPE.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dGeo
	ON dGeo.GeographyKey = dPol.PrimaryInsuredGeographyKey

WHERE	1=1
		AND LEFT(fMPE.DateKey,4) >= YEAR(SYSDATETIME())-2
		AND CAST(CAST(fMPE.DateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY 
	fMPE.DateKey
	,dPol.ConformedPolicyNumber		
	,dLOB.LOBProductLineCode
	,CASE
		WHEN dPol.ConformedPolicyNumber = '55-013722' THEN 'Cargo'
		WHEN dLOB.LOBCode = 'JB' THEN 'IM'
		WHEN dLOB.LOBCode = 'JS' THEN 'IM'
		WHEN dLOB.LOBCode = 'BOP' THEN 'CMP'
		WHEN dLOB.LOBCode = 'UMB' THEN 'CMP'
		ELSE dLOB.LOBCode
	 END	
	,dPol.PolicyGroup
	,dPol.PolicyVersion
	,dGeo.GeographyCountryCode
