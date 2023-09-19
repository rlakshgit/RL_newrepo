
SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(vFClaim.AccountingDateKey)	AS AccountingDate
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
	,SUM(vFClaim.[Claim Incurred Loss Net Recovery])							AS IncurredLossNetRecovery
	,SUM(vFClaim.[Claim Incurred ALAE Net Recovery])							AS IncurredALAENetRecovery
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate

FROM DW_DDS_CURRENT.bief_dds.vwFactClaim AS vFClaim

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBusType
	ON dBusType.BusinessTypeKey = vFClaim.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = vFClaim.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = vFClaim.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dGeo
	ON dGeo.GeographyKey = dPol.PrimaryInsuredGeographyKey

WHERE	1=1
		AND LEFT(vFClaim.AccountingDateKey,4) >= YEAR(SYSDATETIME())-4
		AND CAST(CAST(vFClaim.AccountingDateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY 
	vFClaim.AccountingDateKey
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
