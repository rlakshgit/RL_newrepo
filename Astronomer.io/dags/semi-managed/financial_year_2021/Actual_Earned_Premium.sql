
SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(fMPE.DateKey)					AS AccountingDate
	,dPol.ConformedPolicyNumber													AS PolicyNumber
	,dLOB.LOBProductLineCode													AS ProductLine
	,SUM(fMPE.PremiumEarned)													AS EarnedPremium
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate

FROM DW_DDS_CURRENT.bi_dds.FactMonthlyPremiumEarned AS fMPE

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBusType
	ON dBusType.BusinessTypeKey = fMPE.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fMPE.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fMPE.PolicyKey

WHERE	1=1
		AND LEFT(fMPE.DateKey,4) >= YEAR(SYSDATETIME())-2
		AND CAST(CAST(fMPE.DateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY 
	fMPE.DateKey
	,dPol.ConformedPolicyNumber	
	,dLOB.LOBProductLineCode
