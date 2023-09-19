
SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(vFClaim.AccountingDateKey)	AS AccountingDate
	,dPol.ConformedPolicyNumber													AS PolicyNumber
	,dLOB.LOBProductLineCode													AS ProductLine
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

WHERE	1=1
		AND LEFT(vFClaim.AccountingDateKey,4) >= YEAR(SYSDATETIME())-2
		AND CAST(CAST(vFClaim.AccountingDateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY 
	vFClaim.AccountingDateKey
	,dPol.ConformedPolicyNumber	
	,dLOB.LOBProductLineCode
