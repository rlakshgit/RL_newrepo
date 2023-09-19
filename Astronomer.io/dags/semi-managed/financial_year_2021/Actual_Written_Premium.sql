
SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(fPW.AccountingDateKey)		AS AccountingDate
	,dPol.ConformedPolicyNumber													AS PolicyNumber
	,dLOB.LOBProductLineCode													AS ProductLine
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 11 THEN 'Submission' 
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 'Submission' 
			ELSE 'Non-Submission' 
	 END																		AS TransactionTypeGroup
	,SUM(fPW.PremiumWritten)													AS WrittenPremium
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))									AS DataQueryTillDate

FROM DW_DDS_CURRENT.bi_dds.FactPremiumWritten AS fPW

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBusType
	ON dBusType.BusinessTypeKey = fPW.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fPW.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fPW.PolicyKey

WHERE	1=1
		AND LEFT(fPW.AccountingDateKey,4) >= YEAR(SYSDATETIME())-2
		AND CAST(CAST(fPW.AccountingDateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY 
	fPW.AccountingDateKey
	,dPol.ConformedPolicyNumber	
	,dLOB.LOBProductLineCode
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 11 THEN 'Submission' 
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 'Submission' 
			ELSE 'Non-Submission' 
	 END
