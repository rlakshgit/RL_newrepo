
;WITH TranCount AS (
SELECT DISTINCT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyAccountingDateKey)		AS AccountingDate
	,dPol.ConformedPolicyNumber														AS PolicyNumber
	,CASE WHEN dPol.AccountSegment = 'Personal Lines' THEN 'PL' ELSE 'CL' END		AS ProductLine
	,CASE	WHEN dPol.SourceSystem = 'PAS' THEN 'TN'+dPol.PolicyGroup+'MN'+CAST(dPol.PolicyVersion AS varchar(250))
			ELSE 'J_'+CAST(dPol.JobNumber AS varchar(250))	
	 END																			AS JobNumberModified
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 11 THEN 'Submission' 
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 'Submission' 
			ELSE 'Non-Submission' 
	 END																			AS TransactionTypeGroup
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 3 THEN -1 
			WHEN dPol.PolicyTransactionTypeKey = 7 THEN 0
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 0
			ELSE 1
	 END																			AS TransactionCount
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))									AS DataQueryTillDate

FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

WHERE	1=1
		AND LEFT(dPol.PolicyAccountingDateKey,4) >= YEAR(SYSDATETIME())-2 
		AND dPol.PolicyAccountingDateKey <= DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(DATEADD(day,-1,CAST(SYSDATETIME() AS date)))
		AND CASE	WHEN dPol.SourceSystem = 'GW' and dPol.PolicyStatus = 'Bound' THEN 1 
					WHEN dPol.SourceSystem = 'PAS' and dPol.PolicyIssueDate <> -1 THEN 1 
					ELSE 0 END = 1
)
SELECT 
	AccountingDate
	,PolicyNumber
	,ProductLine
	,TransactionTypeGroup
	,DataQueryTillDate
	,sum(TransactionCount) AS TransactionCount
FROM TranCount
GROUP BY 
	AccountingDate
	,PolicyNumber
	,ProductLine
	,TransactionTypeGroup
	,DataQueryTillDate
