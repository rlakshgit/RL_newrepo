
SELECT 
	PolicyNumber
	,ProductLine
	,convert(datetime, convert(varchar(10),DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(PolicyOrigOrigDate)))  AS PolicyOrigOrigDate 
FROM
(
SELECT
    dPol.ConformedPolicyNumber						AS PolicyNumber
	,CASE	WHEN dPol.AccountSegment = 'Personal Lines' AND dPol.PolicyType = 'PJ' THEN 'PJ'
			WHEN dPol.AccountSegment = 'Personal Lines' AND dPol.PolicyType = 'JPAS' THEN 'PA' 
			ELSE 'CL' END							AS ProductLine
    ,MIN(dPol.TransactionEffectiveDate)				AS PolicyOrigOrigDate

FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

WHERE	1 = 1
		AND dPol.PolicyVersion <> 0
		AND CASE	WHEN dPol.SourceSystem = 'GW' AND dPol.PolicyStatus = 'Bound' THEN 1 
					WHEN dPol.SourceSystem = 'PAS' AND dPol.PolicyIssueDate<> -1 AND dPol.PolicyStatus NOT IN ('V','IT','DE','DA','DC') THEN 1 
					ELSE 0 END = 1
		
GROUP BY 
	dPol.ConformedPolicyNumber
	,CASE	WHEN dPol.AccountSegment = 'Personal Lines' AND dPol.PolicyType = 'PJ' THEN 'PJ'
			WHEN dPol.AccountSegment = 'Personal Lines' AND dPol.PolicyType = 'JPAS' THEN 'PA' 
			ELSE 'CL' END	
)t
WHERE	PolicyOrigOrigDate >= 20130101
		AND ProductLine = 'PJ'
