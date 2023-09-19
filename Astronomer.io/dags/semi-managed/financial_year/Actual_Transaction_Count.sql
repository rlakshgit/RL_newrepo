
;WITH Orig AS 
(
	SELECT
		dPol.ConformedPolicyNumber	
		,MIN(dPol.TransactionEffectiveDate)			AS PolicyOrigOrigDate

	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

	WHERE	1 = 1
			AND dPol.PolicyVersion <> 0
			AND CASE	WHEN dPol.SourceSystem = 'GW' AND dPol.PolicyStatus = 'Bound' THEN 1 
						WHEN dPol.SourceSystem = 'PAS' AND dPol.PolicyIssueDate<> -1 AND dPol.PolicyStatus NOT IN ('V','IT','DE','DA','DC') THEN 1 
						ELSE 0 END = 1
			--AND dPol.AccountSegment = 'Personal Lines'
			--AND dPol.PolicyType = 'PJ'
	GROUP BY 
		dPol.ConformedPolicyNumber
)
,assumedDetails AS
(
	SELECT
		AccountingDate																AS AccountingDate
		,PolicyNumber																AS PolicyNumber
		,'PL'																		AS ProductLine
		,ProductCode																AS ProductCode
		,TermNumber																	AS TermNumber
		,'-1'																		AS ModelNumber
		,CASE	WHEN TransactionType = 'NEW' THEN 'Submission'
				WHEN RankPN = 1 AND TransactionType = 'ENDORSEMENT' THEN 'Submission'
				ELSE 'Non-Submission'
		 END																		AS TransactionTypeGroup
		,NULL																		AS InsuredCountryCode
		,NULL																		AS PolicyTenure
		,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate
		,CASE	WHEN TransactionType IN ('NEW','RENEWAL','REINSTATEMENT') THEN 1
				WHEN RankPN = 1 AND TransactionType = 'ENDORSEMENT' THEN 1
				WHEN TransactionType = 'CANCELLATION' THEN -1
				ELSE 0
		 END																		AS TransactionCount

	FROM (
		SELECT
			LEFT(aPrem.PolicyNumber,CHARINDEX('-', aPrem.PolicyNumber,4)-1)							AS PolicyNumber
			,CAST(SUBSTRING(aPrem.PolicyNumber,CHARINDEX('-', aPrem.PolicyNumber,4)+1,2) AS INT)	AS TermNumber
			,aPrem.TransactionType																	AS TransactionType
			,CAST(aPrem.AccountingDate AS date)														AS AccountingDate
			,aPrem.Source																			AS AssumedSource
			,CASE WHEN CompanyCode = 'JMIC' THEN 'PJ' END											AS ProductCode
			,aPrem.TotalWrittenPremium																AS WrittenPremium
			,DENSE_RANK() OVER(PARTITION BY PolicyNumber
							ORDER BY	AccountingDate
										,TransactionEffectiveDate
										,CAST(SUBSTRING(aPrem.PolicyNumber,CHARINDEX('-', aPrem.PolicyNumber,4)+1,2) AS INT)
										,CASE	WHEN TransactionType = 'NEW' THEN 1
												WHEN TransactionType IN ('RENEWAL','REINSTATEMENT') THEN 2
												ELSE 3 END ASC) RankPN

		FROM GW_Reporting.PartnerData.Earnings AS aPrem
		WHERE	aPrem.TotalWrittenPremium != 0
				AND PolicyNumber IS NOT NULL
				AND YEAR(aPrem.AccountingDate) >= YEAR(SYSDATETIME())-4
				AND CAST(aPrem.AccountingDate AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
	)t
)
,dwData AS
(
	SELECT DISTINCT
		dPol.JobNumber
		,dPol.PolicyAccountingDateKey	AS AccountingDate
		,dGeo.GeographyCountryCode		AS CountryCode

	FROM DW_DDS_CURRENT.bi_dds.DimPolicy as dPol

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dGeo
		ON dGeo.GeographyKey = dPol.PrimaryInsuredGeographyKey
	WHERE	1=1
			AND LEFT(dPol.PolicyAccountingDateKey,4) >= YEAR(SYSDATETIME())-4
			AND dPol.PolicyAccountingDateKey <= DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(DATEADD(day,-1,CAST(SYSDATETIME() AS date)))
			AND dPol.SourceSystem = 'GW'
			AND dPol.PolicyStatus = 'Bound'
)
,TranCount AS
(
	SELECT DISTINCT
		DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dwData.AccountingDate)		AS AccountingDate
		,pc_policyperiod.PolicyNumber												AS PolicyNumber
		,CASE	WHEN pctl_segment.NAME = 'Personal Lines' THEN 'PL' ELSE 'CL' END	AS ProductLine
		,CASE	WHEN pc_policyperiod.PolicyNumber = '55-013722' THEN 'Cargo'
				WHEN pc_policyline.PatternCode = 'ILMLine' THEN 'IM'
				WHEN pc_policyline.PatternCode = 'BOPLine' THEN 'CMP'
				WHEN pc_policyline.PatternCode = 'UMBLine' THEN 'CMP'
				WHEN pc_policyline.PatternCode = 'JMICPJLine' THEN 'PJ'
				WHEN pc_policyline.PatternCode = 'JPALine' THEN 'PA'
		 END																		AS ProductCode
		,pc_job.JobNumber															AS JobNumber
		,CAST(pc_policyperiod.TermNumber AS varchar(20))							AS TermNumber
		,pc_policyperiod.ModelNumber												AS ModelNumber
		,CASE	WHEN pctl_job.NAME = 'Submission' THEN 'Submission'
				WHEN pctl_job.NAME = 'Issuance' THEN 'Submission'
				ELSE 'Non-Submission'
		 END																		AS TransactionTypeGroup
		,CASE	WHEN pctl_job.NAME = 'Cancellation' THEN -1
				WHEN pctl_job.NAME = 'Policy Change' THEN 0
				WHEN pctl_job.NAME = 'Issuance' THEN 0
				ELSE 1
		 END																		AS TransactionCount
		,dwData.CountryCode															AS InsuredCountryCode
		,CAST(DATEDIFF(YEAR
					,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(Orig.PolicyOrigOrigDate)
					,CAST(pc_policyperiod.PeriodStart AS date))	AS char)					AS PolicyTenure

	FROM PolicyCenter.dbo.pc_policyperiod AS pc_policyperiod

		INNER JOIN PolicyCenter.dbo.pctl_segment AS pctl_segment
		ON pctl_segment.ID = pc_policyperiod.Segment

		INNER JOIN PolicyCenter.dbo.pc_job AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN PolicyCenter.dbo.pctl_job AS pctl_job
		ON pctl_job.ID = pc_job.Subtype

		INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		INNER JOIN PolicyCenter.dbo.pc_policyline AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN dwData
		ON dwData.JobNumber = pc_job.JobNumber

		LEFT JOIN Orig 
		ON Orig.ConformedPolicyNumber = pc_policyperiod.PolicyNumber

	WHERE	1 = 1
			AND pctl_policyperiodstatus.NAME = 'Bound'
			AND LEFT(dwData.AccountingDate,4) >= YEAR(SYSDATETIME())-4
			AND dwData.AccountingDate <= DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(DATEADD(day,-1,CAST(SYSDATETIME() AS date)))
)
SELECT
	AccountingDate
	,PolicyNumber
	,ProductLine
	,ProductCode
	,TermNumber
	,CAST(ModelNumber as VARCHAR) ModelNumber
	,TransactionTypeGroup
	,InsuredCountryCode
	,PolicyTenure
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date)) AS DataQueryTillDate
	,sum(TransactionCount) AS TransactionCount
FROM TranCount
GROUP BY
	AccountingDate
	,PolicyNumber
	,ProductLine
	,ProductCode
	,TermNumber
	,ModelNumber
	,TransactionTypeGroup
	,InsuredCountryCode
	,PolicyTenure

UNION ALL

SELECT * FROM assumedDetails
