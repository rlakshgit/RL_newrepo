
;WITH assumedDetails AS
(
	SELECT
		AccountingDate																AS AccountingDate
		,PolicyNumber																AS PolicyNumber
		,'PL'																		AS ProductLine
		,ProductCode																AS ProductCode
		,TermNumber																	AS TermNumber
		,-1																			AS ModelNumber
		,CASE	WHEN TransactionType = 'NEW' THEN 'Submission'
				WHEN RankPN = 1 AND TransactionType = 'ENDORSEMENT' THEN 'Submission'
				ELSE 'Non-Submission'
		 END																		AS TransactionTypeGroup
		,NULL																		AS InsuredCountryCode
		,WrittenPremium																AS WrittenPremium
		,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate

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

SELECT
	DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(fPW.AccountingDateKey)		AS AccountingDate
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
	,dPol.PolicyGroup															AS TermNumber
	,dPol.PolicyVersion															AS ModelNumber
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 11 THEN 'Submission'
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 'Submission'
			ELSE 'Non-Submission'
	 END																		AS TransactionTypeGroup
	,dGeo.GeographyCountryCode													AS InsuredCountryCode
	,SUM(fPW.PremiumWritten)													AS WrittenPremium
	,DATEADD(day,-1,CAST(SYSDATETIME() AS date))								AS DataQueryTillDate

FROM DW_DDS_CURRENT.bi_dds.FactPremiumWritten AS fPW

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBusType
	ON dBusType.BusinessTypeKey = fPW.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fPW.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fPW.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dGeo
	ON dGeo.GeographyKey = dPol.PrimaryInsuredGeographyKey

WHERE	1=1
		AND LEFT(fPW.AccountingDateKey,4) >= YEAR(SYSDATETIME())-4
		AND CAST(CAST(fPW.AccountingDateKey AS VARCHAR(10)) AS date) <= DATEADD(day,-1,CAST(SYSDATETIME() AS date))
		AND dBusType.BusinessTypeDesc = 'Direct'

GROUP BY
	fPW.AccountingDateKey
	,dLOB.LOBProductLineCode
	,CASE
		WHEN dPol.ConformedPolicyNumber = '55-013722' THEN 'Cargo'
		WHEN dLOB.LOBCode = 'JB' THEN 'IM'
		WHEN dLOB.LOBCode = 'JS' THEN 'IM'
		WHEN dLOB.LOBCode = 'BOP' THEN 'CMP'
		WHEN dLOB.LOBCode = 'UMB' THEN 'CMP'
		ELSE dLOB.LOBCode
	 END
	,dPol.ConformedPolicyNumber
	,dPol.PolicyGroup
	,dPol.PolicyVersion
	,CASE	WHEN dPol.PolicyTransactionTypeKey = 11 THEN 'Submission'
			WHEN dPol.PolicyTransactionTypeKey = 5 THEN 'Submission'
			ELSE 'Non-Submission'
	 END
	,dGeo.GeographyCountryCode

UNION ALL

SELECT * FROM assumedDetails
