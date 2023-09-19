
SELECT
	t.ConformedPolicyNumber AS PolicyNumber
	,t.SubPremBin			AS PremiumBin
	,t.SubAcctSeg			AS Segment
FROM (
	SELECT 
		dPol.ConformedPolicyNumber
		,fPW.AccountingDateKey
		,SUM(fPW.PremiumWritten) AS SubPrem
		,CASE
			WHEN dPol.ConformedPolicyNumber = '55-013722' THEN 'Cargo'
			WHEN SUM(fPW.PremiumWritten) <= 5000  THEN '<= 5,000'
			WHEN SUM(fPW.PremiumWritten) <= 25000 THEN '5,001 to 25,000'
			WHEN SUM(fPW.PremiumWritten) >  25000 THEN '>= 25,001' 
			END AS SubPremBin
		,CASE WHEN dAgency.AgencyMasterName like 'Wexler%' THEN 'JM - Wexler' ELSE 'JM - All Other' END AS SubAcctSeg
		,DENSE_RANK() OVER(PARTITION BY dPol.ConformedPolicyNumber ORDER BY fPW.AccountingDateKey ASC,dPol.PolicyTransactionTypeKey DESC) AS RankSelect

	FROM DW_DDS_CURRENT.bi_dds.FactPremiumWritten as fPW

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType as dBType
		ON dBType.BusinessTypeKey = fPW.BusinessTypeKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
		ON dLOB.LineOfBusinessKey = fPW.LineOfBusinessKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy as dPol
		ON dPol.PolicyKey = fPW.PolicyKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dAgency
		ON dAgency.AgencyKey = dPol.ProducerOfRecordAgencyKey

	WHERE	1=1
			AND dBType.BusinessTypeDesc = 'Direct'
			AND dLOB.LOBProductLineCode = 'CL'
			AND dPol.PolicyTransactionTypeKey IN (11,9,8,10)
			AND fPW.PremiumWritten IS NOT NULL

	GROUP BY
		dPol.ConformedPolicyNumber
		,fPW.AccountingDateKey
		,dPol.PolicyTransactionTypeKey
		,CASE WHEN dAgency.AgencyMasterName like 'Wexler%' THEN 'JM - Wexler' ELSE 'JM - All Other' END
)t
WHERE t.RankSelect = 1
