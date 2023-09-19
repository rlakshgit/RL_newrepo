
SELECT
	'J_' + dPol.JobNumber								AS JobNumber
	,SUM(fMPW.PremiumWritten)							AS WrittenPremium

FROM DW_DDS_CURRENT.bi_dds.FactMonthlyPremiumWritten AS fMPW

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBType
	ON dBType.BusinessTypeKey = fMPW.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fMPW.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fMPW.PolicyKey

WHERE	1 = 1 
		AND dBType.BusinessTypeDesc = 'Direct'
		AND dLOB.LOBProductLineCode = 'PL'
		AND dPol.SourceSystem = 'GW'
		AND LEFT(dPol.PolicyEffectiveDate,4) >= 2017

GROUP BY
	'J_' + dPol.JobNumber	

HAVING SUM(fMPW.PremiumWritten) <> 0
		
