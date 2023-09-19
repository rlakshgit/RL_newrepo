
SELECT
	'J_' + dPol.JobNumber								AS JobNumber
	,SUM(fMEW.ExposureValue)							AS WrittenExposures

FROM DW_DDS_CURRENT.bi_dds.FactMonthlyExposureUnitsWritten AS fMEW

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = fMEW.PolicyKey

WHERE	1 = 1 
		AND dPol.AccountSegment = 'Personal Lines'
		AND dPol.SourceSystem = 'GW'
		AND LEFT(dPol.PolicyEffectiveDate,4) >= 2017

GROUP BY
	'J_' + dPol.JobNumber	

HAVING SUM(fMEW.ExposureValue) <> 0	
	
