
SELECT 
	JobNumber
	,ItemNumber
	,RiskType
	,CoverageTypeCode
	,ItemClass
	,ItemLimit
FROM
(
	SELECT DISTINCT
		'J_' + dPol.JobNumber																					AS JobNumber
		,dRSI.ItemNumber																						AS ItemNumber

		,dRSC.RiskType																							AS RiskType
		,dCType.ConformedCoverageTypeCode																		AS CoverageTypeCode
		,dIC.ItemClassDescription																				AS ItemClass
		,COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0)														AS ItemLimit
	
		,DENSE_RANK() OVER(PARTITION BY	dPol.JobNumber
							ORDER BY	dPol.PolicyInforceFromDate ASC	
										,dPol.PolicyKey DESC
										,dPol.ProducerOfRecordAgencyKey DESC
							)																					AS PolRank

	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentCore AS dRSC
		ON dRSC.PolicyKey = dPol.PolicyKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentJewelryItem AS dRSI
		ON dRSI.RiskSegmentKey = dRSC.RiskSegmentJewelryItemKey
		--AND dRSC.RiskType = 'ITEM'

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimItemClass AS dIC
		ON dIC.ItemClassKey = dRSI.ItemClassKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverage AS dCov 
		ON dCov.RiskSegmentKey = dRSC.RiskSegmentKey
		AND CoverageKey<> - 1

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverageType AS dCType
		ON dCType.CoverageTypeKey = dCov.CoverageTypeKey
		--AND dCType.ConformedCoverageTypeCode = 'SCH'

		INNER JOIN DW_DDS_CURRENT.bi_keymap.KeymapDimPolicy KDP
		ON dPol.PolicyKey = KDP.PolicyKey
	
	
	WHERE	dPol.AccountSegment = 'Personal Lines'
			AND dPol.SourceSystem = 'GW' AND dPol.PolicyStatus = 'Bound' 
			AND dPol.ConformedPolicyNumber <> 'Unassigned'
			AND dPol.PolicyVersion <> 0
			AND dPol.PolicyType = 'PJ'
			AND LEFT(dPol.PolicyEffectiveDate,4) >= 2017
			--AND LEFT(dPol.PolicyEffectiveDate,6) = 202201
			AND CASE	WHEN dPol.SourceSystem = 'PAS' THEN 1
						WHEN CAST(KDP.gwSubEffectiveDate AS DATE) = CAST(CAST(dPol.TransactionEffectiveDate AS VARCHAR(10)) AS DATE) THEN 1 
						ELSE 0 
				 END = 1
			AND dRSC.IsInactive = 0
			AND CASE	WHEN dRSC.RiskType = 'LOB' AND COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0) <= 0 THEN 0
						ELSE 1
				 END = 1
)t
WHERE PolRank = 1
