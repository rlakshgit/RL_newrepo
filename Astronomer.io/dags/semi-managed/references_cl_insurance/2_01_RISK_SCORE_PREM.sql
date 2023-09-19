--DECLARE @POL_BEG_DATE AS DATE = '20150101'
DECLARE @POL_END_DATE AS DATE = '{load_date}'

;WITH factInfo AS 
(
SELECT 
	dPol.ConformedPolicyNumber AS PolicyNumber
	,dPol.PolicyGroup AS TermNumber
	,ISNULL(CASE	WHEN dRSC.RiskType IN ('LOC','BLD','STK') THEN rsl.LocationNumber
				WHEN dRSC.RiskType IN ('LOB') THEN rsLOB.PrimaryRatingLocationNumber
		END,-1)	AS LocationNumber
	,fGeo.GeographyCountryCode AS Country
	,SUM(fPW.PremiumWritten) AS PremiumWritten

FROM DW_DDS_CURRENT.bi_dds.FactPremiumWritten as fPW

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType as dBType
	ON dBType.BusinessTypeKey = fPW.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = fPW.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy as dPol
	ON dPol.PolicyKey = fPW.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentCore AS dRSC
	ON dRSC.RiskSegmentKey = fPW.RiskSegmentKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentLOB AS rsLOB 
	ON dRSC.RiskSegmentLOBKey = rsLOB.RiskSegmentKey 

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentLocation AS rsl 
	ON dRSC.RiskSegmentLocationKey = rsl.RiskSegmentKey 

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS fGeo
	ON fGeo.GeographyKey = fPW.LocationGeographyKey

WHERE	1 = 1
		AND dBType.BusinessTypeDesc = 'Direct'
		AND dLOB.LOBProductLineCode = 'CL'
		AND dPol.PolicyVersion = 1
		AND dLOB.LOBCode IN ('JB','JS')
		AND dPol.SourceSystem = 'GW'

GROUP BY 
	dPol.ConformedPolicyNumber 
	,dPol.PolicyGroup
	,ISNULL(CASE	WHEN dRSC.RiskType IN ('LOC','BLD','STK') THEN rsl.LocationNumber
				WHEN dRSC.RiskType IN ('LOB') THEN rsLOB.PrimaryRatingLocationNumber
		END,-1)
	,fGeo.GeographyCountryCode
)
SELECT DISTINCT
	pcPolPer.PolicyNumber									AS PolicyNumber
	,pcPolPer.TermNumber 									AS TermNumber
	,factInfo.LocationNumber
	,factInfo.Country
	,factInfo.PremiumWritten
	--,pcJob.JobNumber

FROM PolicyCenter.dbo.pc_policyperiod AS pcPolPer

	INNER JOIN PolicyCenter.dbo.pc_job AS pcJob
	ON pcJob.ID = pcPolPer.JobID

	INNER JOIN PolicyCenter.dbo.pctl_job AS pctlJob
	ON pctlJob.ID = pcJob.Subtype

	INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctlPolPerSta
	ON pctlPolPerSta.ID = pcPolPer.Status

	INNER JOIN PolicyCenter.dbo.pc_policyline AS pcPolLine
	ON pcPolLine.BranchID = pcPolPer.ID
	AND pcPolPer.EditEffectiveDate >= COALESCE(pcPolLine.EffectiveDate,pcPolPer.PeriodStart)
	AND pcPolPer.EditEffectiveDate <  COALESCE(pcPolLine.ExpirationDate,pcPolPer.PeriodEnd)

	LEFT JOIN factInfo
	ON factInfo.PolicyNumber = pcPolPer.PolicyNumber
	AND factInfo.TermNumber = pcPolPer.TermNumber

WHERE	1 = 1
		AND pctlPolPerSta.NAME = 'Bound'
		AND pcPolLine.PatternCode = 'ILMLine'
		AND CASE	WHEN CAST(pcPolPer.EditEffectiveDate AS date) >= CAST(pcJob.CloseDate AS date) THEN CAST(pcPolPer.EditEffectiveDate AS date) 
				ELSE CAST(pcJob.CloseDate AS date) END <= @POL_END_DATE
		AND pcPolPer.ModelNumber = 1 
