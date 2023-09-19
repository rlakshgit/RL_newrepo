
;WITH LossDateDetails AS (
SELECT DISTINCT
	dClaim.ClaimNumber				
	,vFC.LossDateKey		
	,dClaim.ClaimStatus	
	,dPol.JobNumber
	,DENSE_RANK() OVER(PARTITION BY dClaim.ClaimNumber
						ORDER BY	vFC.AccountingDateKey DESC) AS LossRank

FROM DW_DDS_CURRENT.bief_dds.vwFactClaim as vFC

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBType
	ON dBType.BusinessTypeKey = vFC.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = vFC.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS dClaim
	ON vFC.ClaimKey = dClaim.ClaimKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = vFC.PolicyKey

WHERE	1=1 
		AND dBType.BusinessTypeDesc = 'Direct'
		AND dLOB.LOBProductLineCode = 'PL'
		AND dPol.SourceSystem = 'GW'		
)
SELECT
	dPol.ConformedPolicyNumber							AS PolicyNumber
	,'J_' + cLDD.JobNumber								AS JobNumber
	,cLDD.LossDateKey									AS LossDateKey
	,dClaim.ClaimNumber									AS ClaimNumber
	,dRSI.ItemNumber									AS ItemNumber
	,cLDD.ClaimStatus									AS ClaimStatus

	,SUM(vFC.[Claim Incurred Loss Net Recovery])		AS ReportedLoss
	,SUM(vFC.[Claim Paid Loss Net Recovery])			AS PaidLoss
	,SUM(vFC.[Claim Incurred DCC Expense Net Recovery])	AS ReportedDCC
	,SUM(vFC.[Claim Paid DCC Expense Net Recovery])		AS PaidDCC
	,SUM(vFC.[Claim Incurred AO Expense Net Recovery])	AS ReportedAO
	,SUM(vFC.[Claim Paid AO Expense Net Recovery])		AS PaidAO
	,SUM(vFC.[Claim Incurred ALAE Net Recovery])		AS ReportedALAE
	,SUM(vFC.[Claim Paid ALAE Net Recovery])			AS PaidALAE

FROM DW_DDS_CURRENT.bief_dds.vwFactClaim as vFC

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBType
	ON dBType.BusinessTypeKey = vFC.BusinessTypeKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
	ON dLOB.LineOfBusinessKey = vFC.LineOfBusinessKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS dClaim
	ON vFC.ClaimKey = dClaim.ClaimKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	ON dPol.PolicyKey = vFC.PolicyKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentCore AS dRSC
	ON dRSC.RiskSegmentKey = vFC.RiskSegmentKey

	INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentJewelryItem AS dRSI
	ON dRSI.RiskSegmentKey = dRSC.RiskSegmentJewelryItemKey
	AND dRSC.RiskType = 'ITEM'

	INNER JOIN LossDateDetails AS cLDD
	ON cLDD.ClaimNumber = dClaim.ClaimNumber
	AND cLDD.LossRank = 1

WHERE	1 = 1 
		AND dBType.BusinessTypeDesc = 'Direct'
		AND dLOB.LOBProductLineCode = 'PL'
		AND dPol.SourceSystem = 'GW'
		AND LEFT(dPol.PolicyEffectiveDate,4) >= 2017

GROUP BY
	dPol.ConformedPolicyNumber	
	,'J_' + cLDD.JobNumber		
	,cLDD.LossDateKey			
	,dClaim.ClaimNumber			
	,dRSI.ItemNumber	
	,cLDD.ClaimStatus
