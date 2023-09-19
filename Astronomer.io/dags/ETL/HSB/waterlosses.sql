/* Direct AY Reported (Incurred) Loss - PL */
/*  starting point: H:\Data\COMMON\Product Management\Data and SQLs\Data Warehouse SQLs\Both Product Lines\AY Claims for Loss Ratio Forecasting.sql   */
USE DW_DDS;
Declare @StartDate Date
Declare @EndDate Date

Set @StartDate = '1/1/2012' --1/1/2012
Set @EndDate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)

DECLARE @StartDtKey int
DECLARE @EndDtKey int

Set @StartDtKey = bief_dds.fn_GetDateKeyFromDate(@StartDate)
Set @EndDtKey = bief_dds.fn_GetDateKeyFromDate(@EndDate)

SELECT      
 Geo.GeographyCountryCode AS Country
,LOB.LOBProductLineDescription AS ProdLine
,LOB.LOBCode AS LOB
,LossDate.YearNumber AS AY
,LossDate.QuarterNumber AS LossQtr
--,LossDate.MonthNumber AS LossMo
,DimClaim.ClaimNumber AS ClaimNo
,DimClaim.ClaimStatus AS ClmStatus
,Convert(varchar(10), LossDate.FullDate, 101) AS LossDate
,Convert(varchar(10), RptDate.FullDate, 101) AS ReportedDate
,Policy.PolicyNumber AS PolNo
--,RiskSgmt.BuildingLocationNumber AS LocNo
,CASE WHEN  RiskType = 'LOB' THEN RiskSgmtLOB.PrimaryRatingLocationNumber
  ELSE RiskSgmtLoc.LocationNumber END AS LocNbr
--added loc address 1/23/17 SLS
--,Replace(Replace(RiskSgmt.BuildingLocationAddress,CHar(13),' '),Char(10),' ') AS LocAddress
--,RiskSgmt.BuildingLocationCity AS LocCity
--added loc address 1/23/17 SLS
--,RiskSgmt.ItemNumber AS ItemNo
,Policy.AccountSegment AS AcctSgmt
,Geo.GeographyStateCode AS LocState
,MAX(LocTypeResults.LocTypeCode) AS LocTypeCode
,MAX(LocTypeResults.LocType) AS LocType
,DimClaim.ClaimEvent
,ClaimCauseOfLoss AS COL
,ClaimCauseOfLossGroup AS COLGrp
,ASL.AnnualStatementLine AS ASL
,ASL.ASLDescription AS ASLDescrip
,CovType.ConformedCoverageTypeCode AS CovType
,CovType.ConformedCoverageTypeDesc AS CovDescrip
,ClaimZeroInd.ClaimIsZero
,ClaimZeroInd.LAEClaimIsZero
,SUM(ClaimFact.[Claim Incurred Loss Net Recovery]) AS IncurLoss
,SUM(ClaimFact.[Claim Paid Loss Net Recovery]) AS PaidLoss
,SUM(ClaimFact.[Claim Reserve Loss]) AS UnpdLoss
,SUM(ClaimFact.[Claim Incurred DCC Expense Net Recovery])AS DCCInc
,SUM(ClaimFact.[Claim Paid DCC Expense Net Recovery]) AS DCCPaid
,SUM(ClaimFact.[Claim Reserve ALAE DCC]) AS DCCUnpd
,SUM(ClaimFact.[Claim Incurred AO Expense Net Recovery] )AS AOInc
,SUM(ClaimFact.[Claim Paid AO Expense Net Recovery]) AS AOPaid
,SUM(ClaimFact.[Claim Reserve ALAE AO]) AS AOUnpd
,SUM(ClaimFact.[Claim Incurred Loss Net Recovery]+ClaimFact.[Claim Incurred DCC Expense Net Recovery]+
  ClaimFact.[Claim Incurred AO Expense Net Recovery]) AS TotalIncWithALAE
,CASE WHEN DimClaim.ClaimDescription like '%Rolex%' THEN 'Rolex' ELSE 'NA' END AS RolexClaim
,MAX(DimCov.PerOccurenceLimit) AS Limit
,MAX(DimCov.PerOccurenceDeductible) AS Ded
,DimClaim.ClaimDescription
,DimClaim.ClaimLossCounty
,DimClaim.ClaimLossCity
,DimClaim.ClaimLossPostalCode
,LossLocGeo.GeographyCountryCode AS LossLocCountry
,LossLocGeo.GeographyStateCode AS LossLocState
,DimClaim.ClaimInvolesRolexWatches

FROM  
 bief_dds.vwFactClaim AS ClaimFact
INNER JOIN bi_dds.DimClaim AS DimClaim
  ON ClaimFact.ClaimKey = DimClaim.ClaimKey
INNER JOIN bief_dds.DimDate LossDate
  ON DimClaim.ClaimLossDateKey = LossDate.DateKey
INNER JOIN bief_dds.DimDate AS RptDate
  ON DimClaim.ClaimReportedDateKey = RptDate.DateKey
INNER JOIN bi_dds.DimLineOfBusiness AS LOB
  ON ClaimFact.LineOfBusinessKey = LOB.LineOfBusinessKey
INNER JOIN bi_dds.DimASL AS ASL
  ON ClaimFact.ASLKey = ASL.ASLKey
INNER JOIN bi_dds.DimCoverageType AS CovType
  ON ClaimFact.CoverageTypeKey = CovType.CoverageTypeKey
INNER JOIN bi_dds.DimPolicy AS Policy
  ON ClaimFact.PolicyKey = Policy.PolicyKey
INNER JOIN bi_dds.DimRiskSegmentCore AS RiskSgmt
  ON ClaimFact.RiskSegmentKey = RiskSgmt.RiskSegmentKey
LEFT OUTER JOIN bi_dds.DimRiskSegmentLOB AS RiskSgmtLOB
  ON RiskSgmt.RiskSegmentLOBKey = RiskSgmtLOB.RiskSegmentKey
LEFT OUTER JOIN bi_dds.DimRiskSegmentLocation AS RiskSgmtLoc
  ON RiskSgmt.RiskSegmentLocationKey = RiskSgmtLoc.RiskSegmentKey
LEFT OUTER JOIN bi_dds.DimRiskSegmentBuilding AS RiskSgmtBldg
  ON RiskSgmt.RiskSegmentBuildingKey = RiskSgmtBldg.RiskSegmentKey
INNER JOIN bi_dds.DimCoverage AS DimCov
  ON ClaimFact.CoverageKey = DimCov.CoverageKey
LEFT OUTER JOIN bi_dds.DimGeography AS Geo
  ON ClaimFact.CovRatedStateGeographyKey = Geo.GeographyKey
LEFT OUTER JOIN bi_dds.DimGeography AS LossLocGeo
  ON DimClaim.ClaimLossGeographyKey = LossLocGeo.GeographyKey

LEFT OUTER JOIN
(
/* Loc Type based ON EP records */
SELECT  
   Policy.PolicyNumber
   ,CYDate.CalendarYear
   ,RSLoc.LocationNumber AS LocNbr
   ,Max(RSLoc.LocationTypeCode) AS LocTypeCode
   ,Max(RSLoc.LocationTypeDesc) AS LocType
  FROM 
   bi_dds.FactMonthlyPremiumEarned AS MonthlyEP
  INNER JOIN bi_dds.DimLineOfBusiness AS LOB 
   ON MonthlyEP.LineOfBusinessKey = LOB.LineOfBusinessKey
  INNER JOIN bi_dds.DimPolicy AS Policy 
   ON MonthlyEP.PolicyKey = Policy.PolicyKey
  INNER JOIN bi_dds.DimBusinessType AS BusType 
   ON MonthlyEP.BusinessTypeKey = BusType.BusinessTypeKey
  INNER JOIN bief_dds.DimDate AS CYDate 
   ON MonthlyEP.DateKey = CYDate.DateKey
  INNER JOIN bi_dds.DimRiskSegmentLocation AS RSLoc
   ON MonthlyEP.RiskSegmentKey = RSLoc.RiskSegmentKey
  WHERE 
   1=1
   AND BusinessTypeDesc = 'Direct'
   AND MonthlyEP.DateKey BETWEEN  @StartDtKey AND @EndDtKey
   AND LOBProductLineDescription = 'Commercial'
   AND RSLoc.LocationNumber <> -1
   --AND Policy.PolicyNumber = '55-011522'
  GROUP BY 
   Policy.PolicyNumber
   ,CYDate.CalendarYear
   ,RSLoc.LocationNumber
) AS LocTypeResults
ON Policy.PolicyNumber = LocTypeResults.PolicyNumber
AND LossDate.YearNumber = LocTypeResults.CalendarYear
AND CASE WHEN  RiskType = 'LOB' THEN RiskSgmtLOB.PrimaryRatingLocationNumber
  ELSE RiskSgmtLoc.LocationNumber END = LocTypeResults.LocNbr

LEFT OUTER JOIN
(
  SELECT      
   DimClaim.ClaimNumber AS ClaimNo
   ,SUM(ClaimFact.[Claim Incurred Loss Net Recovery]) AS IncurLoss
   ,SUM(ClaimFact.[Claim Incurred Loss Net Recovery]+ClaimFact.[Claim Incurred DCC Expense Net Recovery]+
    ClaimFact.[Claim Incurred AO Expense Net Recovery]) AS TotalIncWithALAE
   ,CASE WHEN (SUM(ClaimFact.[Claim Incurred Loss Net Recovery]) = 0) THEN 'True' ELSE 'False' END AS ClaimIsZero
   ,CASE WHEN (SUM(ClaimFact.[Claim Incurred Loss Net Recovery]+ClaimFact.[Claim Incurred DCC Expense Net Recovery]+
    ClaimFact.[Claim Incurred AO Expense Net Recovery]) = 0) THEN 'True' ELSE 'False' END AS LAEClaimIsZero
  FROM  
   bief_dds.vwFactClaim AS ClaimFact
   INNER JOIN bi_dds.DimClaim AS DimClaim
    ON ClaimFact.ClaimKey = DimClaim.ClaimKey
   INNER JOIN bief_dds.DimDate LossDate
    ON DimClaim.ClaimLossDateKey = LossDate.DateKey
  WHERE 
   ClaimFact.[Claim Business Type] = 'Direct' 
   --AND LOB.LOBProductLineCode = 'CL'
   AND AccountingDateKey <= @EndDtKey --20180731 --make sure matches selection below
   AND LossDate.YearNumber > '2003' --makes sure matches selection below
  GROUP BY
  DimClaim.ClaimNumber
) AS ClaimZeroInd
  ON DimClaim.ClaimNumber = ClaimZeroInd.ClaimNo

WHERE 
 ClaimFact.[Claim Business Type] = 'Direct' 
 AND LOB.LOBProductLineCode = 'CL'
AND AccountingDateKey <= @EndDtKey --20180731 --make sure matches selection above
--AND DimClaim.ClaimNumber = '45-001568'
      
GROUP BY
Geo.GeographyCountryCode
,LOB.LOBProductLineDescription
,LOB.LOBCode
,LossDate.YearNumber
,LossDate.QuarterNumber
--,LossDate.MonthNumber
,DimClaim.ClaimNumber
,DimClaim.ClaimStatus
,LossDate.FullDate
,RptDate.FullDate
,Policy.PolicyNumber
,CASE WHEN  RiskType = 'LOB' THEN RiskSgmtLOB.PrimaryRatingLocationNumber
  ELSE RiskSgmtLoc.LocationNumber END
--,RiskSgmt.ItemNumber
,Policy.AccountSegment
,Geo.GeographyStateCode
,DimClaim.ClaimEvent
,ClaimCauseOfLoss
,ClaimCauseOfLossGroup
,CovType.ConformedCoverageTypeCode
,CovType.ConformedCoverageTypeDesc
,ASL.AnnualStatementLine
,ASL.ASLDescription
,ClaimZeroInd.ClaimIsZero
,ClaimZeroInd.LAEClaimIsZero
--added loc address 1/23/17 SLS
--,Replace(Replace(RiskSgmt.BuildingLocationAddress,CHar(13),' '),Char(10),' ')
--,RiskSgmt.BuildingLocationCity
--added loc address 1/23/17 SLS
,CASE WHEN DimClaim.ClaimDescription like '%Rolex%' THEN 'Rolex' ELSE 'NA' END
,DimClaim.ClaimDescription
,DimClaim.ClaimLossCounty
,DimClaim.ClaimLossCity
,DimClaim.ClaimLossPostalCode
,LossLocGeo.GeographyCountryCode
,LossLocGeo.GeographyStateCode
,DimClaim.ClaimInvolesRolexWatches

HAVING      
 LossDate.YearNumber > '2003' --make sure matches selection above

ORDER BY    
 LossDate.YearNumber
