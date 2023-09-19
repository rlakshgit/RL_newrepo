USE DW_DDS;
Declare @StartDate Date
Set @StartDate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)
DECLARE @StartDtKey int
Set @StartDtKey = bief_dds.fn_GetDateKeyFromDate(@StartDate)


;WITH CTE_PlumbingYear AS (
SELECT pp.PolicyNumber
       ,ploc.LocationNum AS LocationNumber
       ,bldg.BuildingNum AS BuildingNumber
       ,MAX(bdimp.YearAdded) AS PlumbingYear
FROM PolicyCenter.dbo.pc_policyperiod pp
       INNER JOIN PolicyCenter.dbo.pc_policylocation ploc
              ON pp.ID = ploc.BranchID
       INNER JOIN PolicyCenter.dbo.pc_building bldg
              ON pp.ID = bldg.BranchID
       INNER JOIN PolicyCenter.dbo.pc_buildingimpr bdimp
              ON bldg.ID = bdimp.Building
       INNER JOIN PolicyCenter.dbo.pctl_buildingimprtype bdit
              ON bdimp.BuildingImprType = bdit.ID
              AND bdit.TYPECODE = 'Plumbing'
--WHERE pp.policynumber = '55-006642'
--     AND ploc.LocationNum = 5
GROUP BY pp.PolicyNumber
       ,ploc.LocationNum
       ,bldg.BuildingNum
)


Select
 Details.AccountNbr
 ,Details.InsuredName
 ,Details.AccountSegment
 ,Details.PolicyNbr
 ,Details.PolicyType
 ,Details.PolState
 ,Details.PolicyEffDt
 ,Details.PolicyExpDt
 ,Details.AgencyMasterCode
 ,Details.AgencyCode
 ,Details.AgencySubCode
 ,Details.LocNbr
 ,Details.BldgNbr
 ,Details.LocCountry
 ,Details.LocState
 ,Details.LocAddr1
 ,Details.LocAddr2
 ,Details.LocCity
 ,Details.LocCntyFIPS
 ,Details.LocCounty
 ,Details.LocPostalCode
 ,Max(Details.LocTypeCode) as LocTypeCode
 ,Max(Details.LocType) as LocType
 ,Max(Details.BldgYr) as BldgYr
 ,Max(Details.ZoneCode) as ZoneCode
 ,Max(Details.HazardCode) as HazardCode
 ,Max(Details.BldgConstructionCode) as BldgConstructionCode
 ,Max(Details.BldgConstruction) as BldgConstruction
 ,Max(Details.BldgFireProtectionCode) as BldgFireProtectionCode
 ,Max(Details.BldgFireProtection) as BldgFireProtection
 ,Max(Details.LocSegmentCode) as LocSegmentCode
 ,Max(Details.AnnualSales) as AnnualSales
 ,Max(Details.InsuredClassCode) as InsuredClassCode
 ,Max(Details.PredominantBldgClassCode) as PredominantBldgClassCode
 --,Sum(Details.JBReplCostIndicator) as JBReplCostIndicator
 ,Sum(Details.TotInfPrem) as TotalInforcePrem
 ,Sum(Details.JBPrem) as JBInforcePrem
 ,Sum(Details.JSPrem) as JSInforcePrem
 ,Sum(Details.BOPPrem) as BOPInforcePrem
 ,Sum(Details.UmbPrem) as UmbInforcePrem
 ,Sum(Details.JBInvPremBase) as JBInvPremBase
 ,Sum(Details.StkLimit) as StkLimit
 ,Max(Details.StkDed) as StkDed
 ,Max(Details.JSOutOfSafeLimit) as JSOutOfSafeLimit
 ,Sum(Details.BldgLimit) as BldgLimit
 ,Max(Details.BldgDed) as BldgDed
 ,Sum(Details.BPPLimit) as BPPLimit
 ,Max(Details.MaxBPPDed) as BPPDed
 ,Max(Details.JBExpCrTierFctr) as JBExpCrTierFctr
 ,Max(Details.Sprinklered) as Sprinklered
 ,Max(Details.BldgSqFt) as BldgSqFt
 ,Max(Details.OccSqFt) as OccSqFt
 ,Max(Details.BldgNoStories) as BldgNoStories
 ,Max(Details.BldgOccupFloorCnt) as BldgOccupFloorCnt
 ,Max(Details.BopTerritory) as BopTerritory
 ,Max(Details.PlumbingYear) as PlumbingYr
From
 (Select
  DimAccount.AccountNumber as AccountNbr
  ,DimPol.AccountSegment as AccountSegment
  ,DimPol.ConformedPolicyNumber as PolicyNbr
  ,DimPol.PolicyType
  ,PolGeo.GeographyStateCode as PolState
  ,DimPol.PolicyEffectiveDate as PolicyEffDt
  ,DimPol.PolicyExpirationDate as PolicyExpDt
  ,DimRisk.BuildingLocationNumber as LocNbr
  ,(Case when DimLOB.LOBCode = 'BOP' then DimRisk.BuildingNumber else 1 end) as BldgNbr
  ,DimLOB.LOBCode
  ,LocGeo.GeographyCountryCode as LocCountry
  ,LocGeo.GeographyStateCode as LocState
  ,DimRisk.LocationAddress1 as LocAddr1
  ,DimRisk.LocationAddress2 as LocAddr2
  ,DimRisk.LocationCity as LocCity
  ,DimRisk.LocationCountyFIPS as LocCntyFIPS
  ,DimRisk.LocationCounty as LocCounty
  ,DimRisk.LocationPostalCode as LocPostalCode
  ,ProducerOfService.AgencyMasterCode as AgencyMasterCode
  ,ProducerofService.AgencyCode as AgencyCode
  ,ProducerofService.AgencySubCode as AgencySubCode
  ,DimPol.PolicyInsuredContactFullName as InsuredName
  ,Max(DimRisk.BuildingTypeOfLocationCode) as LocTypeCode
  ,Max(DimRisk.BuildingTypeOfLocationDesc) as LocType
  ,Max(DimRisk.InlandMarineZoneCode) as ZoneCode
  ,Max(DimRisk.InlandMarineHazardCode) as HazardCode
  ,Max(DimRisk.BuildingYearOfConstruction) as BldgYr
  ,Max(DimRisk.BuildingConstructionCode) as BldgConstructionCode
  ,Max(DimRisk.BuildingConstructionDesc) as BldgConstruction
  ,Max(DimRisk.BuildingProtectionClassCode) as BldgFireProtectionCode
  ,Max(DimRisk.BuildingProtectionClassDesc) as BldgFireProtection
  ,Max(DimRisk.LocationSegmentationCode) as LocSegmentCode
  ,Max(DimRisk.BuildingLocationAnnualSales) as AnnualSales
  ,Max(DimRisk.BOPInsuredsBusinessClassCode) as InsuredClassCode
  ,Max(DimRisk.BOPPredominantBuildingOccupancyClassCode) as PredominantBldgClassCode
  ,DimRisk.InlandMarineReplacementCostInd as JBReplCostIndicator
  ,Sum(Inforce.PremiumInforce) as TotInfPrem
  ,Sum(Case when DimLOB.LOBCode = 'JB' then Inforce.PremiumInforce else 0 end) as JBPrem
  ,Sum(Case when DimLOB.LOBCode = 'JS' then Inforce.PremiumInforce else 0 end) as JSPrem
  ,Sum(Case when DimLOB.LOBCode = 'BOP' then Inforce.PremiumInforce else 0 end) as BOPPrem
  ,Sum(Case when DimLOB.LOBCode = 'UMB' then Inforce.PremiumInforce else 0 end) as UmbPrem
  ,Max(Case when DimLOB.LOBCode = 'JB' then DimRisk.StockPremiumBaseAOP else 0 end) as JBInvPremBase
  ,Max(Case when CovType.ConformedCoverageTypeCode ='401A' then Cov.PerOccurenceLimit else 0 end) as StkLimit
  ,Max(Case when CovType.ConformedCoverageTypeCode ='401A' then Cov.PerOccurenceDeductible else 0 end) as StkDed
  ,Max(Case when CovType.ConformedCoverageTypeCode = '407A' then Cov.PerOccurenceLimit else 0 end) as JSOutOfSafeLimit
  ,Max(Case when CovType.ConformedCoverageTypeCode ='850A' then Cov.PerOccurenceLimit else 0 end) as BldgLimit
  ,Max(Case when CovType.ConformedCoverageTypeCode ='850A' then Cov.PerOccurenceDeductible else 0 end) as BldgDed
  ,(Max(Case when CovType.ConformedCoverageTypeCode ='871A' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871B' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871C' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871D' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871E' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871F' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871G' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871H' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871I' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871J' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871K' then Cov.PerOccurenceLimit else 0 end)+
   Max(Case when CovType.ConformedCoverageTypeCode ='871L' then Cov.PerOccurenceLimit else 0 end)
   ) as BPPLimit --Includes all BPP - BPP unscheduled, BPP scheduled, and BPP pawn options
  ,Max((Case when CovType.ConformedCoverageTypeCode in ('871A', '871B', '871C', '871D', '871E', '871F', '871G', '871H', '871I', '871J', '871K', '871L') then Cov.PerOccurenceDeductible else 0 end)) as MaxBPPDed
  ,Max(Case when DimLOB.LOBCode = 'JB' then DimRisk.InlandMarineExperienceTierFactor else '0' end) as JBExpCrTierFctr
  ,Max(Cast(RiskBldg.BuildingCompletelySprinklered as INT)) as Sprinklered
  ,Max(RiskBldg.BuildingTotalArea) as BldgSqFt
  ,Max(RiskBldg.BuildingAreaOccupied) as OccSqFt
  ,Max(RiskBldg.BuildingNumStories) as BldgNoStories
  ,Max(RiskLoc.LocationBuildingOccupiedFloorCount) as BldgOccupFloorCnt
  ,Max(RiskLoc.LocationTerritoryCode) as BopTerritory
  ,PlumbYr.PlumbingYear
 From
  bi_dds.FactMonthlyPremiumInforce Inforce
  Inner Join bi_dds.DimLineOfBusiness DimLOB
  on Inforce.LineOfBusinessKey = DimLOB.LineOfBusinessKey
  Inner Join bi_dds.DimBusinessType DimBusType
  on Inforce.BusinessTypeKey = DimBusType.BusinessTypeKey
  Inner Join bi_dds.DimPolicy DimPol
  on Inforce.PolicyKey = DimPol.PolicyKey
  Inner Join bi_dds.DimAgency as ProducerofService
  on ProducerofService.AgencyKey = DimPol.AgencyKey
  Inner Join bi_dds.DimAccount as DimAccount
  on DimPol.AccountKey = DimAccount.AccountKey
  Inner Join bi_dds.vwDimRiskSegment as DimRisk
  on Inforce.RiskSegmentKey = DimRisk.RiskSegmentKey
  Inner join bi_dds.DimGeography LocGeo
  on DimRisk.BuildingLocationGeographyKey = LocGeo.GeographyKey
  Inner join bi_dds.DimCoverageType as CovType
  on Inforce.CoverageTypeKey = CovType.CoverageTypeKey
  Inner join bi_dds.DimCoverage as Cov
  on Inforce.CoverageKey = Cov.CoverageKey
  Inner join bi_dds.DimGeography PolGeo
  on DimPol.PrimaryInsuredGeographyKey = PolGeo.GeographyKey
  Left Join bi_dds.DimRiskSegmentLocation as RiskLoc
  on Inforce.RiskSegmentKey = RiskLoc.RiskSegmentKey
  Left Join bi_dds.DimRiskSegmentBuilding as RiskBldg
  on Inforce.RiskSegmentKey = RiskBldg.RiskSegmentKey
  Left Outer Join CTE_PlumbingYear as PlumbYr
  on DimPol.PolicyNumber = PlumbYr.PolicyNumber
  and DimRisk.BuildingLocationNumber = PlumbYr.LocationNumber
  and DimRisk.BuildingNumber = PlumbYr.BuildingNumber
 Where
  1=1
  and Inforce.DateKey = @StartDtKey
  and DimBusType.BusinessTypeDesc = 'Direct'
  and DimLOB.LOBProductLineDescription = 'Commercial'
 Group By
  DimAccount.AccountNumber
  ,DimPol.AccountSegment
  ,DimPol.ConformedPolicyNumber
  ,DimPol.PolicyType
  ,PolGeo.GeographyStateCode
  ,DimPol.PolicyEffectiveDate
  ,DimPol.PolicyExpirationDate
  ,DimRisk.BuildingLocationNumber
  ,DimRisk.BuildingNumber
  ,DimLOB.LOBCode
  ,LocGeo.GeographyCountryCode
  ,LocGeo.GeographyStateCode
  ,DimRisk.LocationAddress1
  ,DimRisk.LocationAddress2
  ,DimRisk.LocationCity
  ,DimRisk.LocationCountyFIPS
  ,DimRisk.LocationCounty
  ,DimRisk.LocationPostalCode
  ,ProducerOfService.AgencyMasterCode
  ,ProducerofService.AgencyCode
  ,ProducerofService.AgencySubCode
  ,DimPol.PolicyInsuredContactFullName
  ,DimRisk.InlandMarineReplacementCostInd
  ,PlumbYr.PlumbingYear
 ) as Details
Group By
 Details.AccountNbr
 ,Details.InsuredName
 ,Details.AccountSegment
 ,Details.PolicyNbr
 ,Details.PolicyType
 ,Details.PolState
 ,Details.PolicyEffDt
 ,Details.PolicyExpDt
 ,Details.AgencyMasterCode
 ,Details.AgencyCode
 ,Details.AgencySubCode
 ,Details.LocNbr
 ,Details.BldgNbr
 ,Details.LocCountry
 ,Details.LocState
 ,Details.LocAddr1
 ,Details.LocAddr2
 ,Details.LocCity
 ,Details.LocCntyFIPS
 ,Details.LocCounty
 ,Details.LocPostalCode
Order By
 Details.AccountNbr
 ,Details.PolicyNbr
 ,Details.LocNbr
 ,Details.BldgNbr
