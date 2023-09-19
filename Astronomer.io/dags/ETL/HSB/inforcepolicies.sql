USE DW_DDS;
Declare @StartDate Date
Set @StartDate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)
DECLARE @StartDtKey int
Set @StartDtKey = bief_dds.fn_GetDateKeyFromDate(@StartDate)

Select
 details.AccountNumber as AcctNbr
 ,details.AccountSegment as AcctSgmt
 ,details.PolicyNumber as PolNbr
 --,details.PolicyType as PolType
 --,details.PolicyEffDt as PolEffDt
 --,details.LocationCountry as Country
 --,details.AgencyMasterCode as AgyMstrCd
 --,details.AgencyCode as AgyCd
 --,details.InsuredName as Insured
 --,count(distinct(details.LocationNumber)) as PolLocCnt
 --,sum(details.JBJSLocCount) as JBJSLocCnt
 --,sum(details.BOPLocCount) as BOPLocCnt
 --,sum(details.TotInfPrem) as PolInfPrem
 --,sum(details.JBJSPrem) as JBJSTotPrem
 --,sum(details.BOPPrem) as BOPTotPrem
 --,sum(details.UmbPrem) as UmbTotPrem
 --,sum(details.JBInvPremBase) as JBTotInvPremBase
 --,sum(details.StkLimit) as TotStkLim
 --,sum(details.BldgLimit) as TotBldgLim
 --,sum(details.BPPLimit) as TotBPPLim
From
(Select
       DimAccount.AccountNumber as AccountNumber
       ,DimPol.AccountSegment as AccountSegment
       ,DimPol.ConformedPolicyNumber as PolicyNumber
       ,DimPol.PolicyType
    ,DimPol.PolicyEffectiveDate as PolicyEffDt
       ,DimRisk.BuildingLocationNumber as LocationNumber
       ,LocGeo.GeographyStateCode as LocationState
       ,LocGeo.GeographyCountryCode as LocationCountry
       ,ProducerOfService.AgencyMasterCode as AgencyMasterCode
       ,ProducerofService.AgencyCode as AgencyCode
       ,DimPol.PolicyInsuredContactFullName as InsuredName
       ,Sum(Inforce.PremiumInforce) as TotInfPrem
       ,Sum(Case when DimLOB.LOBCode in ('JB', 'JS') then Inforce.PremiumInforce else 0 end) as JBJSPrem
       ,Sum(Case when DimLOB.LOBCode = 'BOP' then Inforce.PremiumInforce else 0 end) as BOPPrem
       ,Sum(Case when DimLOB.LOBCode = 'UMB' then Inforce.PremiumInforce else 0 end) as UmbPrem
    ,Max(Case when DimLOB.LOBCode = 'JB' then DimRisk.StockPremiumBaseAOP else 0 end) as JBInvPremBase
       ,Max(Case when CovType.ConformedCoverageTypeCode ='401A' then Cov.PerOccurenceLimit else 0 end) as StkLimit
       ,Max(Case when CovType.ConformedCoverageTypeCode ='850A' then Cov.PerOccurenceLimit else 0 end) as BldgLimit
       ,(     Max(Case when CovType.ConformedCoverageTypeCode ='871A' then Cov.PerOccurenceLimit else 0 end)+
              Max(Case when CovType.ConformedCoverageTypeCode ='871B' then Cov.PerOccurenceLimit else 0 end)+
              Max(Case when CovType.ConformedCoverageTypeCode ='871C' then Cov.PerOccurenceLimit else 0 end)+
              Max(Case when CovType.ConformedCoverageTypeCode ='871D' then Cov.PerOccurenceLimit else 0 end)+
              Max(Case when CovType.ConformedCoverageTypeCode ='871H' then Cov.PerOccurenceLimit else 0 end)
       ) as BPPLimit
       ,Max(Case when DimLOB.LOBCode = 'JB' then DimRisk.InlandMarineExperienceTierFactor else '0' end) as JBExpCrTierFctr
       --,Max(Case when DimLOB.LOBCode in ('JB', 'JS') and IRPM.RateModificationType in ('IRPM', 'Judgment') then IRPM.RateModificationFactor else 1 end) as JBJSIRPM
       --,Max(Case when DimLOB.LOBCode = 'BOP' and IRPM.RateModificationType in ('IRPM', 'Judgment') then IRPM.RateModificationFactor else 1 end) as BOPIRPM
       ,Max(Case when DimLOB.LOBCode in ('JB', 'JS') then 1 else 0 end) as JBJSLocCount
       ,Max(Case when DimLOB.LOBCode = 'BOP' then 1 else 0 end) as BOPLocCount
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
       --Left Outer Join bi_dds.vwFactRateModificationFactorSummary as IRPM
       --       on IRPM.PolicyKey = DimPol.PolicyKey
       --       and IRPM.LineOfBusinessKey = Inforce.LineOfBusinessKey
       --       and IRPM.BusinessTypeKey = Inforce.BusinessTypeKey
       --       and IRPM.Loc_Item = DimRisk.BuildingLocationNumber
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
    ,DimPol.PolicyEffectiveDate
       ,DimRisk.BuildingLocationNumber
       ,LocGeo.GeographyStateCode
       ,LocGeo.GeographyCountryCode
       ,ProducerOfService.AgencyMasterCode
       ,ProducerofService.AgencyCode
       ,DimPol.PolicyInsuredContactFullName
--Order By
--       DimAccount.AccountNumber
--       ,DimPol.ConformedPolicyNumber
--       ,DimRisk.BuildingLocationNumber
) as details
Group By
 details.AccountNumber
 ,details.AccountSegment
 ,details.PolicyNumber
 ,details.PolicyType
 ,details.PolicyEffDt
 ,details.LocationCountry
 ,details.AgencyMasterCode
 ,details.AgencyCode
 ,details.InsuredName
Order By
 details.PolicyNumber