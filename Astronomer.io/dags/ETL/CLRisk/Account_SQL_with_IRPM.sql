USE DW_DDS_CURRENT;

;with LocDetails as (
       Select 
	   ConformedPolicyNumber
	   ,AccountNumber
	   ,JobNumber
	   ,JobGroupType
       ,PolicyEffectiveDate
       ,DPolicy.PolicyKey
	   ,dPolicy.PolicyGroup
	   ,dPolicy.PolicyVersion
	   ,dPolicy.PolicyInforceFromDate
	   ,dPolicy.PolicyStatus
	   ,LocationSegmentationCode
       ,LocationNumber
	   ,RatedAsDescription as LocationRatedAs
	   ,dLob.LOBCode
	   ,PrimaryRatingAddress1
	   ,PrimaryRatingAddress2
	   ,LocationAddress1
	   ,LocationAddress2
	   ,PolicyCurrentUnderwriterUserKey
	   ,PolicyCurrentUnderwriterUserFullName
	   ,PolicyIssuingUnderwriterUserKey
	   ,PolicyIssuingUnderwriterUserFullName
	   ,PolicyPrintQuoteUnderwriterUserKey
	   ,PolicyPrintQuoteUnderwriterUserFullName
	   ,AgencyKey
	   ,UpdateTimeStamp
	   ,TotalPremiumRPT as TotalPolicyPrem
       --,Max(LocationSegmentationCode) as LocationSegmentationCode
	   --,Max(Case when dLob.lobcode = 'JB' then 1 else 0 end) as JBFlag
	   --,Max(Case when dLob.lobcode = 'JS' then 1 else 0 end) as JSFlag
	   --,Max(Case when dLob.lobcode = 'BOP' then 1 else 0 end) as BOPFlag
	   --,Max(Case when dLob.lobcode = 'UMB' then 1 else 0 end) as UMBFlag


       From [bi_dds].[vwDimRiskSegmentAll] dRSAll
	   
	   Left Join [bi_dds].[DimRatedAs] as RatedAs
			on dRSAll.LocationRatedAsKey = RatedAs.RatedAsKey

       Inner Join [bi_dds].[DimLineOfBusiness] as dLob
              on Dlob.LineOfBusinessKey = dRSAll.LineOfBusinessKey 
			  
       Inner Join [bi_dds].[DimPolicy] as dPolicy
              on dPolicy.PolicyKey = dRSAll.PolicyKey

		INNER JOIN [bi_dds].[DimAccount] as dAccount
		on dPolicy.AccountKey = dAccount.AccountKey
	
	  Inner Join [bi_dds].[DimCoverage] as dCov
			on dcov.risksegmentkey = drsall.risksegmentkey

	Where 
		--1=1
		--and 
		drsAll.LocationNumber <> -1 
		and PolicyInforceFromDate >= '20190501'
		--and dLob.LOBCode in ('JB','JS') 
		-- and IsInactive=0

	Group by 

	   ConformedPolicyNumber
	   ,AccountNumber
	   ,JobNumber
	   ,JobGroupType
       ,PolicyEffectiveDate
       ,DPolicy.PolicyKey
	   ,dPolicy.PolicyGroup
	   ,dPolicy.PolicyVersion
	   ,dPolicy.PolicyInforceFromDate
	   ,dPolicy.PolicyStatus
	   ,LocationSegmentationCode
       ,LocationNumber
	   ,RatedAsDescription
	   ,dLob.LOBCode
	   ,PrimaryRatingAddress1
	   ,PrimaryRatingAddress2
	   ,LocationAddress1
	   ,LocationAddress2
	   ,PolicyCurrentUnderwriterUserKey
	   ,PolicyCurrentUnderwriterUserFullName
	   ,PolicyIssuingUnderwriterUserKey
	   ,PolicyIssuingUnderwriterUserFullName
	   ,PolicyPrintQuoteUnderwriterUserKey
	   ,PolicyPrintQuoteUnderwriterUserFullName
	   ,AgencyKey
	   ,UpdateTimeStamp
	   ,TotalPremiumRPT

)

,RateModBldgOne AS (
SELECT 'GW' AS SourceSystem
	,detail.PolicyKey
	,detail.PolicyNumber
	,LineOfBusinessKey
	,LOBCode
	,Loc_Item
	,RateModType
	,CASE WHEN RateModType = 'IRPM' AND LOBCode = 'PJ' 
			THEN 1 + SUM((RateModificationFactor)/100) 
		WHEN RateModType in ('IRPM')
			THEN 1 + SUM(RateModificationFactor)	
        WHEN RateModType in ('Judgment')and LOBCode in( 'JB','JS')
			THEN 1 + SUM(RateModificationFactor)		 
		ELSE SUM(RateModificationFactor)	
		END AS RateModFactor
	,ProrataFactor
	,DENSE_RANK() OVER(	PARTITION BY Policykey,lineOfBusinessKey,Loc_Item--,RateModType
		ORDER BY Expirationdate desc) AS RNK
---Add in Row is current logic....should eliminate line above---------

FROM 
	(
	  SELECT m.PolicyKey
		,m.PolicyTermKey
		,p.PolicyNumber
		,m.LineOfBusinessKey
		,m.BusinessTypeKey
		--,m.GeographyKey
		,l.LOBCode
		,CASE WHEN l.LOBCode = 'PJ' THEN r.ItemNumber ELSE r.BuildingLocationNumber END as Loc_Item 
		,c.RateModificationType 
		,c.RateModificationCategoryCode
		,CASE WHEN c.RateModificationType = 'IRPM' THEN 'IRPM' 
			WHEN c.RateModificationType = 'RiskModification' AND c.RateModificationCategoryCode like '%judg%' THEN 'Judgment'
			WHEN c.RateModificationType = 'RiskModification' AND l.LOBCode = 'PJ' THEN c.RateModificationCategoryDesc
			ELSE m.RateModificationComments END AS RateModType
		,CASE WHEN LOBCode = 'BOP' AND RateModificationType = 'IRPM' AND BuildingNumber <> 1 THEN 0 ELSE m.RateModificationFactor END as RateModificationFactor
		,m.RateModificationComments 
		,m.ProrataFactor
		,m.EffectiveDate
		,m.ExpirationDate
	FROM bi_dds.FactRateModificationFactor m
		INNER JOIN bi_dds.DimRateModificationCategory c
			ON m.RateModificationCategoryKey = c.RateModificationCategoryKey
		INNER JOIN bi_dds.DimLineOfBusiness l	
			ON m.LineOfBusinessKey = l.LineOfBusinessKey
		INNER JOIN bi_dds.DimPolicy p
			ON m.PolicyKey = p.PolicyKey
		INNER JOIN bi_dds.DimRiskSegment  r
			ON m.RiskSegmentKey = r.RiskSegmentKey		
	) detail

WHERE RateModType in ('IRPM','Judgment')

GROUP BY detail.PolicyKey
	,detail.PolicyTermKey
	,LineOfBusinessKey
	,BusinessTypeKey
	,LOBCode
	,Loc_Item
	,RateModType
	,ProrataFactor
	,ExpirationDate
	,detail.PolicyNumber
) 

, IRPMSummary as (
Select 
PolicyNumber
,PolicyKey
,RNK
,Sum(case when LOBCode='JS' then RateModFactor else 0 end) as JSIRPM
,Sum(Case when LOBCode = 'BOP' then RateModFactor else 0 end) as BOPIRPM
,Sum(Case when LOBCode = 'JB' then RateModFactor else 0 end) as JBIRPM
,Sum(Case when LOBCode = 'UMB' then RateModFactor else 0 end) as UMBIRPM
,Loc_Item



From RateModBldgOne 

where  RateModType in ('IRPM','Judgment')  

Group by
PolicyNumber
,PolicyKey
,Loc_Item
,RNK

)

, IRPMResults as (
	Select 
	ConformedPolicyNumber
	,AccountNumber
	,JobNumber
	,JobGroupType
    ,PolicyEffectiveDate
    ,Locdetails.PolicyKey
	,PolicyGroup
	,PolicyVersion
	,PolicyInforceFromDate
	,PolicyStatus
	,LocationSegmentationCode
    ,LocationNumber
	,LocationRatedAs
	--,LOBCode
	,PrimaryRatingAddress1
	,PrimaryRatingAddress2
	,LocationAddress1
	,LocationAddress2
	,PolicyCurrentUnderwriterUserKey
	,PolicyCurrentUnderwriterUserFullName
	,PolicyIssuingUnderwriterUserKey
	,PolicyIssuingUnderwriterUserFullName
	,PolicyPrintQuoteUnderwriterUserKey
	,PolicyPrintQuoteUnderwriterUserFullName
	,AgencyKey
	--,UpdateTimeStamp
	,JSIRPM
	,JBIRPM
	,BOPIRPM
	,UMBIRPM
	,TotalPolicyPrem

	From LocDetails

	Left Join IRPMSummary on 
		IRPMSummary.PolicyKey= LocDetails.PolicyKey
		and IRPMSummary.Loc_Item=LocDetails.LocationNumber

	WHERE RNK = 1 or RNK is null -- added RNK is null to capture policies without an IRPM
		and	PolicyInforceFromDate >= '20190501'
		--and AccountNumber = '3001075133'

	GROUP BY ConformedPolicyNumber
	,AccountNumber
	,JobNumber
	,JobGroupType
	,PolicyEffectiveDate
	,Locdetails.PolicyKey
	,PolicyGroup
	,PolicyVersion
	,PolicyInforceFromDate
	,PolicyStatus
	,LocationSegmentationCode
	,LocationNumber
	,LocationRatedAs
	--,LOBCode
	,PrimaryRatingAddress1
	,PrimaryRatingAddress2
	,LocationAddress1
	,LocationAddress2
	,PolicyCurrentUnderwriterUserKey
	,PolicyCurrentUnderwriterUserFullName
	,PolicyIssuingUnderwriterUserKey
	,PolicyIssuingUnderwriterUserFullName
	,PolicyPrintQuoteUnderwriterUserKey
	,PolicyPrintQuoteUnderwriterUserFullName
	,AgencyKey
	--,UpdateTimeStamp
	,JSIRPM
	,JBIRPM
	,BOPIRPM
	,UMBIRPM
	,TotalPolicyPrem
)

	Select * from IRPMResults