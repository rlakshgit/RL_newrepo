select
	InforceDateKey
	,InforceDate
	,AccountNumber
	,PolicyNumber
	,PolicyNumber_LocationNumber
	,PolicyOrigEffDate	
	,PolicyEffectiveDate
	,PolicyExpirationDate
	,AccountSegment
	,ProductLineCode
	,LOB_Code
	,NewRenewal								
	,AutoPayIndicator
	,PaymentMethod
	,Producer_Service_Agency_Master_Code
	,Producer_Service_Agency_Master_Name
	,Producer_Service_Agency_SubCode
	,Producer_Service_Agency_Code
	,Producer_Service_Agency_Name
	,Primary_Insured_State
	,Primary_Insured_Country
	,sum(LocationJB_JS_Premium) as Policy_JB_JS_Premium 
	,sum(LocationBOP_Premium) as Policy_BOP_Premium
	,sum(LocationUMB_Premium) as Policy_UMB_Premium
	,sum(LocationPremiumInforce) as Policy_InforcePremium
    ,bq_load_date

from `{project}.{dataset}.cl_location_monthly_inforce`
Where bq_load_date = DATE('{date}')
group by
	InforceDateKey
	,InforceDate
	,AccountNumber
	,PolicyNumber
	,PolicyNumber_LocationNumber
	,PolicyOrigEffDate	
	,PolicyEffectiveDate
	,PolicyExpirationDate
	,AccountSegment
	,ProductLineCode
	,LOB_Code
	,NewRenewal								
	,AutoPayIndicator
	,PaymentMethod
	,Producer_Service_Agency_Master_Code
	,Producer_Service_Agency_Master_Name
	,Producer_Service_Agency_SubCode
	,Producer_Service_Agency_Code
	,Producer_Service_Agency_Name
	,Primary_Insured_State
	,Primary_Insured_Country
    ,bq_load_date