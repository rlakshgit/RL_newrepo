CREATE TABLE if not exists `{project}.{dataset}.cl_policy_monthly_inforce`
(
	InforceDateKey INT64,,
	InforceDate DATE,
	AccountNumber STRING,
	PolicyNumber STRING,
	PolicyNumber_LocationNumber STRING,
	PolicyOrigEffDate INT64,	
	PolicyEffectiveDate INT64,
	PolicyExpirationDate INT64,
	AccountSegment STRING,
	ProductLineCode STRING,
	LOB_Code STRING,
	NewRenewal STRING,								
	AutoPayIndicator BOOL,
	PaymentMethod STRING,
	Producer_Service_Agency_Master_Code STRING,
    Producer_Service_Agency_Master_Name STRING,
	Producer_Service_Agency_SubCode STRING,
	Producer_Service_Agency_Code STRING,
	Producer_Service_Agency_Name STRING,
	Primary_Insured_State STRING,
	Primary_Insured_Country STRING,
	Policy_JB_JS_Premium NUMERIC, 
	Policy_BOP_Premium NUMERIC,
	Policy_UMB_Premium NUMERIC,
	Policy_InforcePremium NUMERIC,
    bq_load_date    DATE
)
PARTITION BY InforceDate;


 