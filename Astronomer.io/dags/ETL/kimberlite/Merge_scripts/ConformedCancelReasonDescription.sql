--SELECT '(''' + [SourceSystem] + ''',''' + [SourceSystemCancelReasonCode] + ''',''' + [ConformedPolicyCancelReasonDescription] + ''',' + ISNULL('''' + [DataCallCancelReasonCategory] + '''','NULL') + ',' + ISNULL('''' + [PartnerCancelReason] + '''','NULL') + ',' + ISNULL('''' + CAST([IsActive] AS VARCHAR(10)) + '''','NULL') + '),'
--	FROM [bief_src].[ConformedCancelReasonDescription]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ConformedCancelReasonDescription`
(
	SourceSystem STRING,
	SourceSystemCancelReasonCode STRING,
	ConformedPolicyCancelReasonDescription STRING,
	DataCallCancelReasonCategory STRING,
	PartnerCancelReason STRING,
	IsActive STRING
);

MERGE INTO `{project}.{dest_dataset}.ConformedCancelReasonDescription` AS Target 
USING (
		SELECT 'GW' as SourceSystem,'agentnolongerrepresents_JMIC' as SourceSystemCancelReasonCode,'Agency/Agent No Longer Represents JM' as ConformedPolicyCancelReasonDescription,'Them' as DataCallCancelReasonCategory,'Underwriting Initiated Cancellation' as PartnerCancelReason,'1' as IsActive UNION ALL
		SELECT 'GW','agentrequest_JMIC','Agency/Agent Request','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','agentservice_JMIC','Dissatisfaction - Agency/Agent Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','alarmrequirement_JMIC','Competition - Alarm Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','cancel','Cancellation of Underlying Insurance','Them',NULL,'1' UNION ALL
		SELECT 'GW','cancelrewrite_JMIC','Internal Cancel/Rewrite','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','changepolcytype_JMIC','Change in Policy Type','Them',NULL,'1' UNION ALL
		SELECT 'GW','changepolicyterm_JMIC','Change in Policy Term','Them',NULL,'1' UNION ALL
		SELECT 'GW','claimservice_JMIC','Dissatisfaction - Claim Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','closed_JMIC','Store Closed','Them',NULL,'1' UNION ALL
		SELECT 'GW','combined_policies_JMIC_PL','Combine Policies','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','condemn','Condemned/Unsafe','Us',NULL,'1' UNION ALL
		SELECT 'GW','conExtFrmsNtRtrnd_JMIC','Conditional Extension - Forms Not Returned','Them',NULL,'1' UNION ALL
		SELECT 'GW','courtesycancelflat_JMIC_CL','Courtesy Flat Cancel - Commerical','Them',NULL,'1' UNION ALL
		SELECT 'GW','courtesycancelflat_JMIC_PL','Courtesy Flat Cancel - Personal','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','coverage_JMIC','Competition - Product / Coverage','Them',NULL,'1' UNION ALL
		SELECT 'GW','covsNotAvailable_JMIC_PL','Requested Coverage/Limit Not Available','Us',NULL,'1' UNION ALL
		SELECT 'GW','creditandother_JMIC_PL','Credit and Other','Us',NULL,'1' UNION ALL
		SELECT 'GW','creditsecurityexposure_JMIC_PL','Credit and Security Exposure','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','credittravelexposure_JMIC_PL','Credit and Travel Exposure','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','criminal','Criminal Conduct by the Insured','Us',NULL,'1' UNION ALL
		SELECT 'GW','criminalrecord_JMIC_PL','Criminal Record','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','divorce_JMIC_PL','Divorce/Break in Relationship','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','doesnotmeetreq_JMIC_PL','Does Not Meet Program or Program Requirements','Us',NULL,'1' UNION ALL
		SELECT 'GW','duplicateSubmissions_JMIC','Duplicate Submissions','Them',NULL,'1' UNION ALL
		SELECT 'GW','eligibility','No Longer Eligible for Group or Program','Us',NULL,'1' UNION ALL
		SELECT 'GW','failcoop','Failure to Cooperate','Us',NULL,'1' UNION ALL
		SELECT 'GW','failsafe','Failure to Comply with Safety Recommendations','US',NULL,'1' UNION ALL
		SELECT 'GW','failterm','Failure to Comply with Terms and Conditions','Us',NULL,'1' UNION ALL
		SELECT 'GW','fincononpay','Insured\'s request - (Finance co. nonpay)','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'GW','flatrewrite','Policy Rewritten','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','formsnotreturned_JMIC','Forms Not Returned','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','fraud','Fraud','Them',NULL,'1' UNION ALL
		SELECT 'GW','infonotprovided_JMIC_PL','Required Information Not Provided','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','insaferequirement_JMIC','Competition - In Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','insured_deceased_JMIC_PL','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insureddeceased_JMIC_CL','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insuredrequestAlmReq_JMIC','Insured Request - Alarm Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','insuredrequestHO_JMIC_PL','Insured Request - Homeowners','Them','Insured Requested - Competitive','1' UNION ALL
		SELECT 'GW','insuredrequestHOconv_JMIC_PL','Add to Homeowners - Convenience','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insuredrequestHOprice_JMIC_PL','Add to Homeowners - Price','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insuredrequestInSafeReq_JMIC','Insured Request - In safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','insuredrequestNoCovNeeded_JMIC_PL','Insured Request - Coverage Not Needed','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insuredrequestPrice_JMIC_PL','Insured Request - Price','Them','Insured Requested - Competitive','1' UNION ALL
		SELECT 'GW','insuredrequestProdCov_JMIC','Insured Request - Product / Coverage','Them',NULL,'1' UNION ALL
		SELECT 'GW','insuredrequestReturned_JMIC_PL','Insured Request - Returned/Sold','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','insuredrequestSafeReq_JMIC','Insured Request - Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','insuredrequestSelfIns_JMIC','Insured Request - Self-Insure','Them',NULL,'1' UNION ALL
		SELECT 'GW','insuredrequestUnknown_JMIC_PL','Policy Not Taken','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','item_value_adjustment','Item Value Adjustment','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','jmservice_JMIC','Dissatisfaction - JM Service','Them',NULL,'1' UNION ALL
		SELECT 'GW','knownbutnotlistedco_JMIC','Known But Not Listed-Co','Us',NULL,'1' UNION ALL
		SELECT 'GW','loss_jewelry_JMIC_PL','Loss of Article','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','LossHistory','Loss History','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','material_change_risk_JMIC_PL','Material Change in Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','materialchangeinrisk_JMIC_CL','Material Change in Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','midtermrewrite','Policy Rewritten - Mid-term','Them',NULL,'1' UNION ALL
		SELECT 'GW','misrepresent_JMIC_CL','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','misrepresentation_JMIC_PL','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','moved_out_of_country_JMIC','Moved Out of Country','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','newownereligable_JMIC','Sold - New Owner Eligible/JM','Them',NULL,'1' UNION ALL
		SELECT 'GW','newownernoteligable_JMIC','Sold - New Owner Not Eligible','Them',NULL,'1' UNION ALL
		SELECT 'GW','no_reason_given_JMIC_PL','No Reason Given','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','noc','Insured\'s request - N.O.C','Them',NULL,'1' UNION ALL
		SELECT 'GW','noemployee','No Employees/Operations','Them',NULL,'1' UNION ALL
		SELECT 'GW','nondisclose','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','nonpayment','Payment Not Received','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'GW','nonreport','Non-report of Payroll or Failure to Cooperate','Them',NULL,'1' UNION ALL
		SELECT 'GW','noreasongiven_JMIC_CL','No Reason Given','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','nosupportBlock_JMIC','No Supporting Block','Us',NULL,'1' UNION ALL
		SELECT 'GW','nosupportingblock_JMIC','No Supporting Block','Us',NULL,'1' UNION ALL
		SELECT 'GW','nottaken','Payment Not Received','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'GW','ny_anti_arson_JMIC','NY Anti-arson','Them',NULL,'1' UNION ALL
		SELECT 'GW','OpsChars','Operations Characteristics','Us',NULL,'1' UNION ALL
		SELECT 'GW','other_JMIC','Other','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','other_JMIC_CL','Other','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','other_JMIC_PL','Other','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','other-carrier_JMIC','Other - Carrier','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','other-carrier_JMIC_PL','Other','Us',NULL,'1' UNION ALL
		SELECT 'GW','PaymentHistory','Payment History','Us',NULL,'1' UNION ALL
		SELECT 'GW','policynottaken_JMIC_PL','Policy Not Taken','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','policyrewritten_JMIC','Policy Rewritten','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','ProdRequirements','Does Not Meet Program or Product Requirements','Us',NULL,'1' UNION ALL
		SELECT 'GW','ProductsChars','Products Characteristics','Us',NULL,'1' UNION ALL
		SELECT 'GW','reinsurance','Loss of Reinsurance','Them',NULL,'1' UNION ALL
		SELECT 'GW','req_info_not_given_JMIC_PL','Required Information Not Provided','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','requestedcoveragenotavailable_JMIC_PL','Requested Coverage/Limit Not Available','Us',NULL,'1' UNION ALL
		SELECT 'GW','requiredinfonotprovided_JMIC','Required Information Not Provided','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','riskchange','Substantial Change in Risk or Increase in Hazard','Us',NULL,'1' UNION ALL
		SELECT 'GW','saferequirement_JMIC','Competition - Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'GW','selfinsure_JMIC','Competition - Self Insure','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'GW','sold','Out of Business/Sold','Them',NULL,'1' UNION ALL
		SELECT 'GW','suspension','Suspension or Revocation of License or Permits','Them',NULL,'1' UNION ALL
		SELECT 'GW','theft_JMIC_PL','Theft of Article','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','unacceptablerisk_JMIC','Unacceptable Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','unfavorablereport_JMIC','Unfavorable Report','Us',NULL,'1' UNION ALL
		SELECT 'GW','uwalarmrequirement','Underwriting - Alarm Requirement','Us',NULL,'1' UNION ALL
		SELECT 'GW','uwinsaferequirement','Underwriting - In Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'GW','uwreasons','Underwriting Reasons','Us',NULL,'1' UNION ALL
		SELECT 'GW','uwsaferequirement','Underwriting - Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'GW','uwunwillingtocomply_JMIC','Underwriting - Unwilling to Comply','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'GW','vacant','Vacant; Below Occupancy Limit','Them',NULL,'1' UNION ALL
		SELECT 'GW','violation','Violation of Health, Safety, Fire, or Codes','Us',NULL,'1' UNION ALL
		SELECT 'GW','wrapup','Participation in Wrap-up Complete','Them',NULL,'1' UNION ALL
		SELECT 'PAS','10','Competition - Price','Them','Insured Requested - Competitive','1' UNION ALL
		SELECT 'PAS','11','Competition - Self Insure','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','12','Competition - Alarm Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PAS','13','Competition - Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PAS','14','Competition - In Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PAS','16','Competition - Other','Them',NULL,'1' UNION ALL
		SELECT 'PAS','17','Competition - Product / Coverage','Them',NULL,'1' UNION ALL
		SELECT 'PAS','21','Non-Pay - Finance Company','Them',NULL,'1' UNION ALL
		SELECT 'PAS','30','Store Closed','Them',NULL,'1' UNION ALL
		SELECT 'PAS','31','Sold - New Owner Eligible/JM','Them',NULL,'1' UNION ALL
		SELECT 'PAS','32','Sold - New Owner Not Eligible','Them',NULL,'1' UNION ALL
		SELECT 'PAS','51','Agency/Agent Request','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','53','Material Change in Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','54','Unacceptable Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','55','Unfavorable Report','Us',NULL,'1' UNION ALL
		SELECT 'PAS','56','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','57','No Supporting Block','Us',NULL,'1' UNION ALL
		SELECT 'PAS','58','Craftsman No Longer Needed','Us',NULL,'1' UNION ALL
		SELECT 'PAS','60','Underwriting - Unwilling to Comply','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','61','Underwriting - Alarm Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PAS','62','Underwriting - Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PAS','63','Underwriting - In Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PAS','64','Underwriting - Loss History','Us',NULL,'1' UNION ALL
		SELECT 'PAS','68','Known But Not Listed-Co','Us',NULL,'1' UNION ALL
		SELECT 'PAS','69','Known But Not Listed-Insd','Them',NULL,'1' UNION ALL
		SELECT 'PAS','70','Unknown','Them',NULL,'1' UNION ALL
		SELECT 'PAS','71','Information Incomplete','Them',NULL,'1' UNION ALL
		SELECT 'PAS','72','Forms Not Returned','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','75','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','80','Dissatisfaction - Claim Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','81','Dissatisfaction - JM Service','Them',NULL,'1' UNION ALL
		SELECT 'PAS','90','Dissatisfaction - Agency/Agent Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','91','Agency/Agent No Longer Represents JM','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','AH','Add to Homeowners','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','AL','Competition - Alarm Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PAS','AR','Agency Request','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','AS','Dissatisfaction - Agency/Agent Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','CP','Combine Policies','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','CR','Material Change in Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','CS','Dissatisfaction - Claim Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','ID','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','IE','Ineligible Applicant','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','IO','Ineligible Applicant','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','IR','Insured Request','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','JS','Dissatisfaction - JM Service','Them',NULL,'1' UNION ALL
		SELECT 'PAS','LS','Loss','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','MA','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','NE','NSF Equity','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'PAS','NF','NSF','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'PAS','NI','No Initial Premium','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'PAS','NP','No Payment','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'PAS','PE','Change in Policy Term','Them',NULL,'1' UNION ALL
		SELECT 'PAS','PF','Premium Finance Company Request','Them',NULL,'1' UNION ALL
		SELECT 'PAS','PR','Competition - Price','Them','Insured Requested - Competitive','1' UNION ALL
		SELECT 'PAS','PT','Change in Policy Type','Them',NULL,'1' UNION ALL
		SELECT 'PAS','RJ','Return Jewelry','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','RW','Internal Cancel/Rewrite','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PAS','SI','Competition - Self Insure','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PAS','SR','Competition - Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PAS','UC','Underwriting - Unwilling to Comply','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','10','Competition - Price','Them','Insured Requested - Competitive','1' UNION ALL
		SELECT 'PCA','11','Competition - Self Insure','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','12','Competition - Alarm Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PCA','13','Competition - Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PCA','14','Competition - In Safe Requirement','Them',NULL,'1' UNION ALL
		SELECT 'PCA','15','Add to Homeowners','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','16','Competition - Other','Them',NULL,'1' UNION ALL
		SELECT 'PCA','17','Competition - Product / Coverage','Them',NULL,'1' UNION ALL
		SELECT 'PCA','20','Non-Pay - JMIC Pay Plan','Them',NULL,'1' UNION ALL
		SELECT 'PCA','21','Non-Pay - Finance Company','Them',NULL,'1' UNION ALL
		SELECT 'PCA','22','Non-Pay - Other','Them',NULL,'1' UNION ALL
		SELECT 'PCA','23','No Payment','Them','Cancellation for Non-Payment','1' UNION ALL
		SELECT 'PCA','30','Store Closed','Them',NULL,'1' UNION ALL
		SELECT 'PCA','31','Sold - New Owner Eligible/JM','Them',NULL,'1' UNION ALL
		SELECT 'PCA','32','Sold - New Owner Not Eligible','Them',NULL,'1' UNION ALL
		SELECT 'PCA','33','Return Jewelry','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','34','Loss of Jewelry','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','35','Ineligible Applicant','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','40','Policy Rewritten','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','41','Combine Policies','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','42','Cancel/Rewrite System Limitation','Them',NULL,'1' UNION ALL
		SELECT 'PCA','51','Agency/Agent Request','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','52','Insured Request','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','53','Material Change in Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','54','Unacceptable Risk','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','55','Unfavorable Report','Us',NULL,'1' UNION ALL
		SELECT 'PCA','56','Misrepresentation','US','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','57','No Supporting Block','Us',NULL,'1' UNION ALL
		SELECT 'PCA','58','Craftsman No Longer Needed','Us',NULL,'1' UNION ALL
		SELECT 'PCA','59','Risk Unit No Longer Needed','Us',NULL,'1' UNION ALL
		SELECT 'PCA','60','Underwriting - Unwilling to Comply','Us','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','61','Underwriting - Alarm Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PCA','62','Underwriting - Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PCA','63','Underwriting - In Safe Requirement','Us',NULL,'1' UNION ALL
		SELECT 'PCA','64','Underwriting - Loss History','Us',NULL,'1' UNION ALL
		SELECT 'PCA','69','Known But Not Listed Reason','Them',NULL,'1' UNION ALL
		SELECT 'PCA','70','Unknown','Them',NULL,'1' UNION ALL
		SELECT 'PCA','71','Information Incomplete','Them',NULL,'1' UNION ALL
		SELECT 'PCA','72','Forms Not Returned','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','73','Disliked Policy - PJ','Them',NULL,'1' UNION ALL
		SELECT 'PCA','74','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','75','Insured Deceased','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','80','Dissatisfaction - Claim Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','81','Dissatisfaction - JM Service','Them',NULL,'1' UNION ALL
		SELECT 'PCA','85','Other','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','86','Misc Closed File - PJ','Them',NULL,'1' UNION ALL
		SELECT 'PCA','87','Event Ended - WP','Them',NULL,'1' UNION ALL
		SELECT 'PCA','90','Dissatisfaction - Agency/Agent Service','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','91','Agency/Agent No Longer Represents JM','Them','Underwriting Initiated Cancellation','1' UNION ALL
		SELECT 'PCA','95','Other','Them','Insured Requested Cancellation','1' UNION ALL
		SELECT 'PCA','AP','Account Payable Lapse','Them',NULL,'1' UNION ALL
		SELECT 'PCA','RE','Renewal Lapse','Them',NULL,'1'
	) 
AS Source --([SourceSystem], [SourceSystemCancelReasonCode], [ConformedPolicyCancelReasonDescription], [DataCallCancelReasonCategory], [PartnerCancelReason], [IsActive]) 
ON Target.SourceSystem = Source.SourceSystem
	AND Target.SourceSystemCancelReasonCode = Source.SourceSystemCancelReasonCode
WHEN MATCHED THEN UPDATE
SET ConformedPolicyCancelReasonDescription = Source.ConformedPolicyCancelReasonDescription
	,DataCallCancelReasonCategory = Source.DataCallCancelReasonCategory
	,PartnerCancelReason = Source.PartnerCancelReason
	,IsActive = Source.IsActive
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (SourceSystem, SourceSystemCancelReasonCode, ConformedPolicyCancelReasonDescription, DataCallCancelReasonCategory, PartnerCancelReason, IsActive) 
	VALUES (SourceSystem, SourceSystemCancelReasonCode, ConformedPolicyCancelReasonDescription, DataCallCancelReasonCategory, PartnerCancelReason, IsActive) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;