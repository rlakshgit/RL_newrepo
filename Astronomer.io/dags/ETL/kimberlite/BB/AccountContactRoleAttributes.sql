-- tag: AccountContactRoleAttributes - tag ends/
/****************************************************************************************************************
	Building Block Extract
		AccountContactRoleAttributes.sql
			BigQuery Converted

	DESC: All Accounts and their Contact Roles, using data from Kimberlite Core Account and AccountContactRole tables.

	Q: May want to join to BB contact table to flesh out all contact details - address, etc. - before calling this DONE

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/14/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--select * from bi_dds.DimAccount where AccountNumber = '3000072802'
*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_AccountContactRoleAttributes`
AS SELECT outerselect.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.AccountContactRoleAttributes`
(
	SourceSystem
	,AccountContactRoleKey
	,AccountKey
	,AccountPublicID
	,AccountNumber
	,AccountCreatedDate
	,AccountOrigInceptionDate
	,AccountDistributionSource
	,ApplicationTakenBy
	,AccountStatus
	,IsJewelerAccount
	,ReferringJewelerPublicID
	,IsSpecialAccount
	,AccountOrgType
	,ProxyReceivedDate
	,IsAccountResolicit
	,AccountResolicitDate
	,IsPaperlessDelivery
	,PaperlessDeliveryStatusDate
	,PreferredCommunicationMethodCode
	,RetiredDateTime			
	,BillingAccountType
	,AutoPayIndicator
	,PaymentMethod
	,CreditCardExpDate
	,SourceCenter
	,AccountContactRolePublicID
	,IsAccountActive
	,RetiredAccount
	,AccountContactRoleType
	,AccountContactRoleName
	,bq_load_date
)

SELECT 
	--Keys
	AccountContactRole.SourceSystem
	,AccountContactRole.AccountContactRoleKey
	,Account.AccountKey
	,Account.AccountPublicID
	--Account attributes
	,Account.AccountNumber
	,Account.AccountCreatedDate
	,Account.AccountOrigInceptionDate
	,Account.AccountDistributionSource
	,Account.ApplicationTakenBy
	,Account.AccountStatus
	,Account.IsJewelerAccount
	,Account.ReferringJewelerPublicID
	,Account.IsSpecialAccount
	,Account.AccountOrgType
	,Account.ProxyReceivedDate
	,Account.IsAccountResolicit
	,Account.AccountResolicitDate
	,Account.IsPaperlessDelivery
	,Account.PaperlessDeliveryStatusDate
	,Account.PreferredCommunicationMethodCode
	,Account.RetiredDateTime			
	,Account.BillingAccountType
	,Account.AutoPayIndicator
	,Account.PaymentMethod
	,Account.CreditCardExpDate
	--AccountContactRole attributes
	,AccountContactRole.SourceCenter
	,AccountContactRole.AccountContactRolePublicID
	,AccountContactRole.IsAccountActive
	,AccountContactRole.RetiredAccount
	,AccountContactRole.AccountContactRoleType
	,AccountContactRole.AccountContactRoleName
	,DATE('{date}')										AS bq_load_date
	--,CURRENT_DATE() AS bq_load_date
		
FROM 
	(SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date})) AS Account
	INNER JOIN (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date})) AS AccountContactRole
		ON AccountContactRole.AccountPublicID= Account.AccountPublicID
--) outerselect