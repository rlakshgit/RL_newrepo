-- tag: AccountAttributes - tag ends/
/****************************************************************************************************************
	Building Block Extract
		AccountAttributes.sql
			BigQuery Converted

	Desc:	This table provides a reportable Account table with a link (AccountHolderContactKey) to the AccountHolder
			info found in the ContactAttributes table (via ContactKey). The scope is Policy Center Account IDs. 
			The Roles included are: pctl_accountcontactrole.Typecode IN ('OwnerOfficer' ,'InsuredContactACR_JMIC',
			'AccountHolder')

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/13/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--select * from bi_dds.DimAccount where AccountNumber = '3000072802'
*******************************************************************************************************************/
---------------------------------------------------------------------------
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_AccountAttributes`
AS SELECT outerselect.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.AccountAttributes`
(
	SourceSystem
	,AccountKey
	,SourceCenter
	,AccountHolderContactKey
	,InsuredContactKey
	,AccountHolderContactPublicID
	,ReferringJewelerPublicID
	,InsuredContactPublicID
	,AccountID
	,AccountPublicID
	,AccountNumber
	,AccountCreatedDate
	,AccountOrigInceptionDate
	,AccountDistributionSource
	,ApplicationTakenBy
	,AccountStatus
	,IsJewelerAccount
	,IsSpecialAccount
	,AccountOrgType
	,ProxyReceivedDate
	,IsAccountResolicit
	,AccountResolicitDate
	,ISPaperlessDelivery
	,PaperlessDeliveryStatusDate
	,PreferredCommunicationMethodCode
	,RetiredDateTime
	,BillingAccountType
	,AutoPayIndicator
	,PaymentMethod
	,CreditCardExpDate
	,JBTRating
	,PrimaryAddressTypeCode
	,PrimaryAddressLine1
	,PrimaryAddressLine2
	,PrimaryAddressLine3
	,PrimaryAddressCity
	,PrimaryAddressCounty
	,PrimaryAddressStateCode
	,PrimaryAddressPostalCode
	,PrimaryAddressCountryCode
	,MailingAddressLine
	,MailingAddressLine2
	,MailingAddressLine3
	,MailingAddressCity
	,MailingAddressCounty
	,MailingAddressStateCode
	,MailingAddressPostalCode
	,MailingAddressCountryCode
	,bq_load_date
)

-- With this select, may want to filter out either the BC sourced records, or just where AccountHolderContactPublicID IS NULL
WITH Config AS (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
)
-- CTE for GW policy center contacts
,CTE_Contacts AS 
(	SELECT AccountContactRole.AccountID 
			,AccountContactRole.AccountContactRoleType
			,Contact.ContactKey		--replaced with Kimberlite Core Contact table
			,Contact.ContactPublicID
			,Contact.PrimaryAddressID
			,Contact.MailingAddressID
			,AccountContactRole.RetiredAccount
			,AccountContactRole.LastUpdateTime
			,AccountContactRole.SourceCenter
			,Contact.JBTRating	--Applies only to Jeweler Roles
			--,pc_mailing_address.AddressTypeCode

			,pc_address_primary.AddressCode							AS AddressCode
			,pc_address_primary.AddressTypeCode						AS PrimaryAddressTypeCode
			,pc_address_primary.AddressLine1						AS PrimaryAddressLine1
			,pc_address_primary.AddressLine2						AS PrimaryAddressLine2
			,pc_address_primary.AddressLine3						AS PrimaryAddressLine3
			,pc_address_primary.City								AS PrimaryAddressCity
			,pc_address_primary.County								AS PrimaryAddressCounty
			,pc_address_primary.StateCode							AS PrimaryAddressStateCode
			,pc_address_primary.PostalCode							AS PrimaryAddressPostalCode
			,pc_address_primary.CountryCode							AS PrimaryAddressCountryCode
			
			,pc_mailing_address.AddressLine1						AS MailingAddressLine
			,pc_mailing_address.AddressLine2						AS MailingAddressLine2
			,pc_mailing_address.AddressLine3						AS MailingAddressLine3
			,pc_mailing_address.City								AS MailingAddressCity
			,pc_mailing_address.County								AS MailingAddressCounty
			,pc_mailing_address.StateCode							AS MailingAddressStateCode
			,pc_mailing_address.PostalCode							AS MailingAddressPostalCode
			,pc_mailing_address.CountryCode							AS MailingAddressCountryCode

	FROM (SELECT * FROM `{project}.{core_dataset}.AccountContactRole` WHERE bq_load_date = DATE({partition_date})) AS AccountContactRole
		INNER JOIN (SELECT * FROM `{project}.{core_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})) AS Contact
			ON AccountContactRole.ContactKey = Contact.ContactKey
		LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date})) AS pc_address_primary
			ON Contact.PrimaryAddressID = pc_address_primary.AddressID
			AND Contact.SourceCenter = pc_address_primary.SourceCenter
		LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date})) AS pc_mailing_address
			ON Contact.MailingAddressID = pc_mailing_address.AddressID --May reference via contactaddress intersect table
			AND Contact.SourceCenter = pc_mailing_address.SourceCenter
			AND pc_mailing_address.AddressTypeCode = 'Mailing_JMIC'
	WHERE 1=1
	AND ( AccountContactRole.RetiredAccount = 0 AND AccountContactRole.AccountContactRoleType IN ('OwnerOfficer' ,'InsuredContactACR_JMIC','AccountHolder')
		 OR	AccountContactRole.AccountContactRoleType IN ('ReferringJeweler_JMIC' ) )
	
)
---Contacts can have more than one entry
,CTE_Contacts1 AS 
(   
	SELECT	AccountID,AccountContactRoleType,ContactKey,ContactPublicID,RetiredAccount,SourceCenter,PrimaryAddressID,MailingAddressID, JBTRating
			,PrimaryAddressTypeCode
			,PrimaryAddressLine1
			,PrimaryAddressLine2
			,PrimaryAddressLine3
			,PrimaryAddressCity
			,PrimaryAddressCounty
			,PrimaryAddressStateCode
			,PrimaryAddressPostalCode
			,PrimaryAddressCountryCode
			,MailingAddressLine
			,MailingAddressLine2
			,MailingAddressLine3
			,MailingAddressCity
			,MailingAddressCounty
			,MailingAddressStateCode
			,MailingAddressPostalCode
			,MailingAddressCountryCode
			,ROW_NUMBER() OVER (PARTITION BY AccountID,AccountContactRoleType ORDER BY AccountID,RetiredAccount,LastUpdateTime DESC) AS Chooseone
	FROM CTE_Contacts  
)

--select * from CTE_Contacts1 

SELECT
	sourceConfig.Value AS SourceSystem
	,CASE WHEN AccountHolderContactPublicID IS NOT NULL
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value, AccountHolderContactPublicID)) 
		END AS AccountKey
	,PCAccount.SourceCenter
	,PCAccount.AccountHolderContactKey
	,PCAccount.InsuredContactKey
	,PCAccount.AccountHolderContactPublicID
	,PCAccount.ReferringJewelerPublicID
	,PCAccount.InsuredContactPublicID
	,PCAccount.AccountID
	,PCAccount.AccountPublicID
	,PCAccount.AccountNumber
	,PCAccount.AccountCreatedDate
	
	,PCAccount.AccountOrigInceptionDate
	,PCAccount.AccountDistributionSource
	,PCAccount.ApplicationTakenBy
	,PCAccount.AccountStatus
	,PCAccount.IsJewelerAccount
	,PCAccount.IsSpecialAccount
	,PCAccount.AccountOrgType
	,PCAccount.ProxyReceivedDate
	,PCAccount.IsAccountResolicit
	,PCAccount.AccountResolicitDate
	,PCAccount.IsPaperlessDelivery
	,PCAccount.PaperlessDeliveryStatusDate
	,PCAccount.PreferredCommunicationMethodCode
	,PCAccount.RetiredDateTime
	,PCAccount.BillingAccountType
	,PCAccount.AutoPayIndicator
	,PCAccount.PaymentMethod
	,PCAccount.CreditCardExpDate
	,PCAccount.JBTRating
	,PCAccount.PrimaryAddressTypeCode
	,PCAccount.PrimaryAddressLine1
	,PCAccount.PrimaryAddressLine2
	,PCAccount.PrimaryAddressLine3
	,PCAccount.PrimaryAddressCity
	,PCAccount.PrimaryAddressCounty
	,PCAccount.PrimaryAddressStateCode
	,PCAccount.PrimaryAddressPostalCode
	,PCAccount.PrimaryAddressCountryCode
	,PCAccount.MailingAddressLine
	,PCAccount.MailingAddressLine2
	,PCAccount.MailingAddressLine3
	,PCAccount.MailingAddressCity
	,PCAccount.MailingAddressCounty
	,PCAccount.MailingAddressStateCode
	,PCAccount.MailingAddressPostalCode
	,PCAccount.MailingAddressCountryCode
	,bq_load_date

FROM (

	SELECT
		AccountHolder.SourceCenter
		,AccountHolder.ContactKey AS AccountHolderContactKey
		,COALESCE(InsuredContact.ContactKey,OwnerContact.ContactKey) AS InsuredContactKey
		,AccountHolder.ContactPublicID AS AccountHolderContactPublicID
		--,AccountHolder.BillingPrimaryPayerContactPublicID
		,Account.ReferringJewelerPublicID
		,COALESCE(InsuredContact.ContactPublicID,OwnerContact.ContactPublicID) AS InsuredContactPublicID
		,Account.AccountID
		,Account.AccountPublicID
		,Account.AccountNumber AS AccountNumber
		,Account.AccountCreatedDate AS AccountCreatedDate
		,Account.AccountOrigInceptionDate
		,Account.AccountDistributionSource
		,Account.ApplicationTakenBy
		,Account.AccountStatus
		,Account.IsJewelerAccount
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
		,AccountHolder.JBTRating
		,AccountHolder.PrimaryAddressTypeCode
		,AccountHolder.PrimaryAddressLine1
		,AccountHolder.PrimaryAddressLine2
		,AccountHolder.PrimaryAddressLine3
		,AccountHolder.PrimaryAddressCity
		,AccountHolder.PrimaryAddressCounty
		,AccountHolder.PrimaryAddressStateCode
		,AccountHolder.PrimaryAddressPostalCode
		,AccountHolder.PrimaryAddressCountryCode
		,AccountHolder.MailingAddressLine
		,AccountHolder.MailingAddressLine2
		,AccountHolder.MailingAddressLine3
		,AccountHolder.MailingAddressCity
		,AccountHolder.MailingAddressCounty
		,AccountHolder.MailingAddressStateCode
		,AccountHolder.MailingAddressPostalCode
		,AccountHolder.MailingAddressCountryCode
		,DATE('{date}') AS bq_load_date
		--,CURRENT_DATE() AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date})) AS Account

		--Use a subselect of CTE to get all possible accountholders
			LEFT OUTER JOIN CTE_Contacts1 as AccountHolder
				ON Account.AccountID = AccountHolder.AccountID
				AND AccountHolder.AccountContactRoleType = 'AccountHolder'
				AND AccountHolder.Chooseone = 1

		--Use a subselect of CTE to get all possible Insured Contacts, defaulting to Owner if none found.
			LEFT OUTER JOIN CTE_Contacts1 as InsuredContact
				ON Account.AccountID = InsuredContact.AccountID
					AND InsuredContact.AccountContactRoleType = 'InsuredContactACR_JMIC'
					AND InsuredContact.Chooseone = 1

			LEFT OUTER JOIN CTE_Contacts1 as OwnerContact
				ON Account.AccountID = OwnerContact.AccountID
					AND OwnerContact.AccountContactRoleType = 'OwnerOfficer'
					AND OwnerContact.Chooseone = 1


	WHERE Account.SourceCenter = 'PC'
	AND	AccountHolder.ContactPublicID IS NOT NULL

	) AS PCAccount

	INNER JOIN Config sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN Config hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN Config hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) outerselect

/*
DQ Checks

SELECT  AccountKey
FROM `qa-edl.B_QA_ref_kimberlite.dar_AccountAttributes`
GROUP BY AccountKey
HAVING COUNT(AccountKey) > 1

*/