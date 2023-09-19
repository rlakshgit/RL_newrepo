-- tag: Account - tag ends/
/************************************************************
	Kimberlite Extract Query
		Account.sql
			Big Query Converted
	Descr:
		This table holds Account, type, and status info mainly from GW PC and BC (without overlap). BC also
		provides limited billing account info such as autopay, paymentmethod, etc. This functions as a raw table
		at the Kimberlite core layer level.

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/27/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--select * from bi_dds.DimAccount where AccountNumber = '3000072802'
*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_Account`
AS SELECT outerselect.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.Account` (
	SourceSystem
	,AccountKey
	,SourceCenter
	,AccountPublicID
	,AccountID
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
	,bq_load_date
)

WITH Config AS (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
)

SELECT
	sourceConfig.Value AS SourceSystem
	,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,AccountPublicID))	AS AccountKey
	,CASE	WHEN SUBSTRING(AccountPublicID,1,3) = 'pc:' THEN 'PC' 
			WHEN SUBSTRING(AccountPublicID,1,3) = 'bc:' THEN 'BC'
		ELSE NULL END AS SourceCenter
	,Account.AccountPublicID
	,Account.AccountID
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
	,DATE('{date}') AS bq_load_date
	--,CURRENT_DATE() AS bq_load_date

FROM (
	   	SELECT
			COALESCE(pc_account.PublicID, bc_account.PublicID) AS AccountPublicID
			,COALESCE(pc_account.AccountNumber,bc_account.AccountNumber) AS AccountNumber				
			,COALESCE(pc_account.CreateTime,bc_account.CreateTime) AS AccountCreatedDate
			,COALESCE(pc_account.ID,bc_account.ID) AS AccountID

		-- pc fields
			,pc_account.OrigPolicyEffDate_JMIC_PL			AS AccountOrigInceptionDate
			,pctl_source_jmic_pl.NAME						AS AccountDistributionSource  /**** ask Sai about this field? ***/
			,pctl_apptakenby_jmic_pl.NAME					AS ApplicationTakenBy
			,pctl_accountstatus.TYPECODE					AS AccountStatus
			,CAST(pc_account.IsJeweler_JMIC_PL AS INT64)					AS IsJewelerAccount
			--,pc_account.referringjeweler_jm				AS ReferringJeweler
			,pcx_jeweler_jm.PublicID						AS ReferringJewelerPublicID
			,COALESCE(CAST(pc_account.SpecialAccount_JMIC AS INT64),0) AS IsSpecialAccount
			,pctl_accountorgtype.TYPECODE					AS AccountOrgType
			,pc_account.ProxyDate_JMIC						AS ProxyReceivedDate			
			,IFNULL(CAST(pc_account.Resolicit_JMIC AS INT64),0)		AS IsAccountResolicit
			,pc_account.ResolicitDate_JMIC					AS AccountResolicitDate
			,CAST(pc_account.IsPaperlessDelivery_JMIC AS INT64)			AS IsPaperlessDelivery
			,pc_account.PaperlessStatusDate_JMIC			AS PaperlessDeliveryStatusDate
			,pctl_preferredmethodcomm_jmic.Name				AS PreferredCommunicationMethodCode
			,CASE WHEN pc_account.Retired>0 
				THEN pc_account.UpdateTime END				AS RetiredDateTime

		-- bc fields			
			,bctl_accounttype.NAME							AS BillingAccountType
			,CASE WHEN bctl_paymentmethod.ID in (1,4) 
				THEN 1 ELSE 0 END							AS AutoPayIndicator  --1=ACH/EFT;4=Credit Card
			,bctl_paymentmethod.Name						AS PaymentMethod
			,bc_paymentinstrument.CreditCardExpDate_JMIC	AS CreditCardExpDate			

	
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
			FULL OUTER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_account` WHERE _PARTITIONTIME = {partition_date}) AS bc_account
				ON bc_account.AccountNumber = pc_account.AccountNumber 
			LEFT JOIN `{project}.{bc_dataset}.bctl_accounttype` AS bctl_accounttype
				ON bc_account.AccountType = bctl_accounttype.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_accountstatus` AS pctl_accountstatus
				ON pctl_accountstatus.ID = pc_account.AccountStatus
			LEFT JOIN `{project}.{pc_dataset}.pctl_accountorgtype` AS pctl_accountorgtype
				ON pctl_accountorgtype.ID = pc_account.AccountOrgType

			LEFT JOIN `{project}.{pc_dataset}.pctl_source_jmic_pl` AS pctl_source_jmic_pl
				ON pc_account.Source_JMIC_PL = pctl_source_jmic_pl.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_apptakenby_jmic_pl` AS pctl_apptakenby_jmic_pl
				ON pc_account.ApplicationTakenBy_JMIC_PL = pctl_apptakenby_jmic_pl.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_preferredmethodcomm_jmic` AS pctl_preferredmethodcomm_jmic
				ON pc_account.Preferred_JMIC_PL = pctl_preferredmethodcomm_jmic.ID	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jeweler_jm
				ON pcx_jeweler_jm.ID = pc_account.referringjeweler_jm

		--AutoPayIndicator and PaymentMethod from GW Account
			LEFT OUTER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_acctpmntinst` WHERE _PARTITIONTIME = {partition_date}) AS bc_acctpmntinst
				ON bc_account.ID = bc_acctpmntinst.OwnerID
			LEFT OUTER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_paymentinstrument` WHERE _PARTITIONTIME = {partition_date}) AS bc_paymentinstrument
				ON bc_acctpmntinst.ForeignEntityID = bc_paymentinstrument.ID
			LEFT OUTER JOIN `{project}.{bc_dataset}.bctl_paymentmethod` AS bctl_paymentmethod
				ON bctl_paymentmethod.ID = bc_paymentinstrument.PaymentMethod
		
		WHERE 1 = 1

	) AS Account

	INNER JOIN Config AS sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN Config AS hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN Config AS hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) outerselect