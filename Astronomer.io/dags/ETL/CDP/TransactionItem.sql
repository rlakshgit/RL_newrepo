WITH cte_ExtAppId
AS	(
		SELECT AccountNumber, ExternalApplicationKey, ApplicationRank
		FROM (
			  SELECT ApplicationId, AccountNumber, ExternalApplicationKey
  					,ROW_NUMBER() OVER(PARTITION BY t_Application.AccountNumber
							ORDER BY 
								IFNULL(t_Application.SubmissionDate,t_Application.CreatedDate) DESC
						) AS ApplicationRank
			  FROM {prefix}{plecom_dataset}.t_Application
			  WHERE DATE(t_Application._PARTITIONTIME) = DATE('{date}')
			) PlecomAccounts
		WHERE 1=1
		AND PlecomAccounts.ApplicationRank = 1
	)
, Config 
AS
 (SELECT 'SourceSystem' as Key, 'GW' as Value UNION ALL
  SELECT 'HashKeySeparator', '_' UNION ALL
  SELECT 'HashingAlgorithm', 'SHA2_256')
  
select distinct  
ConfigSource.Value AS SourceSystem
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, TransactionPublicID,ConfigHashSep.Value,PolicyLineCode)) AS SourceTransactionItemNumber
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, PolicyPeriodPublicID)) AS SourceTransactionNumber
	,Policytransaction.*
from
( 

	/**************************************/
	/***** PJ DIRECT SCH TRANSACTIONS *****/
	/**************************************/
	SELECT
		pcx_jmtransaction.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,pcx_jewelryitem_jmic_pl.ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,pcx_jewelryitem_jmic_pl.PublicID AS ItemPublicID
		,pcx_jwryitemcov_jmic_pl.PublicID AS CoveragePublicID
		,'SCH' AS CoverageTypeCode
		,pcx_jmtransaction.Amount AS TransactionAmount
		,pcx_jwryitemcov_jmic_pl.DirectTerm1 AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate ---pc_policyperiod.PeriodEnd
		,CASE WHEN pcx_cost_jmic.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		, concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.InspectionDate as DATETIME)), ' UTC') AS AppraisalDate --pcx_jewelryitem_jmic_pl.InspectionDate	
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.EffectiveDate as DATETIME)), ' UTC'),
 		          concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodStart as DATETIME)), ' UTC')
				  ) AS ItemEffectiveDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.ExpirationDate as DATETIME)), ' UTC'), 
				  concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodEnd as DATETIME)), ' UTC')
				  ) AS ItemExpirationDate
		,pcx_jewelryitem_jmic_pl.AppraisalReceived_JMIC	AS IsAppraisalReceived
		,pctl_brandtype_jmic_pl.NAME AS BrandName
		,pctl_jewelryitemstyle_jmic_pl.NAME AS ItemStyle
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.IVADate_JMIC as DATETIME)), ' UTC') AS IVADate  --pcx_jewelryitem_jmic_pl.IVADate_JMIC 
		,CAST(pcx_jewelryitem_jmic_pl.Vault AS INT64) as Vault
		,CAST(pcx_jewelryitem_jmic_pl.Safe AS INT64) AS Safe
		,LocatedWithContact.PublicID AS LocatedWithCustomerNumber
		,CAST(pcx_jewelryitem_jmic_pl.IsItemInactive AS INT64) AS IsItemInactive
		--- Find way to join to Products Table
		,'Personal Jewelry' AS JMProduct
		,NULLIF(REPLACE(REPLACE (pctl_classcodetype_jmic_pl.NAME, 'Gents ', ''), 'Ladies ', ''), '?') AS ItemClassDescription
		,CASE WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Gents%' THEN 'Gents' 
				WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Ladies%' THEN 'Ladies' 
				ELSE NULL END
		AS ItemClassGender
		/* Needed ? 
		,DATEDIFF(YEAR,OrigPolicyEffDate_JMIC_PL, pc_policyperiod.PeriodStart)	AS NumberOfYearsInsured
		,pc_policyperiod.TermNumber									AS TermNumber
		,pc_policyperiod.ModelNumber								AS ModelNumber 
		*/
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jmtransaction.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM {prefix}{pc_dataset}.pcx_jmtransaction
		INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic 
			ON pcx_jmtransaction.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryItemCov_JMIC_PL IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jmtransaction.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod perCost ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
		    AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
	-- Jewelry Item Coverage
		INNER JOIN {prefix}{pc_dataset}.pcx_jwryitemcov_jmic_pl
			ON pcx_jwryitemcov_jmic_pl.FixedID = pcx_cost_jmic.JewelryItemCov_JMIC_PL
			AND pcx_jwryitemcov_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pcx_jewelryitem_jmic_pl
			ON pcx_jewelryitem_jmic_pl.BranchID = pcx_jwryitemcov_jmic_pl.BranchID
			AND pcx_jewelryitem_jmic_pl.FixedID = pcx_jwryitemcov_jmic_pl.JewelryItem_JMIC_PL
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,perCost.PeriodEnd)

		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN {prefix}{pc_dataset}.pctl_brandtype_jmic_pl ON pctl_brandtype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemBrand_JM
		LEFT JOIN {prefix}{pc_dataset}.pctl_jewelryitemstyle_jmic_pl ON pctl_jewelryitemstyle_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemStyle_JM
		LEFT JOIN {prefix}{pc_dataset}.pctl_classcodetype_jmic_pl AS pctl_classcodetype_jmic_pl ON pctl_classcodetype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.ClassCodeType
	--logic to get the "Located WITH contact"
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_policycontactrole Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithRole
			ON LocatedWithRole.FixedID = pcx_jewelryitem_jmic_pl.LocatedWith
			AND LocatedWithRole.BranchID = pcx_jewelryitem_jmic_pl.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(LocatedWithRole.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(LocatedWithRole.ExpirationDate,perCost.PeriodEnd)
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontactrole 
		ON pc_accountcontactrole.ID = LocatedWithRole.AccountContactRole
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontact 
		ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_contact Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithContact 
		ON LocatedWithContact.ID = pc_accountcontact.Contact
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	--AND pcx_jmtransaction.PostedDate IS NOT NULL
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(perCost._PARTITIONTIME) = DATE('{date}')
        AND DATE(pcx_jmtransaction._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jwryitemcov_jmic_pl._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jewelryitem_jmic_pl._PARTITIONTIME) = DATE('{date}')	
		

	UNION ALL

	/**************************************/
	/***** PJ DIRECT UNS TRANSACTIONS *****/
	/**************************************/
	SELECT
		pcx_jmtransaction.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,NULL AS ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,'' AS ItemPublicID
		,pcx_jmpersonallinecov.PublicID AS CoveragePublicID
		,'UNS' AS CoverageTypeCode
		,pcx_jmtransaction.Amount AS TransactionAmount
		,NULL AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime							
		--,pc_job.UpdateTime
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_cost_jmic.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		,CAST(NULL AS STRING) AS AppraisalDate
		,CAST(NULL AS STRING) AS ItemEffectiveDate
		,CAST(NULL AS STRING) AS ItemExpirationDate
		,CAST(NULL AS BOOL) AS IsAppraisalReceived
		,CAST(NULL AS STRING) AS BrandName
		,CAST(NULL AS STRING) AS ItemStyle
		,CAST(NULL AS STRING) AS IVADate
		,NULL AS Vault
		,NULL AS Safe
		,CAST(NULL AS STRING) AS LocatedWithCustomerNumber
		,NULL AS IsItemInactive
		--- Find way to join to Products Table
		,CAST(NULL AS STRING) AS JMProduct
		,CAST(NULL AS STRING) AS ItemClassDescription
		,CAST(NULL AS STRING) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jmtransaction.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM {prefix}{pc_dataset}.pcx_jmtransaction
		INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic 
			ON pcx_jmtransaction.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryLineCov_JMIC_PL IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jmtransaction.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod perCost ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		-- Personal Line Coverage (unscheduled)
		INNER JOIN {prefix}{pc_dataset}.pcx_jmpersonallinecov
			ON pcx_jmpersonallinecov.FixedID = pcx_cost_jmic.JewelryLineCov_JMIC_PL
			AND pcx_jmpersonallinecov.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_jmpersonallinecov.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm-- COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	--AND pcx_jmtransaction.PostedDate IS NOT NULL
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(perCost._PARTITIONTIME) = DATE('{date}')
        AND DATE(pcx_jmtransaction._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jmpersonallinecov._PARTITIONTIME) = DATE('{date}')
	
	

	UNION ALL

	/**************************************/
	/***** PJ CEDED SCH TRANSACTIONS *****/
	/**************************************/
	SELECT
		pcx_plcededpremiumtrans_jmic.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,pcx_jewelryitem_jmic_pl.ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,pcx_jewelryitem_jmic_pl.PublicID AS ItemPublicID
		,pcx_jwryitemcov_jmic_pl.PublicID AS CoveragePublicID
		,'SCH' AS CoverageTypeCode
		,pcx_plcededpremiumtrans_jmic.CededPremium AS TransactionAmount
		,pcx_jwryitemcov_jmic_pl.DirectTerm1 AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime							
		--,pc_job.UpdateTime
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_cost_jmic.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		, concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.InspectionDate as DATETIME)), ' UTC') AS AppraisalDate --pcx_jewelryitem_jmic_pl.InspectionDate	
		--,COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate, pc_policyPeriod.PeriodStart) AS ItemEffectiveDate
		--,COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate, pc_policyPeriod.PeriodEnd) AS ItemExpirationDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.EffectiveDate as DATETIME)), ' UTC'),
 		          concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodStart as DATETIME)), ' UTC')
				  ) AS ItemEffectiveDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.ExpirationDate as DATETIME)), ' UTC'), 
				  concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodEnd as DATETIME)), ' UTC')
				  ) AS ItemExpirationDate
		,pcx_jewelryitem_jmic_pl.AppraisalReceived_JMIC	AS IsAppraisalReceived
		,pctl_brandtype_jmic_pl.NAME AS BrandName
		,pctl_jewelryitemstyle_jmic_pl.NAME AS ItemStyle
		--,pcx_jewelryitem_jmic_pl.IVADate_JMIC AS IVADate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_jewelryitem_jmic_pl.IVADate_JMIC as DATETIME)), ' UTC') AS IVADate
		,CAST(pcx_jewelryitem_jmic_pl.Vault as INT64) AS Vault
		,CAST(pcx_jewelryitem_jmic_pl.Safe AS INT64) AS Safe
		,LocatedWithContact.PublicID AS LocatedWithCustomerNumber
		,CAST(pcx_jewelryitem_jmic_pl.IsItemInactive AS INT64) AS IsItemInactive
		--- Find way to join to Products Table
		,'Personal Jewelry' AS JMProduct
		,NULLIF(REPLACE(REPLACE (pctl_classcodetype_jmic_pl.NAME, 'Gents ', ''), 'Ladies ', ''), '?') AS ItemClassDescription
		,CASE WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Gents%' THEN 'Gents' 
				WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Ladies%' THEN 'Ladies' 
				ELSE NULL END --don't like this... need to link to the item gender. Shortcut to get sample feeds out.
		AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_plcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM {prefix}{pc_dataset}.pcx_plcededpremiumtrans_jmic
		INNER JOIN {prefix}{pc_dataset}.pcx_plcededpremiumjmic
			ON pcx_plcededpremiumjmic.ID = pcx_plcededpremiumtrans_jmic.PLCededPremium_JMIC
		INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic 
			ON pcx_plcededpremiumjmic.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryItemCov_JMIC_PL IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
	-- Jewelry Item Coverage
		INNER JOIN {prefix}{pc_dataset}.pcx_jwryitemcov_jmic_pl
			ON pcx_jwryitemcov_jmic_pl.FixedID = pcx_cost_jmic.JewelryItemCov_JMIC_PL
			AND pcx_jwryitemcov_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pcx_jewelryitem_jmic_pl
			ON pcx_jewelryitem_jmic_pl.BranchID = pcx_jwryitemcov_jmic_pl.BranchID
			AND pcx_jewelryitem_jmic_pl.FixedID = pcx_jwryitemcov_jmic_pl.JewelryItem_JMIC_PL
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN {prefix}{pc_dataset}.pctl_brandtype_jmic_pl ON pctl_brandtype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemBrand_JM
		LEFT JOIN {prefix}{pc_dataset}.pctl_jewelryitemstyle_jmic_pl ON pctl_jewelryitemstyle_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemStyle_JM
		LEFT JOIN {prefix}{pc_dataset}.pctl_classcodetype_jmic_pl AS pctl_classcodetype_jmic_pl ON pctl_classcodetype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.ClassCodeType
	--logic to get the "Located WITH contact"
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_policycontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))AS LocatedWithRole
			ON LocatedWithRole.FixedID = pcx_jewelryitem_jmic_pl.LocatedWith
			AND LocatedWithRole.BranchID = pcx_jewelryitem_jmic_pl.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(LocatedWithRole.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(LocatedWithRole.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontactrole
		ON pc_accountcontactrole.ID = LocatedWithRole.AccountContactRole
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontact
		ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_contact Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithContact 
		ON LocatedWithContact.ID = pc_accountcontact.Contact
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_plcededpremiumtrans_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_plcededpremiumjmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jwryitemcov_jmic_pl._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jewelryitem_jmic_pl._PARTITIONTIME) = DATE('{date}')	
	

	UNION ALL

	/*************************************/
	/***** PJ CEDED UNS TRANSACTIONS *****/
	/*************************************/
	SELECT
		pcx_plcededpremiumtrans_jmic.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID		AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID			AS SourceOrganizationNumber
		,pc_job.JobNumber
		,NULL AS ItemNumber
		,pc_contact.PublicID			AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,'' AS ItemPublicID
		,pcx_jmpersonallinecov.PublicID AS CoveragePublicID
		,'UNS' AS CoverageTypeCode
		,pcx_plcededpremiumtrans_jmic.CededPremium AS TransactionAmount
		,NULL AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated						
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_cost_jmic.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		,CAST(NULL AS STRING) AS AppraisalDate
		,CAST(NULL AS STRING) AS ItemEffectiveDate
		,CAST(NULL AS STRING) AS ItemExpirationDate
		,CAST(NULL AS BOOL) AS IsAppraisalReceived
		,CAST(NULL AS STRING) AS BrandName
		,CAST(NULL AS STRING) AS ItemStyle
		,CAST(NULL AS STRING) AS IVADate
		,NULL AS Vault
		,NULL AS Safe
		,CAST(NULL AS STRING) AS LocatedWithCustomerNumber
		,NULL AS IsItemInactive
		--- Find way to join to Products Table
		,CAST(NULL AS STRING) AS JMProduct
		,CAST(NULL AS STRING) AS ItemClassDescription
		,CAST(NULL AS STRING) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_plcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM {prefix}{pc_dataset}.pcx_plcededpremiumtrans_jmic
		
		INNER JOIN {prefix}{pc_dataset}.pcx_plcededpremiumjmic
			ON pcx_plcededpremiumjmic.ID = pcx_plcededpremiumtrans_jmic.PLCededPremium_JMIC
		INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic 
			ON pcx_plcededpremiumjmic.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryLineCov_JMIC_PL IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		-- Personal Line Coverage (unscheduled)
		INNER JOIN {prefix}{pc_dataset}.pcx_jmpersonallinecov
			ON pcx_jmpersonallinecov.FixedID = pcx_cost_jmic.JewelryLineCov_JMIC_PL
			AND pcx_jmpersonallinecov.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_plcededpremiumtrans_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_plcededpremiumjmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jmpersonallinecov._PARTITIONTIME) = DATE('{date}')




	UNION ALL

	/**************************************/
	/***** PA DIRECT SCH TRANSACTIONS *****/
	/**************************************/
	SELECT 
		pcx_jpatransaction_jm.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,pcx_personalarticle_jm.ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,pcx_personalarticle_jm.PublicID AS ItemPublicID
		,pcx_personalarticlecov_jm.PublicID AS CoveragePublicID
		,'SCH' AS CoverageTypeCode
		,pcx_jpatransaction_jm.Amount AS TransactionAmount
		,pcx_personalarticlecov_jm.DirectTerm1 AS ItemValue			
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated					
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_jpacost_jm.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		--,pcx_personalarticle_jm.AppraisalDate AS AppraisalDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.AppraisalDate as DATETIME)), ' UTC') AS AppraisalDate 
		--,COALESCE(pcx_personalarticle_jm.EffectiveDate, pc_policyPeriod.PeriodStart) AS ItemEffectiveDate
		--,COALESCE(pcx_personalarticle_jm.ExpirationDate, pc_policyPeriod.PeriodEnd) AS ItemExpirationDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.EffectiveDate as DATETIME)), ' UTC'),
 		          concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodStart as DATETIME)), ' UTC')
				  ) AS ItemEffectiveDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.ExpirationDate as DATETIME)), ' UTC'), 
				  concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodEnd as DATETIME)), ' UTC')
				  ) AS ItemExpirationDate
		,pcx_personalarticle_jm.IsAppraisalReceived
		,pctl_jpaitembrand_jm.Name AS BrandName
		,pctl_jpaitemstyle_jm.Name AS ItemStyle
		--,pctl_jpaitemtype_jm.Name AS ItemClassType
		--,pcx_personalarticle_jm.IVADate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.IVADate as DATETIME)), ' UTC') AS IVADate
		,CASE WHEN pctl_jpastoragetype_jm.Name = 'Vault' THEN 1 ELSE 0 END AS Vault
		,CASE WHEN pctl_jpastoragetype_jm.Name = 'Safe' THEN 1 ELSE 0 END AS Safe
		,LocatedWithContact.PublicID AS LocatedWithCustomerNumber
		,CAST(pcx_personalarticle_jm.IsItemInactive AS INT64) AS IsItemInactive
		--- Find way to join to Products Table
		,'Personal Articles' AS JMProduct
		,LTRIM(RTRIM(pctl_jpaitemtype_jm.NAME)) AS ItemClassDescription
		,LTRIM(RTRIM(pctl_jpaitemgendertype_jm.NAME)) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
			) AS TransactionRank
		-- select pcx_jpatransaction_jm.*
		FROM {prefix}{pc_dataset}.pcx_jpatransaction_jm
		
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacost_jm 
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			--AND pcx_jpacost_jm.PersonalArtclLineCov IS NULL
			AND pcx_jpacost_jm.PersonalArticleCov IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		INNER JOIN (select * from {prefix}{pc_dataset}.pcx_personalarticlecov_jm Where Date(_PARTITIONTIME)=DATE('{date}'))pcx_personalarticlecov_jm
			ON pcx_personalarticlecov_jm.FixedID = pcx_jpacost_jm.PersonalArticleCov
			AND pcx_personalarticlecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalarticlecov_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalarticlecov_jm.ExpirationDate,perCost.PeriodEnd)			
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pcx_personalarticle_jm Where Date(_PARTITIONTIME)=DATE('{date}'))pcx_personalarticle_jm
			ON pcx_personalarticle_jm.ID = pcx_jpacost_jm.PersonalArticle
			AND pcx_personalarticle_jm.BranchID = pcx_jpacost_jm.BranchID 
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalarticle_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalarticle_jm.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode		
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpastoragetype_jm ON pcx_personalarticle_jm.StorageType = pctl_jpastoragetype_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitembrand_jm ON pcx_personalArticle_jm.Brand = pctl_jpaitembrand_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemstyle_jm ON pcx_personalarticle_jm.Style = pctl_jpaitemstyle_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemtype_jm ON pcx_personalarticle_jm.ItemType = pctl_jpaitemtype_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemgendertype_jm ON pcx_personalarticle_jm.ItemGenderType= pctl_jpaitemgendertype_jm.ID
		--logic to get the "Located WITH contact"
		LEFT OUTER JOIN  (select * from {prefix}{pc_dataset}.pc_policycontactrole Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithRole
			ON LocatedWithRole.FixedID = pcx_personalarticle_jm.LocatedWith
			AND LocatedWithRole.BranchID = pcx_personalarticle_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(LocatedWithRole.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(LocatedWithRole.ExpirationDate,perCost.PeriodEnd)
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontactrole
		ON pc_accountcontactrole.ID = pc_policycontactrole.AccountContactRole
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontact
		ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_contact Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithContact 
		ON LocatedWithContact.ID = pc_accountcontact.Contact
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE-- Latin1_General_CI_AS
		--LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemtype_jm  ON [pcx_personalarticle_jm].ItemType = [pctl_jpaitemtype_jm].ID --ITEM TYPE 

	WHERE	1 = 1
	--AND pcx_jpatransaction_jm.PostedDate IS NOT NULL
	AND pcx_jpatransaction_jm.PublicID IS NOT NULL
	AND pctl_policyperiodstatus.NAME = 'Bound'
    AND DATE(pcx_jpatransaction_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacost_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(percost._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	

	UNION ALL

	/**************************************/
	/***** PA DIRECT UNS TRANSACTIONS *****/
	/**************************************/
	SELECT
		pcx_jpatransaction_jm.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID 	AS SourceOrganizationNumber
		,pc_job.JobNumber
		,NULL AS ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,'' AS ItemPublicID
		,pcx_personalartcllinecov_jm.PublicID AS CoveragePublicID
		,'UNS' AS CoverageTypeCode
		,pcx_jpatransaction_jm.Amount AS TransactionAmount
		,NULL AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated				
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_jpacost_jm.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		,CAST(NULL AS STRING) AS AppraisalDate
		,CAST(NULL AS STRING) AS ItemEffectiveDate
		,CAST(NULL AS  STRING) AS ItemExpirationDate
		,CAST(NULL AS BOOL) AS IsAppraisalReceived
		,CAST(NULL AS STRING) AS BrandName
		,CAST(NULL AS STRING) AS ItemStyle
		,CAST(NULL AS  STRING) AS IVADate
		,NULL AS Vault
		,NULL AS Safe
		,CAST(NULL AS STRING) AS LocatedWithCustomerNumber
		,NULL AS IsItemInactive
		--- Find way to join to Products Table
		,CAST(NULL AS STRING) AS JMProduct
		,CAST(NULL AS STRING) AS ItemClassDescription
		,CAST(NULL AS STRING) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
			) AS TransactionRank
		-- select pcx_jpatransaction_jm.*
		FROM {prefix}{pc_dataset}.pcx_jpatransaction_jm
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacost_jm 
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		INNER JOIN {prefix}{pc_dataset}.pcx_personalartcllinecov_jm
			ON pcx_personalartcllinecov_jm.FixedID = pcx_jpacost_jm.PersonalArtclLineCov
			AND pcx_personalartcllinecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm-- COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	--AND pcx_jpatransaction_jm.PostedDate IS NOT NULL
	AND pcx_jpatransaction_jm.PublicID IS NOT NULL	
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_jpatransaction_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacost_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(percost._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_personalartcllinecov_jm._PARTITIONTIME) = DATE('{date}')	

	UNION ALL

	/*************************************/
	/***** PA CEDED SCH TRANSACTIONS *****/
	/*************************************/
	SELECT
		pcx_jpacededpremiumtrans_jm.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,pcx_personalarticle_jm.ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,pcx_personalarticle_jm.PublicID AS ItemPublicID
		,pcx_personalarticlecov_jm.PublicID AS CoveragePublicID
		,'SCH' AS CoverageTypeCode
		,pcx_jpacededpremiumtrans_jm.CededPremium AS TransactionAmount 
		,pcx_personalarticlecov_jm.DirectTerm1 AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated					
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME											AS TranType
		--,pc_policyperiod.PeriodEnd								AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_jpacost_jm.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		--,pcx_personalarticle_jm.AppraisalDate AS AppraisalDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.AppraisalDate as DATETIME)), ' UTC') AS AppraisalDate 
		--,COALESCE(pcx_personalarticle_jm.EffectiveDate, pc_policyPeriod.PeriodStart) AS ItemEffectiveDate
		--,COALESCE(pcx_personalarticle_jm.ExpirationDate, pc_policyPeriod.PeriodEnd) AS ItemExpirationDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.EffectiveDate as DATETIME)), ' UTC'),
 		          concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodStart as DATETIME)), ' UTC')
				  ) AS ItemEffectiveDate
		,COALESCE(concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.ExpirationDate as DATETIME)), ' UTC'), 
				  concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyPeriod.PeriodEnd as DATETIME)), ' UTC')
				  ) AS ItemExpirationDate
		,pcx_personalarticle_jm.IsAppraisalReceived
		,pctl_jpaitembrand_jm.Name AS BrandName
		,pctl_jpaitemstyle_jm.Name AS ItemStyle
		--,pctl_jpaitemtype_jm.Name AS ItemClassType
		--,pcx_personalarticle_jm.IVADate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pcx_personalarticle_jm.IVADate as DATETIME)), ' UTC') AS IVADate
		
		,CASE WHEN pctl_jpastoragetype_jm.Name = 'Vault' THEN 1 ELSE 0 END AS Vault
		,CASE WHEN pctl_jpastoragetype_jm.Name = 'Safe' THEN 1 ELSE 0 END AS Safe
		,LocatedWithContact.PublicID AS LocatedWithCustomerNumber
		,CAST(pcx_personalarticle_jm.IsItemInactive AS INT64) AS IsItemInactive
		--- Find way to join to Products Table
		,'Personal Articles' AS JMProduct
		,LTRIM(RTRIM(pctl_jpaitemtype_jm.NAME)) AS ItemClassDescription
		,LTRIM(RTRIM(pctl_jpaitemgendertype_jm.NAME)) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jpacededpremiumtrans_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	-- select pcx_jpatransaction_jm.*
		FROM {prefix}{pc_dataset}.pcx_jpacededpremiumtrans_jm
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacededpremium_jm
			ON pcx_jpacededpremium_jm.ID = pcx_jpacededpremiumtrans_jm.JPACededPremium_JM
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacost_jm 
			ON pcx_jpacededpremium_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jpacost_jm.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'	
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		INNER JOIN (Select * from {prefix}{pc_dataset}.pcx_personalarticlecov_jm Where Date(_PARTITIONTIME)=DATE('{date}'))pcx_personalarticlecov_jm
			ON pcx_personalarticlecov_jm.FixedID = pcx_jpacost_jm.PersonalArticleCov
			AND pcx_personalarticlecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd)			
		INNER JOIN (Select * from {prefix}{pc_dataset}.pcx_personalarticle_jm Where Date(_PARTITIONTIME)=DATE('{date}'))pcx_personalarticle_jm
			ON pcx_personalarticle_jm.ID = pcx_jpacost_jm.PersonalArticle
			AND pcx_personalarticle_jm.BranchID = pcx_jpacost_jm.BranchID 
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpastoragetype_jm ON pcx_personalarticle_jm.StorageType = pctl_jpastoragetype_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitembrand_jm ON pcx_personalArticle_jm.Brand = pctl_jpaitembrand_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemstyle_jm ON pcx_personalarticle_jm.Style = pctl_jpaitemstyle_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemtype_jm ON pcx_personalarticle_jm.ItemType = pctl_jpaitemtype_jm.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemgendertype_jm ON pcx_personalarticle_jm.ItemGenderType= pctl_jpaitemgendertype_jm.ID
		--logic to get the "Located WITH contact"
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_policycontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))
		AS LocatedWithRole
			ON LocatedWithRole.FixedID = pcx_personalarticle_jm.LocatedWith
			AND LocatedWithRole.BranchID = pcx_personalarticle_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(LocatedWithRole.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(LocatedWithRole.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontactrole Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontactrole
		ON pc_accountcontactrole.ID = pc_policycontactrole.AccountContactRole
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontact Where Date(_PARTITIONTIME)=DATE('{date}'))pc_accountcontact
		ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_contact Where Date(_PARTITIONTIME)=DATE('{date}')) AS LocatedWithContact 
		ON LocatedWithContact.ID = pc_accountcontact.Contact
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE Latin1_General_CI_AS
		--LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemtype_jm  ON [pcx_personalarticle_jm].ItemType = [pctl_jpaitemtype_jm].ID --ITEM TYPE 

	WHERE	1 = 1
	--AND pc_job.JobNumber = '7000010'
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_jpacededpremiumtrans_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacededpremium_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacost_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')

	UNION ALL

	/*************************************/
	/***** PA CEDED UNS TRANSACTIONS *****/
	/*************************************/
	SELECT
		pcx_jpacededpremiumtrans_jm.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_job.JobNumber
		,NULL AS ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,'' AS ItemPublicID
		,pcx_personalartcllinecov_jm.PublicID AS CoveragePublicID
		,'UNS' AS CoverageTypeCode
		,pcx_jpacededpremiumtrans_jm.CededPremium AS TransactionAmount 
		,NULL AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		,pctl_currency.NAME AS Currency
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated					
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_jpacost_jm.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		,CAST(NULL AS STRING) AS AppraisalDate
		,CAST(NULL AS  STRING) AS ItemEffectiveDate
		,CAST(NULL AS  STRING) AS ItemExpirationDate
		,CAST(NULL AS BOOL) AS IsAppraisalReceived
		,CAST(NULL AS STRING) AS BrandName
		,CAST(NULL AS STRING) AS ItemStyle
		,CAST(NULL AS  STRING) AS IVADate
		,NULL AS Vault
		,NULL AS Safe
		,CAST(NULL AS STRING) AS LocatedWithCustomerNumber
		,NULL AS IsItemInactive
		--- Find way to join to Products Table
		,CAST(NULL AS STRING) AS JMProduct
		,CAST(NULL AS STRING) AS ItemClassDescription
		,CAST(NULL AS STRING) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jpacededpremiumtrans_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM {prefix}{pc_dataset}.pcx_jpacededpremiumtrans_jm
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacededpremium_jm
			ON pcx_jpacededpremium_jm.ID = pcx_jpacededpremiumtrans_jm.JPACededPremium_JM
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacost_jm 
			ON pcx_jpacededpremium_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArticleCov IS NULL
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NOT NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jpacost_jm.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pcx_personalartcllinecov_jm
			ON pcx_personalartcllinecov_jm.FixedID = pcx_jpacost_jm.PersonalArticleCov
			AND pcx_personalartcllinecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm --COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	--AND pc_job.JobNumber = '7000010'
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_jpacededpremiumtrans_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacededpremium_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacost_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_personalartcllinecov_jm._PARTITIONTIME) = DATE('{date}')
    UNION ALL

    /*****************************************/
	/***** PA DIRECT COST LEVEL COVERAGE *****/
	/*****************************************/
	SELECT
		pcx_jpatransaction_jm.PublicID AS TransactionPublicID
		,pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_uwcompany.PublicID 	AS SourceOrganizationNumber
		,pc_job.JobNumber
		,NULL AS ItemNumber
		,pc_contact.PublicID AS SourceCustomerNumber --which customer?? Going with primary insured for now...
		,'' AS ItemPublicID
		,pcx_jpacost_jm.PublicID AS CoveragePublicID
		,'COST' AS CoverageTypeCode
		,pcx_jpatransaction_jm.Amount AS TransactionAmount
		,NULL AS ItemValue
		,pc_policyperiod.TransactionCostRPT				
		  ,pctl_currency.NAME AS Currency
		,CASE 
			WHEN COALESCE(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= COALESCE(pc_job.CloseDate, '1990-01-01') 
				THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
			END	AS AccountingDate
		--,pc_job.CreateTime AS DateCreated				
		--,pc_job.UpdateTime AS DateModified
		--,pc_job.CloseDate AS JobCloseDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated  --pc_job.CreateTime 					
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime  as DATETIME)), ' UTC') AS DateModified   --pc_job.UpdateTime 
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate  as DATETIME)), ' UTC') AS JobCloseDate  --pc_job.CloseDate 
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,cte_ExtAppId.ExternalApplicationKey
		,pc_policyperiod.PolicyNumber
		,pc_policyperiod.PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd  as DATETIME)), ' UTC') AS PolicyExpirationDate
		,CASE WHEN pcx_jpacost_jm.ChargeSubGroup = 'PREMIUM' THEN 'PREMIUM' ELSE 'ADJUSTMENTS' END AS ChargeSubGroup
		,CAST(NULL AS STRING) AS AppraisalDate
		,CAST(NULL AS  STRING) AS ItemEffectiveDate
		,CAST(NULL AS  STRING) AS ItemExpirationDate
		,CAST(NULL AS BOOL) AS IsAppraisalReceived
		,CAST(NULL AS STRING) AS BrandName
		,CAST(NULL AS STRING) AS ItemStyle
		,CAST(NULL AS  STRING) AS IVADate
		,NULL AS Vault
		,NULL AS Safe
        ,CAST(NULL AS STRING) AS LocatedWithCustomerNumber
	
		
		
		
		
		,NULL AS IsItemInactive
        
	
		--- Find way to join to Products Table
	    ,CAST(NULL AS STRING) AS JMProduct
		,CAST(NULL AS STRING) AS ItemClassDescription
		,CAST(NULL AS STRING) AS ItemClassGender
		,pctl_policyline.TYPECODE AS PolicyLineCode
		,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					COALESCE(pcx_jpacost_jm.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
					,COALESCE(pc_policyline.ExpirationDate,pc_policyPeriod.PeriodEnd) DESC
			) AS TransactionRank

		FROM {prefix}{pc_dataset}.pcx_jpatransaction_jm
		INNER JOIN {prefix}{pc_dataset}.pcx_jpacost_jm 
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NULL 
			AND pcx_jpacost_jm.PersonalArticleCov IS NULL
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN {prefix}{pc_dataset}.pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
			AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'
		INNER JOIN {prefix}{pc_dataset}.pctl_currency ON pctl_currency.ID = pc_policyline.PreferredCoverageCurrency
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policycontactrole ON pc_policycontactrole.BranchID = pc_policyperiod.ID
			AND (pc_policycontactrole.EffectiveDate <= pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.EffectiveDate IS NULL)
			AND (pc_policycontactrole.ExpirationDate > pc_policyperiod.EditEffectiveDate OR pc_policycontactrole.ExpirationDate IS NULL)
		INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole 
			ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
			AND pctl_policycontactrole.TYPECODE = 'PolicyPriNamedInsured'
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = pc_policycontactrole.ContactDenorm
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode
		LEFT JOIN cte_ExtAppId ON cte_ExtAppId.AccountNumber = pc_account.AccountNumberDenorm-- COLLATE Latin1_General_CI_AS

	WHERE	1 = 1
	--AND pcx_jpatransaction_jm.PostedDate IS NOT NULL
	AND pcx_jpatransaction_jm.PublicID IS NOT NULL	
	AND pctl_policyperiodstatus.NAME = 'Bound'
	AND DATE(pcx_jpatransaction_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pcx_jpacost_jm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(percost._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')	
	AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
--	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
	
) 
policytransaction

INNER JOIN Config AS ConfigSource
ON ConfigSource.Key='SourceSystem'

INNER JOIN Config AS ConfigHashSep
ON ConfigHashSep.Key='HashKeySeparator'

INNER JOIN Config AS ConfigHashAlgo
ON ConfigHashAlgo.Key='HashingAlgorithm'

WHERE 1=1
AND TransactionRank = 1

