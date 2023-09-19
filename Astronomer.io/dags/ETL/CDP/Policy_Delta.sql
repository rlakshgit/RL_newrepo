/************************* C D P   D A T A   F E E D ***********************************************************************************
	SECTIONS of CODE
		1. Airflow task to get:  LTV scores of LTV and PP from prod-edl; then merge with other 2 result sets
		2. Airflow task to get:  Recast Source of Business Value from semi-managed; then merge with other 2 sets
		3. Airflow task to get:  The main select of Policy details for CDP from GW tables (or Kimberlite? in future)
****************************************************************************************************************************************/

/*************************************************************************
	SECTION 1
	-- run to get LTV Scores
	-- include these 2 main fields in data feed to Acquia (rest is extra)
		--LifetimeValueScore
		--LifetimeValueScore_PolicyProfile
***************************************************************************************/

WITH LTV as (
    -- Ramesh L - 2023/05/04 - replacing the query to point to the new table that replaced this one...
		/* SELECT
		  a.AccountNumber,
		  a.PolicyNumber,
		  MAX(a.LTV) AS LTV,	--LifetimeValueScore
		  a.LTV_run_date,
		  a.LTV_model_version,
		  MAX(b.PP) AS PP,		--LifetimeValueScore_PolicyProfile
		  b.PP_run_date,
		  b.PP_model_version
		FROM  `prod-edl.jm_gld_model_pl_ltv.t_ltv_currentversion` a
		INNER JOIN `prod-edl.jm_gld_model_pl_ltv.t_pp_currentversion` b
			ON a.AccountNumber = b.AccountNumber
			AND a.PolicyNumber = b.PolicyNumber
		WHERE LTV > -1
		AND LTV < 30

		GROUP BY
		  a.AccountNumber,
		  a.PolicyNumber,
		  a.LTV_run_date,
		  a.LTV_model_version,
		  b.PP_run_date,
		  b.PP_model_version */
		select a.AccountNumber, a.PolicyNumber, LTV, a.currentversion_load_date LTV_run_date, a.LTV_model_version, PolicyProfile as PP, a.currentversion_load_date PP_run_date, PP_model_version
		FROM  `prod-edl.jm_model.t_ltv_currentversions` a
		WHERE a.currentversion_load_date = (SELECT max(currentversion_load_date) FROM `prod-edl.jm_model.t_ltv_currentversions` )
		and LTV > -1 AND LTV < 30		  
),


/*****************************************************************************************
	SECTION 2
	-- run to get RECAST source of business value per account and policy
	-- notice this is year dependent which means annual maintenance
		OPTION: 1) have them create Recast table with date/year as a column, not tablename
				2) create generic view over recast table, only change SQL yearly
	-- SQL:
			SELECT RECAST AS RecastSourceOfBusiness
			FROM `semi-managed-reporting.core_insurance_pjpa.promoted_recast_fy_2020_FULL`
******************************************************************************************/
	RECAST as (
			SELECT AccountNumber
			, PolicyNumber
			, RECAST AS RecastSourceOfBusiness
			FROM `{recast_project}.{cdp_recast_table}`
	),

/*****************************************************************************************
	SECTION 3
	-- NOTES:
		Remove Logic for SAMPLE queries before deploying to QA/Prod
		IsJPASubmitted	--new GW field for JPA Quick Quotes
		Remove Referring Jeweler Hash for sample queries
	-- Additional Fields not mapped below; with notes from Seamus (11/2/2020)
		Investigations	-- where from ?  is on the GW screens, but I can't remember if I found it in the data.
		OpenActivities (Boolean) -- is worth skipping for now. Nancy wanted it, Bryan has similar requests, but it might be a difficult one that doesn't get us a great use case.
		AbandonedDate	--- what is this ?  where from ? -- I think we'll have to get from webtagging, but the Hubspot integration has this field.
	-- Fields not mapped
		--,PolicyInforceFromDate	-- have Policy Effective dates; this is a bit more difficult to get
		--,PolicyInforceToDate		-- have Policy Effective dates; this is a bit more difficult to get
*******************************************************************************************/

/* V6 CHanges
	1. name changed -- 		,pc_uwcompany.PublicID AS SourceOrganizationNumber
	2. name changed --		,pc_contact.PublicID AS SourceCustomerNumber
	3. Added CTE_PremiumsDue
	4. Name change from IsActive to IsActivePolicy  (I am not sure which is final per Acquia)
	test with
	AcctNumber = AccountID = Policy
	3000015482	= 17740  = 24-013094
	3000992744 = 1039736 = 24-941088
	3001325787 = 1431876 = 24-1217738
	3001283131 = 1381513 = P000000310
	3001212590 = 1294507 = 24-1125538
*/
cte_Agencies
AS (
	SELECT pc_producercode.ID,IfNULL(pc_group.Name,'') AS AgencyName
	FROM {prefix}{pc_dataset}.pc_producercode pc_producercode
	LEFT JOIN (Select * from {prefix}{pc_dataset}.pc_groupproducercode where DATE(_PARTITIONTIME) = DATE('{date}'))pc_groupproducercode
		ON pc_producercode.ID = pc_groupproducercode.ProducerCodeID
	LEFT JOIN (Select * from {prefix}{pc_dataset}.pc_group where DATE(_PARTITIONTIME) = DATE('{date}'))pc_group
		ON pc_groupproducercode.GroupID = pc_group.ID
		AND pc_producercode.OrganizationID = pc_group.OrganizationID
	INNER JOIN  {prefix}{pc_dataset}.pctl_grouptype
		ON pctl_grouptype.ID = pc_group.GroupType
		AND pctl_grouptype.TYPECODE = 'agency_jmic'
	WHERE 1= 1
	AND DATE(pc_producercode._PARTITIONTIME) = DATE('{date}')
),
cte_Agencies_y
AS (
	SELECT pc_producercode.ID,IfNULL(pc_group.Name,'') AS AgencyName
	FROM {prefix}{pc_dataset}.pc_producercode pc_producercode
	LEFT JOIN (Select * from {prefix}{pc_dataset}.pc_groupproducercode where DATE(_PARTITIONTIME) = DATE('{ydate}'))pc_groupproducercode
		ON pc_producercode.ID = pc_groupproducercode.ProducerCodeID
	LEFT JOIN (Select * from {prefix}{pc_dataset}.pc_group where DATE(_PARTITIONTIME) = DATE('{ydate}'))pc_group
		ON pc_groupproducercode.GroupID = pc_group.ID
		AND pc_producercode.OrganizationID = pc_group.OrganizationID
	INNER JOIN  {prefix}{pc_dataset}.pctl_grouptype
		ON pctl_grouptype.ID = pc_group.GroupType
		AND pctl_grouptype.TYPECODE = 'agency_jmic'
	WHERE 1= 1
	AND DATE(pc_producercode._PARTITIONTIME) = DATE('{ydate}')
),
cte_PremiumsDue AS (

--declare @AccountID varchar(18)
--select @AccountID = 17740

		SELECT	Invoice.AccountID, Invoice.AccountNumber
				,SUM(COALESCE(InvoiceDue.NetAmountDue,InvoicePastDue.NetAmountDue,Invoice.NetAmountDue)) AS PremiumAmountDue
				,COALESCE(InvoiceDue.PaymentDueDate, InvoicePastDue.PaymentDueDate, '9999-12-31') AS PaymentDueDate
				,COALESCE(InvoiceDue.TYPECODE, InvoicePastDue.TYPECODE,'?') AS InvoiceStatusDescription
				--,InvoiceRow
		FROM (--Get most recent Invoice for PremiumAmountDue (NetAmountDue)
				SELECT bc_invoice.AccountID, pc_account.AccountNumber, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
				,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
					ORDER BY bc_invoice.CreateTime DESC, bc_invoice.InvoiceNumberDenorm DESC
				) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0) > 0
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{date}')
			) Invoice
		LEFT OUTER JOIN
			(
				SELECT bc_invoice.AccountID, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
						,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate, bctl_invoicestatus.TYPECODE
						,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
							ORDER BY bc_invoice.PaymentDueDate, bc_invoice.InvoiceNumberDenorm
						) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) > 0
				AND  DATE(IFNULL(bc_invoice.PaymentDueDate,'9999-12-31')) >= CAST(DATE('{date}') AS date)  --has come due
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{date}')
			) InvoiceDue
			ON Invoice.AccountID = InvoiceDue.AccountID
			AND InvoiceDue.InvoiceRow=1
		LEFT OUTER JOIN
			(
				SELECT bc_invoice.AccountID, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
						,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate, bctl_invoicestatus.TYPECODE
						,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
							ORDER BY bc_invoice.PaymentDueDate DESC, bc_invoice.InvoiceNumberDenorm DESC
						) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) > 0
				AND  DATE(IFNULL(bc_invoice.PaymentDueDate,'9999-12-31')) < CAST(DATE('{date}') AS date)  --has come due
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{date}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{date}')
			) InvoicePastDue
			ON InvoicePastDue.AccountID = Invoice.AccountID
			AND InvoicePastDue.InvoiceRow = 1
	WHERE Invoice.InvoiceRow = 1
	GROUP BY Invoice.AccountID, Invoice.AccountNumber, InvoiceDue.PaymentDueDate, InvoicePastDue.PaymentDueDate, InvoiceDue.TYPECODE, InvoicePastDue.TYPECODE
)
,
cte_PremiumsDueY AS (

--declare @AccountID varchar(18)
--select @AccountID = 17740

		SELECT	Invoice.AccountID, Invoice.AccountNumber
				,SUM(COALESCE(InvoiceDue.NetAmountDue,InvoicePastDue.NetAmountDue,Invoice.NetAmountDue)) AS PremiumAmountDue
				,COALESCE(InvoiceDue.PaymentDueDate, InvoicePastDue.PaymentDueDate, '9999-12-31') AS PaymentDueDate
				,COALESCE(InvoiceDue.TYPECODE, InvoicePastDue.TYPECODE,'?') AS InvoiceStatusDescription
				--,InvoiceRow
		FROM (--Get most recent Invoice for PremiumAmountDue (NetAmountDue)
				SELECT bc_invoice.AccountID, pc_account.AccountNumber, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
				,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
					ORDER BY bc_invoice.CreateTime DESC, bc_invoice.InvoiceNumberDenorm DESC
				) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0) > 0
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{ydate}')
			) Invoice
		LEFT OUTER JOIN
			(
				SELECT bc_invoice.AccountID, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
						,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate, bctl_invoicestatus.TYPECODE
						,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
							ORDER BY bc_invoice.PaymentDueDate, bc_invoice.InvoiceNumberDenorm
						) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) > 0
				AND  DATE(IFNULL(bc_invoice.PaymentDueDate,'9999-12-31')) >= CAST(DATE('{ydate}') AS date)  --has come due
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{ydate}')
			) InvoiceDue
			ON Invoice.AccountID = InvoiceDue.AccountID
			AND InvoiceDue.InvoiceRow=1
		LEFT OUTER JOIN
			(
				SELECT bc_invoice.AccountID, (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) AS NetAmountDue
						,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate, bctl_invoicestatus.TYPECODE
						,ROW_NUMBER() OVER(PARTITION BY bc_invoice.accountid
							ORDER BY bc_invoice.PaymentDueDate DESC, bc_invoice.InvoiceNumberDenorm DESC
						) AS InvoiceRow
				FROM {prefix}{pc_dataset}.pc_account
				INNER JOIN {prefix}{bc_dataset}.bc_account
					ON bc_account.AccountNumber = pc_account.AccountNumber
				INNER JOIN {prefix}{bc_dataset}.bc_invoice
					ON bc_invoice.accountid = bc_account.id
				INNER JOIN {prefix}{bc_dataset}.bctl_invoicestatus
					ON bc_invoice.Status = bctl_invoicestatus.ID
				WHERE 1=1
				AND bctl_invoicestatus.ID IN (1, 3) -- billed, due
				AND (COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0)) > 0
				AND  DATE(IFNULL(bc_invoice.PaymentDueDate,'9999-12-31')) < CAST(DATE('{ydate}') AS date)  --has come due
				--and bc_invoice.AccountID = @AccountID
				AND DATE(pc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_account._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(bc_invoice._PARTITIONTIME) = DATE('{ydate}')
			) InvoicePastDue
			ON InvoicePastDue.AccountID = Invoice.AccountID
			AND InvoicePastDue.InvoiceRow = 1
	WHERE Invoice.InvoiceRow = 1
	GROUP BY Invoice.AccountID, Invoice.AccountNumber, InvoiceDue.PaymentDueDate, InvoicePastDue.PaymentDueDate, InvoiceDue.TYPECODE, InvoicePastDue.TYPECODE
)


SELECT
	SourceSystem
	,SourceOrganizationNumber
	,FullSet.PolicyNumber
	,PolicyTermID
	,SourceCustomerNumber
	,PolicyPeriodStatus
	,FullSet.AccountNumber
	,AccountID
	,PolicyPaymentPlan
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(PolicyEffectiveDate as DATETIME)), ' UTC') PolicyEffectiveDate --PolicyEffectiveDate
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(PolicyExpirationDate as DATETIME)), ' UTC') PolicyExpirationDate --PolicyExpirationDate
	,CurrentRenewalType
	,NextRenewalType
	,PolicyType
	,SourceOfBusiness
	,PolicyBillingMethod
	,PolicyNonRenewDirection
	,ApplicationTakenBy
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(PolicyOrigEffDate as DATETIME)), ' UTC') PolicyOrigEffDate
	,PolicyCancelReason
	,PolicyCancellationDate
	,IsActivePolicy
	,ReferringJewelerName
	,TotalSchedPremiumRPT
	,MinPremium
	,PaperlessDeliveryIndicator
	,AgencyName
	,AutoPayIndicator
	,PremiumAmountDue
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(PaymentDueDate as DATETIME)), ' UTC') PaymentDueDate --PaymentDueDate
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(CreditCardExpirationDate as DATETIME)), ' UTC') CreditCardExpirationDate --CreditCardExpirationDate
	,IsJPASubmitted
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(PolicyPeriodCreateTime as DATETIME)), ' UTC') PeriodTransactionDate
	,LTV.LTV AS LifetimeValueScore
	,LTV.PP AS LifetimeValueScore_PolicyProfile
	,RECAST.RecastSourceOfBusiness
FROM (
(
		SELECT
			'GW' AS SourceSystem
			,pc_policyperiod.PolicyNumber
			,pc_policyterm.ID AS PolicyTermID
			,pc_uwcompany.PublicID AS SourceOrganizationNumber
			,pc_contact.PublicID AS SourceCustomerNumber
			,pctl_policyperiodstatus.NAME AS PolicyPeriodStatus
			,pc_account.AccountNumber, cte_PremiumsDue.AccountID
			--,COALESCE(pc_paymentplansummary.Name,'Unknown') AS PolicyPaymentPlan
			,COALESCE(MaxPaymentPlan.Name,'Unknown') AS PolicyPaymentPlan
			,pc_policyperiod.PeriodStart AS PolicyEffectiveDate
			--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
			,MaxPeriodEnd.PolicyExpirationDate
			,CASE WHEN pc_policyperiod.LastManualRenewalDate_JMIC = pc_policyperiod.PeriodStart THEN 'Manual' ELSE 'Automatic' END AS CurrentRenewalType
			,CASE WHEN pcx_policystatus_jmic.NextManualRenewalDate_JMIC = pc_policyperiod.PeriodEnd THEN 'Manual' ELSE 'Automatic' END AS NextRenewalType
			--,COALESCE(pc_effectivedatedfields.OfferingCode,pc_policy.ProductCode) AS PolicyType
			,pc_policy.ProductCode AS PolicyType
			,COALESCE(pctl_source_jmic_pl.NAME,'?') AS SourceOfBusiness
			--,PolicyInforceFromDate
			--,PolicyInforceToDate
			,COALESCE(bctl_policyperiodbillingmethod.NAME, pctl_billingmethod.NAME,'?') AS PolicyBillingMethod
			,CASE WHEN IFNULL(pctl_prerenewaldirection.NAME,'n/a') LIKE 'Non-Renew%' Then IFNULL(pctl_prerenewaldirection.NAME,'n/a') ELSE NULL END AS PolicyNonRenewDirection
			,COALESCE(pctl_apptakenby_jmic_pl.NAME,'') AS ApplicationTakenBy
			,CASE WHEN pc_policyperiod.PolicyNumber = 'Unassigned'
					THEN (pc_account.OrigPolicyEffDate_JMIC_PL)
					ELSE (IFNULL(pc_account.OrigPolicyEffDate_JMIC_PL,'1900-01-01')) END AS PolicyOrigEffDate
			,COALESCE(pctl_reasoncode.DESCRIPTION,'') AS PolicyCancelReason
			,CASE WHEN pc_policyperiod.CancellationDate IS NOT NULL
				THEN
					CASE WHEN pc_job.CloseDate IS NOT NULL AND pc_job.CloseDate >= pc_policyperiod.CancellationDate THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')--pc_job.CloseDate
						WHEN pc_job.CloseDate IS NOT NULL AND pc_job.CloseDate < pc_policyperiod.CancellationDate THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.CancellationDate as DATETIME)), ' UTC')--pc_policyperiod.CancellationDate
						WHEN pc_job.CloseDate IS NULL THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.CancellationDate as DATETIME)), ' UTC')--pc_policyperiod.CancellationDate
					END
				END AS PolicyCancellationDate
			,CASE WHEN (DATE(pc_policyperiod.PeriodEnd) >= DATE('{date}')
						AND pc_policyperiod.CancellationDate IS NULL
						--AND pc_policyperiod.Status in (9, 15, 10007) --('Bound', 'cancelling', 'renewing')
						AND pctl_policyperiodstatus.TYPECODE IN ('Bound', 'Canceling', 'Renewing')
						AND pc_policy.retired = 0)
				THEN 1 ELSE 0
				END AS IsActivePolicy
			--,RecastSourceOfBusiness
			,pcx_jeweler_jm.Name AS ReferringJewelerName --encode for sample data feeds
			,pc_policyperiod.TotalSchedPremiumRPT_JMIC AS TotalSchedPremiumRPT
			,pc_policyperiod.TotalMinimumPremiumRPT_JMIC AS MinPremium
			,pc_account.IsPaperlessDelivery_JMIC AS PaperlessDeliveryIndicator
			--,IFNULL(pc_group.Name,'') AS AgencyName
			,cte_Agencies.AgencyName
			,CASE WHEN bctl_paymentmethod.ID in (1,4) THEN 1 ELSE 0 END as AutoPayIndicator  --1=ACH/EFT;4=Credit Card
			--,COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0) AS PremiumAmountDue
			--,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate
			,COALESCE(cte_PremiumsDue.PremiumAmountDue, 0) AS PremiumAmountDue
			,COALESCE(cte_PremiumsDue.PaymentDueDate,'9999-12-31') AS PaymentDueDate
			,bc_paymentinstrument.CreditCardExpDate_JMIC AS CreditCardExpirationDate
			,pc_job.IsSubmittedApplication_JM AS IsJPASubmitted
			/*,pc_policyperiod.CreateTime AS PolicyPeriodCreateTime
			,ROW_NUMBER() OVER(PARTITION BY pc_policyperiod.PeriodEnd
				ORDER BY
					pc_policyperiod.CreateTime DESC
				) AS TransactionRank*/
			,pc_policyperiod.ModelDate AS PolicyPeriodCreateTime
			,ROW_NUMBER() OVER(PARTITION BY pc_policyperiod.PolicyTermID
				ORDER BY pc_policyperiod.EditEffectiveDate, pc_policyperiod.ModelDate DESC
				) AS TransactionRank

		FROM {prefix}{pc_dataset}.pc_policy
			INNER JOIN {prefix}{pc_dataset}.pc_account
				ON pc_account.ID = pc_policy.AccountID
			INNER JOIN {prefix}{pc_dataset}.pc_policyterm
				ON pc_policyterm.PolicyID = pc_policy.ID
			INNER JOIN {prefix}{pc_dataset}.pc_policyperiod
				ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
			INNER JOIN {prefix}{pc_dataset}.pctl_segment
				ON pctl_segment.ID = pc_policyperiod.Segment
			INNER JOIN {prefix}{pc_dataset}.pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN {prefix}{pc_dataset}.pctl_job
				ON pctl_job.ID = pc_job.Subtype
			--INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic
				--ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus
				ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
			INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID


			/*INNER JOIN {prefix}{pc_dataset}.pc_policyline
				ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
				AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'*/


		-- Get Max PeriodEnd Date
			INNER JOIN  (
				--SELECT pc_policyperiod.PolicyNumber, pc_policyperiod.PolicyTermID, MAX(pc_policyperiod.PeriodEnd) AS PolicyExpirationDate
				--FROM {prefix}{pc_dataset}.pc_policyperiod
				SELECT pc_policyperiod.PolicyID, pc_policyperiod.PolicyTermID, MAX(pc_policyperiod.PeriodEnd) AS PolicyExpirationDate
				FROM {prefix}{pc_dataset}.pc_policyperiod
				INNER JOIN {prefix}{pc_dataset}.pc_policy
					ON pc_policyperiod.PolicyID = pc_policy.ID


				WHERE 1=1
				AND  pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
				AND pc_policyperiod.ModelDate IS NOT NULL
				AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
				AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
				GROUP BY PolicyID, PolicyTermID


			) MaxPeriodEnd
				--ON MaxPeriodEnd.PolicyNumber = pc_policyperiod.PolicyNumber
				ON MaxPeriodEnd.PolicyID = pc_policyperiod.PolicyID
				AND MaxPeriodEnd.PolicyTermID = pc_policyperiod.PolicyTermID
				AND MaxPeriodEnd.PolicyExpirationDate = pc_policyperiod.PeriodEnd
				
			---subquery to get payment plan when exists
			LEFT JOIN	(
				SELECT pc_policyperiod.PolicyTermID, pc_policyperiod.ID,  pc_paymentplansummary.Name
				FROM {prefix}{pc_dataset}.pc_policy pc_policy
				INNER JOIN {prefix}{pc_dataset}.pc_policyterm pc_policyterm
					ON pc_policyterm.PolicyID = pc_policy.ID
				INNER JOIN {prefix}{pc_dataset}.pc_policyperiod pc_policyperiod
					ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				LEFT JOIN (select * from {prefix}{pc_dataset}.pc_paymentplansummary WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				WHERE 
				1=1
				AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
				AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
				AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
				AND pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
				AND pc_paymentplansummary.ID IS NOT NULL) MaxPaymentPlan
				ON MaxPaymentPlan.ID = pc_policyperiod.ID
				AND MaxPaymentPlan.PolicyTermID = pc_policyperiod.PolicyTermID

			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_contact  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_contact
				ON pc_policyperiod.PNIContactDenorm = pc_contact.ID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_paymentplansummary   WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			/*LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_effectivedatedfields   WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
					AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)*/
			LEFT JOIN {prefix}{pc_dataset}.pctl_nonrenewalcode
				ON pctl_nonrenewalcode.ID = pc_policyterm.NonRenewReason
			LEFT JOIN {prefix}{pc_dataset}.pctl_nottakencode_jmic
				ON pctl_nottakencode_jmic.ID = pc_policyterm.NotTakenReason_JMIC
			LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl
				ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
			LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode
				ON pctl_reasoncode.ID = pc_job.CancelReasonCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_cancellationsource
				ON pctl_cancellationsource.ID = pc_job.Source
			LEFT JOIN {prefix}{pc_dataset}.pctl_reinstatecode
				ON pctl_reinstatecode.ID = pc_job.ReinstateCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_rewritetype
				ON pctl_rewritetype.ID = pc_job.RewriteType
			LEFT JOIN {prefix}{pc_dataset}.pctl_renewalcode
				ON pctl_renewalcode.ID = pc_job.RenewalCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_prerenewaldirection
				ON pctl_prerenewaldirection.ID = pc_policyterm.PreRenewalDirection
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pcx_policystatus_jmic  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pcx_policystatus_jmic
				ON pc_policy.PolicyStatus_JMIC = pcx_policystatus_jmic.ID
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_source_jmic_pl
				ON pc_account.Source_JMIC_PL = pctl_source_jmic_pl.ID
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_apptakenby_jmic_pl
				ON pc_account.ApplicationTakenBy_JMIC_PL = pctl_apptakenby_jmic_pl.ID
			/*LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_producercode  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_producercode
				ON pc_policyperiod.ProducerCodeOfRecordID = pc_producercode.ID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_groupproducercode  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_groupproducercode
				ON pc_producercode.ID = pc_groupproducercode.ProducerCodeID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_group  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_group
				ON pc_groupproducercode.GroupID = pc_group.ID
				AND pc_producercode.OrganizationID = pc_group.OrganizationID*/
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pcx_jeweler_jm   WHERE DATE(_PARTITIONTIME) = DATE('{date}')) AS pcx_jeweler_jm
				ON pcx_jeweler_jm.ID = pc_account.referringjeweler_jm
			LEFT JOIN cte_Agencies ON cte_Agencies.ID = pc_policyperiod.ProducerCodeOfRecordID


		--BILLING CENTER JOINS
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_policyperiod  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) bc_policyperiod
				ON bc_policyperiod.PolicyNumber = pc_policyperiod.PolicyNumber
					AND bc_policyperiod.TermNumber = pc_policyperiod.TermNumber
					AND CAST(bc_policyperiod.PolicyPerEffDate AS DATE) = CAST(pc_policyperiod.EditEffectiveDate AS DATE)
			LEFT OUTER JOIN {prefix}{bc_dataset}.bctl_policyperiodbillingmethod
				ON bctl_policyperiodbillingmethod.ID = bc_policyperiod.BillingMethod
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_billingmethod
				ON pc_policyperiod.BillingMethod = pctl_billingmethod.ID
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_account AS bc_account  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) bc_account
				ON bc_account.AccountNumber = pc_account.AccountNumber
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_acctpmntinst  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) bc_acctpmntinst
				ON bc_account.ID = bc_acctpmntinst.OwnerID
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_paymentinstrument  WHERE DATE(_PARTITIONTIME) = DATE('{date}')) bc_paymentinstrument
				ON ForeignEntityID = bc_paymentinstrument.ID
			LEFT OUTER JOIN {prefix}{bc_dataset}.bctl_paymentmethod
				ON bctl_paymentmethod.ID = bc_paymentinstrument.PaymentMethod

			LEFT JOIN cte_PremiumsDue ON cte_PremiumsDue.AccountID = bc_account.id
		WHERE 1=1
		AND pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
		AND pc_policyperiod.PolicyNumber IS NOT NULL
		AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
		AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
		AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
		AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
		AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
		--AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{date}')
		AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')
		--AND DATE(pc_policyline._PARTITIONTIME) = DATE('{date}')
)

EXCEPT DISTINCT

(
SELECT
			'GW' AS SourceSystem
			,pc_policyperiod.PolicyNumber
			,pc_policyterm.ID AS PolicyTermID
			,pc_uwcompany.PublicID AS SourceOrganizationNumber
			,pc_contact.PublicID AS SourceCustomerNumber
			,pctl_policyperiodstatus.NAME AS PolicyPeriodStatus
			,pc_account.AccountNumber, cte_PremiumsDueY.AccountID
			--,COALESCE(pc_paymentplansummary.Name,'Unknown') AS PolicyPaymentPlan
			,COALESCE(MaxPaymentPlan.Name,'Unknown') AS PolicyPaymentPlan
			,pc_policyperiod.PeriodStart AS PolicyEffectiveDate
			--,pc_policyperiod.PeriodEnd AS PolicyExpirationDate
			,MaxPeriodEnd.PolicyExpirationDate
			,CASE WHEN pc_policyperiod.LastManualRenewalDate_JMIC = pc_policyperiod.PeriodStart THEN 'Manual' ELSE 'Automatic' END AS CurrentRenewalType
			,CASE WHEN pcx_policystatus_jmic.NextManualRenewalDate_JMIC = pc_policyperiod.PeriodEnd THEN 'Manual' ELSE 'Automatic' END AS NextRenewalType
			--,COALESCE(pc_effectivedatedfields.OfferingCode,pc_policy.ProductCode) AS PolicyType
			,pc_policy.ProductCode AS PolicyType
			,COALESCE(pctl_source_jmic_pl.NAME,'?') AS SourceOfBusiness
			--,PolicyInforceFromDate
			--,PolicyInforceToDate
			,COALESCE(bctl_policyperiodbillingmethod.NAME, pctl_billingmethod.NAME,'?') AS PolicyBillingMethod
			,CASE WHEN IFNULL(pctl_prerenewaldirection.NAME,'n/a') LIKE 'Non-Renew%' Then IFNULL(pctl_prerenewaldirection.NAME,'n/a') ELSE NULL END AS PolicyNonRenewDirection
			,COALESCE(pctl_apptakenby_jmic_pl.NAME,'') AS ApplicationTakenBy
			,CASE WHEN pc_policyperiod.PolicyNumber = 'Unassigned'
					THEN (pc_account.OrigPolicyEffDate_JMIC_PL)
					ELSE (IFNULL(pc_account.OrigPolicyEffDate_JMIC_PL,'1900-01-01')) END AS PolicyOrigEffDate
			,COALESCE(pctl_reasoncode.DESCRIPTION,'') AS PolicyCancelReason
			,CASE WHEN pc_policyperiod.CancellationDate IS NOT NULL
				THEN
					CASE WHEN pc_job.CloseDate IS NOT NULL AND pc_job.CloseDate >= pc_policyperiod.CancellationDate THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')--pc_job.CloseDate
						WHEN pc_job.CloseDate IS NOT NULL AND pc_job.CloseDate < pc_policyperiod.CancellationDate THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.CancellationDate as DATETIME)), ' UTC')--pc_policyperiod.CancellationDate
						WHEN pc_job.CloseDate IS NULL THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.CancellationDate as DATETIME)), ' UTC')--pc_policyperiod.CancellationDate
					END
				END AS PolicyCancellationDate
			,CASE WHEN (DATE(pc_policyperiod.PeriodEnd) >= DATE('{ydate}')
						AND pc_policyperiod.CancellationDate IS NULL
						--AND pc_policyperiod.Status in (9, 15, 10007) --('Bound', 'cancelling', 'renewing')
						AND pctl_policyperiodstatus.TYPECODE IN ('Bound', 'Canceling', 'Renewing')
						AND pc_policy.retired = 0)
				THEN 1 ELSE 0
				END AS IsActivePolicy
			--,RecastSourceOfBusiness
			,pcx_jeweler_jm.Name AS ReferringJewelerName --encode for sample data feeds
			,pc_policyperiod.TotalSchedPremiumRPT_JMIC AS TotalSchedPremiumRPT
			,pc_policyperiod.TotalMinimumPremiumRPT_JMIC AS MinPremium
			,pc_account.IsPaperlessDelivery_JMIC AS PaperlessDeliveryIndicator
			--,IFNULL(pc_group.Name,'') AS AgencyName
			,cte_Agencies_Y.AgencyName
			,CASE WHEN bctl_paymentmethod.ID in (1,4) THEN 1 ELSE 0 END as AutoPayIndicator  --1=ACH/EFT;4=Credit Card
			--,COALESCE(bc_invoice.NetAmount, bc_invoice.AmountDue,0) AS PremiumAmountDue
			--,COALESCE(bc_invoice.PaymentDueDate,'9999-12-31') AS PaymentDueDate
			,COALESCE(cte_PremiumsDueY.PremiumAmountDue, 0) AS PremiumAmountDue
			,COALESCE(cte_PremiumsDueY.PaymentDueDate,'9999-12-31') AS PaymentDueDate
			,bc_paymentinstrument.CreditCardExpDate_JMIC AS CreditCardExpirationDate
			,pc_job.IsSubmittedApplication_JM AS IsJPASubmitted
			/*,pc_policyperiod.CreateTime AS PolicyPeriodCreateTime
			,ROW_NUMBER() OVER(PARTITION BY pc_policyperiod.PeriodEnd
				ORDER BY
					pc_policyperiod.CreateTime DESC
				) AS TransactionRank*/
			,pc_policyperiod.ModelDate AS PolicyPeriodCreateTime
			,ROW_NUMBER() OVER(PARTITION BY pc_policyperiod.PolicyTermID
				ORDER BY pc_policyperiod.EditEffectiveDate, pc_policyperiod.ModelDate DESC
				) AS TransactionRank

		FROM {prefix}{pc_dataset}.pc_policy
			INNER JOIN {prefix}{pc_dataset}.pc_account
				ON pc_account.ID = pc_policy.AccountID
			INNER JOIN {prefix}{pc_dataset}.pc_policyterm
				ON pc_policyterm.PolicyID = pc_policy.ID
			INNER JOIN {prefix}{pc_dataset}.pc_policyperiod
				ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
			INNER JOIN {prefix}{pc_dataset}.pctl_segment
				ON pctl_segment.ID = pc_policyperiod.Segment
			INNER JOIN {prefix}{pc_dataset}.pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN {prefix}{pc_dataset}.pctl_job
				ON pctl_job.ID = pc_job.Subtype
			--INNER JOIN {prefix}{pc_dataset}.pcx_cost_jmic
				--ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus
				ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
			INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID


			/*INNER JOIN {prefix}{pc_dataset}.pc_policyline
				ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN {prefix}{pc_dataset}.pctl_policyline
			ON pc_policyline.SubType = pctl_policyline.ID
				AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'*/


		-- Get Max PeriodEnd Date
			INNER JOIN  (
				--SELECT pc_policyperiod.PolicyNumber, pc_policyperiod.PolicyTermID, MAX(pc_policyperiod.PeriodEnd) AS PolicyExpirationDate
				--FROM {prefix}{pc_dataset}.pc_policyperiod
				SELECT pc_policyperiod.PolicyID, pc_policyperiod.PolicyTermID, MAX(pc_policyperiod.PeriodEnd) AS PolicyExpirationDate
				FROM {prefix}{pc_dataset}.pc_policyperiod
				INNER JOIN {prefix}{pc_dataset}.pc_policy
					ON pc_policyperiod.PolicyID = pc_policy.ID


				WHERE 1=1
				AND  pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
				AND pc_policyperiod.ModelDate IS NOT NULL
				AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(pc_policy._PARTITIONTIME) = DATE('{ydate}')
				GROUP BY PolicyID, PolicyTermID


			) MaxPeriodEnd
				--ON MaxPeriodEnd.PolicyNumber = pc_policyperiod.PolicyNumber
				ON MaxPeriodEnd.PolicyID = pc_policyperiod.PolicyID
				AND MaxPeriodEnd.PolicyTermID = pc_policyperiod.PolicyTermID
				AND MaxPeriodEnd.PolicyExpirationDate = pc_policyperiod.PeriodEnd
				
		---subquery to get payment plan when exists
			LEFT JOIN	(
				SELECT pc_policyperiod.PolicyTermID, pc_policyperiod.ID,  pc_paymentplansummary.Name
				FROM {prefix}{pc_dataset}.pc_policy pc_policy
				INNER JOIN {prefix}{pc_dataset}.pc_policyterm pc_policyterm
					ON pc_policyterm.PolicyID = pc_policy.ID
				INNER JOIN {prefix}{pc_dataset}.pc_policyperiod pc_policyperiod
					ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				LEFT JOIN (select * from {prefix}{pc_dataset}.pc_paymentplansummary WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				WHERE 
				1=1
				AND DATE(pc_policy._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{ydate}')
				AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{ydate}')
				AND pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
				AND pc_paymentplansummary.ID IS NOT NULL) MaxPaymentPlan
				ON MaxPaymentPlan.ID = pc_policyperiod.ID
				AND MaxPaymentPlan.PolicyTermID = pc_policyperiod.PolicyTermID

			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_contact  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_contact
				ON pc_policyperiod.PNIContactDenorm = pc_contact.ID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_paymentplansummary   WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			/*LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_effectivedatedfields   WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
					AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)*/
			LEFT JOIN {prefix}{pc_dataset}.pctl_nonrenewalcode
				ON pctl_nonrenewalcode.ID = pc_policyterm.NonRenewReason
			LEFT JOIN {prefix}{pc_dataset}.pctl_nottakencode_jmic
				ON pctl_nottakencode_jmic.ID = pc_policyterm.NotTakenReason_JMIC
			LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl
				ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
			LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode
				ON pctl_reasoncode.ID = pc_job.CancelReasonCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_cancellationsource
				ON pctl_cancellationsource.ID = pc_job.Source
			LEFT JOIN {prefix}{pc_dataset}.pctl_reinstatecode
				ON pctl_reinstatecode.ID = pc_job.ReinstateCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_rewritetype
				ON pctl_rewritetype.ID = pc_job.RewriteType
			LEFT JOIN {prefix}{pc_dataset}.pctl_renewalcode
				ON pctl_renewalcode.ID = pc_job.RenewalCode
			LEFT JOIN {prefix}{pc_dataset}.pctl_prerenewaldirection
				ON pctl_prerenewaldirection.ID = pc_policyterm.PreRenewalDirection
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pcx_policystatus_jmic  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pcx_policystatus_jmic
				ON pc_policy.PolicyStatus_JMIC = pcx_policystatus_jmic.ID
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_source_jmic_pl
				ON pc_account.Source_JMIC_PL = pctl_source_jmic_pl.ID
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_apptakenby_jmic_pl
				ON pc_account.ApplicationTakenBy_JMIC_PL = pctl_apptakenby_jmic_pl.ID
			/*LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_producercode  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_producercode
				ON pc_policyperiod.ProducerCodeOfRecordID = pc_producercode.ID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_groupproducercode  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_groupproducercode
				ON pc_producercode.ID = pc_groupproducercode.ProducerCodeID
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_group  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) pc_group
				ON pc_groupproducercode.GroupID = pc_group.ID
				AND pc_producercode.OrganizationID = pc_group.OrganizationID*/
			LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pcx_jeweler_jm   WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) AS pcx_jeweler_jm
				ON pcx_jeweler_jm.ID = pc_account.referringjeweler_jm
			LEFT JOIN cte_Agencies_Y ON cte_Agencies_Y.ID = pc_policyperiod.ProducerCodeOfRecordID


		--BILLING CENTER JOINS
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_policyperiod  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) bc_policyperiod
				ON bc_policyperiod.PolicyNumber = pc_policyperiod.PolicyNumber
					AND bc_policyperiod.TermNumber = pc_policyperiod.TermNumber
					AND CAST(bc_policyperiod.PolicyPerEffDate AS DATE) = CAST(pc_policyperiod.EditEffectiveDate AS DATE)
			LEFT OUTER JOIN {prefix}{bc_dataset}.bctl_policyperiodbillingmethod
				ON bctl_policyperiodbillingmethod.ID = bc_policyperiod.BillingMethod
			LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_billingmethod
				ON pc_policyperiod.BillingMethod = pctl_billingmethod.ID
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_account AS bc_account  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) bc_account
				ON bc_account.AccountNumber = pc_account.AccountNumber
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_acctpmntinst  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) bc_acctpmntinst
				ON bc_account.ID = bc_acctpmntinst.OwnerID
			LEFT OUTER JOIN (select * from {prefix}{bc_dataset}.bc_paymentinstrument  WHERE DATE(_PARTITIONTIME) = DATE('{ydate}')) bc_paymentinstrument
				ON ForeignEntityID = bc_paymentinstrument.ID
			LEFT OUTER JOIN {prefix}{bc_dataset}.bctl_paymentmethod
				ON bctl_paymentmethod.ID = bc_paymentinstrument.PaymentMethod

			LEFT JOIN cte_PremiumsDueY ON cte_PremiumsDueY.AccountID = bc_account.id
		WHERE 1=1
		AND pc_policy.ProductCode IN ('JMIC_PL', 'JPAPersonalArticles')
		AND pc_policyperiod.PolicyNumber IS NOT NULL
		AND DATE(pc_policy._PARTITIONTIME) = DATE('{ydate}')
		AND DATE(pc_account._PARTITIONTIME) = DATE('{ydate}')
		AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{ydate}')
		AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{ydate}')
		AND DATE(pc_job._PARTITIONTIME) = DATE('{ydate}')
		--AND DATE(pcx_cost_jmic._PARTITIONTIME) = DATE('{ydate}')
		AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{ydate}')
		--AND DATE(pc_policyline._PARTITIONTIME) = DATE('{ydate}')
)
	) AS FullSet
	LEFT JOIN LTV
		ON FullSet.AccountNumber=LTV.AccountNumber
		AND FullSet.PolicyNumber=LTV.PolicyNumber
    LEFT JOIN RECAST
		ON FullSet.AccountNumber=RECAST.AccountNumber
		AND FullSet.PolicyNumber=RECAST.PolicyNumber
	WHERE TransactionRank = 1
