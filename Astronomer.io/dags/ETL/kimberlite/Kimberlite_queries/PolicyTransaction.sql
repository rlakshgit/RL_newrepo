/*****************************************
  KIMBERLITE EXTRACT
    PolicyTransaction
		Converted to BigQuery
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	04/09/2021	DROBAK		Changed join to pc_segment from INNER to LEFT
	04/26/2021	DROBAK		Added field: pc_policyperiod.quoteserviceid
	05/23/2022	DROBAK		Add pc_effectivedatedfields join and OfferingCode, OriginalPolicyEffDate; Remove NumberOfYearsInsured (moved calculated field to BB Level)
	06/01/2022	DROBAK		Add PolicyTermPublicID

-------------------------------------------------------------------------------------------------------------------
*/
WITH ConfigPolicyTransaction AS (
	SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL	
	SELECT 'HashKeySeparator', '_' 
	UNION ALL SELECT 'HashingAlgorithm', 'SHA2_256'
)

SELECT 
	ConfigSource.Value AS SourceSystem
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, PolicyPeriodPublicID)) AS PolicyTransactionKey
	,PolicyTransaction.*	
    ,DATE('{date}') AS bq_load_date		
FROM (
  SELECT 
		pc_account.AccountNumberDenorm								AS AccountNumber
		,pc_policyperiod.PolicyNumber								AS PolicyNumber
		,pc_policy.LegacyPolicyNumber_JMIC							AS LegacyPolicyNumber
		,pctl_segment.NAME											AS Segment
		,pc_job.JobNumber										    AS JobNumber
		,pc_policyperiod.PublicID									AS PolicyPeriodPublicID
		,pc_policyterm.PublicID										AS PolicyTermPublicID
		,pc_effectivedatedfields.OfferingCode						AS OfferingCode
		,pctl_job.NAME											    AS TranType
		,pc_policyperiod.PeriodStart								AS PeriodEffDate
		,pc_policyperiod.PeriodEnd									AS PeriodEndDate
		,pc_account.OrigPolicyEffDate_JMIC_PL						AS OriginalPolicyEffDate
		,pc_policyperiod.EditEffectiveDate							AS TransEffDate
		,pc_policyperiod.WrittenDate								AS WrittenDate
		,pc_job.SubmissionDate										AS SubmissionDate 
		,pc_job.CloseDate										    AS JobCloseDate
		,pc_policyperiod.DeclinationDate_JMIC_PL					AS DeclineDate
		,pc_policyperiod.TermNumber									AS TermNumber
		,pc_policyperiod.ModelNumber								AS ModelNumber 
		,pctl_policyperiodstatus.NAME								AS TransactionStatus
		,pc_policyperiod.TransactionCostRPT							AS TransactionCostRPT
		,pc_policyperiod.TotalCostRPT								AS TotalCostRPT
		,pc_policyperiod.EstimatedPremium							AS EstimatedPremium
		,pc_policyperiod.TotalMinimumPremiumRPT_JMIC				AS TotalMinimumPremiumRPT
		,pc_policyperiod.TotalMinimumPremiumFT_RPT_JMIC				AS TotalMinimumPremiumRPTft
		,pc_policyperiod.TotalPremiumRPT							AS TotalPremiumRPT
		,pc_policyperiod.TotalSchedPremiumRPT_JMIC					AS TotalSchedPremiumRPT
		,pc_policyperiod.TotalUnschedPremiumRPT_JMIC				AS TotalUnschedPremiumRPT
		,pctl_nottakencode_jmic.NAME								AS NotTakenReason
		,pc_policyterm.NotTakenAdditionalText_JMIC					AS NotTakenExplanation
		,pctl_policychangerea_jmic_pl.NAME							AS PolicyChangeReason
		,pctl_cancellationsource.NAME								AS CancelSource
		,pctl_reasoncode.TYPECODE									AS CancelType
		,pctl_reasoncode.NAME										AS CancelReason
		,pctl_reasoncode.DESCRIPTION								AS CancelReasonDescription
		,pc_policyperiod.CancellationDate							AS CancelEffectiveDate
		,pctl_reinstatecode.NAME									AS ReinstReason
		,pctl_rewritetype.NAME										AS RewriteType
		,pctl_renewalcode.NAME										AS RenewalCode
		,pctl_prerenewaldirection.NAME								AS PreRenewalDirection
		,pctl_nonrenewalcode.NAME									AS NonRenewReason
		,pc_policyterm.NonRenewAddExplanation						AS NonRenewExplanation	
		,CAST(pc_policyperiod.IsConditionalRenewal_JMIC	AS INT64)	AS IsConditionalRenewal
		,CAST(pc_policyperiod.StraightThrough_JM AS INT64)			AS IsStraightThrough
		,pc_policyperiod.quoteserviceid								AS QuoteServiceID

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
		ON pc_account.ID = pc_policy.AccountID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm
		ON pc_policyterm.PolicyID = pc_policy.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
		ON pc_policyperiod.PolicyTermID = pc_policyterm.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_job`) AS pctl_job
		ON pctl_job.ID = pc_job.Subtype

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyperiodstatus`) AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_segment`) AS pctl_segment
		ON pctl_segment.ID = pc_policyperiod.Segment

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_nonrenewalcode`) AS pctl_nonrenewalcode
		ON pctl_nonrenewalcode.ID = pc_policyterm.NonRenewReason

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_nottakencode_jmic`) AS pctl_nottakencode_jmic
		ON pctl_nottakencode_jmic.ID = pc_policyterm.NotTakenReason_JMIC

	    LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policychangerea_jmic_pl`) AS pctl_policychangerea_jmic_pl
	    ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
    
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_reasoncode`) AS pctl_reasoncode
		ON pctl_reasoncode.ID = pc_job.CancelReasonCode

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_cancellationsource`) AS pctl_cancellationsource
		ON pctl_cancellationsource.ID = pc_job.Source

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_reinstatecode`) AS pctl_reinstatecode
		ON pctl_reinstatecode.ID = pc_job.ReinstateCode

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_rewritetype`) AS pctl_rewritetype
		ON pctl_rewritetype.ID = pc_job.RewriteType

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_renewalcode`) AS pctl_renewalcode
		ON pctl_renewalcode.ID = pc_job.RenewalCode

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_prerenewaldirection`) AS pctl_prerenewaldirection
		ON pctl_prerenewaldirection.ID = pc_policyterm.PreRenewalDirection

	WHERE	1 = 1
	--AND pc_policyperiod.policynumber = '24-053914'
       
) PolicyTransaction

	INNER JOIN ConfigPolicyTransaction  AS ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN ConfigPolicyTransaction  AS ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN ConfigPolicyTransaction AS ConfigHashAlgo
	ON ConfigHashAlgo.Key='HashingAlgorithm';
