/* Query for the CDP - Transaction Data Feed

v2 - [X] Remove transaction item count
	 [X] Convert to transaction summary table
	 [X] Policy Number / Term --pending above
	 [X] Add accounting date

v3 - [X] Remove QuoteExpirationDate
	 [X] Revert back to other tables
	 [X] Fix UW company
	 [X] Union all the zero premium policy changes

20201118 Needs:
	[ ] Need to add in PA transactions
	[ ] Remove dimPolicy dependency

v6
	[X] Need to add in PA transactions
	[X] Remove dimPolicy dependency

v7- [X] Added CTE for PolicyContactRole to remove duplicates

v8-	[X] Restrict output to BOUND transaction status only (in Transaction header file feed)

*/

WITH Config 
AS
 (SELECT 'SourceSystem' as Key, 'GW' as Value UNION ALL
  SELECT 'HashKeySeparator', '_' UNION ALL
  SELECT 'HashingAlgorithm', 'SHA2_256')

/* CTE Created to remove possible duplicate Primary Insured Named records from COntact Role table */
, cte_policycontactrole 
AS (
	SELECT
	PrimaryPolicyContact.*
	FROM (
			SELECT 
				BranchID
				,ContactDenorm
				,ROW_NUMBER() OVER(PARTITION BY pc_policycontactrole.BranchID
					ORDER BY 
						pc_policycontactrole.ID DESC
				) AS TransactionRank
			FROM {prefix}{pc_dataset}.pc_policycontactrole
			INNER JOIN {prefix}{pc_dataset}.pctl_policycontactrole	
				ON pctl_policycontactrole.ID = pc_policycontactrole.Subtype
				AND pc_policycontactrole.Subtype = 19 --PolicyPriNamedInsured
				--and BranchID IN ( 6261521, 7693574)
			WHERE DATE(pc_policycontactrole._PARTITIONTIME) = DATE('{date}')				
		) PrimaryPolicyContact
	WHERE TransactionRank = 1
)

SELECT 
	ConfigSource.Value AS SourceSystem
	,SHA256( CONCAT(ConfigSource.Value,ConfigHashSep.Value, PolicyPeriodPublicID)) AS SourceTransactionNumber
	,PolicyTransaction.*	
FROM (
	SELECT 
		pc_policyperiod.PublicID AS PolicyPeriodPublicID
		,pc_job.JobNumber
		,pc_contact.PublicID AS SourceCustomerNumber --primary insured
		,pc_uwcompany.PublicID AS SourceOrganizationNumber
		,pc_policyperiod.TransactionCostRPT				
		,'USD' AS Currency							--Probably good to clean up, actually making a link, don't make the assumption
		,CASE 
			WHEN IFNULL(pc_policyperiod.EditEffectiveDate,'1990-01-01') >= IFNULL(pc_job.CloseDate, '1990-01-01')
			    THEN concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.EditEffectiveDate as DATETIME)), ' UTC')
				ELSE concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')
				--THEN pc_policyperiod.EditEffectiveDate
				--ELSE pc_job.CloseDate
			END	AS AccountingDate
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CreateTime as DATETIME)), ' UTC') AS DateCreated	--pc_job.CreateTime
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.UpdateTime as DATETIME)), ' UTC')  AS DateModified --pc_job.UpdateTime
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_job.CloseDate as DATETIME)), ' UTC')   AS JobCloseDate --pc_job.CloseDate
		,pctl_reasoncode.NAME AS PolicyCancelReason
		,pctl_policychangerea_jmic_pl.NAME AS PolicyChangeReason
		,NULL AS ExternalApplicationKey --Have to get from PLEcom.QuoteApp.t_application, but not sure the best key to link
		,pc_policyperiod.PolicyNumber
		,pc_policyterm.ID AS PolicyTermID
		,pc_account.AccountNumberDenorm	AS AccountNumber		--feels important to include, don't see 
		,pctl_Job.NAME AS TransactionType
		,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(pc_policyperiod.PeriodEnd as DATETIME)), ' UTC')   AS PolicyExpirationDate --pc_policyperiod.PeriodEnd
		,pctl_policyperiodstatus.NAME AS TransactionStatus
		,pc_policyperiod.ID
	FROM {prefix}{pc_dataset}.pc_policy
		INNER JOIN {prefix}{pc_dataset}.pc_account ON pc_account.ID = pc_policy.AccountID
		INNER JOIN {prefix}{pc_dataset}.pc_policyterm ON pc_policyterm.PolicyID = pc_policy.ID
		INNER JOIN {prefix}{pc_dataset}.pc_policyperiod ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
		INNER JOIN {prefix}{pc_dataset}.pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN {prefix}{pc_dataset}.pctl_job ON pctl_job.ID = pc_job.Subtype
		INNER JOIN {prefix}{pc_dataset}.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		INNER JOIN {prefix}{pc_dataset}.pc_uwcompany ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		INNER JOIN cte_policycontactrole	ON cte_policycontactrole.BranchID = pc_policyperiod.ID
		INNER JOIN {prefix}{pc_dataset}.pc_contact ON pc_contact.ID = cte_policycontactrole.ContactDenorm
		LEFT JOIN {prefix}{pc_dataset}.pctl_policychangerea_jmic_pl ON pctl_policychangerea_jmic_pl.ID = pc_job.ChangeReason_JMIC
		LEFT JOIN {prefix}{pc_dataset}.pctl_reasoncode ON pctl_reasoncode.ID = pc_job.CancelReasonCode

	WHERE 1 = 1
	AND pctl_policyperiodstatus.NAME = 'Bound'
	
	AND DATE(pc_policy._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyterm._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_policyperiod._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_job._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')
	AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')	
	--AND pc_account.AccountNumberDenorm IN ('3001351713', '3000001339')
	--AND pc_policyperiod.PolicyNumber = '24-1039286' --'24-027446'  --'24-046251' --
	--and pc_job.jobnumber = 7000031
	
	
) PolicyTransaction

	INNER JOIN Config AS ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN Config AS ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN Config AS ConfigHashAlgo
	ON ConfigHashAlgo.Key='HashingAlgorithm'
