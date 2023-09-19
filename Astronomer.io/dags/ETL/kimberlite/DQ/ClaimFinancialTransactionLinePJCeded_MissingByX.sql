-- tag: ClaimFinancialTransactionLinePJCeded_MissingByX - tag ends/
/*** ClaimFinancialTransactionLinePJCeded_MissingByX.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Updated select output; added PostedDate Clause
	11/07/2022	DROBAK		Kimberlite Table Name Change
-----------------------------------------------------------------------------
*/

--Missing Trxn
WITH ClaimPJCededFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'LineCode','JMICPJLine' UNION ALL			--JPALine --GLLine	--3rdPartyLine
  SELECT 'UWCompany', 'uwc:10'						--zgojeqek81h0c3isqtper9n5kb9
)
	SELECT	'MISSING'										AS UnitTest
			, cc_ritransaction.PublicID						AS TransactionPublicID
			, cc_claim.ClaimNumber
			, cc_claim.LegacyClaimNumber_JMIC				AS LegacyClaimNumber
			, cc_claim.PublicID								AS ClaimPublicId
			, pc_policyperiod.PublicID						AS PolicyPeriodPublicID
			, DATE('{date}')								AS bq_load_date 
	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) cc_ritransaction
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
		ON cc_claim.ID = cc_ritransaction.ClaimID	
	INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		AND cctl_lobcode.TYPECODE='JMICPJLine'
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod 
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
	INNER JOIN ClaimPJCededFinancialsConfig uwCompany 
		ON uwCompany.Key = 'UWCompany' 
		--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
		AND COALESCE(pc_uwcompany.PublicID,'uwc:10') = 'uwc:10'
	LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
	LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_ritransaction.Status
	--left join (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job on pc_job.id = pc_policyperiod.JobID
	--left join (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) cost on cost.id = trxn.Cost_JMIC
	WHERE cc_ritransaction.PublicID not in (select TransactionPublicID from `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded` WHERE bq_load_date = DATE({partition_date}))
	AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed
	AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided')
