-- tag: ClaimFinancialTransactionLinePADirect_MissingByX - tag ends/
/*** ClaimFinancialTransactionLinePADirect_MissingByX.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Updated select output; added PostedDate Clause
	11/07/2022	DROBAK		Kimberlite Table Name Changed
-----------------------------------------------------------------------------
*/
--Missing Trxn
WITH ClaimPADirectFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'LineCode','JPALine' UNION ALL
  SELECT 'UWCompany', 'zgojeqek81h0c3isqtper9n5kb9'	
)
	SELECT	'MISSING'										AS UnitTest
			, cc_transaction.PublicID						AS TransactionPublicID
			, cc_claim.ClaimNumber
			, cc_claim.LegacyClaimNumber_JMIC				AS LegacyClaimNumber
			, cc_claim.PublicID								AS ClaimPublicId
			, cc_policy.PolicyNumber
			, pc_policyperiod.PublicID						AS PolicyPeriodPublicID
			, DATE('{date}')								AS bq_load_date 
	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) cc_transaction
	INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
		ON cc_claim.ID = cc_transaction.ClaimID
	INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		--AND cctl_lobcode.TYPECODE='JPALine'	
	INNER JOIN ClaimPADirectFinancialsConfig lineConfig 
		ON lineConfig.Key = 'LineCode' 
		AND lineConfig.Value=cctl_lobcode.TYPECODE
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
	INNER JOIN ClaimPADirectFinancialsConfig uwCompany 
		ON uwCompany.Key = 'UWCompany' 
		--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
		AND COALESCE(pc_uwcompany.PublicID,uwCompany.Value) = uwCompany.Value
	--LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
	--	ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
	--LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
	--	ON cctl_transactionstatus.ID = cc_transaction.Status
	WHERE cc_transaction.PublicID NOT IN (SELECT TransactionPublicID FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePADirect` WHERE bq_load_date = DATE({partition_date}))
	--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed
	--AND COALESCE(cc_transaction.AuthorizationCode_JMIC,'') NOT IN ('Credit Card Payment Pending Notification')
	--AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided')

	/*
	WITH ClaimPJDirectFinancialsConfig AS 
	(
		SELECT 'BusinessType' as Key, 'Direct' as Value UNION ALL
		SELECT 'SourceSystem','GW' UNION ALL
		SELECT 'LineCode','JPALine' UNION ALL
		SELECT 'UWCompany', 'zgojeqek81h0c3isqtper9n5kb9'
	)
		SELECT	'MISSING'									AS UnitTest
			, cc_transaction.PublicID						AS TransactionPublicID
			, cc_claim.ClaimNumber
			, cc_claim.LegacyClaimNumber_JMIC				AS LegacyClaimNumber
			, cc_claim.PublicID								AS ClaimPublicId
			, cc_policy.PolicyNumber
			, pc_policyperiod.PublicID						AS PolicyPeriodPublicID
			--, cc_reserveline.PublicID						AS ReserveLinePublicID
			--, DATE('{date}')								AS bq_load_date 
	FROM (SELECT * FROM `qa-edl.B_QA_ref_cc_current.cc_transaction` WHERE _PARTITIONTIME = "2022-10-11") cc_transaction
	INNER JOIN (SELECT * FROM `qa-edl.B_QA_ref_cc_current.cc_transactionlineitem` WHERE _PARTITIONTIME = "2022-10-11") AS cc_transactionlineitem 
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
	LEFT JOIN (SELECT * FROM `qa-edl.B_QA_ref_cc_current.cc_claim` WHERE _PARTITIONTIME = "2022-10-11") AS cc_claim
		ON cc_claim.ID = cc_transaction.ClaimID
	INNER JOIN `qa-edl.B_QA_ref_cc_current.cctl_lobcode` AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		AND cctl_lobcode.TYPECODE='JPALine'
	LEFT JOIN (SELECT * FROM `qa-edl.B_QA_ref_cc_current.cc_policy` WHERE _PARTITIONTIME = "2022-10-11") AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
	LEFT JOIN (SELECT * FROM `qa-edl.B_QA_ref_pc_current.pc_policyperiod` WHERE _PARTITIONTIME = "2022-10-11") pc_policyperiod 
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
	LEFT OUTER JOIN (SELECT * FROM `qa-edl.B_QA_ref_pc_current.pc_uwcompany` WHERE _PARTITIONTIME = "2022-10-11") AS pc_uwcompany
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
	INNER JOIN ClaimPJDirectFinancialsConfig uwCompany 
		ON uwCompany.Key = 'UWCompany' 
		--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
		AND COALESCE(pc_uwcompany.PublicID,'uwc:10') = 'uwc:10'
	--LEFT JOIN (SELECT * FROM `qa-edl.B_QA_ref_cc_current.cc_reserveline` WHERE _PARTITIONTIME = "2022-10-11") AS cc_reserveline
		--ON cc_reserveline.ID = cc_transaction.ReserveLineID
	--LEFT JOIN `qa-edl.B_QA_ref_cc_current.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
	--	ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
	--LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
	--	ON cctl_transactionstatus.ID = cc_transaction.Status
	WHERE cc_transaction.PublicID not in (select TransactionPublicID from `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLinePADirect`)
	--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed
	--AND COALESCE(cc_transaction.AuthorizationCode_JMIC,'') NOT IN ('Credit Card Payment Pending Notification')
	--AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided')
*/