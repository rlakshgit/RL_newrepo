-- tag: ClaimFinancialTransactionLineBOPDirect_MissingByX - tag ends/
/*** ClaimFinancialTransactionLineBOPDirect_MissingByX.sql ***

	*****  Change History  *****

	01/13/2023	DROBAK		Init
	05/09/2023	DROBAK		Replaced hardcoded dataset with parameter

-----------------------------------------------------------------------------
*/
--Missing Trxn
WITH ClaimBOPDirectFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'ClaimLineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
  SELECT 'PCLineCode', 'BOPLine' UNION ALL
  SELECT 'LineCode','BusinessOwnersLine' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
  SELECT 'StockLevelCoverage','Stock' UNION ALL
  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC' UNION ALL
  SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
  SELECT 'BlanketLevelCoverage', 'Blanket' UNION ALL
  SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
  SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding' UNION ALL
  SELECT 'StockLevelRisk','IMStock'
)
	SELECT	'MISSING'										AS UnitTest
			, cc_transactionlineitem.PublicID				AS TransactionLinePublicID
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
	INNER JOIN ClaimBOPDirectFinancialsConfig lineConfigClaim 
			ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID		
	INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
			ON cc_reserveline.ID = cc_transaction.ReserveLineID
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS ccexposureTrxn 
		ON ccexposureTrxn.ID = cc_transaction.ExposureID 
	LEFT JOIN
	(
		--exposures for this claim that are not associated with a claim transaction
		select exposure.ID, exposure.ClaimId, ROW_NUMBER() OVER (Partition By exposure.ClaimID order by exposure.CloseDate desc) rowNumber 
		from (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS exposure
			left join (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS trxn 
			on trxn.ExposureID = exposure.ID
		where trxn.ID is null
	) ccexposureClaim on ccexposureClaim.ClaimID = cc_claim.ID and rowNumber = 1

	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
		ON cc_exposure.ID = COALESCE(ccexposureTrxn.ID, ccexposureClaim.ID)
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
		ON cc_coverage.ID = cc_exposure.CoverageID 

	INNER JOIN ClaimBOPDirectFinancialsConfig AS PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

	WHERE (cc_transaction.PublicID NOT IN (SELECT TransactionPublicID FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect` WHERE bq_load_date = DATE({partition_date}))
		OR cc_transactionlineitem.PublicID NOT IN (SELECT TransactionLinePublicID FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect` WHERE bq_load_date = DATE({partition_date})))
	--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed
	--AND COALESCE(cc_transaction.AuthorizationCode_JMIC,'') NOT IN ('Credit Card Payment Pending Notification')
	--AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided')