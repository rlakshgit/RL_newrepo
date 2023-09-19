-- tag: ClaimFinancialTransactionLineBOPCeded_MissingByX - tag ends/
-----------------------------------------------------------------------------------------
/*** ClaimFinancialTransactionLineBOPCeded_MissingByX.sql ***

	*****  Change History  *****

	01/23/2023	DROBAK		Init
	02/08/2023	DROBAK		Added joins to Coverage tbale to eliminate false positives

-----------------------------------------------------------------------------------------
*/
--Missing Trxn
WITH ClaimBOPCededFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Ceded' AS Value UNION ALL
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
			, cc_ritransaction.PublicID						AS TransactionPublicID
			, cc_claim.ClaimNumber
			, cc_claim.LegacyClaimNumber_JMIC				AS LegacyClaimNumber
			, cc_claim.PublicID								AS ClaimPublicId
			, cc_policy.PolicyNumber
			, DATE('{date}')								AS bq_load_date 
	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) cc_ritransaction
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
		ON cc_claim.ID = cc_ritransaction.ClaimID
	--ceded/ri joins
	INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
		ON cc_riagreement.ID = cc_ritransaction.RIAgreement
	INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
		ON cctl_riagreement.ID = cc_riagreement.SubType
	INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
		ON cc_ricoding.ID = cc_ritransaction.RICodingID
	INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_ricoding.ReserveLineID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement 
		ON pc_reinsuranceagreement.PublicID = cc_riagreement.PC_Publicid_JMIC
	INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
	INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
		ON cc_exposure.ID = cc_ritransaction.ExposureID 
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
		ON cc_coverage.ID = cc_exposure.CoverageID 
	---- BOP Line Coverages (applied to PolicyLine) to separate from IM coverage transactions
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov`  WHERE _PARTITIONTIME = {partition_date}) AS pc_businessownerscov
		ON cc_coverage.PC_CovPublicId_JMIC = pc_businessownerscov.PublicID
		AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BusinessOwnersCov'
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID

	WHERE cc_ritransaction.PublicID NOT IN (SELECT TransactionPublicID FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPCeded` WHERE bq_load_date = DATE({partition_date}))
