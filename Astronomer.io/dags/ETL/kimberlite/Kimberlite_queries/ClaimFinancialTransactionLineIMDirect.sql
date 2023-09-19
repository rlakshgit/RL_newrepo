-- tag: ClaimFinancialTransactionLineIMDirect - tag ends/
/**** Kimberlite - Financial Transactions ***********
    ClaimFinancialTransactionLineIMDirect.sql
			BigQuery Converted
*****************************************************
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	11/01/2022	KMATAM		Initial
	11/18/2022	DROBAK		Split for Kimberlite (raw) and Building Block (transformations) layers
	11/21/2022	DROBAK		New Version Optimized for BigQuery
	12/05/2022	DROBAK		Added CASE EXISTS to only build Primary Locn table if not existing)
	02/14/2023	DROBAK		For table consistency, included IsPrimaryLocation in PolicyVersionLOB_PrimaryRatingLocation

-----------------------------------------------------------------------------------------------------------------------------------
 *****  Foreign Keys Origin *****
-----------------------------------------------------------------------------------------------------------------------------------
  ClaimTransactionKey -- use to join ClaimFinancialTransactionLineIMDirect with ClaimTransaction table
  cc_claim.PublicId					AS ClaimPublicID		- ClaimTransactionKey
  pc_policyPeriod.PublicID			AS PolicyPeriodPublicID - PolicyTransactionKey
  cc_coverage.PC_CovPublicId_JMIC   AS CoveragePublicID     - IMCoverageKey
  pcx_ilmlocation_jmic.PublicID     AS IMLocationPublicID   - RiskLocationKey
  pcx_jewelrystock_jmic.PublicID	AS IMStockPublicID      - RiskStockKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DWH Source *****
-----------------------------------------------------------------------------------------------------------------------------------
  sp_helptext '[bi_stage].[spSTG_FactClaim_Extract_GW]'
  sp_helptext '.cc.s_trxn_denorm_batch_DIRECT'

-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLineIMDirect`
AS SELECT extractData.*
FROM (
		---with ClaimIMDirectFinancialsConfig
		---etc code
) extractData
*/  
/**********************************************************************************************************************************/
DECLARE vdefaultCLPESegment STRING;
  SET vdefaultCLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultCommercialLineSegment = true ORDER BY peSegment LIMIT 1);
/*
CASE
  WHEN
    EXISTS
      ( SELECT *
        FROM `{project}.{dest_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_schema = {dest_dataset}
        AND table_name = 'PolicyVersionLOB_PrimaryRatingLocation'
        AND CAST(last_modified_time AS DATE) = CURRENT_DATE
      ) 
  THEN SELECT 1=1; --'Primary Location Table Exists';
ELSE*/
	--SELECT 'Recreate the Table';
	--This code is also used in RiskLocationIM, CoverageIM, ClaimFinancialTransactionLineIMCeded, ClaimFinancialTransactionLineIMDirect
	--so the two tables use SAME PublicID from SAME Table (pcx_ilmlocation_jmic)
	CREATE OR REPLACE TABLE `{project}.{dest_dataset}.PolicyVersionLOB_PrimaryRatingLocation`
	AS SELECT *
	FROM (	SELECT 
				pc_policyperiod.ID	AS PolicyPeriodID
				,pc_policyperiod.EditEffectiveDate AS SubEffectiveDate
				,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
				--This flag displays whether or not the LOB Location matches the PrimaryLocation
				,MAX(CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
				--If the Primary loc matches the LOB loc, use it, otherwise use the MIN location num's corresponding LOB Location
				,COALESCE(MIN(CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
						,MIN(pc_policylocation.LocationNum)) AS RatingLocationNum
			FROM 
				(SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				--Blow out to include all policy locations for policy version / date segment
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
					ON pc_policyperiod.ID = pc_policylocation.BranchID
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
					ON pc_policyperiod.ID = pc_policyline.BranchID
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
					ON pc_policyline.SubType = pctl_policyline.ID			
					AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'
				--Inland Marine Location
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
					ON pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
					AND pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
					AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
				--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date} ) AS pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date} ) AS PrimaryPolicyLocation
					ON PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation 
					AND PrimaryPolicyLocation.BranchID = pc_effectivedatedfields.BranchID 
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)
				--LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS PrimaryPolicyLocation_state
				--	ON PrimaryPolicyLocation_state.ID = PrimaryPolicyLocation.StateInternal
			GROUP BY
				pc_policyperiod.ID
				,pc_policyperiod.EditEffectiveDate
				,pctl_policyline.TYPECODE
		) AS PrimaryRatingLocations;
--END CASE;

CREATE TEMP TABLE ClaimIMDirectFinancialsConfig
AS SELECT *
FROM (
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'ClaimLineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
  SELECT 'PCLineCode', 'ILMLine' UNION ALL
  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'StockLevelCoverage','Stock' UNION ALL
  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
  SELECT 'LocationLevelRisk','IMLocation' UNION ALL
  SELECT 'StockLevelRisk','IMStock'
);
/*
WITH ClaimIMDirectFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'ClaimLineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
  SELECT 'PCLineCode', 'ILMLine' UNION ALL
  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'StockLevelCoverage','Stock' UNION ALL
  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
  SELECT 'LocationLevelRisk','IMLocation' UNION ALL
  SELECT 'StockLevelRisk','IMStock'
)
*/

INSERT INTO `{project}.{dest_dataset}.ClaimFinancialTransactionLineIMDirect` (
	SourceSystem
	,FinancialTransactionKey
	,FinancialTransactionLineKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,IMCoverageKey
	,RiskLocationKey
	,RiskStockKey
	,BusinessType
	,TransactionLinePublicID
	,TransactionLineDate
	,TransactionAmount
	,TransactionPublicID
	,TransactionDate
	,TransactionSetID
	,DoesNotErodeReserves
	,TransactionStatus
	,ClaimContactID
	,ClaimPublicId
	,InsuredID
	,LossLocationID
	,ClaimNumber
	,LossDate
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,ClaimPolicyPublicId
	,ProducerCode
	,AccountNumber
	,ccPolicyID
	,LOBCode
	,ReserveLinePublicID
	,IsAverageReserveSource
	,PolicyPeriodPublicID
	,PolicyNumber
	,TermNumber
	,peSegment
	,LineCategory
	,TransactionType
	,TransactionStatusCode
	,AccountingDate
	,TransactionsSubmittedPrior
	,OfferingCode
	,CostType
	,ClaimTransactionType
	,CostCategory
	,PaymentType
	,ccCoveragePublicID
	,CoveragePublicID
	,LineCode
	,CoverageCode
	,CoverageLevel
	,ClaimRecoveryType
	,IncidentPublicID
	,IncidentPropertyID
	,ExposurePublicID
	,UnderWritingCompanyType
	,IsTransactionSliceEffective
	,RiskLocationAddressID
	,RiskUnitTypeCode
	,PolicyAddressPublicId
	,PolicyAddressStateID
	,PolicyAddressCountryID
	,PolicyAddressPostalCode
	,UWCompanyPublicID
	,ReserveLineCategoryCode
	,TransactionOrigin
	,TransactionChangeType
	,IMLocationPublicID
	,IMStockPublicID
	,ProducerPublicID
	,JewelerContactPublicID
	,VendorID
	,DefaultSegment
	,bq_load_date
)

SELECT         
    sourceConfig.Value AS SourceSystem
    ,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode))   AS FinancialTransactionKey
    ,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionLinePublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionLineKey
    --SK For FK [<Source>_<PolicyPeriodPublicID>]
    ,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPublicID))                       AS ClaimTransactionKey
    ,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
        THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
      END                                                                   AS PolicyTransactionKey
    --SK For PK [<Source>_<CoveragePublicID>_<CoverageLevel>_<Level>]
    ,CASE WHEN CoveragePublicID IS NOT NULL 
        THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
      END                                                                   AS IMCoverageKey
    ,CASE WHEN IMLocationPublicID IS NOT NULL 
        THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMLocationPublicID,hashKeySeparator.Value,locationLevelRisk.value))
       END                                                                  AS RiskLocationKey
    ,CASE WHEN IMStockPublicID IS NOT NULL 
      THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMStockPublicID, hashKeySeparator.Value,stockLevelRisk.value))
      END                                                                   AS RiskStockKey
    ,businessTypeConfig.Value                                               AS BusinessType
    ,FinTrans.*
    ,DATE('{date}')															AS bq_load_date
	--CURRENT_DATE()															AS bq_load_date

FROM
	(
	--Line Level Coverage
	SELECT
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END			AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCode
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--ccruaddress
		--,ccruaddress.publicid															AS RiskUnitPublicID
		--,ccruaddress.ID																	AS RiskUnitAddressID
		--,ccruaddress.addressLine1														AS RiskUnitAddress

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,NULL																			AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN  `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN  `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN  `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS ccexposureTrxn 
		ON ccexposureTrxn.ID = cc_transaction.ExposureID

		LEFT JOIN
		(
		--exposures for this claim that are not associated with a claim transaction
		select exposure.ID, exposure.ClaimId, ROW_NUMBER() OVER (Partition By exposure.ClaimID order by exposure.CloseDate desc) rowNumber 
		from(SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS exposure
			left join (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS trxn 
			on trxn.ExposureID = exposure.ID
		where trxn.ID is null
		) ccexposureClaim on ccexposureClaim.ClaimID = cc_claim.ID and rowNumber = 1

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
		ON cc_exposure.ID = COALESCE(ccexposureTrxn.ID, ccexposureClaim.ID)
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage
		ON cc_coverage.ID = cc_exposure.CoverageID 
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN`{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN  `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)
		
		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN  `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN  (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON  cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

		--NEW  
		-- Inland Marine Line Coverages (applied to PolicyLine)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlinecov_jmic
			ON cc_coverage.PC_CovPublicId_JMIC = pcx_ilmlinecov_jmic.PublicID
			AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.ILMLineCov_JMIC'

		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.FixedID = pcx_ilmlinecov_jmic.ILMLine
			AND pc_policyline.BranchID = pcx_ilmlinecov_jmic.BranchID
			AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
			AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN  `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype	
			--AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'LineLevelCoverage' 
		
		--Join in the PolicyVersionLOB_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN `{project}.{dest_dataset}.PolicyVersionLOB_PrimaryRatingLocation` PolicyVersionLOB_PrimaryRatingLocation
		ON PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID
			AND pc_policylocation.LocationNum = PolicyVersionLOB_PrimaryRatingLocation.RatingLocationNum 
			AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		--Inland Marine Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
			ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
			AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
			AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
			AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
			ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
			ON pc_account.ID = pc_policy.AccountID      
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
			ON cc_check.ID = cc_transaction.CheckID      
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
			ON pc_producercode.Code = cc_policy.ProducerCode
			AND pc_producercode.Retired = 0 --exclude archived producer records
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
			ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM

	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')  

	UNION ALL

	-- Sub-Line Level Coverage
		SELECT      
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 
			cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCod
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,NULL																			AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN  `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN  `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN  `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS ccexposureTrxn 
		ON ccexposureTrxn.ID = cc_transaction.ExposureID

		LEFT JOIN
		(
		--exposures for this claim that are not associated with a claim transaction
		select exposure.ID, exposure.ClaimId, ROW_NUMBER() OVER (Partition By exposure.ClaimID order by exposure.CloseDate desc) rowNumber 
		from(SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS exposure
			left join (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS trxn 
			on trxn.ExposureID = exposure.ID
		where trxn.ID is null
		) ccexposureClaim on ccexposureClaim.ClaimID = cc_claim.ID and rowNumber = 1

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
		ON cc_exposure.ID = COALESCE(ccexposureTrxn.ID, ccexposureClaim.ID)
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage
		ON cc_coverage.ID = cc_exposure.CoverageID 
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN`{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN  `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)
		
		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN  `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN  (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

		--NEW  
		-- Inland Marine Line Coverages (applied to PolicyLine)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublinecov_jmic
        ON cc_coverage.PC_CovPublicId_JMIC = pcx_ilmsublinecov_jmic.PublicID
        AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.ILMSubLineCov_JMIC'
		
  		-- Inland Marine Sub-Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubline_jmic
			ON pcx_ilmsublinecov_jmic.ILMSubLine = pcx_ilmsubline_jmic.FixedID
			AND pcx_ilmsublinecov_jmic.BranchID = pcx_ilmsubline_jmic.BranchID
			AND (pcx_ilmsubline_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmsubline_jmic.EffectiveDate IS NULL)
			AND (pcx_ilmsubline_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmsubline_jmic.ExpirationDate IS NULL)

		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.FixedID = pcx_ilmsubline_jmic.ILMLine
			AND pc_policyline.BranchID = pcx_ilmsubline_jmic.BranchID
			AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
			AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN  `{project}.{pc_dataset}.pctl_policyline`  AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype	
			--AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 

		--Join in the PolicyVersionLOB_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN `{project}.{dest_dataset}.PolicyVersionLOB_PrimaryRatingLocation` PolicyVersionLOB_PrimaryRatingLocation
		ON PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and PolicyVersionLOB_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
		ON pc_policylocation.BranchID = PolicyVersionLOB_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation.LocationNum = PolicyVersionLOB_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(PolicyVersionLOB_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

		--Inland Marine Location
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
		ON  pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
		AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
		AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)	
--				AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
--				AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
		ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
		ON pc_account.ID = pc_policy.AccountID
        
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
		ON cc_check.ID = cc_transaction.CheckID      
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
        ON pc_producercode.Code = cc_policy.ProducerCode
        AND pc_producercode.Retired = 0 --exclude archived producer records
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
        ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM

	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')  

	UNION ALL

	-- Location Level Coverage
	SELECT      
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 
			cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCode
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--ccruaddress
		--,ccruaddress.publicid															AS RiskUnitPublicID
		--,ccruaddress.ID																	AS RiskUnitAddressID
		--,ccruaddress.addressLine1														AS RiskUnitAddress

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,NULL																			AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

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
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN  `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)
		
		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN  (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON  cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

		--NEW
		-- Inland Marine Location Coverages
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocationcov_jmic
		ON cc_coverage.PC_CovPublicId_JMIC = pcx_ilmlocationcov_jmic.PublicID
		AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':') - 1) = 'entity.ILMLocationCov_JMIC'

		--Inland Marine Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date})  AS pcx_ilmlocation_jmic
		ON (pcx_ilmlocation_jmic.FixedID = pcx_ilmlocationcov_jmic.ILMLocation)
		AND (pcx_ilmlocation_jmic.BranchID = pcx_ilmlocationcov_jmic.BranchID)
		AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
		AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)
  
		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.FixedID = pcx_ilmlocation_jmic.ILMLine
		AND pc_policyline.BranchID = pcx_ilmlocation_jmic.BranchID
		AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
		AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'LocationLevelCoverage' 

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
		ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
		ON pc_account.ID = pc_policy.AccountID
        
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
		ON cc_check.ID = cc_transaction.CheckID      
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
        ON pc_producercode.Code = cc_policy.ProducerCode
        AND pc_producercode.Retired = 0 --exclude archived producer records
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
        ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM

	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')  

	UNION ALL

	-- Sub-location Level Coverage
	SELECT      
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 
			cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCode
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--ccruaddress
		--,ccruaddress.publicid															AS RiskUnitPublicID
		--,ccruaddress.ID																	AS RiskUnitAddressID
		--,ccruaddress.addressLine1														AS RiskUnitAddress

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,NULL																			AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

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
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN  `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)

		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN  (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON  cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

		--NEW
		-- Inland Marine Sub-Location Coverages
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloccov_jmic
		ON cc_coverage.PC_CovPublicId_JMIC = pcx_ilmsubloccov_jmic.PublicID
		AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')- 1) = 'entity.ILMSubLocCov_JMIC'

		-- Inland Marine Sub-Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloc_jmic
		ON pcx_ilmsubloccov_jmic.ILMSubLoc = pcx_ilmsubloc_jmic.FixedID
		AND pcx_ilmsubloccov_jmic.BranchID = pcx_ilmsubloc_jmic.BranchID
		AND (pcx_ilmsubloc_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmsubloc_jmic.EffectiveDate IS NULL)
		AND (pcx_ilmsubloc_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmsubloc_jmic.ExpirationDate IS NULL)

		--Inland Marine Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date})  AS pcx_ilmlocation_jmic
		ON (pcx_ilmlocation_jmic.FixedID = pcx_ilmsubloc_jmic.ILMLocation)
		AND (pcx_ilmlocation_jmic.BranchID = pcx_ilmsubloc_jmic.BranchID)
		AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
		AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)
 
		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.FixedID = pcx_ilmlocation_jmic.ILMLine
		AND pc_policyline.BranchID = pcx_ilmlocation_jmic.BranchID
		AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
		AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN  `{project}.{pc_dataset}.pctl_policyline`  AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
		ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
		ON pc_account.ID = pc_policy.AccountID
        
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
		ON cc_check.ID = cc_transaction.CheckID      
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
        ON pc_producercode.Code = cc_policy.ProducerCode
        AND pc_producercode.Retired = 0 --exclude archived producer records
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
        ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM
		
	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')

	UNION ALL
	
	-- Stock Level Coverages
	SELECT      
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 
			cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCode
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--ccruaddress
		--,ccruaddress.publicid															AS RiskUnitPublicID
		--,ccruaddress.ID																	AS RiskUnitAddressID
		--,ccruaddress.addressLine1														AS RiskUnitAddress

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,pcx_jewelrystock_jmic.PublicID													AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

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
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)

		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON  cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

		--NEW
		-- Inland Marine Stock Coverages
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockcov_jmic
		ON cc_coverage.PC_CovPublicId_JMIC = pcx_jewelrystockcov_jmic.PublicID
		AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')- 1) = 'entity.JewelryStockCov_JMIC'
			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
		ON (pcx_jewelrystock_jmic.FixedID = pcx_jewelrystockcov_jmic.JewelryStock)
		AND (pcx_jewelrystock_jmic.BranchID = pcx_jewelrystockcov_jmic.BranchID)
		AND (pcx_jewelrystock_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_jewelrystock_jmic.EffectiveDate IS NULL)
		AND (pcx_jewelrystock_jmic.ExpirationDate > cc_claim.LossDate OR pcx_jewelrystock_jmic.ExpirationDate IS NULL)

		--Inland Marine Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date})  AS pcx_ilmlocation_jmic
		ON (pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation)
		AND (pcx_ilmlocation_jmic.BranchID = pcx_jewelrystock_jmic.BranchID)
		AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
		AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)
  
		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyline.FixedID = pcx_ilmlocation_jmic.ILMLine
		AND pc_policyline.BranchID = pcx_ilmlocation_jmic.BranchID
		AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
		AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN `{project}.{pc_dataset}.pctl_policyline`  AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'StockLevelCoverage' 

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
		ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
		ON pc_account.ID = pc_policy.AccountID
        
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
		ON cc_check.ID = cc_transaction.CheckID      
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
        ON pc_producercode.Code = cc_policy.ProducerCode
        AND pc_producercode.Retired = 0 --exclude archived producer records
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
        ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM

	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')  

	UNION ALL

	-- Sub-Stock Level Coverage
	SELECT      
		--Transactionlineitem
		cc_transactionlineitem.PublicID													AS TransactionLinePublicID
		,cc_transactionlineitem.CreateTime												AS TransactionLineDate
		,cc_transactionlineitem.TransactionAmount										AS TransactionAmount

		--Transaction
		,cc_transaction.PublicID														AS TransactionPublicID
		,cc_transaction.CreateTime														AS TransactionDate
		,cc_transaction.TransactionSetID												AS TransactionSetID
		,cc_transaction.DoesNotErodeReserves											AS DoesNotErodeReserves
		,cc_transaction.Status															AS TransactionStatus
		--,CASE cc_transaction.DoesNotErodeReserves WHEN FALSE THEN TRUE ELSE FALSE END AS IsErodingReserves
		,cc_transaction.ClaimContactID													AS ClaimContactID
      
		--cc_claim
		,cc_claim.PublicId																AS ClaimPublicId
		,cc_claim.InsuredDenormID														AS InsuredID
		,cc_claim.LossLocationID														AS LossLocationID
		,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)				AS ClaimNumber
		,cc_claim.LossDate																AS LossDate
		,cc_claim.isClaimForLegacyPolicy_JMIC											AS IsClaimForLegacyPolicy
		,cc_claim.LegacyPolicyNumber_JMIC												AS LegacyPolicyNumber
		,cc_claim.LegacyClaimNumber_JMIC												AS LegacyClaimNumber

		--cc_policy
		,cc_policy.PublicId																AS ClaimPolicyPublicId
		,cc_policy.PRODUCERCODE															AS ProducerCode
		,cc_policy.AccountNumber														AS AccountNumber
		,cc_policy.ID																	AS ccPolicyID
      
		--cctl_lobcode
		,cctl_lobcode.TYPECODE															AS LOBCode
	  
		--cc_reserveline
		,cc_reserveline.PublicID														AS ReserveLinePublicID
		,cc_reserveline.IsAverageReserveSource_jmic										AS IsAverageReserveSource
  
		--pc_policyperiod
		,pc_policyperiod.PublicID														AS PolicyPeriodPublicID
		,pc_policyperiod.PolicyNumber													AS PolicyNumber
		,pc_policyperiod.TermNumber														AS TermNumber

		--pctl_segment
		,pctl_segment.TYPECODE															AS peSegment

		--cctl_linecategory
		,cctl_linecategory.TypeCode														AS LineCategory

		--cctl_transaction
		,cctl_transaction.TYPECODE														AS TransactionType

		--cctl_transactionstatus
		,cctl_transactionstatus.TYPECODE												AS TransactionStatusCode
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 
			cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
		,CASE WHEN 
			cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
			THEN 1 ELSE NULL END														AS TransactionsSubmittedPrior
		  
		,pc_effectivedatedfields.OfferingCode											AS OfferingCode

		--cctl_costtype
		,cctl_costtype.TYPECODE															AS CostType
		,cctl_costtype.NAME																AS ClaimTransactionType

		--cctl_costcategory
		,cctl_costcategory.TYPECODE														AS CostCategory

		--cctl_paymenttype
		,cctl_paymenttype.TYPECODE														AS PaymentType
      
		--cc_coverage
		,cc_coverage.PublicID															AS ccCoveragePublicID
		,cc_coverage.PC_CovPublicId_JMIC												AS CoveragePublicID
		,cc_coverage.PC_LineCode_JMIC													AS LineCode		--aka PCLineCode
		--,cc_coverage.Type																AS CoverageType
      
		--cctl_coveragetype
		,cctl_coveragetype.TypeCode														AS CoverageCode

		--,cctl_coveragesubtype.TYPECODE													AS CoverageLevel
		,coverageLevelConfig.Value														AS	CoverageLevel
		,cctl_recoverycategory.NAME														AS ClaimRecoveryType
      
		--cc_incident
		,cc_incident.PublicID															AS IncidentPublicID
		,cc_incident.PropertyID															AS IncidentPropertyID
    
		--cc_exposure
		,cc_exposure.PublicID															AS ExposurePublicID

		--cctl_underwritingcompanytype
		,cctl_underwritingcompanytype.TYPECODE											AS UnderWritingCompanyType
      
		--cc_transaction, cctl_transactionstatus, cctl_underwritingcompanytype 
		,CASE  WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
			NOT IN ('Credit Card Payment Pending Notification')
			AND cctl_transactionstatus.TYPECODE 
				IN ('submitting','pendingrecode','pendingstop',
				'pendingtransfer','pendingvoid','submitted',
				'recoded','stopped','transferred','voided')
			AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
			NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
			THEN 1 ELSE 0 END                                                           AS IsTransactionSliceEffective     
      
		--ccrupolicyLocation
		,ccrupolicyLocation.AddressID													AS RiskLocationAddressID

		--ccruaddress
		--,ccruaddress.publicid															AS RiskUnitPublicID
		--,ccruaddress.ID																	AS RiskUnitAddressID
		--,ccruaddress.addressLine1														AS RiskUnitAddress

		--cctl_riskunit
		,cctl_riskunit.TYPECODE															AS RiskUnitTypeCode
		,pc_policyaddress.PublicID														AS PolicyAddressPublicId
		--,pc_policyaddress.ID															AS PolicyUnitAddressID
		--,pc_policyaddress.Address														AS PolicyUnitAddress
		,pc_policyaddress.StateInternal													AS PolicyAddressStateID
		,pc_policyaddress.CountryInternal												AS PolicyAddressCountryID
		,pc_policyaddress.PostalCodeInternal											AS PolicyAddressPostalCode
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)   AS UWCompanyPublicID

		--cctl_reservelinecategory_jmic
		,cctl_reservelinecategory_jmic.TYPECODE											AS ReserveLineCategoryCode
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Onset'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Offset'
			ELSE 'Original'
		END																				AS TransactionOrigin
      
		,CASE --short circuit/bail out order is important here...
			WHEN cc_transactiononset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL 
			AND cc_transactionoffset2onset.OnsetID IS NOT NULL THEN 'Move'
			WHEN cc_transactionoffset.TransactionID IS NOT NULL THEN 'Reversal'
			ELSE ''
		END																				AS TransactionChangeType
      
		--,COALESCE(pcx_ilmlinecov_jmic.PublicID,pcx_ilmsublinecov_jmic.PublicID)			AS IMLinePublicID
		,pcx_ilmlocation_jmic.PublicID													AS IMLocationPublicID
		,pcx_jewelrystock_jmic.PublicID													AS IMStockPublicID
		,pc_producercode.PublicID														AS ProducerPublicID
		,AccountLevelJeweler.PublicID													AS JewelerContactPublicID
		,cc_check.VendorID_JMIC															AS VendorID
		,vdefaultCLPESegment															AS DefaultSegment

	FROM
		(SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
		ON cc_transactionlineitem.TransactionID = cc_transaction.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
		ON cc_claim.ID = cc_transaction.ClaimID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
		ON cc_policy.id = cc_claim.PolicyID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
		ON cc_reserveline.ID = cc_transaction.ReserveLineID
		INNER JOIN `{project}.{cc_dataset}.cctl_lobcode`AS cctl_lobcode 
		ON cctl_lobcode.ID = cc_claim.LOBCode
		INNER JOIN ClaimIMDirectFinancialsConfig lineConfigClaim 
		ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
		ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC  
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
		ON pctl_segment.Id = pc_policyPeriod.Segment
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})AS pc_uwcompany 
		ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
		ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
		ON cctl_transaction.ID = cc_transaction.Subtype
		LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
		ON cctl_transactionstatus.ID = cc_transaction.Status
		LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
		ON cctl_costtype.ID = cc_transaction.CostType
		LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
		ON cctl_costcategory.ID = cc_transaction.CostCategory
		LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
		ON cctl_paymenttype.ID = cc_transaction.PaymentType

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
		INNER JOIN ClaimIMDirectFinancialsConfig PCLineConfig 
		ON PCLineConfig.Key = 'PCLineCode' AND PCLineConfig.Value=cc_coverage.PC_LineCode_JMIC

		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
		ON cctl_coveragetype.ID = cc_coverage.Type 
		LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
		on ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID   
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
		ON cc_incident.ID = ccexposureTrxn.IncidentID --cc_exposure.IncidentID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  -- Direct Only attributes??
		ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
		ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
		ON cc_riskunit.ID = cc_coverage.RiskUnitID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit`  AS cctl_riskunit 
		ON cctl_riskunit.ID = cc_riskunit.SubType
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
		ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 

		--This is temporary until I can test MAX() needs or replace with CTE/temp table
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
		ON 	pc_policyaddress.BranchID=pc_policyperiod.id
		AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
		AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)

		--If join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company
		LEFT JOIN  `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype 
		ON cc_policy.PolicyType = cctl_policytype.ID
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
		ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
        
		-----------------------------
		--Code from FactClaim Starts
		-----------------------------

		--If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.
		--this means the current transaction is the "onset" part of a move transaction
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
		ON cc_transactiononset.OnsetID = cc_transaction.ID 
		--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
		ON cc_transactionoffset.OffsetID = cc_transaction.ID
		--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
		--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)
		--This means the current transaction is an offset transaction part of a move.
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset
		ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID
    
		LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic
		ON  cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC
        
		--Identify the specific Policy Period subEffectiveDate based on cc.LossDate
		--This temp table includes Sub-EffectiveDates within the Policy Period based on children entities
		--This assumes that the Loss Date should always be >= the Claim's PolicyPeriod EditEffectiveDate
		-- Lookup PolicyType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyPeriod.ID
		AND cc_claim.LossDate >= COALESCE(pc_policyPeriod.EditEffectiveDate,pc_policyPeriod.PeriodStart)
		AND cc_claim.LossDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)  
		AND pc_policyPeriod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyPeriod.PeriodStart)
		AND pc_policyPeriod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyPeriod.PeriodEnd)

	--NEW
	-- Inland Marine Sub-Stock Coverages
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstockcov_jmic
			ON cc_coverage.PC_CovPublicId_JMIC = pcx_ilmsubstockcov_jmic.PublicID
			AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')- 1) = 'entity.ILMSubStockCov_JMIC'
  
	-- Inland Marine Sub-Stock
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstock_jmic
			ON pcx_ilmsubstockcov_jmic.ILMSubStock = pcx_ilmsubstock_jmic.FixedID
			AND pcx_ilmsubstockcov_jmic.BranchID = pcx_ilmsubstock_jmic.BranchID
			AND (pcx_ilmsubstock_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmsubstock_jmic.EffectiveDate IS NULL)
			AND (pcx_ilmsubstock_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmsubstock_jmic.ExpirationDate IS NULL)
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
			ON (pcx_jewelrystock_jmic.FixedID = pcx_ilmsubstock_jmic.JewelryStock)
			AND (pcx_jewelrystock_jmic.BranchID = pcx_ilmsubstock_jmic.BranchID)
			AND (pcx_jewelrystock_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_jewelrystock_jmic.EffectiveDate IS NULL)
			AND (pcx_jewelrystock_jmic.ExpirationDate > cc_claim.LossDate OR pcx_jewelrystock_jmic.ExpirationDate IS NULL)

	--Inland Marine Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date})  AS pcx_ilmlocation_jmic
			ON (pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation)
			AND (pcx_ilmlocation_jmic.BranchID = pcx_jewelrystock_jmic.BranchID)
			AND (pcx_ilmlocation_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_ilmlocation_jmic.EffectiveDate IS NULL)
			AND (pcx_ilmlocation_jmic.ExpirationDate > cc_claim.LossDate OR pcx_ilmlocation_jmic.ExpirationDate IS NULL)
  
		--Policy Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.FixedID = pcx_ilmlocation_jmic.ILMLine
			AND pc_policyline.BranchID = pcx_ilmlocation_jmic.BranchID
			AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
			AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)
                                  
		LEFT JOIN  `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype

		INNER JOIN ClaimIMDirectFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN ClaimIMDirectFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'SubStockLevelCoverage' 

		--add joins to get back to pc_account to allocate the Account Level Jeweler
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
			ON pc_policy.ID = pc_policyPeriod.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
			ON pc_account.ID = pc_policy.AccountID
		LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date})   AS cc_check 
			ON cc_check.ID = cc_transaction.CheckID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode  --properly resolve agency.
			ON pc_producercode.Code = cc_policy.ProducerCode
			AND pc_producercode.Retired = 0 --exclude archived producer records	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) AS AccountLevelJeweler
			ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM

	WHERE 1 = 1
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO')
		AND cc_coverage.PC_LineCode_JMIC in ('ILMLine')--,'BOPLine')

	) FinTrans

    INNER JOIN ClaimIMDirectFinancialsConfig sourceConfig
      ON sourceConfig.Key='SourceSystem'
    INNER JOIN ClaimIMDirectFinancialsConfig hashKeySeparator
      ON hashKeySeparator.Key='HashKeySeparator'
    INNER JOIN ClaimIMDirectFinancialsConfig hashAlgorithm
      ON hashAlgorithm.Key = 'HashAlgorithm'
    INNER JOIN ClaimIMDirectFinancialsConfig businessTypeConfig
      ON businessTypeConfig.Key = 'BusinessType'
    INNER JOIN ClaimIMDirectFinancialsConfig locationLevelRisk
      ON locationLevelRisk.Key = 'LocationLevelRisk'
    INNER JOIN ClaimIMDirectFinancialsConfig stockLevelRisk
      ON stockLevelRisk.Key = 'StockLevelRisk'

    WHERE 1 = 1
    AND TransactionPublicID IS NOT NULL

--) extractData