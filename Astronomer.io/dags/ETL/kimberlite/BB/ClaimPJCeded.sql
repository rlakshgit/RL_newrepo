-- tag: ClaimPJCeded - tag ends/
/**** Kimberlite - Financial Transactions **********
		ClaimPJCeded.sql
			BigQuery Converted
****************************************************
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	09/21/2022	DROBAK		Initial
	11/01/2022	DROBAK		Split between Kimberlite (raw) and BB layers (metrics)
	02/06/2023	DROBAK		Added Insert Line
	02/20/2023	DROBAK		Add ContactKey; Add ClaimPrimaryContactID

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	ClaimTransactionKey -- use to join ClaimFinancialTransactionLinePJCeded or ClaimPJCeded with ClaimTransaction table
	cc_claim.PublicId						AS ClaimPublicId			- ClaimTransactionKey
	pc_policyPeriod.PublicID				AS PolicyPeriodPublicID		- PolicyTransactionKey
	cc_coverage.PC_CovPublicId_JMIC			AS CoveragePublicID			- ItemCoverageKey
	pcx_jewelryitem_jmic_pl.PublicID		AS ItemPublicId				- RiskJewelryItemKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DWH Source *****
-----------------------------------------------------------------------------------------------------------------------------------
 	sp_helptext 'bi_stage.spSTG_FactClaim_Extract_GW'
	sp_helptext 'cc.s_trxn_denorm_batch_CEDED'
	
-----------------------------------------------------------------------------------------------------------------------------------
*/	
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultPLPESegment STRING;
	--prod-edl.ref_pe_dbo.gw_gl_SegmentMap
	SET vdefaultPLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultPersonalLineSegment = true ORDER BY peSegment LIMIT 1);

CREATE TEMPORARY TABLE temp_MAXAgreementNumbers
 (
	AgreementNumber			STRING,
	pcRIAgreementPublicID	INTEGER,
	pcRIAgreementID			INTEGER,
	pcRICoverageGroupID		INTEGER
 );
INSERT INTO temp_MAXAgreementNumbers
	SELECT cc_riagreement.AgreementNumber, MAX(ripcagreementByPublicID.ID) AS pcRIAgreementPublicID, MAX(ripcagreementByAgreementNumber.ID) AS pcRIAgreementID, MAX(pc_ricoveragegroup.ID) AS pcRICoverageGroupID
	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByAgreementNumber 
		ON ripcagreementByAgreementNumber.AgreementNumber = cc_riagreement.AgreementNumber
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
		ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
		ON pc_ricoveragegroup.Agreement = COALESCE(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID)
	GROUP BY cc_riagreement.AgreementNumber;

CREATE TEMP TABLE ClaimPJCededFinancialsConfig
AS SELECT *
FROM (
  SELECT 'BusinessType' AS Key, 'Ceded' AS Value UNION ALL
  SELECT 'BusinessSubType','Policy' UNION ALL 
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','JMICPJLine' UNION ALL				--JPALine --GLLine	--3rdPartyLine
  SELECT 'UWCompany', 'uwc:10' UNION ALL				--
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
  SELECT 'ScheduledItemRisk','PersonalJewelryItem'		--PersonalArticleJewelry
);

/* For Testing
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimPJCeded`
--CREATE OR REPLACE TABLE `{project}.{dest_dataset}.ClaimPJCeded`
AS SELECT extractData.*
FROM (
*/

INSERT INTO `{project}.{dest_dataset}.ClaimPJCeded`(
	SourceSystem
	,FinancialTransactionKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,ContactKey						--new field
	,BusinessType
	,TransactionPublicID
	,ClaimPublicId
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ItemPublicId
	,ccItemPublicID
	,CoverageLevel
	,CoverageTypeCode
	,CoverageCode
	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,ItemNumber
	,ItemClassCode
	,TransactionType
	,ClaimTransactionType
	,CostType
	,CostCategory
	,IsErodingReserves
	,LossDate
	,TransactionDate
	,AccountingDate
	,TransactionStatusCode	
	,TransactionAmount
	,ClaimReserveLoss
	,ClaimReserveDCCExpense
	,ClaimReserveAOExpense
	,ClaimPaidLossExcludingRecovery
	,ClaimPaidDCCExpense
	,ClaimPaidAOExpense
	,PolicyLineCode
	,LineCode
	,glLineCode
	,IsAverageReserveSource
	,TransactionSetID
	,TransactionsSubmittedPrior
	,TransactionOrigin
	,TransactionChangeType
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,ProducerCode
 	,UWCompanyPublicID	
	,CompanyCode
	,AnnualStmntLine
	,RatedStateCode
	,RatedPostalCode
	,LossCountry
	,FinSegment
	,ItemState
	,LossState
	,PolicyPeriodState		
	,LossAddressPublicID
	,ItemAddressPublicID
	,ExposurePublicId
	,IncidentPublicID
	,JewelerContactPublicID
	,PolicyAddressPublicID
	,ClaimPrimaryContactID			--new field
	,RIAgreementNumber
	,RIAgreementType
	,RIAgreementID
	,RIAgreementPublicID
	,RICodingID
	,RIAgreementGroupID
	,RIPCCoverageGroupType
	,RIType
	,bq_load_date
)

SELECT
	SourceSystem
	,FinancialTransactionKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPrimaryContactID,hashKeySeparator.Value,'CC'))	AS ContactKey
	,BusinessType
	,TransactionPublicID
	,ClaimPublicId
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ItemPublicId
	,ccItemPublicID
	,CoverageLevel
	,CoverageTypeCode
	,CoverageCode
	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,ItemNumber
	,ItemClassCode
	,TransactionType
	,ClaimTransactionType
	,CostType
	,CostCategory
	,IsErodingReserves
	,LossDate
	,TransactionDate
	,AccountingDate
	,TransactionStatusCode	
	,TransactionAmount
	,ClaimReserveLoss
	,ClaimReserveDCCExpense
	,ClaimReserveAOExpense
	,ClaimPaidLossExcludingRecovery
	,ClaimPaidDCCExpense
	,ClaimPaidAOExpense
	,PolicyLineCode
	,LineCode
	,glLineCode
	,IsAverageReserveSource
	,TransactionSetID
	,TransactionsSubmittedPrior
	,TransactionOrigin
	,TransactionChangeType
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,ProducerCode
 	,UWCompanyPublicID	
	,CompanyCode
	,AnnualStmntLine
	,RatedStateCode
	,RatedPostalCode
	,LossCountry
	,FinSegment
	,ItemState
	,LossState
	,PolicyPeriodState		
	,LossAddressPublicID
	,ItemAddressPublicID
	,ExposurePublicId
	,IncidentPublicID
	,JewelerContactPublicID
	,PolicyAddressPublicID
	,ClaimPrimaryContactID
	,RIAgreementNumber
	,RIAgreementType
	,RIAgreementID
	,RIAgreementPublicID
	,RICodingID
	,RIAgreementGroupID
	,RIPCCoverageGroupType
	,RIType
	,bq_load_date

FROM (

	SELECT
		SourceSystem
		,FinancialTransactionKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,ItemCoverageKey
		,RiskJewelryItemKey
		,primaryContact.PublicID																		AS ClaimPrimaryContactID
		,BusinessType
		,TransactionPublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		--,ClaimPolicyPublicId
		,ItemPublicId
		,ccItemPublicID
		,CoverageLevel
		--,CoverageSubTypeCode
		,CASE	WHEN ClaimFinancialTransactionLinePJCeded.CoverageLevel = 'UnScheduledCov' THEN 'UNS'
				WHEN ClaimFinancialTransactionLinePJCeded.CoverageLevel = 'ScheduledCov' THEN 'SCH'
			ELSE gw_cov_ASLMap.gwCovRef END																AS CoverageTypeCode
		,CoverageCode
		,ClaimNumber
		--,IsTransactionSliceEffective
		,PolicyNumber
		,TermNumber
		,AccountNumber
		,ItemNumber
		,ItemClassCode
		,TransactionType
		,ClaimTransactionType
		,CostType
		,CostCategory
		,1																								AS IsErodingReserves	--assume its always bringing down ceded reserves  
		,LossDate
		,TransactionDate
		,AccountingDate
		,TransactionStatusCode	
		,TransactionAmount
		
	--RESERVES
		--Indemnity Reserves only
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'claimcost' 
					AND ClaimFinancialTransactionLinePJCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount 
				ELSE 0 
			END																							AS ClaimReserveLoss

		--rename to: ClaimReserveALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePJCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount 
				ELSE 0 
			END																							AS ClaimReserveDCCExpense

		--rename to: ClaimReserveALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePJCeded.CostCategory = 'aoexpense_jmic'
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount 
				ELSE 0 
			END																							AS ClaimReserveAOExpense

	--PAID
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'claimcost' 
					And ClaimFinancialTransactionLinePJCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount 
				ELSE 0 
			END																							AS ClaimPaidLossExcludingRecovery

		--rename to: ClaimPaidALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePJCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount 
				ELSE 0 
			END																							AS ClaimPaidDCCExpense

		--rename to: ClaimPaidALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLinePJCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLinePJCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePJCeded.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLinePJCeded.TransactionAmount
				ELSE 0 
			END																							AS ClaimPaidAOExpense
	
		,COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode)									AS PolicyLineCode
		,LineCode
		,COALESCE(LOBCode, LineCode)																	AS glLineCode
		,IsAverageReserveSource
		,TransactionSetID
		,TransactionsSubmittedPrior
		,TransactionOrigin
		,TransactionChangeType
		,IsClaimForLegacyPolicy
		,LegacyPolicyNumber
		,LegacyClaimNumber
		,pc_producercode.Code																			AS ProducerCode
 		,UWCompanyPublicID	
		,CASE
			WHEN COALESCE(claimCountry.TYPECODE, pctl_country.TYPECODE) = 'CA' THEN 'JMCN' 
			WHEN COALESCE(claimCountry.TYPECODE, pctl_country.TYPECODE) = 'US' THEN 'JMIC' 
		END																								AS CompanyCode
		,COALESCE(gw_cov_ASLMap.peASL,gw_gl_LobMap_Line.DefaultASL,gw_gl_LobMap.DefaultASL)				AS AnnualStmntLine
		,COALESCE( claimState.TYPECODE, pctl_state.TYPECODE)											AS RatedStateCode
		,COALESCE( claimAddress.PostalCode, PolicyAddressPostalCode)									AS RatedPostalCode
		,lossCountry.TYPECODE																			AS LossCountry
		,COALESCE(
			--coverage based
			gw_gl_SegmentMap.peSegment, 
			--for legacy claims
			CASE 
				WHEN ClaimFinancialTransactionLinePJCeded.IsClaimForLegacyPolicy = true
				AND COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode) = 'PJ' 
				THEN vdefaultPLPESegment
			END,
			NULL)																						AS FinSegment	--Is this needed in Kimberlite if we are NOT doing Financial reporting here?
		,cctl_state.TYPECODE																			AS ItemState
		,cctl_state_loss.TYPECODE																		AS LossState
		,pctl_state.TYPECODE																			AS PolicyPeriodState		
		,cc_address_loss.publicid																		AS LossAddressPublicID
		,cc_address.publicid																			AS ItemAddressPublicID
		--,ReserveLinePublicID
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicID
		,PolicyAddressPublicID
		--Ceded Fields
		,RIAgreementNumber
		,RIAgreementType
		,RIAgreementID
		,RIAgreementPublicID
		,RICodingID
		,RIAgreementGroupID
		,RIPCCoverageGroupType
		,RIType
		,DATE('{date}')	 AS bq_load_date
		--,CURRENT_DATE() AS bq_load_date

	FROM
		(SELECT * FROM `{project}.{core_dataset}.ClaimFinancialTransactionLinePJCeded` WHERE bq_load_date = DATE({partition_date})) AS ClaimFinancialTransactionLinePJCeded

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
			ON cc_riagreementgroup.ID = ClaimFinancialTransactionLinePJCeded.RIAgreementGroupID
		LEFT JOIN temp_MAXAgreementNumbers
			ON ClaimFinancialTransactionLinePJCeded.RIAgreementNumber = temp_MAXAgreementNumbers.AgreementNumber
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement 
			ON pc_reinsuranceagreement.PublicID = ClaimFinancialTransactionLinePJCeded.RIAgreementPublicID		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.ID = temp_MAXAgreementNumbers.pcRICoverageGroupID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
			ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
			ON riAgreementMap.gwRICode = ClaimFinancialTransactionLinePJCeded.RIAgreementType 
			AND riAgreementMap.gwSource = 'Agreement'
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
			ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
			AND riCoverageGroupMap.gwSource = 'CoverageGroup'

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
			ON pctl_state.ID = ClaimFinancialTransactionLinePJCeded.PolicyAddressStateId
		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country 
			ON pctl_country.ID = ClaimFinancialTransactionLinePJCeded.PolicyAddressCountryId

		--if a item exists, it has to be located somewhere (someone)
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact 
			ON cc_contact.ID = ClaimFinancialTransactionLinePJCeded.LocatedWith
		--if a contact was found, then there is a primary address
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
			ON cc_address.ID = cc_contact.PrimaryAddressID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state 
			ON cctl_state.ID = cc_address.State			

		--Loss Contact Info
		--claim may have a loss location
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_loss 
			ON cc_address_loss.ID = ClaimFinancialTransactionLinePJCeded.LossLocationID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_loss ON cctl_state_loss.ID = cc_address_loss.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS lossCountry ON lossCountry.ID = cc_address_loss.Country

		-- All tables needed?
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS primaryContact 
			ON primaryContact.id = ClaimFinancialTransactionLinePJCeded.InsuredID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS primaryAddress 
			ON primaryAddress.id = primaryContact.PrimaryAddressID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS claimAddress 
			ON claimAddress.ID = CASE 
									WHEN ClaimFinancialTransactionLinePJCeded.IsClaimForLegacyPolicy = false
									THEN 
										/*GW claims: This includes verified and unverified policies [future consideration]*/
										primaryAddress.ID -- PL items use located with/at someone/where. 	
									ELSE
										/*Legacy claims: This includes manually verified policies*/
										coalesce
										(	-- PL Items are located with/at someone/where
											CASE 
												WHEN ClaimFinancialTransactionLinePJCeded.CoverageCode = 'JewelryItemCov_JMIC_PL' 
												THEN primaryAddress.ID 
												ELSE null 
											END, 
											/*
												Both for PL and CL, LOB level coverages will follow this rule:
												For PL: Transactions for UNSchedule coverages are allocated at the policy' primary address. This is entered as the claimants primary address [per requirement]
											*/
											primaryAddress.ID
											/* Use the Loss Location. This is not ideal, and may skew loss ratio. Noted in SOP */
											,ClaimFinancialTransactionLinePJCeded.LossLocationID
										)
								END

		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS claimState ON claimState.ID = claimAddress.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS claimCountry on claimCountry.ID = claimAddress.Country

		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap 
			ON gw_gl_LobMap.gwLobCode = ClaimFinancialTransactionLinePJCeded.LOBCode			
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap_Line 
			ON gw_gl_LobMap_Line.gwLobCode = ClaimFinancialTransactionLinePJCeded.LineCode
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
			ON ClaimFinancialTransactionLinePJCeded.peSegment = gw_gl_SegmentMap.gwSegment
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_cov_ASLMap` AS gw_cov_ASLMap 
			ON gw_cov_ASLMap.gwCov = ClaimFinancialTransactionLinePJCeded.CoverageCode

		--properly resolve agency
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
			ON pc_producercode.CODE = ClaimFinancialTransactionLinePJCeded.ProducerCode
			AND pc_producercode.RETIRED = 0                              --exclude archived producer records  
		
		WHERE 1=1
		AND	IsTransactionSliceEffective = 1		--matches filter used in DWH
		--AND TransactionsSubmittedPrior = 0 --ignore updated transactions (only the original will have a 0)

	) innerselect

	INNER JOIN ClaimPJCededFinancialsConfig sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN ClaimPJCededFinancialsConfig hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN ClaimPJCededFinancialsConfig hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) extractData