-- tag: ClaimPADirect - tag ends/
/**** Kimberlite - Building Block - Financial Transactions ********
		ClaimPADirect.sql
			BigQuery converted
*******************************************************************/
/*
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	10/08/2022	DROBAK		Initial
	11/07/2022	DROBAK		Split for Kimberlite (raw) and Building Block (transformations) layers
	02/06/2023	DROBAK		Added Insert Line
	02/20/2023	DROBAK		Add ContactKey; Add ClaimPrimaryContactID, Remove ClaimContactID

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	ClaimTransactionKey -- use to join ClaimFinancialTransactionLinePADirect with ClaimTransaction table
	cc_claim.PublicId						AS ClaimPublicId			- ClaimTransactionKey
	pc_policyPeriod.PublicID				AS PolicyPeriodPublicID		- PolicyTransactionKey
	cc_coverage.PC_CovPublicId_JMIC			AS CoveragePublicID			- PAJewelryCoverageKey
	pcx_personalarticle_jm.PublicID			AS PAJewelryPublicID		- RiskPAJewelryKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DWH Source *****
-----------------------------------------------------------------------------------------------------------------------------------
 	sp_helptext '[bi_stage].[spSTG_FactClaim_Extract_GW]'
	sp_helptext 'cc.s_trxn_denorm_batch_DIRECT'

-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimPADirect`
AS SELECT extractData.*
FROM (
		---with ClaimPADirectFinancialsConfig
		---etc code
) extractData
*/	
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultPLPESegment STRING;
	--prod-edl.ref_pe_dbo.gw_gl_SegmentMap
	SET vdefaultPLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultPersonalLineSegment = true ORDER BY peSegment LIMIT 1);

CREATE TEMP TABLE ClaimPADirectFinancialsConfig
AS SELECT *
FROM (
	  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
	  SELECT 'SourceSystem','GW' UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
	  SELECT 'LineCode','JPALine' UNION ALL				--JPALine --GLLine	--3rdPartyLine --JMICPJLine
	  SELECT 'UWCompany', 'zgojeqek81h0c3isqtper9n5kb9' UNION ALL
	  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
	  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
	  SELECT 'NoCoverage','NoCoverage' UNION ALL
	  SELECT 'CostCoverage','CostCoverage' UNION ALL
	  SELECT 'ScheduledItemRisk','PersonalArticleJewelry'
	);
	
INSERT INTO `{project}.{dest_dataset}.ClaimPADirect`(
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,PAJewelryCoverageKey
		,RiskPAJewelryKey
		,ContactKey						--new field
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,PAJewelryPublicID
		,ccPAJewelryPublicID
		,CoverageLevel
		,CoverageTypeCode	
		,CoverageCode
		,ClaimNumber
		,PolicyNumber
		,TermNumber
		,AccountNumber
		,JewelryArticleNumber
		,ArticleTypeCode
		,ArticleGenderCode
		,TransactionType
		,ClaimTransactionType
		,CostType
		,CostCategory
		,LineCategory
		,PaymentType
		,IsErodingReserves
		,LossDate
		,TransactionDate
		,TransactionLineDate
		,AccountingDate
		,TransactionStatusCode
		,TransactionAmount
		,ClaimReserveLoss
		,ClaimReserveLossRecovery
		,ClaimReserveDCCExpense
		,ClaimReserveDCCExpenseRecovery
		,ClaimReserveAOExpense
		,ClaimReserveAOExpenseRecovery
		,ClaimPaidLossExcludingRecovery
		,ClaimPaidDCCExpense
		,ClaimPaidAOExpense
		,ClaimLossRecovery	
		,ClaimRecoveryDCCExpense
		,ClaimRecoveryAOExpense
		,ClaimRecoveryType
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
		--,ClaimContactID
		,ReserveLineCategoryCode
		,ProducerCode
 		,UWCompanyPublicID
		,CompanyCode
		,AnnualStmntLine
		,RatedStateCode
		,RatedPostalCode
		,LossCountry
		,FinSegment
		,ClaimPaymentContactFullName
		,ClaimPaymentContactIsJeweler
		,ClaimPaymentContactIsJMJeweler
		,ClaimPaymentContactPublicID
		,ItemState
		,LossState
		,PolicyPeriodState		
		,LossAddressPublicId
		,ItemAddressPublicId
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicId
		,PolicyAddressPublicId
		,ClaimPrimaryContactID			--new field
		,bq_load_date
	)

	SELECT
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,PAJewelryCoverageKey
		,RiskPAJewelryKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPrimaryContactID,hashKeySeparator.Value,'CC'))	AS ContactKey	
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,PAJewelryPublicID
		,ccPAJewelryPublicID
		,CoverageLevel
		,CoverageTypeCode	
		,CoverageCode
		,ClaimNumber
		,PolicyNumber
		,TermNumber
		,AccountNumber
		,JewelryArticleNumber
		,ArticleTypeCode
		,ArticleGenderCode
		,TransactionType
		,ClaimTransactionType
		,CostType
		,CostCategory
		,LineCategory
		,PaymentType
		,IsErodingReserves
		,LossDate
		,TransactionDate
		,TransactionLineDate
		,AccountingDate
		,TransactionStatusCode
		,TransactionAmount
		,ClaimReserveLoss
		,ClaimReserveLossRecovery
		,ClaimReserveDCCExpense
		,ClaimReserveDCCExpenseRecovery
		,ClaimReserveAOExpense
		,ClaimReserveAOExpenseRecovery
		,ClaimPaidLossExcludingRecovery
		,ClaimPaidDCCExpense
		,ClaimPaidAOExpense
		,ClaimLossRecovery	
		,ClaimRecoveryDCCExpense
		,ClaimRecoveryAOExpense
		,ClaimRecoveryType
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
		--,ClaimContactID
		,ReserveLineCategoryCode
		,ProducerCode
 		,UWCompanyPublicID
		,CompanyCode
		,AnnualStmntLine
		,RatedStateCode
		,RatedPostalCode
		,LossCountry
		,FinSegment
		,ClaimPaymentContactFullName
		,ClaimPaymentContactIsJeweler
		,ClaimPaymentContactIsJMJeweler
		,ClaimPaymentContactPublicID
		,ItemState
		,LossState
		,PolicyPeriodState		
		,LossAddressPublicId
		,ItemAddressPublicId
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicId
		,PolicyAddressPublicId
		,ClaimPrimaryContactID
		,bq_load_date

	FROM (

	SELECT
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,PAJewelryCoverageKey
		,RiskPAJewelryKey
		,primaryContact.PublicID																				AS ClaimPrimaryContactID
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		--,ccCoveragePublicID
		--,ClaimPolicyPublicId
		,PAJewelryPublicID
		,ccPAJewelryPublicID
		,CoverageLevel
		--,CoverageSubTypeCode
		,CASE	WHEN ClaimFinancialTransactionLinePADirect.CoverageLevel = 'UnscheduledJewelryCovType_JM' THEN 'UNS'
				WHEN ClaimFinancialTransactionLinePADirect.CoverageLevel = 'JewelryItemCovType_JM' THEN 'SCH'
			ELSE gw_cov_ASLMap.gwCovRef END																		AS CoverageTypeCode	
		,CoverageCode
		,ClaimNumber
		--,IsTransactionSliceEffective
		,PolicyNumber
		,TermNumber
		,AccountNumber
		,JewelryArticleNumber
		,ArticleTypeCode
		,ArticleGenderCode
		,TransactionType
		,ClaimTransactionType
		,CostType
		,CostCategory
		,LineCategory
		,PaymentType
		,CASE ClaimFinancialTransactionLinePADirect.DoesNotErodeReserves WHEN false THEN true ELSE false END	AS IsErodingReserves
		,LossDate
		,TransactionDate
		,TransactionLineDate
		,AccountingDate
		,TransactionStatusCode
		,TransactionAmount

	--RESERVES
		--Indemnity Reserves only
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Reserve'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'unspecified_jmic'
					AND ClaimFinancialTransactionLinePADirect.DoesNotErodeReserves = false
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveLoss
		--contra to ClaimReserveLoss
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'RecoveryReserve' 
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery' 
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveLossRecovery
		--rename to: ClaimReserveALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Reserve'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic'
					AND ClaimFinancialTransactionLinePADirect.DoesNotErodeReserves = false
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveDCCExpense
		--contra to ClaimReserveDCCExpense NC
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'RecoveryReserve'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveDCCExpenseRecovery
		--rename to: ClaimReserveALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Reserve'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic'
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic'
					AND ClaimFinancialTransactionLinePADirect.DoesNotErodeReserves = false
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveAOExpense
		--contra to ClaimReserveAOExpense NC
		,CASE  WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'RecoveryReserve'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount * -1 
				ELSE 0 
			END																				AS ClaimReserveAOExpenseRecovery

	--PAID
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
					And ClaimFinancialTransactionLinePADirect.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				ELSE 0 
			END																				AS ClaimPaidLossExcludingRecovery
		--rename to: ClaimPaidALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				ELSE 0 
			END																				AS ClaimPaidDCCExpense
		--rename to: ClaimPaidALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Payment'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount
				ELSE 0 
			END																				AS ClaimPaidAOExpense
			
	--RECOVERY
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'claimcost' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				ELSE 0 
			END																				AS ClaimLossRecovery	
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				ELSE 0 
			END																				AS ClaimRecoveryDCCExpense
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType = 'Recovery'
					AND ClaimFinancialTransactionLinePADirect.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLinePADirect.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLinePADirect.TransactionAmount 
				ELSE 0 
			END																				AS ClaimRecoveryAOExpense	

		,ClaimRecoveryType
		,COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode)						AS PolicyLineCode
		,LineCode
		,COALESCE(LOBCode, LineCode)														AS glLineCode
		,IsAverageReserveSource
		,TransactionSetID
		,TransactionsSubmittedPrior
		,TransactionOrigin
		,TransactionChangeType
		,IsClaimForLegacyPolicy
		,LegacyPolicyNumber
		,LegacyClaimNumber
		--,ClaimFinancialTransactionLinePADirect.ClaimContactID
		,ReserveLineCategoryCode
		,pc_producercode.Code																AS ProducerCode
 		,UWCompanyPublicID

		,CASE
			WHEN COALESCE(claimCountry.TYPECODE, pctl_country.TYPECODE) = 'CA' THEN 'JMCN' 
			WHEN COALESCE(claimCountry.TYPECODE, pctl_country.TYPECODE) = 'US' THEN 'JMIC' 
		END																					AS CompanyCode
		,COALESCE(gw_cov_ASLMap.peASL,gw_gl_LobMap_Line.DefaultASL,gw_gl_LobMap.DefaultASL)	AS AnnualStmntLine
		,COALESCE( claimState.TYPECODE, pctl_state.TYPECODE)								AS RatedStateCode
		,COALESCE( claimAddress.PostalCode, PolicyAddressPostalCode)						AS RatedPostalCode
		,lossCountry.TYPECODE																AS LossCountry
		--coverage based
		,COALESCE(gw_gl_SegmentMap.peSegment, NULL)											AS FinSegment
		--ClaimPaymentContact NK/FK/Attrib fields
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType IN('Payment', 'Recovery')
			THEN CASE WHEN TrxnClaimContactRoleSubset.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
				THEN CONCAT(COALESCE(CONCAT(ccc.FirstName, ' '),''), COALESCE(ccc.LastName,''))
				ELSE COALESCE(checkvendor.name, CONCAT(COALESCE(CONCAT(checkvendor.FirstName, ' '),''), COALESCE(checkVendor.LastName,'')))	--get directly from check
			END	ELSE NULL
		END																				AS ClaimPaymentContactFullName
		,CASE	WHEN ContactJeweler.ContactID IS NOT NULL THEN 1 
				WHEN ContactIsJeweler.ContactID IS NOT NULL THEN 1 ELSE 0 
			END																				AS ClaimPaymentContactIsJeweler
		,CASE WHEN ContactIsJMJeweler.ContactID IS NOT NULL THEN 1 ELSE 0 END				AS ClaimPaymentContactIsJMJeweler
		,CASE WHEN ClaimFinancialTransactionLinePADirect.TransactionType IN('Payment', 'Recovery')
				THEN CASE WHEN TrxnClaimContactRoleSubset.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
						THEN ccc.PublicID  --ID whould have never worked.
						ELSE checkvendor.PublicID
				END 
				ELSE NULL 
			END																				AS ClaimPaymentContactPublicID
		,cctl_state.TYPECODE																AS ItemState
		,cctl_state_loss.TYPECODE															AS LossState
		,pctl_state.TYPECODE																AS PolicyPeriodState		
		,cc_address_loss.publicid															AS LossAddressPublicId
		,cc_address.publicid																AS ItemAddressPublicId
		--,ReserveLinePublicID
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicId
		,PolicyAddressPublicId
		--,AS LossDateAgencyEvolvedKey	--Need pc_producercode.PublicID and cc_claim.LossDate to lookup DimAgency eveolved table in Kimberlite, when available
		,DATE('{date}')	 AS bq_load_date
		--,CURRENT_DATE() AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.ClaimFinancialTransactionLinePADirect` WHERE bq_load_date = DATE({partition_date})) AS ClaimFinancialTransactionLinePADirect

	LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
		ON pctl_state.ID = ClaimFinancialTransactionLinePADirect.PolicyAddressStateId
	LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country 
		ON pctl_country.ID = ClaimFinancialTransactionLinePADirect.PolicyAddressCountryId

	--if a item exists, it has to be located somewhere (someone)
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact 
		ON cc_contact.ID = ClaimFinancialTransactionLinePADirect.LocatedWith
	--if a contact was found, then there is a primary address
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
		ON cc_address.ID = cc_contact.PrimaryAddressID 
	LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state 
		ON cctl_state.ID = cc_address.State			

	--Loss Contact Info
	--claim may have a loss location
	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_loss 
		ON cc_address_loss.ID = ClaimFinancialTransactionLinePADirect.LossLocationID 
	LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_loss 
		ON cctl_state_loss.ID = cc_address.State
	LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS lossCountry
		ON lossCountry.ID = cc_address_loss.Country

				-- All tables needed?
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS primaryContact 
					ON primaryContact.id = ClaimFinancialTransactionLinePADirect.InsuredID
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS primaryAddress
					ON primaryAddress.id = primaryContact.PrimaryAddressID
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS claimAddress
					ON claimAddress.ID = 
											case 
												when ClaimFinancialTransactionLinePADirect.IsClaimForLegacyPolicy = false
												then 
													/*GW claims: This includes verified and unverified policies [future consideration]*/
													primaryAddress.ID -- PL items use located with/at someone/where. 	
												else
													/*Legacy claims: This includes manually verified policies*/
													coalesce
													(	-- PL Items are located with/at someone/where
														case 
															when ClaimFinancialTransactionLinePADirect.CoverageCode = 'JewelryItemCov_JMIC_PL' 
															then primaryAddress.ID 
															else null 
														end, 
														/*
															Both for PL and CL, LOB level coverages will follow this rule:
															For PL: Transactions for UNSchedule coverages are allocated at the policy' primary address. This is entered as the claimants primary address [per requirement]
															For CL: "True" Line level coverages will use the primary address. 
																Assumption that locations and building level coverages entered at Line level should not get this far - confirmed with Nathaniel/Todd
																What if the LOB Level coverage is at primary location which is different from the primary insured's address? Is this possible, and if so how will this come through? Per Leslie, SOP will prevent this
																What if primary insured address is not entered. Per BA, we will have SOP to prevent this
														*/
														primaryAddress.ID
														/* Use the Loss Location. This is not ideal, and may skew loss ratio. Noted in SOP */
														,ClaimFinancialTransactionLinePADirect.LossLocationID
													)
											end
										
	LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS claimState ON claimState.ID = claimAddress.State
	LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS claimCountry on claimCountry.ID = claimAddress.Country

	LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap 
		ON gw_gl_LobMap.gwLobCode = ClaimFinancialTransactionLinePADirect.LOBCode			
	LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap_Line 
		ON gw_gl_LobMap_Line.gwLobCode = ClaimFinancialTransactionLinePADirect.LineCode
	LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
		ON ClaimFinancialTransactionLinePADirect.peSegment = gw_gl_SegmentMap.gwSegment
	LEFT JOIN `{project}.{pe_dbo_dataset}.gw_cov_ASLMap` AS gw_cov_ASLMap 
		ON gw_cov_ASLMap.gwCov = ClaimFinancialTransactionLinePADirect.CoverageCode

	LEFT JOIN 
	(	SELECT	cccontact.Name, cccontact.PublicID, cccontact.FirstName, cccontact.LastName, cccontact.AddressBookUID, cccontact.ID   
				,ROW_NUMBER() OVER(PARTITION BY cccontact.AddressBookUID ORDER BY cccontact.ID DESC) rownumber  
		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) cccontact 
	) checkvendor  
	ON checkvendor.AddressBookUID = ClaimFinancialTransactionLinePADirect.VendorID   
	AND checkvendor.rownumber = 1  

	--properly resolve agency
	LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
		ON pc_producercode.CODE = ClaimFinancialTransactionLinePADirect.ProducerCode
		AND pc_producercode.RETIRED = 0                              --exclude archived producer records  

	LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS ccc  
		ON ccc.ID = ClaimFinancialTransactionLinePADirect.ClaimContactID

	LEFT JOIN 
	(	SELECT	cc_transaction_inner.ClaimContactID, cctl_contactrole.TYPECODE, cctl_contactrole.NAME
				,ROW_NUMBER() OVER ( PARTITION BY cc_transaction_inner.ClaimContactID ORDER BY cctl_contactrole.TYPECODE) RowNum
		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction_inner
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claimcontactrole` WHERE _PARTITIONTIME = {partition_date}) AS cc_claimcontactrole
			ON cc_transaction_inner.ClaimContactID = cc_claimcontactrole.ClaimContactID
		INNER JOIN `{project}.{cc_dataset}.cctl_contactrole` AS cctl_contactrole
			ON cc_claimcontactrole.Role = cctl_contactrole.ID
			AND cctl_contactrole.TYPECODE in ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
	) TrxnClaimContactRoleSubset
		ON ClaimFinancialTransactionLinePADirect.ClaimContactID = TrxnClaimContactRoleSubset.ClaimContactID
		AND TrxnClaimContactRoleSubset.RowNum = 1

	--Determine if the check vendor is a Jeweler or JM Jeweler
	LEFT OUTER JOIN
	(	SELECT	clmcc.ContactID  
				,ROW_NUMBER() OVER(PARTITION BY clmcc.ContactID ORDER BY contactrole.ID DESC) rownumber  
		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claimcontact` WHERE _PARTITIONTIME = {partition_date}) AS clmcc
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) clm
			ON clmcc.ClaimID = clm.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claimcontactrole` WHERE _PARTITIONTIME = {partition_date}) AS ccr
			ON ccr.ClaimContactID = clmcc.ID
		INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS con
			ON clmcc.ContactID = con.ID
		INNER JOIN `{project}.{cc_dataset}.cctl_contactrole` AS contactrole
			ON ccr.Role = contactrole.ID
		WHERE contactrole.TYPECODE = 'jeweler_JMIC'
	) ContactJeweler
	ON checkvendor.ID = ContactJeweler.ContactID
	AND ContactJeweler.rownumber = 1

	LEFT JOIN 
	(	SELECT	ctag.ContactID  
				,ROW_NUMBER() OVER(PARTITION BY ctag.ContactID ORDER BY cttype.ID DESC) rownumber  
		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contacttag` WHERE _PARTITIONTIME = {partition_date}) AS ctag  
		INNER JOIN `{project}.{cc_dataset}.cctl_contacttagtype` AS cttype 
			ON ctag.Type = cttype.ID
		WHERE cttype.TYPECODE = 'jeweler_jmic'  
		AND ctag.Retired = 0  
	) ContactIsJeweler
	ON checkvendor.ID = ContactIsJeweler.ContactID 
	AND ContactIsJeweler.rownumber = 1

	LEFT JOIN 
	(	SELECT	ct.ContactID  
				,ROW_NUMBER() OVER(PARTITION BY ct.ContactID ORDER BY ctt.ID DESC) rownumber  
		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contacttag` WHERE _PARTITIONTIME = {partition_date}) AS ct  
		INNER JOIN `{project}.{cc_dataset}.cctl_contacttagtype` AS ctt  
			ON ct.Type = ctt.ID  
		WHERE ctt.TYPECODE = 'jmjeweler_jmic'  
		AND ct.Retired = 0
	) ContactIsJMJeweler  
	ON checkvendor.ID = ContactIsJMJeweler.ContactID  
	AND ContactIsJMJeweler.rownumber = 1

	WHERE  1=1
	AND	IsTransactionSliceEffective = 1		--matches filter used in DWH
	--AND TransactionsSubmittedPrior = 0 --ignore updated transactions (only the original will have a 0) 
	
	) innerselect

	INNER JOIN ClaimPADirectFinancialsConfig sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN ClaimPADirectFinancialsConfig hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN ClaimPADirectFinancialsConfig hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) extractData