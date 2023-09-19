-- tag: ClaimIMDirect - tag ends/
/**** Kimberlite - Building Block - Financial Transactions ********
		ClaimIMDirect.sql
			BigQuery Converted
*******************************************************************/
/*
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****
 
	11/01/2022	KMATAM		Initial
	11/18/2022	DROBAK		Split for Kimberlite (raw) and Building Block (transformations) layers
	02/06/2023	DROBAK		Added Insert Line
	02/20/2023	DROBAK		Add ContactKey; Add ClaimPrimaryContactID, Remove ClaimContactID

-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{project}.{core_dataset}.dar_ClaimIMDirect`
AS SELECT extractData.*
FROM (
		---with ClaimPJDirectFinancialsConfig
		---etc code
--) extractData
*/
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultCLPESegment STRING;
  SET vdefaultCLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultCommercialLineSegment = true ORDER BY peSegment LIMIT 1);

CREATE TEMP TABLE ClaimIMDirectFinancialsConfig
AS SELECT *
FROM (
  SELECT 'BusinessType' AS Key, 'Direct' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
  SELECT 'PCLineCode', 'ILMLine'
);

INSERT INTO `{project}.{dest_dataset}.ClaimIMDirect`(
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,ContactKey						--new field
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,IMLocationPublicID 
		,IMStockPublicID
		,CoverageLevel
		,CoverageTypeCode
		,CoverageCode
		,ClaimNumber
		,PolicyNumber
		,TermNumber
		,LocationNumber
		,AccountNumber
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
		,LossAddressPublicID
		,ItemAddressPublicID
		,RiskUnitPublicID
		,RiskUnitAddress
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicID
		,PolicyAddressPublicID
		,ClaimPrimaryContactID			--new field
		,bq_load_date
	)

SELECT
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPrimaryContactID,hashKeySeparator.Value,'CC'))	AS ContactKey
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,IMLocationPublicID 
		,IMStockPublicID
		,CoverageLevel
		,CoverageTypeCode
		,CoverageCode
		,ClaimNumber
		,PolicyNumber
		,TermNumber
		,LocationNumber
		,AccountNumber
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
		,LossAddressPublicID
		,ItemAddressPublicID
		,RiskUnitPublicID
		,RiskUnitAddress
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicID
		,PolicyAddressPublicID
		,ClaimPrimaryContactID
		,bq_load_date

FROM (

	SELECT 
		SourceSystem
		,FinancialTransactionKey
		,FinancialTransactionLineKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,primaryContact.PublicID																AS ClaimPrimaryContactID
		,BusinessType
		,TransactionPublicID
		,TransactionLinePublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		--,ccCoveragePublicID
		--,ClaimPolicyPublicId
		,IMLocationPublicID 
		,IMStockPublicID
		,CoverageLevel
		,gw_cov_ASLMap.gwCovRef																	AS CoverageTypeCode
		,CoverageCode
		,ClaimNumber
		--,IsTransactionSliceEffective
		,PolicyNumber
		,TermNumber
		,COALESCE(primaryLocationFlagged.LocationNumber,primaryLocationUnFlagged.LocationNumber) AS LocationNumber
		,AccountNumber
		--,ItemNumber
		--,ItemClassCode
		,TransactionType
		,ClaimTransactionType
		,CostType
		,CostCategory
		,LineCategory
		,PaymentType
		,CASE ClaimFinancialTransactionLineIMDirect.DoesNotErodeReserves WHEN false THEN true ELSE false END	AS IsErodingReserves
		,LossDate
		,TransactionDate
		,TransactionLineDate
		,AccountingDate
		,TransactionStatusCode
		,TransactionAmount

	--RESERVES
	--Indemnity Reserves only
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Reserve'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'unspecified_jmic'
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'unspecified_jmic'
				AND ClaimFinancialTransactionLineIMDirect.DoesNotErodeReserves = false
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveLoss

	--contra to ClaimReserveLoss
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'RecoveryReserve' 
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery' 
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveLossRecovery

	--rename to: ClaimReserveALAEDCC ?
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Reserve'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.DoesNotErodeReserves = false
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveDCCExpense

	--contra to ClaimReserveDCCExpense NC
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'RecoveryReserve'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveDCCExpenseRecovery

	--rename to: ClaimReserveALAEAO ?
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Reserve'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic'
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.DoesNotErodeReserves = false
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveAOExpense

	--contra to ClaimReserveAOExpense NC
	,CASE  WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'RecoveryReserve'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount * -1 
			ELSE 0 
		END																				AS ClaimReserveAOExpenseRecovery

		--PAID
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
				And ClaimFinancialTransactionLineIMDirect.CostCategory = 'unspecified_jmic'
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			ELSE 0 
		END																				AS ClaimPaidLossExcludingRecovery
	--rename to: ClaimPaidALAEDCC ?
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			ELSE 0 
		END																				AS ClaimPaidDCCExpense
	--rename to: ClaimPaidALAEAO ?
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Payment'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount
			ELSE 0 
		END																				AS ClaimPaidAOExpense
			
--RECOVERY
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'claimcost' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount
			ELSE 0 
		END																				AS ClaimLossRecovery	
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'dccexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
			ELSE 0 
		END																				AS ClaimRecoveryDCCExpense
	,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType = 'Recovery'
				AND ClaimFinancialTransactionLineIMDirect.CostType = 'expense_jmic'
				AND ClaimFinancialTransactionLineIMDirect.CostCategory = 'aoexpense_jmic' 
			THEN ClaimFinancialTransactionLineIMDirect.TransactionAmount 
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
		--,ClaimFinancialTransactionLineIMDirect.ClaimContactID
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
		,COALESCE(gw_gl_SegmentMap.peSegment, --coverage based
					--for legacy claims
				  CASE WHEN ClaimFinancialTransactionLineIMDirect.IsClaimForLegacyPolicy = TRUE 
				  AND COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode) = 'GL'
					THEN vdefaultCLPESegment
				  END,
				  NULL)																		AS FinSegment		
		--ClaimPaymentContact NK/FK/Attrib fields
		,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType IN('Payment', 'Recovery')
				THEN CASE WHEN TrxnClaimContactRoleSubset.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
					THEN CONCAT(COALESCE(CONCAT(ccc.FirstName, ' '),''), COALESCE(ccc.LastName,''))
					ELSE COALESCE(checkvendor.name, CONCAT(COALESCE(CONCAT(checkvendor.FirstName, ' '),''), COALESCE(checkVendor.LastName,'')))	--get directly from check
				END	ELSE NULL
			END																				AS ClaimPaymentContactFullName

		,CASE	WHEN ContactJeweler.ContactID IS NOT NULL THEN 1 
				WHEN ContactIsJeweler.ContactID IS NOT NULL THEN 1 ELSE 0 
			END																				AS ClaimPaymentContactIsJeweler
		,CASE WHEN ContactIsJMJeweler.ContactID IS NOT NULL THEN 1 ELSE 0 END				AS ClaimPaymentContactIsJMJeweler
		,CASE WHEN ClaimFinancialTransactionLineIMDirect.TransactionType IN('Payment', 'Recovery')
				THEN CASE WHEN TrxnClaimContactRoleSubset.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
						THEN ccc.PublicID  --ID whould have never worked.
						ELSE checkvendor.PublicID
				END 
				ELSE NULL 
			END																				AS ClaimPaymentContactPublicID
		,cctl_state.TYPECODE																AS ItemState
		,cctl_state_loss.TYPECODE															AS LossState
		,pctl_state.TYPECODE																AS PolicyPeriodState		
		,cc_address_loss.publicid															AS LossAddressPublicID
		,cc_address.publicid																AS ItemAddressPublicID
		--ccruaddress
		,ccruaddress.publicid																AS RiskUnitPublicID
		,ccruaddress.addressLine1															AS RiskUnitAddress
		--,ReserveLinePublicID
		,ExposurePublicId
		,IncidentPublicID
		,JewelerContactPublicID
		,PolicyAddressPublicID
		,DATE('{date}')	 AS bq_load_date
		--,CURRENT_DATE() AS bq_load_date
		
	FROM
		(SELECT * FROM `{project}.{core_dataset}.ClaimFinancialTransactionLineIMDirect` WHERE bq_load_date = DATE({partition_date})) AS ClaimFinancialTransactionLineIMDirect

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
			ON pctl_state.ID = ClaimFinancialTransactionLineIMDirect.PolicyAddressStateId
		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country 
			ON pctl_country.ID = ClaimFinancialTransactionLineIMDirect.PolicyAddressCountryId

		--if a item exists, it has to be located somewhere (someone)
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact 
			ON cc_contact.ID = ClaimFinancialTransactionLineIMDirect.ClaimContactID
		--if a contact was found, then there is a primary address
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
			ON cc_address.ID = cc_contact.PrimaryAddressID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state 
			ON cctl_state.ID = cc_address.State			

		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
			ON ClaimFinancialTransactionLineIMDirect.peSegment = gw_gl_SegmentMap.gwSegment
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_cov_ASLMap` AS gw_cov_ASLMap 
			ON gw_cov_ASLMap.gwCov = ClaimFinancialTransactionLineIMDirect.CoverageCode
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap 
			ON gw_gl_LobMap.gwLobCode = ClaimFinancialTransactionLineIMDirect.LOBCode     
		LEFT JOIN  `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap_Line 
			ON gw_gl_LobMap_Line.gwLobCode = ClaimFinancialTransactionLineIMDirect.LineCode

		--Loss Contact Info
		--claim may have a loss location
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_loss 
			ON cc_address_loss.ID = ClaimFinancialTransactionLineIMDirect.LossLocationID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_loss ON cctl_state_loss.ID = cc_address_loss.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS lossCountry ON lossCountry.ID = cc_address_loss.Country
		
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS primaryContact 
			ON primaryContact.id = ClaimFinancialTransactionLineIMDirect.InsuredID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS primaryAddress 
			ON primaryAddress.id = primaryContact.PrimaryAddressID
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS insuredstate 
			ON insuredstate.id = primaryAddress.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS cc_policylocation 
		ON cc_policylocation.ID = ClaimFinancialTransactionLineIMDirect.IncidentPropertyID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccaddress_prop 
		ON ccaddress_prop.ID = cc_policylocation.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_prop 
		ON cctlstate_prop.ID = ccaddress_prop.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccruAddress 
		ON ccruAddress.ID = ClaimFinancialTransactionLineIMDirect.RiskLocationAddressID --risk unit address

      --Primary Location (the location with the smallest ID whose primary location flag is set)
		LEFT JOIN (
			SELECT minLoc.ID, PolicyID, AddressID, LocationNumber
				,ROW_NUMBER() OVER(PARTITION BY minLoc.PolicyID ORDER BY minLoc.LocationNumber) rownumber
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS minLoc
			WHERE minLoc.PrimaryLocation = TRUE
			) primaryLocationFlagged
				ON ClaimFinancialTransactionLineIMDirect.ccPolicyID = primaryLocationFlagged.PolicyID
				AND primaryLocationFlagged.rownumber = 1
		LEFT JOIN(SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccprimaryFlaggedAddress 
			ON ccprimaryFlaggedAddress.ID = primaryLocationFlagged.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_primary_loc_flagged 
			ON cctlstate_primary_loc_flagged.ID = ccprimaryFlaggedAddress.State


	-- Primary Location (the location with the smallest ID whose primary location flag is NOT set)
		LEFT JOIN (
			SELECT minLocUn.ID, PolicyID, AddressID, LocationNumber
				,ROW_NUMBER() OVER(PARTITION BY minLocUn.PolicyID ORDER BY minLocUn.LocationNumber) rownumber
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS minLocUn
			) primaryLocationUnFlagged
				ON ClaimFinancialTransactionLineIMDirect.ccPolicyID = primaryLocationUnFlagged.PolicyID
				AND primaryLocationUnFlagged.rownumber = 1
		
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccprimaryUnFlaggedAddress 
			ON ccprimaryUnFlaggedAddress.ID = primaryLocationUnFlagged.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_primary_loc_UnFlagged 
			ON cctlstate_primary_loc_UnFlagged.ID = ccprimaryUnFlaggedAddress.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS claimAddress 
			ON claimAddress.ID = 
			--ccaddress_claim
				CASE 
					WHEN 
					ClaimFinancialTransactionLineIMDirect.IsClaimForLegacyPolicy = FALSE
					THEN 
					--GW claims: This includes verified and unverified policies [future consideration]
					coalesce
					(
						primaryAddress.ID, -- PL items use located with/at someone/where. 
						CASE
						WHEN coalesce(gw_gl_LobMap.glLineCode,gw_gl_LobMap_Line.glLineCode) = 'CL' THEN cc_address_loss.ID
						ELSE NULL --Loss State ID for CL only
						END,
						ccprimaryFlaggedAddress.ID, -- primary location flagged exclusively for LOB coverages
						ccprimaryUnFlaggedAddress.ID -- min location (from a list of all locations) exclusively for LOB coverages 
					)
					ELSE
					--Legacy claims: This includes manually verified policies
					coalesce
					(
						-- PL Items are located with/at someone/where
						CASE 
						WHEN CoverageCode = 'JewelryItemCov_JMIC_PL' 
						THEN primaryAddress.ID 
						ELSE null 
						END, 
                                        
						--Note, min or primary locations for unverified legacy policy should be bypassed
						CASE 
						WHEN coalesce(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode) = 'GL' -- coming in as General Liability
						THEN 
							coalesce
							(           
							--  For CL, GWCC seems to already take care of the various levels in the RU (since there is no policy) 
							--  except Line => Building, Location, Stock, SubLocation, SubStock etc
                            
							ccruaddress.ID,
                            
							--  An exception to the above rule is for coverages
							--  that are entered at location or building level. Note these could be at policy line/LOB level
							--  but "tricked/faked" into a location/building level
                            
							CASE 
								WHEN ClaimFinancialTransactionLineIMDirect.RiskUnitTypeCode in ('LocationBasedRU','LocationMiscRU','BuildingRU','PropertyRU')
								THEN ccaddress_prop.ID 
								ELSE 
								null
							END,
                            
							--  Holds true for exposures for line level coverages that are entered at 
							--  Stock, SubLoc, Substock levels
							--  Per GW, building and locations for unverified claims are not marked primary. 
                            
							ccprimaryFlaggedAddress.ID
							)
						ELSE 
							null 
						END, 
                      
						--  Both for PL and CL, LOB level coverages will follow this rule:
						--  For PL: Transactions for UNSchedule coverages are allocated at the policy' primary address. This is entered as the claimants primary address [per requirement]
						--  For CL: "True" Line level coverages will use the primary address. 
						--    Assumption that locations and building level coverages entered at Line level should not get this far - confirmed with Nathaniel/Todd
						--    What if the LOB Level coverage is at primary location which is different from the primary insured's address? Is this possible, and if so how will this come through? Per Leslie, SOP will prevent this
						--    What if primary insured address is not entered. Per BA, we will have SOP to prevent this
                      
						primaryAddress.ID
						--Use the Loss Location. This is not ideal, and may skew loss ratio. Noted in SOP
						,cc_address_loss.ID

					)
				END
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS claimState 
		ON claimState.ID = claimAddress.State 
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS claimCountry 
		ON claimCountry.ID = claimAddress.Country    

		LEFT JOIN 
		(	SELECT	cccontact.Name, cccontact.PublicID, cccontact.FirstName, cccontact.LastName, cccontact.AddressBookUID, cccontact.ID   
					,ROW_NUMBER() OVER(PARTITION BY cccontact.AddressBookUID ORDER BY cccontact.ID DESC) rownumber  
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) cccontact 
		) checkvendor  
		ON checkvendor.AddressBookUID = ClaimFinancialTransactionLineIMDirect.VendorID   
		AND checkvendor.rownumber = 1

		--properly resolve agency
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
			ON pc_producercode.CODE = ClaimFinancialTransactionLineIMDirect.ProducerCode
			AND pc_producercode.RETIRED = 0                              --exclude archived producer records  

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS ccc  
			ON ccc.ID = ClaimFinancialTransactionLineIMDirect.ClaimContactID

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
			ON ClaimFinancialTransactionLineIMDirect.ClaimContactID = TrxnClaimContactRoleSubset.ClaimContactID
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

	INNER JOIN ClaimIMDirectFinancialsConfig sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN ClaimIMDirectFinancialsConfig hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN ClaimIMDirectFinancialsConfig hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) extractData