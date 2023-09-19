-- tag: ClaimIMCeded - tag ends/
/**** Kimberlite - Building Block - Financial Transactions ********
		ClaimIMCeded.sql
			BigQuery Converted
*******************************************************************/
/*
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****
 
	11/01/2022	KMATAM		Initial
	12/05/2022	DROBAK		Split for Kimberlite (raw) and Building Block (transformations) layers
	02/06/2023	DROBAK		Added Insert Line
	02/20/2023	DROBAK		Replace ClaimContactID with ContactKey; Add ClaimPrimaryContactID

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	ClaimTransactionKey -- use to join ClaimFinancialTransactionLineIMCeded or ClaimIMCeded with ClaimTransaction table
	cc_claim.PublicId						AS ClaimPublicID			- ClaimTransactionKey
	pc_policyPeriod.PublicID				AS PolicyPeriodPublicID		- PolicyTransactionKey
	cc_coverage.PC_CovPublicId_JMIC			AS CoveragePublicID			- IMCoverageKey
	pcx_ilmlocation_jmic.PublicID			AS IMLocationPublicID		- RiskLocationKey
	pcx_jewelrystock_jmic.PublicID			AS IMStockPublicID			- RiskStockKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DWH Source *****
-----------------------------------------------------------------------------------------------------------------------------------
 	sp_helptext 'bi_stage.spSTG_FactClaim_Extract_GW'
	sp_helptext 'cc.s_trxn_denorm_batch_CEDED'
	
-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{project}.{core_dataset}.dar_ClaimIMCeded`
AS SELECT extractData.*
FROM (
		---with ClaimIMCededFinancialsConfig
		---etc code
) extractData
*/  
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultCLPESegment STRING;
  SET vdefaultCLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultCommercialLineSegment = true ORDER BY peSegment LIMIT 1);
  
CREATE OR REPLACE TEMPORARY TABLE temp_MAXAgreementNumbers
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

CREATE TEMP TABLE ClaimIMDirectFinancialsConfig
AS SELECT *
FROM (
	  SELECT 'BusinessType' AS Key, 'Ceded' AS Value UNION ALL
	  SELECT 'SourceSystem','GW' UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
	  SELECT 'ClaimLineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
	  SELECT 'PCLineCode', 'ILMLine'
	);
/*
WITH ClaimIMDirectFinancialsConfig AS 
(
  SELECT 'BusinessType' AS Key, 'Ceded' AS Value UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','GLLine' UNION ALL        --JPALine --GLLine  --3rdPartyLine
  SELECT 'PCLineCode', 'ILMLine' UNION ALL
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
  SELECT 'LineLevelRisk','IMLine' UNION ALL
  SELECT 'LocationLevelRisk','IMLocation' UNION ALL
  SELECT 'StockLevelRisk','IMStock'
)
*/
INSERT INTO `{project}.{dest_dataset}.ClaimIMCeded`(
		SourceSystem
		,FinancialTransactionKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,ContactKey						--new field
		,BusinessType
		,TransactionPublicID
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
		,PropertyState
		,LossState
		,PolicyPeriodState		
		,LossAddressPublicID
		,PropertyAddressPublicID
		,RiskUnitPublicID
		,RiskUnitAddress
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
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPrimaryContactID,hashKeySeparator.Value,'CC'))	AS ContactKey
		,BusinessType
		,TransactionPublicID
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
		,IsErodingReserves  --assume its always bringing down ceded reserves  
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
		,PropertyState
		,LossState
		,PolicyPeriodState		
		,LossAddressPublicID
		,PropertyAddressPublicID
		,RiskUnitPublicID
		,RiskUnitAddress
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
		,IMCoverageKey
		,RiskLocationKey
		,RiskStockKey
		,primaryContact.PublicID																AS ClaimPrimaryContactID
		,BusinessType
		,TransactionPublicID
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
		--,LineCategory
		--,PaymentType
		,1																						AS IsErodingReserves  --assume its always bringing down ceded reserves  
		,LossDate
		,TransactionDate
		,AccountingDate
		,TransactionStatusCode
		,TransactionAmount
	--RESERVES
		--Indemnity Reserves only
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'claimcost' 
					AND ClaimFinancialTransactionLineIMCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveLoss
		
		--rename to: ClaimReserveALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineIMCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveDCCExpense
		
		--rename to: ClaimReserveALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineIMCeded.CostCategory = 'aoexpense_jmic'
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveAOExpense
		
	--PAID
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'claimcost' 
					And ClaimFinancialTransactionLineIMCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimPaidLossExcludingRecovery

		--rename to: ClaimPaidALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineIMCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimPaidDCCExpense

		--rename to: ClaimPaidALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLineIMCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineIMCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineIMCeded.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLineIMCeded.TransactionAmount
				ELSE 0 
			END																					AS ClaimPaidAOExpense

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
				  CASE WHEN ClaimFinancialTransactionLineIMCeded.IsClaimForLegacyPolicy = TRUE 
				  AND COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode) = 'GL'
					THEN vdefaultCLPESegment
				  END,
				  NULL)																		AS FinSegment		

		,cctlstate_prop.TYPECODE															AS PropertyState
		,cctl_state_loss.TYPECODE															AS LossState
		,pctl_state.TYPECODE																AS PolicyPeriodState		
		,cc_address_loss.publicid															AS LossAddressPublicID
		,ccaddress_prop.publicid															AS PropertyAddressPublicID
		
		--ccruaddress
		,ccruaddress.publicid																AS RiskUnitPublicID
		,ccruaddress.addressLine1															AS RiskUnitAddress

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
		(SELECT * FROM `{project}.{core_dataset}.ClaimFinancialTransactionLineIMCeded` WHERE bq_load_date = DATE({partition_date})) AS ClaimFinancialTransactionLineIMCeded
		
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
			ON cc_riagreementgroup.ID = ClaimFinancialTransactionLineIMCeded.RIAgreementGroupID
		LEFT JOIN temp_MAXAgreementNumbers
			ON ClaimFinancialTransactionLineIMCeded.RIAgreementNumber = temp_MAXAgreementNumbers.AgreementNumber
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement 
			ON pc_reinsuranceagreement.PublicID = ClaimFinancialTransactionLineIMCeded.RIAgreementPublicID		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.ID = temp_MAXAgreementNumbers.pcRICoverageGroupID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
			ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
			ON riAgreementMap.gwRICode = ClaimFinancialTransactionLineIMCeded.RIAgreementType 
			AND riAgreementMap.gwSource = 'Agreement'
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
			ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
			AND riCoverageGroupMap.gwSource = 'CoverageGroup'

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
			ON pctl_state.ID = ClaimFinancialTransactionLineIMCeded.PolicyAddressStateId
		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country 
			ON pctl_country.ID = ClaimFinancialTransactionLineIMCeded.PolicyAddressCountryId

		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap 
			ON gw_gl_LobMap.gwLobCode = ClaimFinancialTransactionLineIMCeded.LOBCode			
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap_Line 
			ON gw_gl_LobMap_Line.gwLobCode = ClaimFinancialTransactionLineIMCeded.LineCode
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
			ON ClaimFinancialTransactionLineIMCeded.peSegment = gw_gl_SegmentMap.gwSegment
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_cov_ASLMap` AS gw_cov_ASLMap 
			ON gw_cov_ASLMap.gwCov = ClaimFinancialTransactionLineIMCeded.CoverageCode

		--Loss Contact Info
		--claim may have a loss location
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_loss 
			ON cc_address_loss.ID = ClaimFinancialTransactionLineIMCeded.LossLocationID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_loss ON cctl_state_loss.ID = cc_address_loss.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS lossCountry ON lossCountry.ID = cc_address_loss.Country

		-- All tables needed?
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS primaryContact 
			ON primaryContact.id = ClaimFinancialTransactionLineIMCeded.InsuredID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS primaryAddress 
			ON primaryAddress.id = primaryContact.PrimaryAddressID
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS insuredstate 
			ON insuredstate.id = primaryAddress.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS cc_policylocation 
		ON cc_policylocation.ID = ClaimFinancialTransactionLineIMCeded.IncidentPropertyID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccaddress_prop 
		ON ccaddress_prop.ID = cc_policylocation.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_prop 
		ON cctlstate_prop.ID = ccaddress_prop.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccruAddress 
		ON ccruAddress.ID = ClaimFinancialTransactionLineIMCeded.RiskLocationAddressID --risk unit address

      --Primary Location (the location with the smallest ID whose primary location flag is set)
		LEFT JOIN (
			SELECT minLoc.ID, PolicyID, AddressID, LocationNumber
				,ROW_NUMBER() OVER(PARTITION BY minLoc.PolicyID ORDER BY minLoc.LocationNumber) rownumber
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS minLoc
			WHERE minLoc.PrimaryLocation = TRUE
			) primaryLocationFlagged
				ON ClaimFinancialTransactionLineIMCeded.ccPolicyID = primaryLocationFlagged.PolicyID
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
				ON ClaimFinancialTransactionLineIMCeded.ccPolicyID = primaryLocationUnFlagged.PolicyID
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
					ClaimFinancialTransactionLineIMCeded.IsClaimForLegacyPolicy = FALSE
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
								WHEN ClaimFinancialTransactionLineIMCeded.RiskUnitTypeCode in ('LocationBasedRU','LocationMiscRU','BuildingRU','PropertyRU')
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

		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS claimState ON claimState.ID = claimAddress.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS claimCountry on claimCountry.ID = claimAddress.Country

		--properly resolve agency
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
			ON pc_producercode.CODE = ClaimFinancialTransactionLineIMCeded.ProducerCode
			AND pc_producercode.RETIRED = 0		--exclude archived producer records  

	WHERE 1=1 
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