-- tag: ClaimBOPCeded - tag ends/
/**** Kimberlite - Building Block - Financial Transactions ********
		ClaimBOPCeded.sql
			BigQuery Converted
*******************************************************************/
/*
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****
 
	01/06/2023	DROBAK		Initial
	02/06/2023	DROBAK		Added Insert Line
	02/20/2023	DROBAK		Add ContactKey; Add ClaimPrimaryContactID

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	ClaimTransactionKey -- use to join ClaimFinancialTransactionLineBOPCeded with ClaimTransaction table
	cc_claim.PublicId						AS ClaimPublicID			- ClaimTransactionKey
	pc_policyPeriod.PublicID				AS PolicyPeriodPublicID		- PolicyTransactionKey
	cc_coverage.PC_CovPublicId_JMIC			AS CoveragePublicID			- IMCoverageKey
	pc_boplocation.PublicID					AS BOPLocationPublicID		- RiskLocationKey
	pc_bopbuilding.PublicID					AS BuildingPublicID			- RiskBuildingKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DWH Source *****
-----------------------------------------------------------------------------------------------------------------------------------
 	sp_helptext 'bi_stage.spSTG_FactClaim_Extract_GW'
	sp_helptext 'cc.s_trxn_denorm_batch_CEDED'
	
-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.dar_ClaimBOPCeded`
AS SELECT extractData.*
FROM (
		---with ClaimBOPCededFinancialsConfig
		---etc code
) extractData
*/  
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultCLPESegment STRING;
  SET vdefaultCLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultCommercialLineSegment = true ORDER BY peSegment LIMIT 1);

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

CREATE TEMP TABLE ClaimBOPCededFinancialsConfig
AS SELECT *
FROM (
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
	);

--CREATE OR REPLACE TABLE `{project}.{dest_dataset}.dar_ClaimBOPCeded`
--AS SELECT extractData.*
--FROM (

INSERT INTO `{project}.{dest_dataset}.ClaimBOPCeded`(
		SourceSystem
		,FinancialTransactionKey
		,ClaimTransactionKey
		,PolicyTransactionKey
		,BOPCoverageKey
		,RiskLocationKey
		,RiskBuildingKey
		,ContactKey				--new field
		,BusinessType
		,TransactionPublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,BOPLocationPublicID 
		,BuildingPublicID
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
		,BOPCoverageKey
		,RiskLocationKey
		,RiskBuildingKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPrimaryContactID,hashKeySeparator.Value,'CC'))	AS ContactKey
		,BusinessType
		,TransactionPublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		,BOPLocationPublicID 
		,BuildingPublicID
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
		,BOPCoverageKey
		,RiskLocationKey
		,RiskBuildingKey
		,primaryContact.PublicID								AS ClaimPrimaryContactID
	
		,BusinessType
		,TransactionPublicID
		,ClaimPublicId
		,PolicyPeriodPublicID
		,CoveragePublicID
		--,ccCoveragePublicID
		--,ClaimPolicyPublicId
		,BOPLocationPublicID 
		,BuildingPublicID
		,CoverageLevel
		,gw_cov_ASLMap.gwCovRef                               AS CoverageTypeCode
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
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'claimcost' 
					AND ClaimFinancialTransactionLineBOPCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveLoss
		
		--rename to: ClaimReserveALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineBOPCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveDCCExpense
		
		--rename to: ClaimReserveALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RICededReserve'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineBOPCeded.CostCategory = 'aoexpense_jmic'
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimReserveAOExpense
		
	--PAID
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'claimcost' 
					And ClaimFinancialTransactionLineBOPCeded.CostCategory = 'unspecified_jmic'
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimPaidLossExcludingRecovery

		--rename to: ClaimPaidALAEDCC ?
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineBOPCeded.CostCategory = 'dccexpense_jmic' 
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount 
				ELSE 0 
			END																					AS ClaimPaidDCCExpense

		--rename to: ClaimPaidALAEAO ?
		,CASE WHEN ClaimFinancialTransactionLineBOPCeded.TransactionType = 'RIRecoverable'
					AND ClaimFinancialTransactionLineBOPCeded.CostType = 'expense_jmic'
					AND ClaimFinancialTransactionLineBOPCeded.CostCategory = 'aoexpense_jmic' 
				THEN ClaimFinancialTransactionLineBOPCeded.TransactionAmount
				ELSE 0 
			END																					AS ClaimPaidAOExpense

		,COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode)						AS PolicyLineCode
		,LineCode
		,COALESCE(LOBCode, LineCode)														AS glLineCode
		--,LOBCode
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
				  CASE WHEN ClaimFinancialTransactionLineBOPCeded.IsClaimForLegacyPolicy = TRUE 
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
		(SELECT * FROM `{project}.{core_dataset}.ClaimFinancialTransactionLineBOPCeded` WHERE bq_load_date = DATE({partition_date})) AS ClaimFinancialTransactionLineBOPCeded	
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
			ON cc_riagreementgroup.ID = ClaimFinancialTransactionLineBOPCeded.RIAgreementGroupID
		LEFT JOIN temp_MAXAgreementNumbers
			ON ClaimFinancialTransactionLineBOPCeded.RIAgreementNumber = temp_MAXAgreementNumbers.AgreementNumber
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement 
			ON pc_reinsuranceagreement.PublicID = ClaimFinancialTransactionLineBOPCeded.RIAgreementPublicID		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.ID = temp_MAXAgreementNumbers.pcRICoverageGroupID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
			ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
			ON riAgreementMap.gwRICode = ClaimFinancialTransactionLineBOPCeded.RIAgreementType 
			AND riAgreementMap.gwSource = 'Agreement'
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
			ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
			AND riCoverageGroupMap.gwSource = 'CoverageGroup'

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
			ON pctl_state.ID = ClaimFinancialTransactionLineBOPCeded.PolicyAddressStateId
		LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country 
			ON pctl_country.ID = ClaimFinancialTransactionLineBOPCeded.PolicyAddressCountryId

		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap 
			ON gw_gl_LobMap.gwLobCode = ClaimFinancialTransactionLineBOPCeded.LOBCode			
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_gl_LobMap` AS gw_gl_LobMap_Line 
			ON gw_gl_LobMap_Line.gwLobCode = ClaimFinancialTransactionLineBOPCeded.LineCode
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
			ON ClaimFinancialTransactionLineBOPCeded.peSegment = gw_gl_SegmentMap.gwSegment
		LEFT JOIN `{project}.{pe_dbo_dataset}.gw_cov_ASLMap` AS gw_cov_ASLMap 
			ON gw_cov_ASLMap.gwCov = ClaimFinancialTransactionLineBOPCeded.CoverageCode

		--Loss Contact Info
		--claim may have a loss location
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_loss 
			ON cc_address_loss.ID = ClaimFinancialTransactionLineBOPCeded.LossLocationID 
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_loss ON cctl_state_loss.ID = cc_address_loss.State
		LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS lossCountry ON lossCountry.ID = cc_address_loss.Country

		-- All tables needed?
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS primaryContact 
			ON primaryContact.id = ClaimFinancialTransactionLineBOPCeded.InsuredID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS primaryAddress 
			ON primaryAddress.id = primaryContact.PrimaryAddressID
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS insuredstate 
			ON insuredstate.id = primaryAddress.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS cc_policylocation 
		ON cc_policylocation.ID = ClaimFinancialTransactionLineBOPCeded.IncidentPropertyID
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccaddress_prop 
		ON ccaddress_prop.ID = cc_policylocation.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_prop 
		ON cctlstate_prop.ID = ccaddress_prop.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccruAddress 
		ON ccruAddress.ID = ClaimFinancialTransactionLineBOPCeded.RiskLocationAddressID --risk unit address

      --Primary Location (the location with the smallest ID whose primary location flag is set)
		LEFT JOIN (
			SELECT minLoc.ID, PolicyID, AddressID, LocationNumber
				,ROW_NUMBER() OVER(PARTITION BY minLoc.PolicyID ORDER BY minLoc.LocationNumber) rownumber
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS minLoc
			WHERE minLoc.PrimaryLocation = TRUE
			) primaryLocationFlagged
				ON ClaimFinancialTransactionLineBOPCeded.ccPolicyID = primaryLocationFlagged.PolicyID
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
				ON ClaimFinancialTransactionLineBOPCeded.ccPolicyID = primaryLocationUnFlagged.PolicyID
				AND primaryLocationUnFlagged.rownumber = 1
		
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS ccprimaryUnFlaggedAddress 
			ON ccprimaryUnFlaggedAddress.ID = primaryLocationUnFlagged.AddressID --get the property's address
		LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctlstate_primary_loc_UnFlagged 
			ON cctlstate_primary_loc_UnFlagged.ID = ccprimaryUnFlaggedAddress.State

		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS claimAddress 
			ON claimAddress.ID = 
			--ccaddress_claim
				CASE 
					WHEN ClaimFinancialTransactionLineBOPCeded.IsClaimForLegacyPolicy = FALSE
					THEN 
					--GW claims: This includes verified and unverified policies [future consideration]
					COALESCE
					(
						primaryAddress.ID, -- PL items use located with/at someone/where. 
						CASE
						WHEN COALESCE(gw_gl_LobMap.glLineCode,gw_gl_LobMap_Line.glLineCode) = 'CL' THEN cc_address_loss.ID
						ELSE NULL --Loss State ID for CL only
						END,
						ccprimaryFlaggedAddress.ID, -- primary location flagged exclusively for LOB coverages
						ccprimaryUnFlaggedAddress.ID -- min location (from a list of all locations) exclusively for LOB coverages 
					)
					ELSE
					--Legacy claims: This includes manually verified policies
					COALESCE
					(	--Note, min or primary locations for unverified legacy policy should be bypassed
						CASE 
						WHEN COALESCE(gw_gl_LobMap.peLobCode,gw_gl_LobMap_Line.peLobCode) = 'GL' -- coming in as General Liability
						THEN 
							COALESCE
							(           
							--  For CL, GWCC seems to already take care of the various levels in the RU (since there is no policy) 
							--  except Line => Building, Location, Stock, SubLocation, SubStock etc
                            
							ccruaddress.ID,
                            
							--  An exception to the above rule is for coverages
							--  that are entered at location or building level. Note these could be at policy line/LOB level
							--  but "tricked/faked" into a location/building level
                            
							CASE 
								WHEN ClaimFinancialTransactionLineBOPCeded.RiskUnitTypeCode in ('LocationBasedRU','LocationMiscRU','BuildingRU','PropertyRU')
								THEN ccaddress_prop.ID 
								ELSE NULL
							END,
                            
							--  Holds true for exposures for line level coverages that are entered at 
							--  Stock, SubLoc, Substock levels
							--  Per GW, building and locations for unverified claims are not marked primary. 
                            
							ccprimaryFlaggedAddress.ID
							)
						ELSE 
							NULL 
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
			ON pc_producercode.CODE = ClaimFinancialTransactionLineBOPCeded.ProducerCode
			AND pc_producercode.RETIRED = 0		--exclude archived producer records  

	WHERE 1=1 
	AND	IsTransactionSliceEffective = 1		--matches filter used in DWH
	--AND TransactionsSubmittedPrior = 0 --ignore updated transactions (only the original will have a 0)

	) innerselect

	INNER JOIN ClaimBOPCededFinancialsConfig sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN ClaimBOPCededFinancialsConfig hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN ClaimBOPCededFinancialsConfig hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) extractData