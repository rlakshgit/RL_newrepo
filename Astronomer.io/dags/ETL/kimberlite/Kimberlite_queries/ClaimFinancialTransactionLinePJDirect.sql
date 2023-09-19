-- tag: ClaimFinancialTransactionLinePJDirect - tag ends/
/**** Kimberlite - Financial Transactions ********
		ClaimFinancialTransactionLinePJDirect.sql
			BigQuery Converted
**************************************************/
/*   
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	09/08/2022	DROBAK		Initial
	01/23/2023	DROBAK		Use LineCode from Config table in Select stmnt

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	ClaimTransactionKey -- use to join ClaimFinancialTransactionLinePJDirect with ClaimTransaction table
	cc_claim.PublicId						AS ClaimPublicID			- ClaimTransactionKey
	pc_policyPeriod.PublicID				AS PolicyPeriodPublicID		- PolicyTransactionKey
	cc_coverage.PC_CovPublicId_JMIC			AS CoveragePublicID			- ItemCoverageKey
	pcx_jewelryitem_jmic_pl.PublicID		AS ItemPublicID				- RiskJewelryItemKey

-----------------------------------------------------------------------------------------------------------------------------------
 ***** Original DW Source *****
-----------------------------------------------------------------------------------------------------------------------------------
 	sp_helptext '[bi_stage].[spSTG_FactClaim_Extract_GW]'
	sp_helptext 'cc.s_trxn_denorm_batch_DIRECT'
	
-----------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLinePJDirect`
AS SELECT overallselect.*
FROM (
		---with ClaimPJDirectFinancialsConfig
		---etc code
) overallselect
*/	
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultPLPESegment STRING;
	--prod-edl.ref_pe_dbo.gw_gl_SegmentMap
	SET vdefaultPLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultPersonalLineSegment = true ORDER BY peSegment LIMIT 1);

INSERT INTO `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect`
(
	SourceSystem
	,FinancialTransactionKey
	,FinancialTransactionLineKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,BusinessType
	,TransactionPublicID
	,TransactionLinePublicID
	,ClaimPublicId
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,ClaimPolicyPublicID
	,ItemPublicID
	,ccItemPublicID
	,CoverageLevel
	,CoverageCode
	,IsTransactionSliceEffective
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
	,LineCategory
	,PaymentType
	,DoesNotErodeReserves
	,LossDate
	,TransactionDate
	,TransactionLineDate
	,TransactionAmount
	,LineCode
	,LOBCode
	,IsAverageReserveSource
	,TransactionSetID
	,AccountingDate
	,TransactionStatusCode
	,TransactionsSubmittedPrior
	,ClaimRecoveryType
	,TransactionOrigin
	,TransactionChangeType
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,ClaimContactID
 	,UWCompanyPublicID
	,ReserveLineCategoryCode
	,ReserveLinePublicID
	,ExposurePublicID
	,IncidentPublicID
	,ProducerPublicID
	,ProducerCode
	,JewelerContactPublicID
	,PolicyAddressPublicID
	,PolicyAddressStateID
	,PolicyAddressCountryID
	,PolicyAddressPostalCode
	,InsuredID
	,LocatedWith
	,LossLocationID
	,VendorID
	,DefaultSegment
	,peSegment
	,bq_load_date
)

WITH ClaimPJDirectFinancialsConfig AS 
	(
	  SELECT 'BusinessType' as Key, 'Direct' as Value UNION ALL
	  SELECT 'SourceSystem','GW' UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
	  SELECT 'LineCode','JMICPJLine' UNION ALL				--JPALine --GLLine	--3rdPartyLine
	  SELECT 'UWCompany', 'uwc:10' UNION ALL				--zgojeqek81h0c3isqtper9n5kb9 = JMSI
	  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
	  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
	  SELECT 'NoCoverage','NoCoverage' UNION ALL
	  SELECT 'CostCoverage','CostCoverage' UNION ALL
	  SELECT 'ScheduledItemRisk','PersonalJewelryItem'		--PersonalArticleJewelry
	)

SELECT
	SourceSystem
	,FinancialTransactionKey
	,FinancialTransactionLineKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,BusinessType
	,TransactionPublicID
	,TransactionLinePublicID
	,ClaimPublicId
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,ClaimPolicyPublicID
	,ItemPublicID
	,ccItemPublicID
	,CoverageLevel
	,CoverageCode
	
	,IsTransactionSliceEffective
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
	,LineCategory
	,PaymentType
	,DoesNotErodeReserves
	,LossDate
	,TransactionDate
	,TransactionLineDate
	,TransactionAmount
	,LineCode
	,LOBCode

	,IsAverageReserveSource
	,TransactionSetID
	,AccountingDate
	,TransactionStatusCode
	,TransactionsSubmittedPrior

	,ClaimRecoveryType
	,TransactionOrigin
	,TransactionChangeType
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,ClaimContactID
 	,UWCompanyPublicID
	,ReserveLineCategoryCode
	,ReserveLinePublicID
	,ExposurePublicID
	,IncidentPublicID
	,ProducerPublicID
	,ProducerCode

	,JewelerContactPublicID
	,PolicyAddressPublicID
	,PolicyAddressStateID
	,PolicyAddressCountryID
	,PolicyAddressPostalCode
	,InsuredID
	,LocatedWith
	,LossLocationID
	,VendorID
	,DefaultSegment
	,peSegment
	,bq_load_date
	
FROM (

	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode))		AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionLinePublicID,businessTypeConfig.Value,LineCode))	AS FinancialTransactionLineKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPublicID))												AS ClaimTransactionKey
		,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
			END																													AS PolicyTransactionKey
		,CASE WHEN CoveragePublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel))
			END																													AS ItemCoverageKey
		,CASE WHEN ItemPublicId IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ItemPublicID,hashKeySeparator.Value,itemRisk.Value)) 
			END																													AS RiskJewelryItemKey
		,businessTypeConfig.Value																								AS BusinessType
		,FinTrans.*	
		,DATE('{date}')																											AS bq_load_date	--CURRENT_DATE()

	FROM (

		SELECT
			cc_transaction.PublicID																AS TransactionPublicID
			,cc_transactionlineitem.PublicID													AS TransactionLinePublicID
			,cc_claim.PublicId																	AS ClaimPublicID
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,pcx_jewelryitem_jmic_pl.PublicID													AS ItemPublicID
			,ccx_jewelryitem_jmic.PublicID														AS ccItemPublicID
			,CASE WHEN cctl_coveragesubtype.TYPECODE = 'UnscheduledCovType_JMIC_PL' THEN 'UnScheduledCov'
                    WHEN cctl_coveragesubtype.TYPECODE = 'JewelryItemCovType_JMIC_PL' THEN 'ScheduledCov'
                END																				AS CoverageLevel
			,cctl_coveragetype.TypeCode															AS CoverageCode
			--,ccexposureTrxn.CoverageSubType														AS CoverageSubTypeCode		--Needed for CL?
			--per requirements that claim of record should follow the legacy number if available
			--But in Kimberlite raw layer we need to show both values; can coalesce in Building Block or Gold
			--,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.ClaimNumber																AS ClaimNumber
			,CASE	WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
						NOT IN ('Credit Card Payment Pending Notification')
					AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
						,'pendingstop','pendingtransfer','pendingvoid','submitted',
						'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
						NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective
			,COALESCE(pc_policyperiod.PolicyNumber,cc_policy.policynumber)						AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber
			,cc_policy.AccountNumber															AS AccountNumber
			--,ccx_jewelryitem_jmic.ItemNumber													AS ItemNumber
			,pcx_jewelryitem_jmic_pl.ItemNumber													AS ItemNumber
			,pctl_classcodetype_jmic_pl.TYPECODE												AS ItemClassCode
			,cctl_transaction.TYPECODE															AS TransactionType
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType
			,cctl_costcategory.TYPECODE															AS CostCategory
			,cctl_linecategory.TypeCode															AS LineCategory
			,cctl_paymenttype.TYPECODE															AS PaymentType
			,cc_transaction.DoesNotErodeReserves												AS DoesNotErodeReserves
			,cc_claim.LossDate																	AS LossDate
			,cc_transaction.CreateTime															AS TransactionDate
			,cc_transactionlineitem.CreateTime													AS TransactionLineDate
			,cc_transactionlineitem.TransactionAmount											AS TransactionAmount
			,lineConfig.Value																	AS LineCode
			--,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode
			,cctl_lobcode.TYPECODE																AS LOBCode
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			,cc_transaction.TransactionSetID													AS TransactionSetID
			--,cc_transaction.UpdateTime															AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_transaction.CreateTime ELSE cc_transaction.UpdateTime END				AS AccountingDate
			--,cc_transaction.Status																AS TransactionStatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			/*--On a full load this will produce different results than DW as it has full history to query
			--On a Delta basis it could work going forward, but does it have value and would we need one separate code base for historical loads?
			,(	--select COUNT(TransactionPublicID) 
				select count(ID)
				from GW_Reporting.cc.ccrt_trxn_summ summ where	--`prod-edl.ref_kimberlite.ClaimFinancialTransactionPJDirect`
				summ.TransactionPublicID=cc_transaction.PublicID and
				summ.TransactionLineItemPublicID=cc_transactionlineitem.PublicID and 
				summ.TransactionStatusCode in ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided') and --Was previously in a valid status
				summ.IsCeded = 0
			)																					AS TransactionsSubmittedPrior
			*/
			--This would indicate a TransactionsSubmittedPrior event occurred
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior
				
			--,pc_effectivedatedfields.OfferingCode AS OfferingCode								--Not applicable to PL
														  
			,cctl_recoverycategory.NAME															AS ClaimRecoveryType
			 --this is for the "loss location" NOT "policy location"
															
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

			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_transaction.ClaimContactID														AS ClaimContactID
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
			,cctl_reservelinecategory_jmic.TYPECODE												AS ReserveLineCategoryCode
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			--,cc_transaction.ReserveLineID														AS ReserveLineID
			--,cc_transaction.ExposureID														AS ExposureID
			,cc_exposure.PublicID																AS ExposurePublicID
			--,cc_exposure.IncidentID																AS IncidentID
			,cc_incident.PublicID																AS IncidentPublicID
			,pc_producercode.PublicID															AS ProducerPublicID
			,cc_policy.PRODUCERCODE																AS ProducerCode		
			--,AS LossDateAgencyEvolvedKey	--Need pc_producercode.PublicID and cc_claim.LossDate to lookup DimAgency eveolved table in Kimberlite, when available
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode
			,cc_claim.InsuredDenormID															AS InsuredID
			,ccx_jewelryitem_jmic.LocatedWith													AS LocatedWith
			,cc_claim.LossLocationID															AS LossLocationID
			,cc_check.VendorID_JMIC																AS VendorID
			,vdefaultPLPESegment																AS DefaultSegment
			,pctl_segment.TYPECODE																AS peSegment

	 	FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_transaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionlineitem` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionlineitem 
				ON cc_transactionlineitem.TransactionID = cc_transaction.ID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_transaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_transaction.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
				--AND cctl_lobcode.TYPECODE='JMICPJLine'
			INNER JOIN ClaimPJDirectFinancialsConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.PublicID = cc_policy.PC_PeriodPublicId_JMIC	
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN ClaimPJDirectFinancialsConfig uwCompany 
				ON uwCompany.Key = 'UWCompany' 
				--AND uwCompany.Value=pc_uwcompany.PublicID
				--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
				AND COALESCE(pc_uwcompany.PublicID,uwCompany.Value) = uwCompany.Value

			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS ccexposureTrxn 
				ON ccexposureTrxn.ID = cc_transaction.ExposureID 
		
			LEFT JOIN
			(
				/*exposures for this claim that are not associated with a claim transaction*/
				SELECT exposure.ID, exposure.ClaimId, ROW_NUMBER() OVER (PARTITION BY exposure.ClaimID ORDER BY exposure.CloseDate desc) rowNumber 
				FROM (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS exposure
					LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transaction` WHERE _PARTITIONTIME = {partition_date}) AS trxn 
					on trxn.ExposureID = exposure.ID
				WHERE trxn.ID is null
			) ccexposureClaim on ccexposureClaim.ClaimID = cc_claim.ID and rowNumber = 1

			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = COALESCE(ccexposureTrxn.ID, ccexposureClaim.ID)
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 

			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_transaction.Status

			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID
				
		    -------------  
			-- COVERAGES  
			-- Attempt to link to Policy Center coverage based on Claim Center [PC_CovPublicId_JMIC] and [PolicySystemId]  
			-------------  
				
			--Personal Line Coverage (applied to PolicyLine) - unscheduled
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jmpersonallinecov` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmpersonallinecov
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_jmpersonallinecov.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId, ':') - 1) = 'entity.JewelryLineCov_JMIC_PL'

	  		-- Jewelry Item Coverage (applied to Jewelry Item) -- Scheduled Coverage (Item)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jwryitemcov_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jwryitemcov_jmic_pl 
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_jwryitemcov_jmic_pl.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId, ':') - 1) = 'entity.JewelryItemCov_JMIC_PL'

			 ------------------  
			 -- RISK SEGMENTS  
			 ------------------  
  
			-- Jewelry Item  
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelryitem_jmic_pl
				ON pcx_jwryitemcov_jmic_pl.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID  
				AND pcx_jwryitemcov_jmic_pl.BranchID = pcx_jewelryitem_jmic_pl.BranchID  
				AND (pcx_jewelryitem_jmic_pl.EffectiveDate <= cc_claim.LossDate OR pcx_jewelryitem_jmic_pl.EffectiveDate IS NULL)  
				AND (pcx_jewelryitem_jmic_pl.ExpirationDate > cc_claim.LossDate OR pcx_jewelryitem_jmic_pl.ExpirationDate IS NULL)  

			--Get Item Class Code for non JPA (we will resolve article classes differently)   
			LEFT JOIN `{project}.{pc_dataset}.pctl_classcodetype_jmic_pl` AS pctl_classcodetype_jmic_pl  
				ON pcx_jewelryitem_jmic_pl.ClassCodeType = pctl_classcodetype_jmic_pl.ID  

			LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  
				ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID

			--if an exposure exists, there should be an incident
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident ON cc_incident.ID = cc_exposure.IncidentID

			--Personal Jewelry Item (PJ) Product
			--if an incident exists, there may be a jewerly item associated, else get it from the Risk Unit
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.ccx_jewelryitem_jmic` WHERE _PARTITIONTIME = {partition_date}) AS ccx_jewelryitem_jmic
				ON ccx_jewelryitem_jmic.ID = cc_incident.JewelryItemID
			
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_transaction` AS cctl_transaction 
				ON cctl_transaction.ID = cc_transaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype` AS cctl_costtype 
				ON cctl_costtype.ID = cc_transaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_transaction.CostCategory
			LEFT JOIN `{project}.{cc_dataset}.cctl_linecategory` AS cctl_linecategory 
				ON cctl_linecategory.ID = cc_transactionlineitem.LineCategory
			LEFT JOIN `{project}.{cc_dataset}.cctl_paymenttype` AS cctl_paymenttype 
				ON cctl_paymenttype.ID = cc_transaction.PaymentType
			
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress
				ON 	pc_policyaddress.BranchID=pc_policyperiod.id
				AND (pc_policyaddress.EffectiveDate <= cc_claim.LossDate or pc_policyaddress.EffectiveDate is null)
				AND (pc_policyaddress.ExpirationDate > cc_claim.LossDate OR pc_policyaddress.ExpirationDate is null)

			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyperiod.Segment

			--If the join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type table to derive the company  
			LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype
				ON cc_policy.PolicyType = cctl_policytype.ID
			--Mapping table to derive the company from policy type  
			LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
				ON cctl_policytype.Name = gw_policytype_company_map.PolicyType  

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

			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date}) AS cc_check 
				ON cc_check.ID = cc_transaction.CheckID

			--properly resolve agency
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
				ON (pc_producercode.CODE = cc_policy.PRODUCERCODE)  
				AND pc_producercode.RETIRED = 0                              --exclude archived producer records  

			LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic  
				ON cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC  
			
			--add joins to get back to PC_Account to allocate the Account Level Jeweler --JP 4/17/2012  
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy  
				ON pc_policy.ID = pc_policyperiod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account  
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM  

		/**** TEST *****/
		WHERE 1=1
			--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed

		) FinTrans
		INNER JOIN ClaimPJDirectFinancialsConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN ClaimPJDirectFinancialsConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN ClaimPJDirectFinancialsConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN ClaimPJDirectFinancialsConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN ClaimPJDirectFinancialsConfig itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
			AND	TransactionPublicID IS NOT NULL
			--AND TransactionRank = 1		

	) extractData