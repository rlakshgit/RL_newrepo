-- tag: ClaimFinancialTransactionLinePADirect - tag ends/
/**** Kimberlite - Financial Transactions ***********
		ClaimFinancialTransactionLinePADirect.sql
			Converted to BigQuery
*****************************************************

-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	10/14/2022	DROBAK		Initial
	11/04/2022	DROBAK		Split for Kimberlite (raw) and Building Block (transformations) layers
	01/23/2023	DROBAK		Use LineCode from Config table
	02/02/2023	DROBAK		Add bq_load_date mapping

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
	--For Testing
	CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLinePADirect`
	--CREATE OR REPLACE TABLE `{project}.{dest_dataset}.ClaimFinancialTransactionLinePADirect`
	AS SELECT outerselect.*
	FROM (
		---WITH ...
		) outerselect

*/	
/**********************************************************************************************************************************/
--Default Segments
DECLARE vdefaultPLPESegment STRING;
	--prod-edl.ref_pe_dbo.gw_gl_SegmentMap
	SET vdefaultPLPESegment = (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultPersonalLineSegment = true ORDER BY peSegment LIMIT 1);

INSERT INTO `{project}.{dest_dataset}.ClaimFinancialTransactionLinePADirect`
(
	SourceSystem
	,FinancialTransactionKey
	,FinancialTransactionLineKey
	,ClaimTransactionKey	
	,PolicyTransactionKey
	,PAJewelryCoverageKey
	,RiskPAJewelryKey
	,BusinessType
	,TransactionPublicID
	,TransactionLinePublicID
	,ClaimPublicID
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,CoverageLevel
	,PAJewelryPublicID
	,ccPAJewelryPublicID
	,ClaimPolicyPublicID
	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,CoverageCode
	,JewelryArticleNumber
	,ArticleTypeCode
	,ArticleGenderCode
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
	,IsTransactionSliceEffective
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

	WITH ClaimPADirectFinancialsConfig AS 
	(
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
	)

SELECT
	SourceSystem
	,FinancialTransactionKey
	,FinancialTransactionLineKey
	,ClaimTransactionKey	
	,PolicyTransactionKey
	,PAJewelryCoverageKey
	,RiskPAJewelryKey
	,BusinessType
	,TransactionPublicID
	,TransactionLinePublicID
	,ClaimPublicID
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,CoverageLevel
	,PAJewelryPublicID
	,ccPAJewelryPublicID
	,ClaimPolicyPublicID
	--,CoverageTypeCode
	
	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,CoverageCode
	,JewelryArticleNumber
	,ArticleTypeCode
	,ArticleGenderCode
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
	--,glLineCode

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
	,IsTransactionSliceEffective
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
		--SK For PK [<Source>_<TransactionPublicID>_<Level>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode))		AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionLinePublicID,businessTypeConfig.Value,LineCode))	AS FinancialTransactionLineKey
		--SK For FK [<Source>_<PolicyPeriodPublicID>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPublicID))												AS ClaimTransactionKey
		,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
			END																													AS PolicyTransactionKey
		--SK For PK [<Source>_<CoveragePublicID>_<CoverageLevel>_<Level>]
		,CASE WHEN CoveragePublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel))		
			END																													AS PAJewelryCoverageKey
		--SK For FK [<Source>_<PAJewelryPublicID>_<Level>]
		,CASE WHEN PAJewelryPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PAJewelryPublicID,hashKeySeparator.Value,itemRisk.Value)) 
			END AS RiskPAJewelryKey
		,businessTypeConfig.Value																								AS BusinessType
		,FinTrans.*
		,DATE('{date}')																											AS bq_load_date	--CURRENT_DATE()

	FROM (
		SELECT
			cc_transaction.PublicID																AS TransactionPublicID
			,cc_transactionlineitem.PublicID													AS TransactionLinePublicID
			,cc_claim.PublicId																	AS ClaimPublicId
			--,coverageLevelConfigScheduledItem.Value												AS CoverageLevel
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,pcx_personalarticle_jm.PublicID													AS PAJewelryPublicID
			,ccx_personalarticle_jm.PublicID													AS ccPAJewelryPublicID
			,CASE   WHEN cctl_coveragesubtype.TYPECODE = 'UnscheduledJewelryCovType_JM' THEN 'UnScheduledCov'
                    WHEN cctl_coveragesubtype.TYPECODE = 'JewelryItemCovType_JM' THEN 'ScheduledCov'
                END																				AS CoverageLevel
			,cctl_coveragetype.TypeCode															AS CoverageCode
			--per requirements that claim of record should follow the legacy number if available
			--But in Kimberlite raw layer we need to show both values; can coalesce in Building Block or Gold
			--,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.ClaimNumber																AS ClaimNumber
			,COALESCE(pc_policyperiod.PolicyNumber,cc_policy.policynumber)						AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber
			,cc_policy.AccountNumber															AS AccountNumber
			--,ccx_personalarticle_jm.ItemNumber													AS JewelryArticleNumber
			,pcx_personalarticle_jm.ItemNumber													AS JewelryArticleNumber
			,pctl_jpaitemtype_jm.TYPECODE														AS ArticleTypeCode
			,pctl_jpaitemgendertype_jm.TYPECODE													AS ArticleGenderCode
			--,pctl_jpaitemsubtype_jm.Name														AS ArticleSubType
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
			--,COALESCE(cctl_lobcode.TYPECODE, cc_coverage.PC_LineCode_JMIC)						AS glLineCode
			-- add Coalesce to pc_policy for Personal or Unknown Commercial (some PJ claim txn didn't have Coverages so wasn't looking up)
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
				summ.TransactionLinePAJewelryPublicID=cc_transactionlineitem.PublicID and 
				summ.TransactionStatusCode in ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided') and --Was previously in a valid status
				summ.IsCeded = 0 --@IsCeded
			)																					AS TransactionsSubmittedPrior
			*/
			--This would indicate a TransactionsSubmittedPrior event occurred
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior
				
				--,cc_transaction.CreateTime AS TransactionsSubmittedPriorDate
				--,pc_effectivedatedfields.OfferingCode AS OfferingCode 

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
			,CASE	WHEN IFNULL(cc_transaction.AuthorizationCode_JMIC,'') 
						NOT IN ('Credit Card Payment Pending Notification')
					AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
						,'pendingstop','pendingtransfer','pendingvoid','submitted',
						'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
						NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective
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
			,ccx_personalarticle_jm.LocatedWith													AS LocatedWith
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
				--AND cctl_lobcode.TYPECODE='JPALine'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.PublicID = cc_policy.PC_PeriodPublicId_JMIC	
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN ClaimPADirectFinancialsConfig uwCompany 
				ON uwCompany.Key = 'UWCompany' 
				--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
				AND COALESCE(pc_uwcompany.PublicID,uwCompany.Value) = uwCompany.Value

			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS ccexposureTrxn 
				ON ccexposureTrxn.ID = cc_transaction.ExposureID 
		
			LEFT JOIN
			(
				/*exposures for this claim that are not associated with a claim transaction*/
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
			INNER JOIN ClaimPADirectFinancialsConfig lineConfig 
				ON lineConfig.Key = 'LineCode' 				
				AND lineConfig.Value = cc_coverage.PC_LineCode_JMIC

			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_transaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON ccexposureTrxn.CoverageSubType = cctl_coveragesubtype.ID

		    -------------  
			-- COVERAGES  
			-- Attempt to link to Policy Center coverage based on Claim Center [PC_CovPublicId_JMIC] and [PolicySystemId]  
			-------------  
			-- Personal Article Coverage (applied to PolicyLine)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalartcllinecov_jm
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_personalartcllinecov_jm.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId, ':') - 1) = 'entity.PersonalArtclLineCov_JM'

			-- Personal Article Coverage (applied to Jewelry Item)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticlecov_jm --Scheduled Coverage (Item)
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_personalarticlecov_jm.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId, ':') - 1) = 'entity.PersonalArticleCov_JM'

			 ------------------  
			 -- RISK SEGMENTS  
			 ------------------ 

  			-- Personal Article
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticle_jm
				ON pcx_personalarticlecov_jm.PersonalArticle = pcx_personalarticle_jm.FixedID
				AND pcx_personalarticlecov_jm.BranchID = pcx_personalarticle_jm.BranchID
				AND (pcx_personalarticle_jm.EffectiveDate <= cc_claim.LossDate OR pcx_personalarticle_jm.EffectiveDate IS NULL)
				AND (pcx_personalarticle_jm.ExpirationDate > cc_claim.LossDate OR pcx_personalarticle_jm.ExpirationDate IS NULL)
 
			LEFT JOIN `{project}.{pc_dataset}.pctl_jpaitemtype_jm`  AS pctl_jpaitemtype_jm
			ON pcx_personalarticle_jm.ItemType = pctl_jpaitemtype_jm.ID

			--LEFT JOIN `{project}.{pc_dataset}.pctl_jpaitemsubtype_jm` AS pctl_jpaitemsubtype_jm
			--ON pcx_personalarticle_jm.ItemSubType = pctl_jpaitemsubtype_jm.ID
			
			LEFT JOIN `{project}.{pc_dataset}.pctl_jpaitemgendertype_jm` AS  pctl_jpaitemgendertype_jm
			ON pcx_personalarticle_jm.ItemGenderType= pctl_jpaitemgendertype_jm.ID

			LEFT JOIN `{project}.{cc_dataset}.cctl_recoverycategory` AS cctl_recoverycategory  
				ON cc_transaction.RecoveryCategory = cctl_recoverycategory.ID

			--if an exposure exists, there should be an incident
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident ON cc_incident.ID = cc_exposure.IncidentID
			
			--Personal Article (PA) Product
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.ccx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS ccx_personalarticle_jm
				ON ccx_personalarticle_jm.ID = cc_incident.PersonalArticleID
			
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
			LEFT OUTER JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype
				ON cc_policy.PolicyType = cctl_policytype.ID
			--Mapping table to derive the company from policy type  
			LEFT OUTER JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
				ON cctl_policytype.Name = gw_policytype_company_map.PolicyType  

		    --If this joins, then this transaction is an "ONSET" transaction, meaning it's moved from another account or transaction.  
			--this means the current transaction is the "onset" part of a move transaction  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_transaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_transaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID  

			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_check` WHERE _PARTITIONTIME = {partition_date}) AS cc_check 
				ON cc_check.ID = cc_transaction.CheckID

			--properly resolve agency
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
				ON (pc_producercode.CODE = cc_policy.PRODUCERCODE)  
				AND pc_producercode.RETIRED = 0                              --exclude archived producer records

			LEFT JOIN `{project}.{cc_dataset}.cctl_reservelinecategory_jmic` AS cctl_reservelinecategory_jmic  
				ON cctl_reservelinecategory_jmic.ID = cc_transaction.ReserveLineCategory_JMIC  
			
			--add joins to get back to PC_Account to allocate the Account Level Jeweler --JP 4/17/2012  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy  
				ON pc_policy.ID = pc_policyperiod.PolicyID
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account  
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM  

		/**** TEST *****/
		WHERE 1=1
		AND pc_uwcompany.PublicID = uwCompany.Value --'zgojeqek81h0c3isqtper9n5kb9' --Want JMSI Only 
			--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed

		) FinTrans
		INNER JOIN ClaimPADirectFinancialsConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN ClaimPADirectFinancialsConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN ClaimPADirectFinancialsConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN ClaimPADirectFinancialsConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN ClaimPADirectFinancialsConfig itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
			AND	TransactionPublicID IS NOT NULL
			--AND TransactionRank = 1		

	) extractData