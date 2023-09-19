-- tag: ClaimFinancialTransactionLinePJCeded - tag ends/
/**** Kimberlite - Financial Transactions **********
		ClaimFinancialTransactionLinePJCeded.sql
			BigQuery Converted
****************************************************

-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	09/21/2022	DROBAK		Initial
	11/01/2022	DROBAK		Split between Kimberlite (raw) and BB layers (metrics)
	01/23/2023	DROBAK		Use LineCode from Config table

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

/* For Testing
--CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLinePJCeded`
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded`
AS SELECT overallselect.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded`
(
	SourceSystem
	,FinancialTransactionKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,BusinessType
	,TransactionPublicID
	,ClaimPublicID
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,CoverageLevel
	,ClaimPolicyPublicID
	,ItemPublicID
	,ccItemPublicID
	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,CoverageCode
	,ItemNumber
	,ItemClassCode
	,TransactionType
	,ClaimTransactionType
	,CostType
	,CostCategory
	,LossDate
	,TransactionDate
	,TransactionAmount
	,LineCode
	,LOBCode
	,IsAverageReserveSource
	,TransactionSetID
	,AccountingDate
	,TransactionStatusCode
	,TransactionsSubmittedPrior
	,TransactionOrigin
	,IsClaimForLegacyPolicy
	,TransactionChangeType
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,IsTransactionSliceEffective
 	,UWCompanyPublicID
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
	,DefaultSegment
	,peSegment
	--Ceded Fields
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
	WITH ClaimPJCededFinancialsConfig AS 
	(
	  SELECT 'BusinessType' as Key, 'Ceded' as Value UNION ALL
	  SELECT 'BusinessSubType','Policy' UNION ALL 
	  SELECT 'SourceSystem','GW' UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
	  SELECT 'LineCode','JMICPJLine' UNION ALL				--JPALine --GLLine	--3rdPartyLine
	  SELECT 'UWCompany', 'uwc:10' UNION ALL				--zgojeqek81h0c3isqtper9n5kb9
	  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
	  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
	  SELECT 'NoCoverage','NoCoverage' UNION ALL
	  SELECT 'CostCoverage','CostCoverage' UNION ALL
	  SELECT 'ScheduledItemRisk','PersonalJewelryItem'		--PersonalArticleJewelry
	)

SELECT
	SourceSystem
	,FinancialTransactionKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,BusinessType
	--,BusinessSubType		--is in DWH; is this really needed in Kimberlite?
	,TransactionPublicID
	,ClaimPublicID
	,PolicyPeriodPublicID
	,CoveragePublicID
	,ccCoveragePublicID
	,CoverageLevel
	,ClaimPolicyPublicID
	,ItemPublicID
	,ccItemPublicID
	--,CoverageTypeCode

	,ClaimNumber
	,PolicyNumber
	,TermNumber
	,AccountNumber
	,CoverageCode
	,ItemNumber
	,ItemClassCode
	,TransactionType
	,ClaimTransactionType
	,CostType
	,CostCategory
	,LossDate
	,TransactionDate
	,TransactionAmount
	,LineCode
	,LOBCode
	,IsAverageReserveSource
	,TransactionSetID
	,AccountingDate
	,TransactionStatusCode
	,TransactionsSubmittedPrior

	--,ClaimRecoveryType
	,TransactionOrigin
	,IsClaimForLegacyPolicy
	,TransactionChangeType
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,IsTransactionSliceEffective
 	,UWCompanyPublicID
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
	,DefaultSegment
	,peSegment

	--Ceded Fields
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
		sourceConfig.Value AS SourceSystem
		--SK For PK [<Source>_<TransactionPublicID>_<Level>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		--SK For FK [<Source>_<PolicyPeriodPublicID>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value, ClaimPublicID)) AS ClaimTransactionKey		
		,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
		END AS PolicyTransactionKey
		--SK For PK [<Source>_<CoveragePublicID>_<Level>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS ItemCoverageKey
		--SK For FK [<Source>_<ItemPublicId>_<Level>]
		,CASE WHEN ItemPublicId IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ItemPublicID,hashKeySeparator.Value,itemRisk.Value)) 
		END AS RiskJewelryItemKey
		,businessTypeConfig.Value AS BusinessType
		--,businessSubTypeConfig.Value AS BusinessSubType
		,FinTrans.*
		,DATE('{date}') AS bq_load_date		--CURRENT_DATE()

	FROM (
		SELECT
			cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_claim.PublicId																	AS ClaimPublicID
			--,coverageLevelConfigScheduledItem.Value												AS CoverageLevel
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
			--per requirements that claim of record should follow the legacy number if available
			--But in Kimberlite raw layer we need to show both values; can coalesce in Building Block or Gold
			--,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.ClaimNumber																AS ClaimNumber
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber
			,cc_policy.AccountNumber															AS AccountNumber
			--,ccx_jewelryitem_jmic.ItemNumber													AS ItemNumber
			,pcx_jewelryitem_jmic_pl.ItemNumber													AS ItemNumber
			 --this is for the "loss location" NOT "policy location"
			,pctl_classcodetype_jmic_pl.TYPECODE												AS ItemClassCode
			,cctl_ritransaction.TYPECODE														AS TransactionType
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType
			,cctl_costcategory.TYPECODE															AS CostCategory
			,cc_claim.LossDate																	AS LossDate
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,lineConfig.Value																	AS LineCode
			--,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode
			,cctl_lobcode.TYPECODE																AS LOBCode
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--,cc_ritransaction.UpdateTime														AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			--,cc_ritransaction.Status															AS TransactionStatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			--,NULL																				AS TransactionsSubmittedPrior
			--This would indicate a TransactionsSubmittedPrior event occurred
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior
			--,pc_effectivedatedfields.OfferingCode AS OfferingCode 
													
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
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
						,'pendingstop','pendingtransfer','pendingvoid','submitted',
						'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
						NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			--,cc_ricoding.ReserveLineID															AS ReserveLineID
			--,cc_ritransaction.ExposureID														AS ExposureID
			,cc_exposure.PublicID																AS ExposurePublicID
			--,cc_exposure.IncidentID																AS IncidentID
			,cc_incident.PublicID																AS IncidentPublicID
			,pc_producercode.PublicID															AS ProducerPublicID
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode
			,cc_claim.InsuredDenormID															AS InsuredID
			,ccx_jewelryitem_jmic.LocatedWith													AS LocatedWith 
			,cc_claim.LossLocationID															AS LossLocationID
			,vdefaultPLPESegment																AS DefaultSegment
			,pctl_segment.TYPECODE																AS peSegment

			--Ceded / RI
			,cc_riagreement.AgreementNumber														AS RIAgreementNumber
			,cctl_riagreement.TYPECODE															AS RIAgreementType
			,cc_riagreement.ID																	AS RIAgreementID
			,cc_riagreement.PC_Publicid_JMIC													AS RIAgreementPublicID
			,cc_ricoding.ID																		AS RICodingID
			,cc_riagreementgroup.ID																AS RIAgreementGroupID 
			,pctl_ricoveragegrouptype.TypeCode													AS RIPCCoverageGroupType
			,COALESCE
			(
				CASE WHEN riAgreementMap.peReceivableCode = 'MiscFac' THEN riAgreementMap.peReceivableCode ELSE NULL END, --first check if its Fac
				riCoverageGroupMap.peReceivableCode, --if not fac, then use the CoverageGroup
				riAgreementMap.peReceivableCode, --if Coverage Group is null and not Fac, then used the mapped agreement
				'' --if nothing maps, then default to blank (unmapped)
			)																					AS RIType

	 	FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
				--AND cctl_lobcode.TYPECODE='JMICPJLine'
			INNER JOIN ClaimPJCededFinancialsConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.PublicID = cc_policy.PC_PeriodPublicId_JMIC	
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN ClaimPJCededFinancialsConfig uwCompany 
				ON uwCompany.Key = 'UWCompany' 
				--AND uwCompany.Value=pc_uwcompany.PublicID
				--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
				AND COALESCE(pc_uwcompany.PublicID,uwCompany.Value) = uwCompany.Value

			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype

			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID

			LEFT JOIN temp_MAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_MAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
				
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_MAXAgreementNumbers.pcRICoverageGroupID

			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType

			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'

			--there may not be any exposures
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			--Needed for Ceded?
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID

		    -------------  
			-- COVERAGES  
			--Attempt to link to Policy Center coverage based on Claim Center [PC_CovPublicId_JMIC] and [PolicySystemId]  
			-------------  
			
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 

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

			--if an exposure exists, there should be an incident
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident ON cc_incident.ID = cc_exposure.IncidentID
 
			--Personal Jewelry Item (PJ) Product
			--if an incident exists, there may be a jewerly item associated, else get it from the Risk Unit (DWH does not look at riskunit for PJ but does for PA)
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.ccx_jewelryitem_jmic` WHERE _PARTITIONTIME = {partition_date}) AS ccx_jewelryitem_jmic
				ON ccx_jewelryitem_jmic.ID = cc_incident.JewelryItemID	

			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype` AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			
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
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID  

			--properly resolve agency
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
				ON (pc_producercode.CODE = cc_policy.PRODUCERCODE)  
				AND pc_producercode.RETIRED = 0                              --exclude archived producer records  

			--add joins to get back to PC_Account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy  
				ON pc_policy.ID = pc_policyperiod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account  
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM  

		/**** TEST *****/
		WHERE 1=1 
		--AND cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode','pendingstop','pendingtransfer','pendingvoid','submitted','recoded','stopped','transferred','voided')
		--AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') --excludes FedNat & TWICO claims from being processed

		) FinTrans
		INNER JOIN ClaimPJCededFinancialsConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN ClaimPJCededFinancialsConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN ClaimPJCededFinancialsConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN ClaimPJCededFinancialsConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		--INNER JOIN ClaimPJCededFinancialsConfig businessSubTypeConfig
		--	ON businessSubTypeConfig.Key = 'BusinessSubType'
		INNER JOIN ClaimPJCededFinancialsConfig itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
			AND	TransactionPublicID IS NOT NULL
			--AND TransactionRank = 1		

	) extractData

--) overallselect

