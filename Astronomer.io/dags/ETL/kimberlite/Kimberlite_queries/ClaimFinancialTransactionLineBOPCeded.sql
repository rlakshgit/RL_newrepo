-- tag: ClaimFinancialTransactionLineBOPCeded - tag ends/
/**** Kimberlite - Financial Transactions *************
		ClaimFinancialTransactionLineBOPCeded.sql
			Converted to BigQuery
*******************************************************

-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

 	01/03/2023	DROBAK		Initial
	01/23/2023	DROBAK		Correct CoverageLevel source & BusinessType

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
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimFinancialTransactionLineBOPCeded`
AS SELECT extractData.*
FROM (
		---with #ClaimBOPCededFinancialsConfig
		---etc code
) extractData
*/	
/**********************************************************************************************************************************/
DECLARE vdefaultCLPESegment STRING;
	SET vdefaultCLPESegment= (SELECT peSegment FROM `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` WHERE IsDefaultCommercialLineSegment = true ORDER BY peSegment LIMIT 1);

CREATE OR REPLACE TEMP TABLE temp_IMCEDEDMAXAgreementNumbers
 (
	AgreementNumber			STRING,
	pcRIAgreementPublicID	INTEGER,
	pcRIAgreementID			INTEGER,
	pcRICoverageGroupID		INTEGER
 );

INSERT INTO temp_IMCEDEDMAXAgreementNumbers
	SELECT cc_riagreement.AgreementNumber, MAX(ripcagreementByPublicID.ID) AS pcRIAgreementPublicID, MAX(ripcagreementByAgreementNumber.ID) AS pcRIAgreementID, MAX(pc_ricoveragegroup.ID) AS pcRICoverageGroupID
	FROM (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByAgreementNumber 
		ON ripcagreementByAgreementNumber.AgreementNumber = cc_riagreement.AgreementNumber
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
		ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
		ON pc_ricoveragegroup.Agreement = COALESCE(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID)
	GROUP BY cc_riagreement.AgreementNumber;
/*
CASE
  WHEN
    EXISTS
      ( SELECT *
        FROM `{project}.{dest_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_schema = {dest_dataset}
        AND table_name = 'PolicyVerLOB_BOP_PrimaryRatingLocn'
        AND CAST(last_modified_time AS DATE) = CURRENT_DATE()
      ) 
  THEN SELECT 'Primary Location Table Exists';
ELSE*/ --SELECT 'Recreate the Table';
	--This code is also used in RiskLocationBusinessOwners, CoverageBOP, CoverageUMB, ClaimFinancialTransactionLineBOPCeded, ClaimFinancialTransactionLineBOPDirect
	--So the two tables use SAME PublicID from SAME Table (pc_boplocation)
	CREATE OR REPLACE TABLE `{project}.{dest_dataset}.PolicyVerLOB_BOP_PrimaryRatingLocn`
	AS SELECT *
	FROM (	SELECT 
				pc_policyperiod.ID	AS PolicyPeriodID
				,pc_policyperiod.EditEffectiveDate AS SubEffectiveDate
				,pctl_policyline.TYPECODE AS PolicyLineOfBusiness 
				--This flag displays whether or not the LOB Location matches the PrimaryLocation
				--,MAX(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
				--If the Primary loc matches the LOB loc, use it, otherwise use the MIN location num's corresponding LOB Location
				,COALESCE(MIN(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
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
					AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'  
				--BOP Location  
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
					ON pc_boplocation.Location = pc_policylocation.FixedID
					AND pc_boplocation.BranchID = pc_policylocation.BranchID  
					AND pc_boplocation.BOPLine = pc_policyline.FixedID  
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
					AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
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

INSERT INTO `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPCeded` (
	SourceSystem
	,FinancialTransactionKey
	,ClaimTransactionKey
	,PolicyTransactionKey
	,BOPCoverageKey
	,RiskLocationKey
	,RiskBuildingKey
	,BusinessType
	,TransactionPublicID
	,TransactionDate
	,TransactionAmount
	,TransactionSetID
	,ClaimPublicID
	,ClaimNumber
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,LossDate
	,InsuredID
	,LossLocationID
	,ClaimPolicyPublicID
	,AccountNumber
	,ProducerCode
	,ccPolicyID
	,LOBCode
	,ReserveLinePublicID
	,IsAverageReserveSource
	,PolicyPeriodPublicID
	,PolicyNumber
	,TermNumber
	,TransactionType
	,TransactionStatusCode
	,AccountingDate
	,TransactionsSubmittedPrior
	,ClaimTransactionType
	,CostType
	,CostCategory
	,CoveragePublicID
	,ccCoveragePublicID
	,LineCode
	,CoverageCode
	,CoverageSubType
	,CoverageLevel
	,IncidentPublicID
	,IncidentPropertyID
	,ExposurePublicID
	,UnderWritingCompanyType
	,IsTransactionSliceEffective
	,RiskLocationAddressID
	,RiskUnitTypeCode
	,PolicyAddressPublicID
	,PolicyAddressStateID
	,PolicyAddressCountryID
	,PolicyAddressPostalCode
	,UWCompanyPublicID
	,TransactionOrigin
	,TransactionChangeType
	,BOPLocationPublicID
	,BuildingPublicID
	,ProducerPublicID
	,JewelerContactPublicID
	,DefaultSegment
	,peSegment
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
	sourceConfig.Value AS SourceSystem
	,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode))							AS FinancialTransactionKey
	--SK For FK [<Source>_<PolicyPeriodPublicID>]
	,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,ClaimPublicID))																	AS ClaimTransactionKey
	,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
		THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) 
		END																																		AS PolicyTransactionKey
	--SK For PK [<Source>_<CoveragePublicID>_<CoverageLevel>_<Level>]
	,CASE WHEN CoveragePublicID IS NOT NULL 
		THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
		END																																		AS BOPCoverageKey
	,CASE WHEN BOPLocationPublicID IS NOT NULL 
		THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationLevelRisk.value))
		END																																		AS RiskLocationKey
	,CASE WHEN BuildingPublicID IS NOT NULL 
		THEN SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,BuildingPublicID, hashKeySeparator.Value,buildingRisk.value))
		END																																		AS RiskBuildingKey
	,businessTypeConfig.Value																													AS BusinessType
	,FinTrans.*
	,DATE('{date}')																																AS bq_load_date
	--,CURRENT_DATE()																																AS bq_load_date

FROM
(
	--Line Level Coverage
	SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--, 1																					AS IsErodingReserves
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID

			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
														
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,NULL																				AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			/*INNER JOIN ClaimBOPCededFinancialsConfig uwCompany 
				ON uwCompany.Key = 'UWCompany' 
				--AND uwCompany.Value=pc_uwcompany.PublicID
				--Added Coalesce to account for Legacy ClaimNumber LIKE 'PJ%' and still prevent Personal Articles from being selected
				AND COALESCE(pc_uwcompany.PublicID,uwCompany.Value) = uwCompany.Value
			*/
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			-------------  
			-- COVERAGES  
			-- Attempt to link to Policy Center coverage based on Claim Center [PC_CovPublicId_JMIC] and [PolicySystemId]  -- Coverage id required for Keys
			-------------  
			---- BOP Line Coverages (applied to PolicyLine)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) AS pc_businessownerscov
				ON cc_coverage.PC_CovPublicId_JMIC = pc_businessownerscov.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BusinessOwnersCov'

			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.FixedID = pc_businessownerscov.BOPLine
				AND pc_policyline.BranchID = pc_businessownerscov.BranchID
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'LineLevelCoverage'

			--Join in the PolicyVerLOB_BOP_PrimaryRatingLocn Table to map the Natural Key for RatingLocationKey		
			LEFT JOIN `{project}.{dest_dataset}.PolicyVerLOB_BOP_PrimaryRatingLocn` AS PolicyVerLOB_BOP_PrimaryRatingLocn
			ON PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyPeriodID = pc_policyperiod.ID
			AND PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				or 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyPeriodID
				AND pc_policylocation.LocationNum = PolicyVerLOB_BOP_PrimaryRatingLocn.RatingLocationNum 
				AND COALESCE(PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			-------------
			-- RISK SEGMENTS
			-------------
			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pc_policylocation.BranchID
				AND pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BOPLine = pc_policyline.FixedID
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')

	UNION ALL

		--Sub Line Level Coverage
		SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID
	
			--cctl_riskunit
			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
													
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,NULL																				AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
--select *
FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			-- BOP Sub-Line Coverages
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_BOPsublinecov_jmic
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_BOPsublinecov_jmic.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BOPSubLineCov_JMIC'
	
			-- BOP Sub-Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopsubline_jmic
				ON pcx_BOPsublinecov_jmic.BOPSubLine = pcx_bopsubline_jmic.FixedID
				AND pcx_BOPsublinecov_jmic.BranchID = pcx_bopsubline_jmic.BranchID
				AND (pcx_bopsubline_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_bopsubline_jmic.EffectiveDate IS NULL)
				AND (pcx_bopsubline_jmic.ExpirationDate > cc_claim.LossDate OR pcx_bopsubline_jmic.ExpirationDate IS NULL)

			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.FixedID = pcx_bopsubline_jmic.BOPLine
				AND pc_policyline.BranchID = pcx_bopsubline_jmic.BranchID
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'SubLineLevelCoverage'

			--Join in the PolicyVerLOB_BOP_PrimaryRatingLocn Table to map the Natural Key for RatingLocationKey		
			LEFT JOIN `{project}.{dest_dataset}.PolicyVerLOB_BOP_PrimaryRatingLocn` AS PolicyVerLOB_BOP_PrimaryRatingLocn
			ON PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyPeriodID = pc_policyperiod.ID
			AND PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
			--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
			AND ((PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
				or 
				(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = PolicyVerLOB_BOP_PrimaryRatingLocn.PolicyPeriodID
				AND pc_policylocation.LocationNum = PolicyVerLOB_BOP_PrimaryRatingLocn.RatingLocationNum 
				AND COALESCE(PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(PolicyVerLOB_BOP_PrimaryRatingLocn.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)

			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pc_policylocation.BranchID
				AND pc_boplocation.Location = pc_policylocation.FixedID
				AND pc_boplocation.BOPLine = pc_policyline.FixedID
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')

	UNION ALL

	--Location Level Coverage
		SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--, 1																					AS IsErodingReserves
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID
	
			--cctl_riskunit
			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
													
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,NULL																				AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
--select *
		FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			-- BOP Location Coverages
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocationcov
				ON cc_coverage.PC_CovPublicId_JMIC = pc_boplocationcov.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BOPLocationCov'
			
			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pc_boplocationcov.BranchID
				AND pc_boplocation.FixedID = pc_boplocationcov.BOPLocation
				--AND pc_boplocation.Location = pc_boplocationcov.FixedID
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)

			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.FixedID = pc_boplocation.BOPLine
				AND pc_policyline.BranchID = pc_boplocation.BranchID
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'LocationLevelCoverage' 

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')

	UNION ALL

		--Sub Location Level Coverage
		SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--, 1																					AS IsErodingReserves
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID
	
			--cctl_riskunit
			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
													
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,NULL																				AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
--select *
FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			-- BOP Sub-Location Coverages
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopsubloccov_jmic
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_bopsubloccov_jmic.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BOPSubLocCov_JMIC'
					
			-- BOP Sub-Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopsubloc_jmic
				ON pcx_bopsubloccov_jmic.BOPSubLoc = pcx_bopsubloc_jmic.FixedID
				AND pcx_bopsubloccov_jmic.BranchID = pcx_bopsubloc_jmic.BranchID
				AND (pcx_bopsubloc_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_bopsubloc_jmic.EffectiveDate IS NULL)
				AND (pcx_bopsubloc_jmic.ExpirationDate > cc_claim.LossDate OR pcx_bopsubloc_jmic.ExpirationDate IS NULL)

			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pcx_bopsubloc_jmic.BranchID
				AND pc_boplocation.FixedID = pcx_bopsubloc_jmic.BOPLocation
				--AND pc_boplocation.Location = pc_boplocationcov.FixedID
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)

			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.FixedID = pc_boplocation.BOPLine
				AND pc_policyline.BranchID = pc_boplocation.BranchID
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')

	UNION ALL

	--Building Level Coverage
		SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--, 1																					AS IsErodingReserves
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID
	
			--cctl_riskunit
			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
													
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,pc_bopbuilding.PublicID															AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
--select *
		FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			--BOP Building Coverages			
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopbuildingcov
				ON cc_coverage.PC_CovPublicId_JMIC = pc_bopbuildingcov.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BOPBuildingCov'

			--BOP Building 			
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopbuilding
				ON pc_bopbuildingcov.BOPBuilding = pc_bopbuilding.FixedID
				AND pc_bopbuildingcov.branchid = pc_bopbuilding.branchid
				AND (pc_bopbuilding.EffectiveDate <= cc_claim.LossDate OR pc_bopbuilding.EffectiveDate IS NULL)
				AND (pc_bopbuilding.ExpirationDate > cc_claim.LossDate OR pc_bopbuilding.ExpirationDate IS NULL)	

			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pc_bopbuilding.BranchID
				AND pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
				--AND pc_boplocation.Location = pc_boplocationcov.FixedID
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)

			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.FixedID = pc_boplocation.BOPLine
				AND pc_policyline.BranchID = pc_boplocation.BranchID
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'BuildingLevelCoverage'

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')

	UNION ALL

	--BOP Blanket Level Coverage
		SELECT
			--cc_ritransaction
			 cc_ritransaction.PublicID															AS TransactionPublicID
			,cc_ritransaction.CreateTime														AS TransactionDate
			,cc_ritransaction.ReportingAmount													AS TransactionAmount
			,cc_ritransaction.TransactionSetID													AS TransactionSetID
			--, 1																					AS IsErodingReserves
		
			--cc_claim
			,cc_claim.PublicId																	AS ClaimPublicID
			,COALESCE(cc_claim.LegacyClaimNumber_JMIC, cc_claim.ClaimNumber)					AS ClaimNumber
			,cc_claim.isClaimForLegacyPolicy_JMIC												AS IsClaimForLegacyPolicy
			,cc_claim.LegacyPolicyNumber_JMIC													AS LegacyPolicyNumber
			,cc_claim.LegacyClaimNumber_JMIC													AS LegacyClaimNumber
			,cc_claim.LossDate																	AS LossDate
			,cc_claim.InsuredDenormID															AS InsuredID
			,cc_claim.LossLocationID															AS LossLocationID
		
			--cc_policy
			,cc_policy.PublicId																	AS ClaimPolicyPublicID
			,cc_policy.AccountNumber															AS AccountNumber
			,cc_policy.PRODUCERCODE																AS ProducerCode
			,cc_policy.ID																		AS ccPolicyID
			
			--cctl_lobcode
			,cctl_lobcode.TYPECODE																AS LOBCode

			--cc_reserveline
			,cc_reserveline.PublicID															AS ReserveLinePublicID
			,cc_reserveline.IsAverageReserveSource_jmic											AS IsAverageReserveSource
			
			--pc_policyPeriod
			,pc_policyPeriod.PublicID															AS PolicyPeriodPublicID
			,pc_policyPeriod.PolicyNumber														AS PolicyNumber
			,pc_policyPeriod.TermNumber															AS TermNumber

			--cctl_ritransaction
			,cctl_ritransaction.TYPECODE														AS TransactionType
				
			--cctl_transactionstatus
			,cctl_transactionstatus.TYPECODE													AS TransactionStatusCode
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN cc_ritransaction.CreateTime ELSE cc_ritransaction.UpdateTime END			AS AccountingDate
			,CASE WHEN cctl_transactionstatus.TYPECODE IN ('pendingvoid', 'pendingstop', 'voided')
				 THEN 1 ELSE NULL END															AS TransactionsSubmittedPrior

			--cctl_costtype
			,cctl_costtype.NAME																	AS ClaimTransactionType
			,cctl_costtype.TYPECODE																AS CostType

			--cctl_costcategory
			,cctl_costcategory.TYPECODE															AS CostCategory

			--cc_coverage
			,cc_coverage.PC_CovPublicId_JMIC													AS CoveragePublicID
			,cc_coverage.PublicID																AS ccCoveragePublicID
			,cc_coverage.PC_LineCode_JMIC														AS LineCode		--aka PCLineCode

			--cctl_coveragetype
			,cctl_coveragetype.TypeCode															AS CoverageCode

			--cctl_coveragesubtype
			,cctl_coveragesubtype.TYPECODE														AS CoverageSubType
			,coverageLevelConfig.Value															AS CoverageLevel

			--cc_incident
			,cc_incident.PublicID																AS IncidentPublicID
			,cc_incident.PropertyID																AS IncidentPropertyID

			--cc_exposure
			,cc_exposure.PublicID																AS ExposurePublicID

			--cctl_underwritingcompanytype
			,cctl_underwritingcompanytype.TYPECODE												AS UnderWritingCompanyType
			,CASE	WHEN cctl_transactionstatus.TYPECODE IN ('submitting','pendingrecode'
								,'pendingstop','pendingtransfer','pendingvoid','submitted',
								'recoded','stopped','transferred','voided')
					AND COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
					NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
					THEN 1 ELSE 0 END															AS IsTransactionSliceEffective

			--ccrupolicyLocation
			,ccrupolicyLocation.AddressID														AS RiskLocationAddressID
	
			--cctl_riskunit
			,cctl_riskunit.TYPECODE																AS RiskUnitTypeCode
		
			--pc_policyaddress
			,pc_policyaddress.PublicID															AS PolicyAddressPublicID
			,pc_policyaddress.StateInternal														AS PolicyAddressStateID
			,pc_policyaddress.CountryInternal													AS PolicyAddressCountryID
			,pc_policyaddress.PostalCodeInternal												AS PolicyAddressPostalCode

			--pc_uwcompany, gw_policytype_company_map
			,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID)		AS UWCompanyPublicID
													
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
		
			--Location & Building tables
			,pc_boplocation.PublicID															AS BOPLocationPublicID
			,NULL																				AS BuildingPublicID

			,pc_producercode.PublicID															AS ProducerPublicID
			,AccountLevelJeweler.PublicID														AS JewelerContactPublicID
			,vdefaultCLPESegment																AS DefaultSegment
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
--select *
FROM
			(SELECT * FROM `{project}.{cc_dataset}.cc_ritransaction` WHERE _PARTITIONTIME = {partition_date}) AS cc_ritransaction
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
				ON cc_claim.ID = cc_ritransaction.ClaimID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy 
				ON cc_policy.id = cc_claim.PolicyID
			--ceded/ri joins
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreement` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreement 
				ON cc_riagreement.ID = cc_ritransaction.RIAgreement
			INNER JOIN `{project}.{cc_dataset}.cctl_riagreement` AS cctl_riagreement 
				ON cctl_riagreement.ID = cc_riagreement.SubType
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_ricoding` WHERE _PARTITIONTIME = {partition_date}) AS cc_ricoding 
				ON cc_ricoding.ID = cc_ritransaction.RICodingID
			INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline 
				ON cc_reserveline.ID = cc_ricoding.ReserveLineID
			INNER JOIN `{project}.{cc_dataset}.cctl_lobcode` AS cctl_lobcode 
				ON cctl_lobcode.ID = cc_claim.LOBCode
			INNER JOIN ClaimBOPCededFinancialsConfig lineConfigClaim 
				ON lineConfigClaim.Key = 'ClaimLineCode' AND lineConfigClaim.Value=cctl_lobcode.TYPECODE

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyPeriod
				ON pc_policyPeriod.PublicID = cc_policy.PC_PeriodPublicId_JMIC
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
				ON pctl_segment.Id = pc_policyPeriod.Segment
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyPeriod.UWCompany = pc_uwcompany.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_ritransaction` AS cctl_ritransaction 
				ON cctl_ritransaction.ID = cc_ritransaction.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_transactionstatus` AS cctl_transactionstatus 
				ON cctl_transactionstatus.ID = cc_ritransaction.Status
			LEFT JOIN `{project}.{cc_dataset}.cctl_costtype`  AS cctl_costtype 
				ON cctl_costtype.ID = cc_ritransaction.CostType
			LEFT JOIN `{project}.{cc_dataset}.cctl_costcategory` AS cctl_costcategory 
				ON cctl_costcategory.ID = cc_ritransaction.CostCategory
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riagreementgroup` WHERE _PARTITIONTIME = {partition_date}) AS cc_riagreementgroup 
				ON cc_riagreementgroup.ID = cc_riagreement.RIAgreementGroupID
			LEFT JOIN temp_IMCEDEDMAXAgreementNumbers AS temp_IMCEDEDMAXAgreementNumbers
				ON cc_riagreement.AgreementNumber = temp_IMCEDEDMAXAgreementNumbers.AgreementNumber
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS ripcagreementByPublicID 
				ON ripcagreementByPublicID.PublicID = cc_riagreement.PC_Publicid_JMIC
			/*LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup ripccoveragegroup ON ripccoveragegroup.ID = 
				(select MAX(ID) from (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup where Agreement = coalesce(ripcagreementByPublicID.ID,ripcagreementByAgreementNumber.ID))
			*/
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
				ON pc_ricoveragegroup.ID = temp_IMCEDEDMAXAgreementNumbers.pcRICoverageGroupID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype 
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riAgreementMap 
				ON riAgreementMap.gwRICode = cctl_riagreement.TYPECODE 
				AND riAgreementMap.gwSource = 'Agreement'
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_RIMap` AS riCoverageGroupMap 
				ON riCoverageGroupMap.gwRICode = pctl_ricoveragegrouptype.TYPECODE 
				AND riCoverageGroupMap.gwSource = 'CoverageGroup'
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_exposure` WHERE _PARTITIONTIME = {partition_date}) AS cc_exposure 
				ON cc_exposure.ID = cc_ritransaction.ExposureID 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_coverage` WHERE _PARTITIONTIME = {partition_date}) AS cc_coverage 
				ON cc_coverage.ID = cc_exposure.CoverageID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragetype` AS cctl_coveragetype 
				ON cctl_coveragetype.ID = cc_coverage.Type 
			LEFT JOIN `{project}.{cc_dataset}.cctl_coveragesubtype` AS cctl_coveragesubtype
				ON cc_exposure.CoverageSubType = cctl_coveragesubtype.ID
			
			LEFT JOIN `{project}.{pe_dbo_dataset}.gw_gl_SegmentMap` AS gw_gl_SegmentMap 
				ON pctl_segment.TYPECODE = gw_gl_SegmentMap.gwSegment
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_incident` WHERE _PARTITIONTIME = {partition_date}) AS cc_incident 
				ON cc_incident.ID = cc_exposure.IncidentID
			LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype 
				ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_riskunit` WHERE _PARTITIONTIME = {partition_date}) AS cc_riskunit --risk unit location. For CL it might be same as loss address
				ON cc_riskunit.ID = cc_coverage.RiskUnitID 
			LEFT JOIN `{project}.{cc_dataset}.cctl_riskunit` AS cctl_riskunit 
				ON cctl_riskunit.ID = cc_riskunit.SubType
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ccrupolicyLocation 
				ON ccrupolicyLocation.ID = cc_riskunit.PolicyLocationID 
		/*	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyaddress --policy address
				ON pc_policyaddress.ID = (
					  select MAX(pc_p_address.id) 
					  from (SELECT * FROM `{project}.{pc_dataset}.pc_policyaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_p_address
					  where pc_p_address.BranchID=pc_policyPeriod.id
					  AND (pc_p_address.EffectiveDate <= cc_claim.LossDate or pc_p_address.EffectiveDate is null)
					  AND (pc_p_address.ExpirationDate > cc_claim.LossDate OR pc_p_address.ExpirationDate is null)                              
				)
		*/
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
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactiononset
				ON cc_transactiononset.OnsetID = cc_ritransaction.ID  

			--If this joins, then this transaction is offsetting transaction, offsetting a different "original" transaction. Could be move or reversal.  
			LEFT OUTER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactionoffset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset
				ON cc_transactionoffset.OffsetID = cc_ritransaction.ID

			--If this joins, then this transaction is an offsetting transaction to an original transaction that also has an onsetting transaction.
			--(this transaction is an offset, but there is an additional onset transaction linked to the transaction that the current transaction is offseting)  
			--This means the current transaction is an offset transaction part of a move.  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_transactiononset` WHERE _PARTITIONTIME = {partition_date}) AS cc_transactionoffset2onset  
				ON cc_transactionoffset2onset.TransactionID = cc_transactionoffset.TransactionID

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

			--BOP Blanket Coverages
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopblanketcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopblanketcov_jmic
				ON cc_coverage.PC_CovPublicId_JMIC = pcx_bopblanketcov_jmic.PublicID
				AND SUBSTR(cc_coverage.PolicySystemId,0,STRPOS(cc_coverage.PolicySystemId,':')-1) = 'entity.BOPBlanketCov_JMIC'
		
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopblanket_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopblanket_jmic
				ON pcx_bopblanket_jmic.BranchID = pcx_bopblanketcov_jmic.BranchID
				AND pcx_bopblanket_jmic.FixedID = pcx_bopblanketcov_jmic.FixedID
				AND (pcx_bopblanket_jmic.EffectiveDate <= cc_claim.LossDate OR pcx_bopblanket_jmic.EffectiveDate IS NULL)
				AND (pcx_bopblanket_jmic.ExpirationDate > cc_claim.LossDate OR pcx_bopblanket_jmic.ExpirationDate IS NULL)

			--BOP Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
				ON  pc_boplocation.BranchID = pcx_bopblanket_jmic.BranchID
				AND pc_boplocation.BOPLine = pcx_bopblanket_jmic.BOPLine
				AND (pc_boplocation.EffectiveDate <= cc_claim.LossDate OR pc_boplocation.EffectiveDate IS NULL)
				AND (pc_boplocation.ExpirationDate > cc_claim.LossDate OR pc_boplocation.ExpirationDate IS NULL)
			
			--Policy Line
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pc_boplocation.BranchID
				AND pc_policyline.FixedID = pc_boplocation.BOPLine	--BOPBlanket_JMIC
				AND (pc_policyline.EffectiveDate <= cc_claim.LossDate OR pc_policyline.EffectiveDate IS NULL)
				AND (pc_policyline.ExpirationDate > cc_claim.LossDate OR pc_policyline.ExpirationDate IS NULL)

			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

			INNER JOIN ClaimBOPCededFinancialsConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN ClaimBOPCededFinancialsConfig coverageLevelConfig ON coverageLevelConfig.Key = 'BlanketLevelCoverage' 

			--add joins to get back to pc_account to allocate the Account Level Jeweler
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
				ON pc_policy.ID = pc_policyPeriod.PolicyID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
				ON pc_account.ID = pc_policy.AccountID
			--Use a subselect to get all possible referring jeweler (Account Level Jeweler)  
       		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jeweler_jm` WHERE _PARTITIONTIME = {partition_date}) As AccountLevelJeweler  
				ON AccountLevelJeweler.ID = pc_account.ReferringJeweler_JM	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode	--properly resolve agency.
				ON pc_producercode.Code = cc_policy.ProducerCode
				AND pc_producercode.Retired = 0     --exclude archived producer records

		/**** TEST *****/
		WHERE 1=1 
			AND cc_coverage.PC_LineCode_JMIC IN ('BOPLine')


) 
  FinTrans
		INNER JOIN ClaimBOPCededFinancialsConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN ClaimBOPCededFinancialsConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN ClaimBOPCededFinancialsConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN ClaimBOPCededFinancialsConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN ClaimBOPCededFinancialsConfig locationLevelRisk
			ON locationLevelRisk.Key = 'LocationLevelRisk'
		INNER JOIN ClaimBOPCededFinancialsConfig buildingRisk
			ON buildingRisk.Key ='BuildingLevelRisk'

	WHERE	1 = 1
	AND	TransactionPublicID IS NOT NULL

--)ExtractData
