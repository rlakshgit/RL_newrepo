/*TEST*/
--DECLARE vpolicynumber STRING; DECLARE {partition_date} Timestamp;
--SET vpolicynumber = 'P000000736';
/* List for Line Cov */  -- 'P000000013', P000000078, P000000070, P000000130, P000000691, P000000591, 
/* list for Item Cov */  -- P000000731, P000000732, P000000734, P000000735, P000000736
/**********************************************************************************************************************************/



/**** Kimberlite - Financial Transactions ********
		FinancialTransactionPACeded.sql
			GCP Converted
**************************************************/
/*
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/23/2021	DROBAK		Modified JOIN for pcx_personalartcllinecov_jm
							Renamed Config table to PACededFinancialsConfig
	04/05/2021	DROBAK		Changed RiskJewelryKey to RiskPAJewelryKey
	04/05/2021	DROBAK		Changed 'ScheduledItemRisk' Value to 'PersonalArticleJewelry'
	04/05/2021	DROBAK		Changed ItemCoverageKey Value to PAJewelryCoverageKey
	06/29/2021	DROBAK		add IsTransactionSliceEffective & Primary Locn; update unit tests
	09/01/2021	DROBAK		Change ArticlePublicId from 'None' to CAST(NULL AS STRING) (BQ only change)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes); fixed NULL for ArticleNumber

--------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------
	pcx_personalarticlecov_jm.PublicID			AS	CoveragePublicID
	pcx_personalartcllinecov_jm.PublicID		AS	CoveragePublicID
-----------------------------------------------------------------------
 *****  Unioned Sections  *****
 -----------------------------------------------------------------------
	Scheduled cov 		--Item Cov - PersonalArticleCov = pcx_personalarticlecov_jm AND pcx_personalarticle_jm
	UnScheduled cov 	--Line Cov - PersonalArtclLineCov = pcx_personalartcllinecov_jm
-----------------------------------------------------------------------------------------------------------------------------------
	TEST RESULTS:
		There are no Ceded transaction in DW to date
-----------------------------------------------------------------------------------------------------------------------------------
*/		
/* Set universal latest partitiontime - In future will come from Airflow DAG (Today - 1) 

SET {partition_date} = '2020-11-30'; -- (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm`);

CREATE OR REPLACE TABLE `{project}.bl_kimberlite.PACededFinancialsConfig`
	(
		key STRING,
		value STRING
	);
	INSERT INTO 
			`{project}.bl_kimberlite.PACededFinancialsConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256') 
			,('LineCode','PersonalArtclLine_JM')
			,('BusinessType', 'Ceded')
			,('ScheduledCoverage','ScheduledCov')
			,('UnScheduledCoverage','UnScheduledCov')
			,('NoCoverage','NoCoverage')
			,('ScheduledItemRisk', 'PersonalArticleJewelry');
*/

WITH PACededFinTrans AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'BusinessType', 'Ceded' UNION ALL
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'ScheduledItemRisk', 'PersonalArticleJewelry'
)


SELECT
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,PAJewelryCoverageKey
	,RiskPAJewelryKey
	,BusinessType
	,ArticlePublicId
	,CoveragePublicID
	,TransactionPublicID
	,PrimaryPolicyLocationNumber
	,PrimaryPolicyLocationPublicID
	,ArticleNumber
	,JobNumber
	,IsTransactionSliceEffective
	,PolicyNumber
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,TransactionAmount
	,TransactionPostedDate
	,TransactionWrittenDate
	,ItemLocatedWith
	,NumDaysInRatedTerm
	,AdjustedFullTermAmount
	,ActualTermAmount 
	,ActualAmount 
	,ChargePattern 
	,ChargeGroup 
	,ChargeSubGroup 
	,RateType
	,CedingRate
	,CededAmount
	,CededCommissions
	,CededTermAmount
	,RIAgreementType
	,RIAgreementNumber
	,CededID
	,CededAgreementID
	--,RICoverageGroupID
	,RICoverageGroupType
	,CostPublicID
	,RatedState
	,PolicyArticleLocationPublicId
	,PolicyPeriodPublicID
	,LineCode
    ,DATE('{date}') as bq_load_date			

--INTO #FinTrans
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS PAJewelryCoverageKey
		,CASE WHEN ArticlePublicId IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ArticlePublicId,hashKeySeparator.Value,itemRisk.Value)) 
		END AS RiskPAJewelryKey
		,businessTypeConfig.Value AS BusinessType
		,FinTrans.*
	FROM (

		/****************************************************************************************************************************************************************/
		/*													P A      C E D E D																							*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 S C H E D U L E D   I T E M	 C O V G	   																			*/
		/****************************************************************************************************************************************************************/
		SELECT
			pcx_personalarticlecov_jm.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jpacededpremiumtrans_jm.PublicID AS TransactionPublicID
			,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jpacededpremiumtrans_jm.CededPremium AS TransactionAmount
			,pcx_jpacededpremiumtrans_jm.CreateTime AS TransactionPostedDate
			--,CASE WHEN pcx_jpacededpremiumtrans_jm.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_jpacededpremiumtrans_jm.DatePosted AS DATE), CAST(pcx_jpacededpremiumtrans_jm.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, perCost.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			,1 AS CanBeEarned
			,CAST(pcx_jpacededpremiumtrans_jm.EffectiveDate AS DATE) AS EffectiveDate
			,CAST(pcx_jpacededpremiumtrans_jm.ExpirationDate AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged
			--,pcx_jpacededpremiumtrans_jm.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_jpacost_jm.ActualBaseRate
			,pcx_jpacost_jm.ActualAdjRate
			,pcx_jpacost_jm.ActualTermAmount
			,pcx_jpacost_jm.ActualAmount
			,pcx_jpacost_jm.ChargeGroup --stored as a code, not ID. 
			,pcx_jpacost_jm.ChargeSubGroup --stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			/*,CASE WHEN pcx_jpacost_jm.EffectiveDate IS NOT NULL
					THEN CAST(pcx_jpacost_jm.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_jpacost_jm.ExpirationDate IS NOT NULL
					THEN CAST(pcx_jpacost_jm.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,1 AS Onset
			,CASE WHEN pcx_jpacost_jm.OverrideBaseRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideAdjRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideTermAmount IS NOT NULL
					OR pcx_jpacost_jm.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_jpacost_jm.RateAmountType AS RateType
			--,CASE WHEN pcx_jpacost_jm.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_jpacost_jm.Subtype
			--,'None' AS RateBookUsed--Per Gatis - no ratebook
			,perCost.PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_jpacost_jm.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			,pcx_personalarticle_jm.PublicID AS ArticlePublicId
			,CAST(pcx_personalarticle_jm.ItemNumber AS STRING) AS ArticleNumber
			,CASE pcx_jpacost_jm.ChargeSubGroup 
				WHEN 'MIN_PREMIUM_ADJ'
				THEN MinPremPolLoc.PublicID
				ELSE pc_policylocation.PublicID
			END AS PolicyArticleLocationPublicId 
			--,pcx_jpacededpremiumtrans_jm.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_jpacost_jm.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate,pcx_jpacededpremium_jm.EffectiveDate), IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate), DAY) AS FLOAT64) <> 0 
				THEN 
					CASE 
						WHEN IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate,pcx_jpacededpremium_jm.EffectiveDate) = perCost.PeriodStart and IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate) = perCost.PeriodEnd
						THEN pcx_jpacededpremiumtrans_jm.CededPremium
						ELSE CAST(CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) * pcx_jpacededpremiumtrans_jm.CededPremium / 
								CAST(TIMESTAMP_DIFF(IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate, pcx_jpacededpremium_jm.EffectiveDate), IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL)
						END 
				ELSE 0 
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,COALESCE(pcx_personalarticlecov_jm.PatternCode, pcx_jpacost_jm.ChargeSubGroup, pcx_jpacost_jm.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(pc_policylocation.PublicID, effectivefieldsprimarypolicylocation.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(pc_policylocation.LocationNum, effectivefieldsprimarypolicylocation.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,CAST(pcx_personalarticle_jm.LocatedWith AS STRING) AS ItemLocatedWith
			--Ceding Data
			,pcx_jpacededpremiumtrans_jm.CedingRate AS CedingRate
			,pcx_jpacededpremiumtrans_jm.CededPremium AS CededAmount
			,pcx_jpacededpremiumtrans_jm.Commission AS CededCommissions
			,pcx_jpacost_jm.ActualTermAmount * pcx_jpacededpremiumtrans_jm.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_jpacededpremium_jm.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			--,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,ROW_NUMBER() OVER(PARTITION BY pcx_jpacededpremiumtrans_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremiumtrans_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacededpremiumtrans_jm
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremium_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacededpremium_jm
			ON pcx_jpacededpremium_jm.ID = pcx_jpacededpremiumtrans_jm.JPACededPremium_JM
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
			ON pcx_jpacededpremium_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArticleCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy ON pc_policy.Id = perCost.PolicyId 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PACededFinTrans lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PACededFinTrans coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'ScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticlecov_jm
			ON pcx_personalarticlecov_jm.FixedID = pcx_jpacost_jm.PersonalArticleCov
			AND pcx_personalarticlecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalarticlecov_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalarticlecov_jm.ExpirationDate,perCost.PeriodEnd)			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticle_jm
			ON pcx_personalarticle_jm.FixedID = pcx_jpacost_jm.PersonalArticle
			AND pcx_personalarticle_jm.BranchID = pcx_jpacost_jm.BranchID 
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalarticle_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalarticle_jm.ExpirationDate,perCost.PeriodEnd)
		--Cede Agreement
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement 
			ON pc_reinsuranceagreement.ID = pcx_jpacededpremiumtrans_jm.Agreement
		INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement 
			ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup  
			ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pacov 
			ON (pacov.ID = pcx_jpacost_jm.PersonalArticleCov)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS pa 
			ON (pa.ID = pacov.PersonalArticle)
		--Scheduled Item 	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON pc_policylocation.BranchID = pcx_jpacost_jm.BranchID
				AND pc_policylocation.FixedID = pcx_personalarticle_jm.JPALocation
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd)	
		--look up ceded premiums transaction's branch id
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm 
			ON pc_policyterm.ID = pcx_jpacededpremium_jm.PolicyTerm
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				AND pc_policyperiod.ModelDate < pcx_jpacededpremiumtrans_jm.CalcTimestamp
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_jpacost_jm.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_jpacost_jm.BranchID
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_jpacost_jm.BranchID
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		LEFT JOIN `{project}.{pc_dataset}.pctl_jurisdiction` AS costratedState
			ON costratedState.ID = pc_policyperiod.BaseState--Per Gatis - get costrated state from policy period base state and Jurisdiction.
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state --downstream processes expect the ID to be a pctl_state id, not jurisdiction.
			ON costratedstate.TYPECODE = pctl_state.TYPECODE 
		--Get the minimum location (number) for this line and branch
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_jpacost_jm.BranchID
			AND MinPremPolLoc.StateInternal = pctl_state.ID
			AND pctl_state.TYPECODE = costratedState.TYPECODE
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState ON effectiveLocState.ID = pc_policylocation.StateInternal

		WHERE	1 = 1
      
		/**** TEST *****/    
	--	AND pc_policyperiod.PolicyNumber = vpolicynumber

	UNION ALL

		/****************************************************************************************************************************************************************/
		/*													P A      C E D E D																							*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										      U N  S C H E D U L E D   C O V G	    																			*/
		/****************************************************************************************************************************************************************/
		SELECT
			pcx_personalartcllinecov_jm.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jpacededpremiumtrans_jm.PublicID AS TransactionPublicID
			,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jpacededpremiumtrans_jm.CededPremium AS TransactionAmount
			,pcx_jpacededpremiumtrans_jm.CreateTime AS TransactionPostedDate
			--,CASE WHEN pcx_jpacededpremiumtrans_jm.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_jpacededpremiumtrans_jm.DatePosted AS DATE), CAST(pcx_jpacededpremiumtrans_jm.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, perCost.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			,1 AS CanBeEarned
			,CAST(pcx_jpacededpremiumtrans_jm.EffectiveDate AS DATE) AS EffectiveDate
			,CAST(pcx_jpacededpremiumtrans_jm.ExpirationDate AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged
			--,pcx_jpacededpremiumtrans_jm.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_jpacost_jm.ActualBaseRate
			,pcx_jpacost_jm.ActualAdjRate
			,pcx_jpacost_jm.ActualTermAmount
			,pcx_jpacost_jm.ActualAmount
			,pcx_jpacost_jm.ChargeGroup --stored as a code, not ID. 
			,pcx_jpacost_jm.ChargeSubGroup --stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			/*,CASE WHEN pcx_jpacost_jm.EffectiveDate IS NOT NULL
					THEN CAST(pcx_jpacost_jm.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_jpacost_jm.ExpirationDate IS NOT NULL
					THEN CAST(pcx_jpacost_jm.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,1 AS Onset
			,CASE WHEN pcx_jpacost_jm.OverrideBaseRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideAdjRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideTermAmount IS NOT NULL
					OR pcx_jpacost_jm.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0 
				END AS CostOverridden
			,pcx_jpacost_jm.RateAmountType AS RateType
			--,CASE WHEN pcx_jpacost_jm.RateAmountType = 3 THEN 0  ELSE 1 END AS IsPremium	-- Tax or surcharge 
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_jpacost_jm.Subtype
			--,'None' AS RateBookUsed--Per Gatis - no ratebook
			,perCost.PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_jpacost_jm.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,'None' AS ArticlePublicId
			,CAST(NULL AS STRING) AS ArticlePublicId
			,CAST(NULL AS STRING) AS ArticleNumber
			,'None' PolicyArticleLocationPublicId 
			--,pcx_jpacededpremiumtrans_jm.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd  AS DATE) AS PeriodEnd
			,pcx_jpacost_jm.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate,pcx_jpacededpremium_jm.EffectiveDate), IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate), DAY) AS FLOAT64) <> 0 
				THEN 
					CASE 
						WHEN IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate,pcx_jpacededpremium_jm.EffectiveDate) = perCost.PeriodStart and IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate) = perCost.PeriodEnd
						THEN pcx_jpacededpremiumtrans_jm.CededPremium
						ELSE CAST(CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) * pcx_jpacededpremiumtrans_jm.CededPremium / 
								CAST(TIMESTAMP_DIFF(IFNULL(pcx_jpacededpremiumtrans_jm.EffectiveDate, pcx_jpacededpremium_jm.EffectiveDate), IFNULL(pcx_jpacededpremiumtrans_jm.ExpirationDate,pcx_jpacededpremium_jm.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL)
						END 
				ELSE 0 
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,COALESCE(pcx_personalartcllinecov_jm.PatternCode, pcx_jpacost_jm.ChargeSubGroup, pcx_jpacost_jm.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(effectivefieldsprimarypolicylocation.PublicID, MinPremPolLoc.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(effectivefieldsprimarypolicylocation.LocationNum, MinPremPolLoc.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,'None' AS ItemLocatedWith
			--Ceding Data
			,pcx_jpacededpremiumtrans_jm.CedingRate AS CedingRate
			,pcx_jpacededpremiumtrans_jm.CededPremium AS CededAmount
			,pcx_jpacededpremiumtrans_jm.Commission AS CededCommissions
			,pcx_jpacost_jm.ActualTermAmount * pcx_jpacededpremiumtrans_jm.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_jpacededpremium_jm.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			--,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,ROW_NUMBER() OVER(PARTITION BY pcx_jpacededpremiumtrans_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
			) AS TransactionRank

		-- select pc_policyline.Subtype
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremiumtrans_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacededpremiumtrans_jm
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacededpremium_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacededpremium_jm
			ON pcx_jpacededpremium_jm.ID = pcx_jpacededpremiumtrans_jm.JPACededPremium_JM
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
			ON pcx_jpacededpremium_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy ON pc_policy.Id = perCost.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PACededFinTrans lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PACededFinTrans coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'ScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalartcllinecov_jm
			ON pcx_personalartcllinecov_jm.FixedID = pcx_jpacost_jm.PersonalArtclLineCov
			AND pcx_personalartcllinecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,perCost.PeriodEnd)
		--Cede Agreement
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement 
			ON pc_reinsuranceagreement.ID = pcx_jpacededpremiumtrans_jm.Agreement
		INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement 
			ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pacov 
			ON (pacov.ID = pcx_jpacost_jm.PersonalArticleCov)
		--LEFT JOIN `{project}.{pc_dataset}.pcx_personalarticle_jm` pa on (pa.ID = pacov.PersonalArticle)
		--look up ceded premiums transaction's branch id
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm 
			ON pc_policyterm.ID = pcx_jpacededpremium_jm.PolicyTerm
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				AND pc_policyperiod.ModelDate < pcx_jpacededpremiumtrans_jm.CalcTimestamp
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_jpacost_jm.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_jpacost_jm.BranchID
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_jpacost_jm.BranchID
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment ON pctl_segment.Id = perCost.Segment
		LEFT JOIN `{project}.{pc_dataset}.pctl_jurisdiction` AS costratedState
			ON costratedState.ID = pc_policyperiod.BaseState--Per Gatis - get costrated state from policy period base state and Jurisdiction.
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state --downstream processes expect the ID to be a pctl_state id, not jurisdiction.
			ON costratedstate.TYPECODE = pctl_state.TYPECODE 
		--Get the minimum location (number) for this line and branch
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_jpacost_jm.BranchID
			AND MinPremPolLoc.StateInternal = pctl_state.ID
			AND pctl_state.TYPECODE = costratedState.TYPECODE
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal

		WHERE	1 = 1
     
		/**** TEST *****/    
	--	AND pc_policyperiod.PolicyNumber = vpolicynumber


		) FinTrans
		INNER JOIN PACededFinTrans sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN PACededFinTrans hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN PACededFinTrans hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN PACededFinTrans businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN PACededFinTrans itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
		--	AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1
		
) extractData 