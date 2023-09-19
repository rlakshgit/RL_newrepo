-- tag: FinancialTransactionPADirect - tag ends/
/**** Kimberlite - Financial Transactions ********
		FinancialTransactionPADirect.sql
			GCP BQ CONVERTED
**************************************************/
/*
--------------------------------------------------------------------------------------------
 *****  Change History  *****
 
	04/05/2021	DROBAK		Changed RiskJewelryKey to RiskPAJewelryKey
	04/05/2021	DROBAK		Changed 'ScheduledItemRisk' Value to 'PersonalArticleJewelry'
	04/05/2021	DROBAK		Changed ItemCoverageKey Value to PAJewelryCoverageKey
	06/25/2021	DROBAK		add IsTransactionSliceEffective & Primary Locn; update unit tests
	09/01/2021	DROBAK		Change ArticlePublicId from 'None' to CAST(NULL AS STRING) (BQ only change)
	10/05/2021  SLJ			Round AdjustedFullTermAmount to 0 decimals
	10/05/2021  SLJ			Change join coalesce from cost effective to transaction effective dates
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes)
	08/10/2022	DROBAK		Fix: cleanup source for PolicyNumber and add pc_policyperiod.PublicID AS PolicyPeriodPublicID -- BQ version was missing this

--------------------------------------------------------------------------------------------	
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------
	pcx_personalarticlecov_jm.PublicID			AS	CoveragePublicID
	pcx_personalartcllinecov_jm.PublicID		AS	CoveragePublicID
	pcx_jpacost_jm.PublicID						AS CoveragePublicID [For cost level coverage]
-----------------------------------------------------------------------
 *****  Unioned Sections  *****
 -----------------------------------------------------------------------
	Scheduled cov 		--Item Cov - PersonalArticleCov = pcx_personalarticlecov_jm AND pcx_personalarticle_jm
	UnScheduled cov 	--Line Cov - PersonalArtclLineCov = pcx_personalartcllinecov_jm
	Cost cov 			--Cost Cov - Cost Cov = pcx_jpacost_jm
-------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_FinancialTransactionPADirect`
AS SELECT outerquery.*
FROM (
		---with
		---etc code
) outerquery

*/		
			
WITH PADirectFinTrans AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'BusinessType', 'Direct' UNION ALL
			/* CoverageLevel Values */
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'CostCoverage','CostCoverage' UNION ALL
			/* Risk Key Values */
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
	,SegmentCode
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,TransactionAmount
	,TransactionPostedDate
	,TransactionWrittenDate
	,ItemLocatedWith
	,NumDaysInRatedTerm
	,AdjustedFullTermAmount
	,ActualBaseRate
	,ActualAdjRate
	,ActualTermAmount
	,ActualAmount
	,ChargePattern
	,ChargeGroup 
	,ChargeSubGroup
	,RateType
	,RatedState
	,CostPublicID
	,PolicyPeriodPublicID
	,PolicyArticleLocationPublicId
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
		/*													P A      D I R E C T																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 S C H E D U L E D   I T E M	  C O V G   																			*/
		/****************************************************************************************************************************************************************/
		SELECT
			pcx_personalarticlecov_jm.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jpatransaction_jm.PublicID AS TransactionPublicID
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_policyperiod.PolicyNumber
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jpatransaction_jm.Amount AS TransactionAmount
			,pcx_jpatransaction_jm.PostedDate AS TransactionPostedDate
			,pcx_jpatransaction_jm.Written AS TrxnWritten
			,CAST(pcx_jpatransaction_jm.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_jpatransaction_jm.ToBeAccrued AS CanBeEarned
			,CAST(pcx_jpatransaction_jm.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_jpatransaction_jm.ExpDate AS DATE) AS ExpirationDate
			,pcx_jpatransaction_jm.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_jpatransaction_jm.Amount = 0 OR pcx_jpacost_jm.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_jpacost_jm.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_jpatransaction_jm.EffDate, pcx_jpatransaction_jm.ExpDate, DAY) / pcx_jpacost_jm.NumDaysInRatedTerm
						) * --rated factor 
					pcx_jpatransaction_jm.Amount / ABS(pcx_jpatransaction_jm.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(pctl_state.TypeCode, primaryLocState.TypeCode) AS RatedState
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
			,CASE WHEN pcx_jpatransaction_jm.BranchID = pcx_jpacost_jm.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_jpacost_jm.OverrideBaseRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideAdjRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideTermAmount IS NOT NULL
					OR pcx_jpacost_jm.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_jpacost_jm.RateAmountType AS RateType
			,CASE WHEN pcx_jpacost_jm.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_jpacost_jm.Subtype
			--,NULL AS RateBookUsed--Per Gatis - no ratebook
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pcx_jpacost_jm.PublicID AS CostPublicID
			,pc_policyline.PublicID AS PolicyLinePublicID
			,pc_policy.PublicId AS PolicyPublicId
			,pcx_personalarticle_jm.PublicID AS ArticlePublicId
			,pcx_personalarticle_jm.ItemNumber AS ArticleNumber
		   ,CASE pcx_jpacost_jm.ChargeSubGroup  
				WHEN 'MIN_PREMIUM_ADJ'
				THEN MinPremPolLoc.PublicID
				ELSE pc_policylocation.PublicID
			END AS PolicyArticleLocationPublicId 
			,pcx_jpatransaction_jm.CreateTime AS TrxnCreateTime
			,pcx_jpacost_jm.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_jpacost_jm.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_jpacost_jm.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_jpacost_jm.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,COALESCE(pcx_personalarticlecov_jm.PatternCode, pcx_jpacost_jm.ChargeSubGroup, pcx_jpacost_jm.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(pc_policylocation.PublicID, effectivefieldsprimarypolicylocation.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(pc_policylocation.LocationNum, effectivefieldsprimarypolicylocation.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_personalarticle_jm.LocatedWith AS ItemLocatedWith
			,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
	
		-- select pc_policyline.Subtype
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jpatransaction_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpatransaction_jm
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job 
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpacost_jm
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArticleCov IS NOT NULL
			--AND pcx_jpacost_jm.PersonalArtclLineCov IS NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost 
			ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany 
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy 
			ON pc_policy.Id = pc_policyperiod.PolicyId
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS date)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PADirectFinTrans lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PADirectFinTrans coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'ScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_personalarticlecov_jm
			ON pcx_personalarticlecov_jm.FixedID = pcx_jpacost_jm.PersonalArticleCov
			AND pcx_personalarticlecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_personalarticlecov_jm.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_personalarticlecov_jm.ExpirationDate,perCost.PeriodEnd) AS date)			
		
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_personalarticle_jm
			ON pcx_personalarticle_jm.FixedID = pcx_jpacost_jm.PersonalArticle
			AND pcx_personalarticle_jm.BranchID = pcx_jpacost_jm.BranchID 
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_personalarticle_jm.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_personalarticle_jm.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
			ON pc_policylocation.BranchID = pcx_jpacost_jm.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpalocation_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpalocation_jm
			ON pcx_jpalocation_jm.Location = pc_policylocation.FixedID
			AND pcx_jpalocation_jm.BranchID = pcx_jpacost_jm.BranchID
			AND pcx_jpalocation_jm.FixedID = pcx_personalarticle_jm.JPALocation
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern 
			ON costChargePattern.ID = pcx_jpacost_jm.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS date)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment 
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_jurisdiction` costratedState
			ON costratedState.ID = pc_policyperiod.BaseState--Per Gatis - get costrated state from policy period base state and Jurisdiction.
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` pctl_state --downstream processes expect the ID to be a pctl_state id, not jurisdiction.
			ON costratedstate.TYPECODE = pctl_state.TYPECODE 
		--Get the minimum location (number) for this line and branch
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_jpacost_jm.BranchID
			AND MinPremPolLoc.StateInternal = pctl_state.ID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState ON effectiveLocState.ID = pc_policylocation.StateInternal

		WHERE	1 = 1
		AND pcx_jpatransaction_jm.PostedDate IS NOT NULL
		/**** TEST *****/    
	--	AND pc_policyperiod.PolicyNumber = vpolicynumber

	UNION ALL

		/****************************************************************************************************************************************************************/
		/*													P A      D I R E C T																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 U N  S C H E D U L E D   L I N E	 C O V G    																		*/
		/****************************************************************************************************************************************************************/
		SELECT
			pcx_personalartcllinecov_jm.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jpatransaction_jm.PublicID AS TransactionPublicID
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_policyperiod.PolicyNumber
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jpatransaction_jm.Amount AS TransactionAmount
			,pcx_jpatransaction_jm.PostedDate AS TransactionPostedDate
			,pcx_jpatransaction_jm.Written AS TrxnWritten
			,CAST(pcx_jpatransaction_jm.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_jpatransaction_jm.ToBeAccrued AS CanBeEarned
			,CAST(pcx_jpatransaction_jm.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_jpatransaction_jm.ExpDate AS DATE) AS ExpirationDate
			,pcx_jpatransaction_jm.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_jpatransaction_jm.Amount = 0 OR pcx_jpacost_jm.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_jpacost_jm.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_jpatransaction_jm.EffDate, pcx_jpatransaction_jm.ExpDate, DAY) / pcx_jpacost_jm.NumDaysInRatedTerm
						) * --rated factor 
					pcx_jpatransaction_jm.Amount / ABS(pcx_jpatransaction_jm.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(pctl_state.TypeCode, primaryLocState.TypeCode) AS RatedState
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
			,CASE WHEN pcx_jpatransaction_jm.BranchID = pcx_jpacost_jm.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_jpacost_jm.OverrideBaseRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideAdjRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideTermAmount IS NOT NULL
					OR pcx_jpacost_jm.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_jpacost_jm.RateAmountType AS RateType
			,CASE WHEN pcx_jpacost_jm.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_jpacost_jm.Subtype
			--,NULL AS RateBookUsed--Per Gatis - no ratebook
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pcx_jpacost_jm.PublicID AS CostPublicID
			,pc_policyline.PublicID AS PolicyLinePublicID
			,pc_policy.PublicId AS PolicyPublicId
			--,'NONE' AS ArticlePublicId
			,CAST(NULL AS STRING) AS ArticlePublicId
			,CAST(NULL AS INT64) AS ArticleNumber
			,'None' AS PolicyArticleLocationPublicId 
			,pcx_jpatransaction_jm.CreateTime AS TrxnCreateTime
			,pcx_jpacost_jm.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_jpacost_jm.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_jpacost_jm.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_jpacost_jm.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,COALESCE(pcx_personalartcllinecov_jm.PatternCode, pcx_jpacost_jm.ChargeSubGroup, pcx_jpacost_jm.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(effectivefieldsprimarypolicylocation.PublicID, MinPremPolLoc.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(effectivefieldsprimarypolicylocation.LocationNum, MinPremPolLoc.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,NULL AS ItemLocatedWith
			,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
			) AS TransactionRank
	
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jpatransaction_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpatransaction_jm
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpacost_jm
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS date)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PADirectFinTrans lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PADirectFinTrans coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'UnScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_personalartcllinecov_jm
			ON pcx_personalartcllinecov_jm.FixedID = pcx_jpacost_jm.PersonalArtclLineCov
			AND pcx_personalartcllinecov_jm.BranchID = pcx_jpacost_jm.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,perCost.PeriodEnd) AS date)			
	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern ON costChargePattern.ID = pcx_jpacost_jm.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS date)
		
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		LEFT JOIN `{project}.{pc_dataset}.pctl_jurisdiction` costratedState
			ON costratedState.ID = pc_policyperiod.BaseState--Per Gatis - get costrated state from policy period base state and Jurisdiction.
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` pctl_state --downstream processes expect the ID to be a pctl_state id, not jurisdiction.
			ON costratedstate.TYPECODE = pctl_state.TYPECODE 
		--Get the minimum location (number) for this line and branch
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_jpacost_jm.BranchID
			AND MinPremPolLoc.StateInternal = pctl_state.ID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState ON effectiveLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal

      WHERE	1 = 1
	  AND pcx_jpatransaction_jm.PostedDate IS NOT NULL
      /**** TEST *****/    
    --  AND pc_policyperiod.PolicyNumber = vpolicynumber
	
	UNION ALL

		/****************************************************************************************************************************************************************/
		/*													P A      D I R E C T																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 													C O S T   L E V E L	 C O V G    																			*/
		/****************************************************************************************************************************************************************/
		SELECT
			pcx_jpacost_jm.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jpatransaction_jm.PublicID AS TransactionPublicID
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_policyperiod.PolicyNumber
			,pc_job.JobNumber AS JobNumber	
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_jpacost_jm.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jpatransaction_jm.Amount AS TransactionAmount
			,pcx_jpatransaction_jm.PostedDate AS TransactionPostedDate
			,pcx_jpatransaction_jm.Written AS TrxnWritten
			,CAST(pcx_jpatransaction_jm.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_jpatransaction_jm.ToBeAccrued AS CanBeEarned
			,CAST(pcx_jpatransaction_jm.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_jpatransaction_jm.ExpDate AS DATE) AS ExpirationDate
			,pcx_jpatransaction_jm.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_jpatransaction_jm.Amount = 0 OR pcx_jpacost_jm.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_jpacost_jm.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_jpatransaction_jm.EffDate, pcx_jpatransaction_jm.ExpDate, DAY) / pcx_jpacost_jm.NumDaysInRatedTerm
						) * --rated factor 
					pcx_jpatransaction_jm.Amount / ABS(pcx_jpatransaction_jm.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(pctl_state.TypeCode, primaryLocState.TypeCode) AS RatedState
			,pcx_jpacost_jm.ActualBaseRate
			,pcx_jpacost_jm.ActualAdjRate
			,pcx_jpacost_jm.ActualTermAmount
			,pcx_jpacost_jm.ActualAmount
			--,pcx_jpacost_jm.ChargePattern
			,pcx_jpacost_jm.ChargeGroup		--stored as a code, not ID. 
			,pcx_jpacost_jm.ChargeSubGroup	--stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			/*,CASE WHEN pcx_jpacost_jm.EffectiveDate IS NOT NULL
					THEN CONVERT(DATETIME, CONVERT(CHAR(10), pcx_jpacost_jm.EffectiveDate, 101))
				END AS CostEffectiveDate 
			,CASE WHEN pcx_jpacost_jm.ExpirationDate IS NOT NULL
					THEN CONVERT(DATETIME, CONVERT(CHAR(10), pcx_jpacost_jm.ExpirationDate, 101))
				ELSE CONVERT(DATETIME, CONVERT(CHAR(10), perCost.PeriodEnd, 101))
				END AS CostExpirationDate
			*/
			,CASE WHEN pcx_jpatransaction_jm.BranchID = pcx_jpacost_jm.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_jpacost_jm.OverrideBaseRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideAdjRate IS NOT NULL
					OR pcx_jpacost_jm.OverrideTermAmount IS NOT NULL
					OR pcx_jpacost_jm.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_jpacost_jm.RateAmountType AS RateType
			,CASE WHEN pcx_jpacost_jm.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_jpacost_jm.Subtype
			--,NULL AS RateBookUsed--Per Gatis - no ratebook
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pcx_jpacost_jm.PublicID AS CostPublicID
			,pc_policyline.PublicID AS PolicyLinePublicID
			,pc_policy.PublicId AS PolicyPublicId
			,CAST(NULL AS STRING) AS ArticlePublicId
			,CAST(NULL AS INT64) AS ArticleNumber
			,CAST(NULL AS STRING) AS PolicyArticleLocationPublicId 
			,pcx_jpatransaction_jm.CreateTime AS TrxnCreateTime
			,pcx_jpacost_jm.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE
				WHEN pcx_jpacost_jm.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_jpacost_jm.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_jpacost_jm.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,COALESCE(pcx_jpacost_jm.ChargeSubGroup, pcx_jpacost_jm.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(effectivefieldsprimarypolicylocation.PublicID, MinPremPolLoc.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(effectivefieldsprimarypolicylocation.LocationNum, MinPremPolLoc.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,NULL AS ItemLocatedWith
			,ROW_NUMBER() OVER(PARTITION BY pcx_jpatransaction_jm.ID
				ORDER BY 
					IFNULL(pcx_jpacost_jm.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jpatransaction_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpatransaction_jm
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod ON pcx_jpatransaction_jm.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) pcx_jpacost_jm
			ON pcx_jpatransaction_jm.JPACost_JM = pcx_jpacost_jm.ID
			AND pcx_jpacost_jm.PersonalArtclLineCov IS NULL AND pcx_jpacost_jm.PersonalArticleCov IS NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost ON pcx_jpacost_jm.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pcx_jpacost_jm.PersonalArtclLine_JM = pc_policyline.FixedID
			AND pcx_jpacost_jm.BranchID = pc_policyline.BranchID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS date)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PADirectFinTrans lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PADirectFinTrans coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'CostCoverage'

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern ON costChargePattern.ID = pcx_jpacost_jm.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS date)

		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_jpacost_jm.BranchID
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		LEFT JOIN `{project}.{pc_dataset}.pctl_jurisdiction` costratedState
			ON costratedState.ID = pc_policyperiod.BaseState--Per Gatis - get costrated state from policy period base state and Jurisdiction.
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` pctl_state --downstream processes expect the ID to be a pctl_state id, not jurisdiction.
			ON costratedstate.TYPECODE = pctl_state.TYPECODE  
		--Get the minimum location (number) for this line and branch
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_jpacost_jm.BranchID
			AND MinPremPolLoc.StateInternal = pctl_state.ID
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jpatransaction_jm.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState ON effectiveLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal

      WHERE	1 = 1
	  AND pcx_jpatransaction_jm.PostedDate IS NOT NULL      
      /**** TEST *****/    
    --  AND pc_policyperiod.PolicyNumber = vpolicynumber


	) FinTrans
	INNER JOIN PADirectFinTrans sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN PADirectFinTrans hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN PADirectFinTrans hashAlgorithm
		ON hashAlgorithm.Key = 'HashAlgorithm'
	INNER JOIN PADirectFinTrans businessTypeConfig
		ON businessTypeConfig.Key = 'BusinessType'
	INNER JOIN PADirectFinTrans itemRisk
		ON itemRisk.Key='ScheduledItemRisk'

	WHERE 1=1
	--	AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
		AND	TransactionPublicID IS NOT NULL
		AND TransactionRank = 1		

) extractData 
