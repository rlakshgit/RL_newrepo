/*TEST*/

--DECLARE vPartition Timestamp;
--SET vpolicynumber = '24-1209765';
/* List for Line Cov */  -- '24-044040', 24-415429, 24-721520, 24-516549, 24-011749, 24-1199418, 24-1209765
/* list for Item Cov */  -- '24-067380'	
/**********************************************************************************************************************************/



/**** Kimberlite - Financial Transactions ********
		FinancialTransactionPJDirect.sql
			Converted to BQ
**************************************************/
/*		
--------------------------------------------------------------------------------------------
	*****  Change History  *****

	02/23/2021	DROBAK		Modified JOIN for PolicyCenter.dbo.pcx_jewelryitem_jmic_pl
	04/05/2021	DROBAK		Changed 'ScheduledItemRisk' to value = 'PersonalJewelryItem'
	04/05/2021	DROBAK		Changed RiskJewelryKey to RiskJewelryItemKey
	06/28/2021	DROBAK		add IsTransactionSliceEffective & Primary Locn; update unit tests 
	09/03/2021	DROBAK		Change ArticlePublicId from 'None' to CAST(NULL AS STRING) (BQ only change); Set ItemNumber back to INT from STRING
	09/10/2021	DROBAK		Changeed Data Type from Float64 to Decimal for AdjustedFullTermAmount
	10/05/2021  SLJ			Round AdjustedFullTermAmount to 0 decimals
	10/05/2021  SLJ			Change join coalesce from cost effective to transaction effective dates
	11/08/2021	DROBAK		Corrected join for pcx_jewelryitem_jmic_pl (to match other queries)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes)

--------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
--------------------------------------------------------------------------------------------
	pcx_jwryitemcov_jmic_pl.PublicID			AS	CoveragePublicID
	pcx_jmpersonallinecov.PublicID				AS	CoveragePublicID
--------------------------------------------------------------------------------------------
 *****  Unioned Sections  *****
 -----------------------------------------------------------------------
	Scheduled cov 		--Item Cov - JewelryItemCov_JMIC_PL = pcx_jwryitemcov_jmic_pl AND JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl
	UnScheduled cov 	--Line Cov - JewelryLineCov_JMIC_PL = pcx_jmpersonallinecov
--------------------------------------------------------------------------------------------
*/

WITH PJDirectFinancialsConfig AS (

  SELECT 'BusinessType' as Key, 'Direct' as Value UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'SourceSystem','GW' UNION ALL
  SELECT 'ScheduledItemRisk','PersonalJewelryItem' UNION ALL 
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'LineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'NoCoverage','NoCoverage'
)
SELECT
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,ItemCoverageKey
	,RiskJewelryItemKey
	,BusinessType
	,CoveragePublicID
	,TransactionPublicID
	,JobNumber
	,IsTransactionSliceEffective
	,PolicyNumber
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,TransactionAmount
	--,TransactionPostedDate
	,TIMESTAMP_TRUNC(TIMESTAMP(TransactionPostedDate), SECOND, 'UTC')  AS TransactionPostedDate
	,TransactionWrittenDate
	,ItemPublicId
	,ItemNumber
	,ItemLocatedWith
	,NumDaysInRatedTerm
	,AdjustedFullTermAmount
	,ActualTermAmount 
	,ActualAmount 
	,ChargePattern
	,ChargeGroup 
	,ChargeSubGroup
	,RateType
	,RatedState
	,CostPublicID
	,PolicyPeriodPublicID
	,PolicyItemLocationPublicId
	,LineCode
	,DATE('{date}') as bq_load_date

FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS ItemCoverageKey
		,CASE WHEN ItemPublicId IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ItemPublicId,hashKeySeparator.Value,itemRisk.Value)) 
		END AS RiskJewelryItemKey
		,businessTypeConfig.Value AS BusinessType
		,FinTrans.*
	FROM (
		/****************************************************************************************************************************************************************/
		/*													P J      D I R E C T																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 S C H E D U L E D   I T E M	  C O V G   																			*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_jwryitemcov_jmic_pl.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_jmtransaction.PublicID AS TransactionPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_policyline.PublicID AS PolicyLinePublicID
			,pc_policyperiod.PolicyNumber
			,CASE pcx_cost_jmic.ChargeSubGroup
				WHEN 'MIN_PREMIUM_ADJ'
					THEN CAST(MinPremPolLoc.PublicID AS STRING)
					ELSE CAST(pc_policylocation.PublicID AS STRING)
				END AS PolicyItemLocationPublicId
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_cost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jmtransaction.Amount AS TransactionAmount
			,pcx_jmtransaction.PostedDate AS TransactionPostedDate
			,CAST(pcx_jmtransaction.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_jmtransaction.ToBeAccrued AS CanBeEarned
			,CAST(pcx_jmtransaction.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_jmtransaction.ExpDate AS DATE) AS ExpirationDate
			--,pcx_jmtransaction.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_jmtransaction.Amount = 0 OR pcx_cost_jmic.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_cost_jmic.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_jmtransaction.EffDate, pcx_jmtransaction.ExpDate, DAY) / pcx_cost_jmic.NumDaysInRatedTerm
						) * --rated factor 
					pcx_jmtransaction.Amount / ABS(pcx_jmtransaction.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_cost_jmic.ActualBaseRate
			,pcx_cost_jmic.ActualAdjRate
			,pcx_cost_jmic.ActualTermAmount
			,pcx_cost_jmic.ActualAmount
			--,pcx_cost_jmic.ChargePattern
			,pcx_cost_jmic.ChargeGroup		--stored as a code, not ID. 
			,pcx_cost_jmic.ChargeSubGroup	--stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			,CASE WHEN pcx_cost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_cost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			,CASE WHEN pcx_jmtransaction.BranchID = pcx_cost_jmic.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_cost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_cost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_cost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_cost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_cost_jmic.RateAmountType AS RateType
			--,CASE WHEN pcx_cost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			--,pcx_cost_jmic.Subtype AS CostSubtypeCode
			,pcx_cost_jmic.RateBookUsed
			,pcx_cost_jmic.PublicID AS CostPublicID
			--,pc_policy.PublicId AS PolicyPublicId
			,pcx_jmtransaction.CreateTime AS TrxnCreateTime
			,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_cost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_cost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_cost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_cost_jmic.NumDaysInRatedTerm AS FLOAT64))AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,pcx_jwryitemcov_jmic_pl.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_jwryitemcov_jmic_pl.PatternCode, pcx_cost_jmic.ChargeSubGroup, pcx_cost_jmic.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(pc_policylocation.PublicID, effectivefieldsprimarypolicylocation.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(pc_policylocation.LocationNum, effectivefieldsprimarypolicylocation.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,CAST(effectiveLocState.TYPECODE AS STRING) AS CoverageLocationEffLocationStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--items
			,CAST(pcx_jewelryitem_jmic_pl.PublicID AS STRING) AS ItemPublicId
			,pcx_jewelryitem_jmic_pl.ItemNumber
			,CAST(pcx_jewelryitem_jmic_pl.LocatedWith AS STRING) AS ItemLocatedWith
			,ROW_NUMBER() OVER(PARTITION BY pcx_jmtransaction.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		-- select pc_policyline.Subtype policynumber = '24-1209765'
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jmtransaction` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmtransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pcx_jmtransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
			ON pcx_jmtransaction.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryItemCov_JMIC_PL IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost
			ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
			ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS date)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PJDirectFinancialsConfig lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PJDirectFinancialsConfig coverageLevelConfigScheduledItem
			ON coverageLevelConfigScheduledItem.Key = 'ScheduledCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jwryitemcov_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jwryitemcov_jmic_pl
			ON pcx_jwryitemcov_jmic_pl.FixedID = pcx_cost_jmic.JewelryItemCov_JMIC_PL
			AND pcx_jwryitemcov_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,perCost.PeriodEnd) AS date)		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelryitem_jmic_pl
			--ON pcx_jewelryitem_jmic_pl.ID = pcx_jwryitemcov_jmic_pl.JewelryItem_JMIC_PL	--11/10/2021 replaced with below 4 lines
			ON pcx_jewelryitem_jmic_pl.FixedID = pcx_cost_jmic.JewelryItem_JMIC_PL
			AND pcx_jewelryitem_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_cost_jmic.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS date)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_cost_jmic.BranchID
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--Get the minimum location (number) for this line and branch
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_cost_jmic.BranchID
			AND MinPremPolLoc.StateInternal = pcx_cost_jmic.RatedState
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costratedState 
			ON costratedState.ID = pcx_cost_jmic.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		--Scheduled Item Joins only
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = pcx_cost_jmic.BranchID
			AND pc_policylocation.FixedID = pcx_jewelryitem_jmic_pl.Location
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal

      WHERE	1 = 1
	
      /**** TEST *****/    
     -- AND pc_policyperiod.PolicyNumber = vpolicynumber

UNION ALL
		/****************************************************************************************************************************************************************/
		/*													P J      D I R E C T																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 U N  S C H E D U L E D   L I N E	C O V G  																			*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_jmpersonallinecov.PublicID AS CoveragePublicID
			,coverageLevelConfigUnScheduled.Value AS CoverageLevel
			,pcx_jmtransaction.PublicID AS TransactionPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_policyline.PublicID AS PolicyLinePublicID
			,pc_policyperiod.PolicyNumber
			,'None' AS PolicyItemLocationPublicId
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_cost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_jmtransaction.Amount AS TransactionAmount
			,pcx_jmtransaction.PostedDate AS TransactionPostedDate
			--,pcx_jmtransaction.Written AS TrxnWritten
			,CAST(pcx_jmtransaction.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_jmtransaction.ToBeAccrued AS CanBeEarned
			,CAST(pcx_jmtransaction.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_jmtransaction.ExpDate AS DATE) AS ExpirationDate
			--,pcx_jmtransaction.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_jmtransaction.Amount = 0 OR pcx_cost_jmic.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_cost_jmic.ActualTermAmount) * --Full term
						TIMESTAMP_DIFF(pcx_jmtransaction.EffDate, pcx_jmtransaction.ExpDate, DAY) / pcx_cost_jmic.NumDaysInRatedTerm
						) * --rated factor 
					pcx_jmtransaction.Amount / ABS(pcx_jmtransaction.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_cost_jmic.ActualBaseRate
			,pcx_cost_jmic.ActualAdjRate
			,pcx_cost_jmic.ActualTermAmount
			,pcx_cost_jmic.ActualAmount
			--,pcx_cost_jmic.ChargePattern
			,pcx_cost_jmic.ChargeGroup		--stored as a code, not ID. 
			,pcx_cost_jmic.ChargeSubGroup	--stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			,CASE WHEN pcx_cost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_cost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			,CASE WHEN pcx_jmtransaction.BranchID = pcx_cost_jmic.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_cost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_cost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_cost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_cost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_cost_jmic.RateAmountType AS RateType
			--,CASE WHEN pcx_cost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			--,pcx_cost_jmic.Subtype AS CostSubtypeCode
			,pcx_cost_jmic.RateBookUsed
			,pcx_cost_jmic.PublicID AS CostPublicID
			--,pc_policy.PublicId AS PolicyPublicId
			,pcx_jmtransaction.CreateTime AS TrxnCreateTime
			,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_cost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_cost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_cost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_cost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,pcx_jmpersonallinecov.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_jmpersonallinecov.PatternCode, pcx_cost_jmic.ChargeSubGroup, pcx_cost_jmic.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(effectivefieldsprimarypolicylocation.PublicID, MinPremPolLoc.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(effectivefieldsprimarypolicylocation.LocationNum, MinPremPolLoc.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--,'None' AS ItemPublicId
			,CAST(NULL AS STRING) AS ItemPublicId
			,NULL AS ItemNumber
			,'None' AS ItemLocatedWith
			,ROW_NUMBER() OVER(PARTITION BY pcx_jmtransaction.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_jmtransaction` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmtransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pcx_jmtransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
			ON pcx_jmtransaction.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryLineCov_JMIC_PL IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost 
			ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy
			ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS date)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PJDirectFinancialsConfig lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PJDirectFinancialsConfig coverageLevelConfigUnScheduled
			ON coverageLevelConfigUnScheduled.Key = 'UnScheduledCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jmpersonallinecov` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmpersonallinecov
			ON pcx_jmpersonallinecov.FixedID = pcx_cost_jmic.JewelryLineCov_JMIC_PL
			AND pcx_jmpersonallinecov.BranchID = pcx_cost_jmic.BranchID
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pcx_jmpersonallinecov.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pcx_jmpersonallinecov.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0	
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_cost_jmic.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS date)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_cost_jmic.BranchID
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS date)
				AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment 
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--Get the minimum location (number) for this line and branch
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_cost_jmic.BranchID
			AND MinPremPolLoc.StateInternal = pcx_cost_jmic.RatedState
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) >= CAST(COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart) AS date)
			AND CAST(COALESCE(pcx_jmtransaction.EffDate,perCost.EditEffectiveDate) AS date) < CAST(COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd) AS date)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS costratedState 
			ON costratedState.ID = pcx_cost_jmic.RatedState
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal

      WHERE	1 = 1
		
      /**** TEST *****/    
     -- AND pc_policyperiod.PolicyNumber = vpolicynumber


		) FinTrans
		INNER JOIN PJDirectFinancialsConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN PJDirectFinancialsConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN PJDirectFinancialsConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN PJDirectFinancialsConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN PJDirectFinancialsConfig itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
			--AND	PolicyNumber = vpolicynumber
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1
		
) extractData