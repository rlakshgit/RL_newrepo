/*TEST*/
--DECLARE vpolicynumber STRING; DECLARE {partition_date} Timestamp;
--SET vpolicynumber = '24-027446';
-- '24-322800', '24-254050', 24-182735, 24-062826, 24-050061, 24-254050, 24-062826, 24-027446
-- Set universal latest partitiontime; In future will come from Airflow DAG (Today - 1)
--SET {partition_date} = '2020-11-30'; 
--SET {partition_date} = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_policyperiod`);


/*******************************************************************************************
	Kimberlite - Financial Transactions 
		FinancialTransactionPJCeded.sql
			GCP CONVERTED
********************************************************************************************
--------------------------------------------------------------------------------------------
 *****  Change History  *****
 
	02/23/2021	DROBAK		Modified JOIN for PolicyCenter.dbo.pcx_jewelryitem_jmic_pl
	04/05/2021	DROBAK		Changed RiskJewelryKey to RiskJewelryItemKey
	04/05/2021	DROBAK		Changed 'ScheduledItemRisk' Value to 'PersonalJewelryItem'
	06/28/2021	DROBAK		add IsTransactionSliceEffective & Primary Locn; update unit tests
	09/03/2021	DROBAK		Change ItemPublicId from 'None' to CAST(NULL AS STRING) (BQ only change); set ItemNumber back to INT from string
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes);
							Renamed RatedStateCode as RatedState for consistency

--------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
--------------------------------------------------------------------------------------------
	pcx_jwryitemcov_jmic_pl.PublicID			AS	CoveragePublicID
	pcx_jmpersonallinecov.PublicID				AS	CoveragePublicID
--------------------------------------------------------------------------------------------
 *****  Unioned Sections  *****
-------------------------------------------------------------------------------------------------------------------
	Scheduled cov 		--Item Cov - JewelryItemCov_JMIC_PL = pcx_jwryitemcov_jmic_pl AND JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl
	UnScheduled cov 	--Line Cov - JewelryLineCov_JMIC_PL = pcx_jmpersonallinecov
-------------------------------------------------------------------------------------------------------------------
	TEST ISSUES:
	1.	select TrxnPublicID, * from gw_reporting.pc.pcrt_trxn_summ where isCeded = 1 and linecode = 'PersonalJewelryLine_JMIC_PL' and policynumber = '24-062826' and TrxnID in (174401, 174402, 174403, 174404) order by TrxnID
		--PublicTrxnID Examples:  pe:pc:174401, pe:pc:174402, etc.
		---Response: based on timestamp, records we manually inserted to reverse bad data from GW during the thursday release window

**********************************************************************************************************************************
CREATE OR REPLACE TABLE `{project}.bl_kimberlite.PJCededConfig`
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO `{project}.bl_kimberlite.PJCededConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256') 
			,('LineCode','PersonalJewelryLine_JMIC_PL')
			,('BusinessType', 'Ceded')
			,('ScheduledCoverage','ScheduledCov')
			,('UnScheduledCoverage','UnScheduledCov')
			,('NoCoverage','NoCoverage')
			,('ScheduledItemRisk', 'PersonalJewelryItem');
*/
	
WITH PJCededConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'BusinessType', 'Ceded' UNION ALL
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'ScheduledItemRisk', 'PersonalJewelryItem'
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
	,PrimaryPolicyLocationNumber
	,PrimaryPolicyLocationPublicID
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
	,ItemPublicId
	,ItemNumber
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
	,RateBookUsed
	,CostPublicID
	,PolicyPeriodPublicID
	,PolicyItemLocationPublicId
	--Ceded Fields
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
		/*													P J      C E D E D  																						*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 S C H E D U L E D   I T E M	  C O V G   																			*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_jwryitemcov_jmic_pl.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_plcededpremiumtrans_jmic.PublicID AS TransactionPublicID
			,CASE pcx_cost_jmic.ChargeSubGroup
				WHEN 'MIN_PREMIUM_ADJ'
					THEN MinPremPolLoc.PublicID
					ELSE pc_policylocation.PublicID
				END AS PolicyItemLocationPublicId
			,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_cost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_plcededpremiumtrans_jmic.CededPremium AS TransactionAmount
			,pcx_plcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate
			--,CASE WHEN pcx_plcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten	
			,custom_functions.fn_GetMaxDate
				(	custom_functions.fn_GetMaxDate(CAST(pcx_plcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_plcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, perCost.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			,1 AS CanBeEarned
			,CAST(pcx_plcededpremiumtrans_jmic.EffectiveDate AS DATE) AS EffectiveDate
			,CAST(pcx_plcededpremiumtrans_jmic.ExpirationDate AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged
			,pcx_plcededpremiumtrans_jmic.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_cost_jmic.ActualBaseRate
			,pcx_cost_jmic.ActualAdjRate
			,pcx_cost_jmic.ActualTermAmount
			,pcx_cost_jmic.ActualAmount
			,pcx_cost_jmic.ChargeGroup --stored as a code, not ID. 
			,pcx_cost_jmic.ChargeSubGroup --stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			/*,CASE WHEN pcx_cost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_cost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,1 AS Onset
			,CASE WHEN pcx_cost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_cost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_cost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_cost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_cost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_cost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_cost_jmic.Subtype
			,pcx_cost_jmic.RateBookUsed
			,perCost.PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_cost_jmic.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_plcededpremiumtrans_jmic.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_cost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_cost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE CAST(pcx_cost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_cost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,pcx_jwryitemcov_jmic_pl.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_jwryitemcov_jmic_pl.PatternCode, pcx_cost_jmic.ChargeSubGroup, pcx_cost_jmic.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(pc_policylocation.PublicID, effectivefieldsprimarypolicylocation.PublicID) AS CoverageLocationEffPublicID
			,COALESCE(pc_policylocation.LocationNum, effectivefieldsprimarypolicylocation.LocationNum) AS CoverageLocationEffNumber
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--items
			,CAST(pcx_jewelryitem_jmic_pl.PublicID AS STRING) AS ItemPublicId
			,pcx_jewelryitem_jmic_pl.ItemNumber
			,CAST(pcx_jewelryitem_jmic_pl.LocatedWith AS STRING) AS ItemLocatedWith
			--Ceding Data
			,pcx_plcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_plcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_plcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_cost_jmic.ActualTermAmount * pcx_plcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_plcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			--,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,ROW_NUMBER() OVER(PARTITION BY pcx_plcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_plcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_plcededpremiumtrans_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_plcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_plcededpremiumjmic
			ON pcx_plcededpremiumjmic.ID = pcx_plcededpremiumtrans_jmic.PLCededPremium_JMIC
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
			ON pcx_plcededpremiumjmic.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryItemCov_JMIC_PL IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy ON pc_policy.Id = perCost.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PJCededConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PJCededConfig coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'ScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jwryitemcov_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jwryitemcov_jmic_pl
			ON pcx_jwryitemcov_jmic_pl.FixedID = pcx_cost_jmic.JewelryItemCov_JMIC_PL
			AND pcx_jwryitemcov_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jwryitemcov_jmic_pl.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_jwryitemcov_jmic_pl.ExpirationDate,perCost.PeriodEnd)			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelryitem_jmic_pl
			ON pcx_jewelryitem_jmic_pl.BranchID = pcx_cost_jmic.BranchID
			AND pcx_jewelryitem_jmic_pl.FixedID = pcx_cost_jmic.JewelryItem_JMIC_PL
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,perCost.PeriodEnd)
		--Cede Agreement
		INNER JOIN `{project}.{pc_dataset}.pc_reinsuranceagreement` AS pc_reinsuranceagreement ON pc_reinsuranceagreement.ID = pcx_plcededpremiumtrans_jmic.Agreement
		INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
		--look up ceded premiums transaction's branch id
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm 
			on pc_policyterm.ID = pcx_plcededpremiumjmic.PolicyTerm
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				AND pc_policyperiod.ModelDate < pcx_plcededpremiumtrans_jmic.CalcTimestamp
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_cost_jmic.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_cost_jmic.BranchID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--Get the minimum location (number) for this line and branch
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_cost_jmic.BranchID
			AND MinPremPolLoc.StateInternal = pcx_cost_jmic.RatedState
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costratedState ON costratedState.ID = pcx_cost_jmic.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		--Scheduled Item Joins only
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
			ON  pc_policylocation.BranchID = pcx_cost_jmic.BranchID
			AND pc_policylocation.FixedID = pcx_jewelryitem_jmic_pl.Location
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState ON effectiveLocState.ID = pc_policylocation.StateInternal

		WHERE 1 = 1
	    /**** TEST *****/    
		--AND pc_policyperiod.PolicyNumber = vpolicynumber	


	UNION ALL

		/****************************************************************************************************************************************************************/
		/*													P J      C E D E D	    																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/* 										 U N  S C H E D U L E D   L I N E	C O V G  																			*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_jmpersonallinecov.PublicID AS CoveragePublicID
			,coverageLevelConfigScheduledItem.Value AS CoverageLevel
			,pcx_plcededpremiumtrans_jmic.PublicID AS TransactionPublicID
			,'None' AS PolicyItemLocationPublicId
			,perCost.PeriodID AS PolicyPeriodPeriodID
			,pc_policyline.ID AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_cost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_plcededpremiumtrans_jmic.CededPremium AS TransactionAmount
			,pcx_plcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate
			--,CASE WHEN pcx_plcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,custom_functions.fn_GetMaxDate
				(	custom_functions.fn_GetMaxDate(CAST(pcx_plcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_plcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, perCost.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			,1 AS CanBeEarned
			,CAST(pcx_plcededpremiumtrans_jmic.EffectiveDate AS DATE) AS EffectiveDate
			,CAST(pcx_plcededpremiumtrans_jmic.ExpirationDate AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged
			,pcx_plcededpremiumtrans_jmic.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, primaryLocState.TYPECODE) AS	RatedState
			,pcx_cost_jmic.ActualBaseRate
			,pcx_cost_jmic.ActualAdjRate
			,pcx_cost_jmic.ActualTermAmount
			,pcx_cost_jmic.ActualAmount
			,pcx_cost_jmic.ChargeGroup		--stored as a code, not ID. 
			,pcx_cost_jmic.ChargeSubGroup	--stored as a code, not ID. 
			,costChargePattern.TYPECODE AS ChargePattern
			,pctl_segment.TYPECODE AS SegmentCode
			/*,CASE WHEN pcx_cost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_cost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_cost_jmic.ExpirationDate AS DATE)
				ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,1 AS Onset
			,CASE WHEN pcx_cost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_cost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_cost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_cost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pcx_cost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_cost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge 
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			,pcx_cost_jmic.Subtype
			,pcx_cost_jmic.RateBookUsed
			,perCost.PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_cost_jmic.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_plcededpremiumtrans_jmic.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_cost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_cost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE CAST(pcx_cost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_cost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL)
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
			--items
			--,'None' AS ItemPublicId
			,CAST(NULL AS STRING) AS ItemPublicId
			,NULL AS ItemNumber
			,'None' AS ItemLocatedWith
			--Ceding Data
			,pcx_plcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_plcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_plcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_cost_jmic.ActualTermAmount * pcx_plcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_plcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			--,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,ROW_NUMBER() OVER(PARTITION BY pcx_plcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_cost_jmic.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,perCost.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) DESC
			) AS TransactionRank

		-- select pc_policyline.Subtype
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_plcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_plcededpremiumtrans_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_plcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_plcededpremiumjmic
			ON pcx_plcededpremiumjmic.ID = pcx_plcededpremiumtrans_jmic.PLCededPremium_JMIC
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_cost_jmic
			ON pcx_plcededpremiumjmic.Cost_JMIC = pcx_cost_jmic.ID
			AND pcx_cost_jmic.JewelryLineCov_JMIC_PL IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS perCost ON pcx_cost_jmic.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job ON pc_job.ID = perCost.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) AS pc_policy ON pc_policy.Id = perCost.PolicyId
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pcx_cost_jmic.PersonalJewelryLine_JMIC_PL = pc_policyline.FixedID
			AND pcx_cost_jmic.BranchID = pc_policyline.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN PJCededConfig lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN PJCededConfig coverageLevelConfigScheduledItem ON coverageLevelConfigScheduledItem.Key = 'UnScheduledCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jmpersonallinecov` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jmpersonallinecov
			ON pcx_jmpersonallinecov.FixedID = pcx_cost_jmic.JewelryLineCov_JMIC_PL
			AND pcx_jmpersonallinecov.BranchID = pcx_cost_jmic.BranchID
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_jmpersonallinecov.ExpirationDate,perCost.PeriodEnd)
		--Cede Agreement
		INNER JOIN `{project}.{pc_dataset}.pc_reinsuranceagreement` AS pc_reinsuranceagreement ON pc_reinsuranceagreement.ID = pcx_plcededpremiumtrans_jmic.Agreement
		INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup 
			ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
		LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
		--look up ceded premiums transaction's branch id
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm 
			on pc_policyterm.ID = pcx_plcededpremiumjmic.PolicyTerm
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
				AND pc_policyperiod.ModelDate < pcx_plcededpremiumtrans_jmic.CalcTimestamp
		LEFT JOIN (SEleCT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary 
			ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0	
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS costChargePattern ON costChargePattern.ID = pcx_cost_jmic.ChargePattern
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pcx_cost_jmic.BranchID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS effectiveFieldsPrimaryPolicyLocation 
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_cost_jmic.BranchID
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` AS pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--Get the minimum location (number) for this line and branch
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS MinPremPolLoc
			ON  MinPremPolLoc.BranchID = pcx_cost_jmic.BranchID
			AND MinPremPolLoc.StateInternal = pcx_cost_jmic.RatedState
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(MinPremPolLoc.EffectiveDate,perCost.PeriodStart)
			AND COALESCE(pcx_cost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(MinPremPolLoc.ExpirationDate,perCost.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costratedState ON costratedState.ID = pcx_cost_jmic.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState ON primaryLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal

		WHERE 1 = 1
	    /**** TEST *****/    
		--AND pc_policyperiod.PolicyNumber = vpolicynumber


		) FinTrans
		INNER JOIN PJCededConfig sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN PJCededConfig hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN PJCededConfig hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN PJCededConfig businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN PJCededConfig itemRisk
			ON itemRisk.Key='ScheduledItemRisk'

		WHERE 1=1
			--AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1
		
) extractData

