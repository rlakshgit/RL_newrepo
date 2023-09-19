/**** Kimberlite - Financial Transactions ********************
		FinancialTransactionUMBDirect.sql
			GCP CONVERTED
-------------------------------------------------------------------------------------------------------------------
	*****  Sections  *****
-------------------------------------------------------------------------------------------------------------------
		UmbrellaLineCov 			--Line
		No Covg						--No Coverage
-------------------------------------------------------------------------------------------------------------------

 *****  Change History  *****

	03/26/2021	DROBAK		Init create
	03/30/2021	DROBAK		Finalized FinancialTransactionKey; Output field cleanup
	04/02/2021	DROBAK		Update No Coverage Section; standardize columns
	06/29/2021	DROBAK		Commented out "Is" fields in Peaks, temps section 
	06/29/2021	DROBAK		Add field IsTransactionSliceEffective; Cov & Locn left joins; updated Unit Tests
	10/12/2021	DROBAK		ROUND to zero AdjustedFullTermAmount
	10/28/2021	DROBAK		Remove use of minBOPPolicyLocationOnBranch - change BOPLocationPublicID (like CoverageUMB) & RatingLocationPublicID (like IM & BOP)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes)

-------------------------------------------------------------------------------------------------------------------
*******************************************************************************************************************/
--CREATE OR REPLACE TABLE `{project}.bl_kimberlite.UMBFinTrans`
--	(
--		Key STRING,
--		Value STRING
--	);
--	INSERT INTO `{project}.bl_kimberlite.UMBFinTrans`
--		VALUES	
--			('SourceSystem','GW')
--			,('HashKeySeparator','_')
--			,('HashAlgorithm', 'SHA2_256') 
--			,('LineCode','UmbrellaLine_JMIC')
--			,('BusinessType', 'Direct')
--			/* CoverageLevel Values */
--			,('LineLevelCoverage','Line')
--			,('NoCoverage','NoCoverage')
--			/* Risk Key Values */
--			,('LocationLevelRisk', 'BusinessOwnersLocation');	
		
WITH UMBFinTrans AS
 (SELECT 'SourceSystem' as Key, 'GW' as Value UNION ALL
  SELECT 'HashKeySeparator', '_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode', 'UmbrellaLine_JMIC' UNION ALL
  SELECT 'BusinessType', 'Direct' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'LocationLevelRisk', 'BusinessOwnersLocation'  ) 
  
SELECT
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,UMBCoverageKey
	,RiskLocationKey
	,BusinessType
	,CoveragePublicID
	,TransactionPublicID
	,BOPLocationPublicID
	,IsPrimaryLocationBOPLocation
	,JobNumber
	,IsTransactionSliceEffective
	,PolicyNumber
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,TransactionAmount
	,TransactionPostedDate
	,TransactionWrittenDate
	,NumDaysInRatedTerm
	,AdjustedFullTermAmount
	,ActualTermAmount 
	,ActualAmount 
	,ChargePattern 
	,ChargeGroup 
	,ChargeSubGroup 
	,RateType
	,RatedState
	,RatingLocationPublicID
	,CostPublicID
	,PolicyPeriodPublicID
	,LineCode
    ,DATE('{date}') as bq_load_date

--#INTO #FinTrans
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) AS UMBCoverageKey
		,CASE WHEN 
			BOPLocationPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
		END AS RiskLocationKey
		,businessTypeConfig.Value AS BusinessType
		,FinTrans.*
	FROM (
		/****************************************************************************************************************************************************************/
		/*												U M B 	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*													L I N E    C O V G   																					    */
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_umbrellalinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfigBusinessOwnersLine.Value AS CoverageLevel
			,pcx_umbtransaction_jmic.PublicID AS TransactionPublicID
			--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS BOPLocationPublicID
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.PublicID
					ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				END AS BOPLocationPublicID		-- f/k/a CoverageLocationEffPublicID
			*/
			--,perCost.PeriodID AS PolicyPeriodPeriodID
			--,pc_policyline.ID AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_umbcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_umbtransaction_jmic.Amount AS TransactionAmount
			,pcx_umbtransaction_jmic.PostedDate AS TransactionPostedDate
			--,pcx_umbtransaction_jmic.Written AS TrxnWritten
			,CAST(pcx_umbtransaction_jmic.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_umbtransaction_jmic.ToBeAccrued AS CanBeEarned
			,CAST(pcx_umbtransaction_jmic.EffDate as DATE) AS EffectiveDate
			,CAST(pcx_umbtransaction_jmic.ExpDate as DATE) AS ExpirationDate
			--,pcx_umbtransaction_jmic.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_umbtransaction_jmic.Amount = 0 OR pcx_umbcost_jmic.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_umbcost_jmic.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_umbtransaction_jmic.EffDate, pcx_umbtransaction_jmic.ExpDate,DAY) / pcx_umbcost_jmic.NumDaysInRatedTerm
						) * --rated factor 
					pcx_umbtransaction_jmic.Amount / ABS(pcx_umbtransaction_jmic.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,pctl_segment.TYPECODE AS SegmentCode
			--,pcx_umbcost_jmic.Basis AS CostBasis
			--,pcx_umbcost_jmic.ActualBaseRate
			--,pcx_umbcost_jmic.ActualAdjRate
			,pcx_umbcost_jmic.ActualTermAmount
			,pcx_umbcost_jmic.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pcx_umbcost_jmic.ChargeGroup --stored as a code, not ID. 
			,pcx_umbcost_jmic.ChargeSubGroup --stored as a code, not ID. 
			/*,CASE WHEN pcx_umbcost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_umbcost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.ExpirationDate AS DATE)
				ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,CASE WHEN pcx_umbtransaction_jmic.BranchID = pcx_umbcost_jmic.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_umbcost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_umbcost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_umbcost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_umbcost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
 			,pcx_umbcost_jmic.RateAmountType AS RateType
			--,CASE WHEN pcx_umbcost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			--,pcx_umbcost_jmic.Subtype AS CostSubtypeCode
			,perCost.PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_umbcost_jmic.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_umbtransaction_jmic.CreateTime AS TrxnCreateTime
			--,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
			--,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_umbcost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_umbcost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_umbcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_umbcost_jmic.NumDaysInRatedTerm AS FLOAT64))AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,pcx_umbrellalinecov_jmic.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_umbrellalinecov_jmic.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CoverageEffPatternCode
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.LocationNum
					ELSE effectiveFieldsPrimaryPolicyLocation.LocationNum
				END AS CoverageLocationEffNumber
			*/
			,COALESCE(umbrellalinecovCost.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CostCoverageCode
			--,ratingLocation.ID AS RatingLocationID
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			--,perCost.EditEffectiveDate AS CostEditEffectiveDate
			--,perCost.EditEffectiveDate AS TrxnEditEffectiveDate
			/*--Peaks and Temp and SERP Related Data
			,0 AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			,0 AS IsAdditionalInsuredCoverage
			,0 AS IsOneTimeCreditCoverage
			*/
			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 1 ELSE 0
				END AS IsPrimaryLocationBOPLocation
			--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_umbtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank


			FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_umbtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbtransaction_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcost_jmic
				ON pcx_umbtransaction_jmic.UMBCost_JMIC = pcx_umbcost_jmic.ID
				AND pcx_umbcost_jmic.UmbrellaLineCov IS NOT NULL

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pcx_umbtransaction_jmic.BranchID = pc_policyperiod.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod_cost
				ON pcx_umbcost_jmic.BranchID = pc_policyperiod_cost.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost 
				ON perCost.ID = COALESCE(pc_policyperiod_cost.ID,pc_policyperiod.ID)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = perCost.JobID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON perCost.UWCompany = pc_uwcompany.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
				ON pc_policy.Id = perCost.PolicyId
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pcx_umbcost_jmic.UMBLine = pc_policyline.FixedID
				AND pcx_umbcost_jmic.BranchID = pc_policyline.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
			LEFT JOIN UMBFinTrans lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			LEFT JOIN UMBFinTrans coverageLevelConfigBusinessOwnersLine
				ON coverageLevelConfigBusinessOwnersLine.Key = 'LineLevelCoverage' 
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbrellalinecov_jmic
				ON pcx_umbrellalinecov_jmic.FixedID = pcx_umbcost_jmic.UmbrellaLineCov
				AND pcx_umbrellalinecov_jmic.BranchID = pcx_umbcost_jmic.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_umbrellalinecov_jmic.ExpirationDate,perCost.PeriodEnd)	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) umbrellalinecovCost 
				ON umbrellalinecovCost.ID = pcx_umbcost_jmic.UmbrellaLineCov 

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
				ON perCost.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_chargepattern`) costChargePattern 
				ON costChargePattern.ID = pcx_umbcost_jmic.ChargePattern
			--Effective Dates Fields
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
			--Effective Fields's Primary Location Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
					--Ensure Effective Primary location matches the BOP's BOP location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd)
/*			--Get the minimum location (number) for this line and branch
			LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) minBOPPolicyLocationOnBranch
				ON  minBOPPolicyLocationOnBranch.BranchID = pcx_umbcost_jmic.BranchID 
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(minBOPPolicyLocationOnBranch.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(minBOPPolicyLocationOnBranch.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) PC_BOPLOCATION2 
				ON minBOPPolicyLocationOnBranch.FixedID = PC_BOPLOCATION2.Location
					AND PC_BOPLOCATION2.BranchID = pcx_umbcost_jmic.BranchID				
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(PC_BOPLOCATION2.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(PC_BOPLOCATION2.ExpirationDate,perCost.PeriodEnd)
*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
			--State and Country
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState 
				ON costratedState.ID = pcx_umbcost_jmic.RatedState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
				ON primaryLocState.ID = CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
					END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState 
				ON taxLocState.ID = pcx_umbcost_jmic.TaxJurisdiction
/*			--Policy Period Cost Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPerEfflocation
				ON costPerEfflocation.BranchID = pcx_umbcost_jmic.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(costPerEfflocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(costPerEfflocation.ExpirationDate,perCost.PeriodEnd)	
				AND costPerEfflocation.StateInternal = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
				ON costPerEfflocationState.ID = costPerEfflocation.StateInternal
*/
			--Coverage Eff Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
				ON covEffLoc.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.ID END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
				ON covEfflocState.ID = covEffloc.StateInternal
/*			--Rating Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
				ON ratingLocation.ID = CASE 
					WHEN covEffLoc.ID IS NOT NULL
						AND covEfflocState.ID IS NOT NULL
						AND covEfflocState.ID = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN covEffLoc.ID
					WHEN costPerEfflocation.PublicID IS NOT NULL
						AND CostPerEfflocationState.ID = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN costPerEfflocation.ID
					ELSE - 1
					END
*/
			WHERE 1 = 1

			/**** TEST *****/
		--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)

	UNION ALL

		/****************************************************************************************************************************************************************/
		/*												U M B 	     D I R E C T																					    */
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*													N O     C O V G   																					        */
		/****************************************************************************************************************************************************************/
		SELECT 
			'None' AS CoveragePublicID
			,coverageLevelConfigOther.Value AS CoverageLevel
			,pcx_umbtransaction_jmic.PublicID AS TransactionPublicID
			--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS BOPLocationPublicID
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.PublicID
					ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				END AS BOPLocationPublicID		-- f/k/a CoverageLocationEffPublicID
			*/
			--,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
			--,'None' AS PolicyLineID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_umbcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,'None' AS LineCode
			,pcx_umbtransaction_jmic.Amount AS TransactionAmount
			,pcx_umbtransaction_jmic.PostedDate AS TransactionPostedDate
			--,pcx_umbtransaction_jmic.Written AS TrxnWritten
			,CAST(pcx_umbtransaction_jmic.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_umbtransaction_jmic.ToBeAccrued AS CanBeEarned
			,CAST(pcx_umbtransaction_jmic.EffDate as DATE) AS EffectiveDate
			,CAST(pcx_umbtransaction_jmic.ExpDate as DATE) AS ExpirationDate
			--,pcx_umbtransaction_jmic.Charged AS TrxnCharged
			,CASE 
				WHEN pcx_umbtransaction_jmic.Amount = 0 OR pcx_umbcost_jmic.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pcx_umbcost_jmic.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pcx_umbtransaction_jmic.EffDate, pcx_umbtransaction_jmic.ExpDate,DAY) / pcx_umbcost_jmic.NumDaysInRatedTerm
						) * --rated factor 
					pcx_umbtransaction_jmic.Amount / ABS(pcx_umbtransaction_jmic.Amount) --Side 
				END AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,pctl_segment.TYPECODE AS SegmentCode
			--,pcx_umbcost_jmic.Basis AS CostBasis
			--,pcx_umbcost_jmic.ActualBaseRate
			--,pcx_umbcost_jmic.ActualAdjRate
			,pcx_umbcost_jmic.ActualTermAmount
			,pcx_umbcost_jmic.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pcx_umbcost_jmic.ChargeGroup --stored as a code, not ID. 
			,pcx_umbcost_jmic.ChargeSubGroup --stored as a code, not ID. 
			/*,CASE WHEN pcx_umbcost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.EffectiveDate AS DATE)
				END AS CostEffectiveDate 
			,CASE WHEN pcx_umbcost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.ExpirationDate AS DATE)
				ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			,CASE WHEN pcx_umbtransaction_jmic.BranchID = pcx_umbcost_jmic.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pcx_umbcost_jmic.OverrideBaseRate IS NOT NULL
					OR pcx_umbcost_jmic.OverrideAdjRate IS NOT NULL
					OR pcx_umbcost_jmic.OverrideTermAmount IS NOT NULL
					OR pcx_umbcost_jmic.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
 			,pcx_umbcost_jmic.RateAmountType AS RateType
			--,CASE WHEN pcx_umbcost_jmic.RateAmountType = 3 THEN 0 ELSE 1 END AS IsPremium	-- Tax or surcharge
			--,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL THEN 1 ELSE 0 END AS IsReportingPolicy
			--,pcx_umbcost_jmic.Subtype AS CostSubtypeCode
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pcx_umbcost_jmic.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_umbtransaction_jmic.CreateTime AS TrxnCreateTime
			--,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
			--,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_umbcost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 
				WHEN pcx_umbcost_jmic.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pcx_umbcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pcx_umbcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,'None' AS CoverageEffPublicID
			,COALESCE(pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CoverageEffPatternCode
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.LocationNum
					ELSE effectiveFieldsPrimaryPolicyLocation.LocationNum
				END AS CoverageLocationEffNumber
			*/
			,COALESCE(umbrellalinecovCost.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CostCoverageCode
			--,ratingLocation.ID AS RatingLocationID
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			--,perCost.EditEffectiveDate AS CostEditEffectiveDate
			--,perCost.EditEffectiveDate AS TrxnEditEffectiveDate
			/*--Peaks and Temp and SERP Related Data
			,0 AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			,0 AS IsAdditionalInsuredCoverage
			,0 AS IsOneTimeCreditCoverage
			*/
			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 1 ELSE 0
				END AS IsPrimaryLocationBOPLocation
			--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_umbtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

			FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_umbtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbtransaction_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcost_jmic
				ON pcx_umbtransaction_jmic.UMBCost_JMIC = pcx_umbcost_jmic.ID
				AND -1=COALESCE(pcx_umbcost_jmic.UmbrellaLineCov,-1)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pcx_umbtransaction_jmic.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost 
				ON pcx_umbcost_jmic.BranchID = perCost.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = perCost.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON perCost.UWCompany = pc_uwcompany.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
				ON pc_policy.Id = pc_policyperiod.PolicyId
			INNER JOIN UMBFinTrans coverageLevelConfigOther
				ON coverageLevelConfigOther.Key = 'NoCoverage'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) umbrellalinecovCost 
				ON umbrellalinecovCost.ID = pcx_umbcost_jmic.UmbrellaLineCov 
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date})  pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_chargepattern`) costChargePattern 
				ON costChargePattern.ID = pcx_umbcost_jmic.ChargePattern
			--Effective Dates Fields
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd)
			--Effective Fields's Primary Location Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd)
					--Ensure Effective Primary location matches the BOP's BOP location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd)
/*			--Get the minimum location (number) for this line and branch
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) minBOPPolicyLocationOnBranch
				ON  minBOPPolicyLocationOnBranch.BranchID = pcx_umbcost_jmic.BranchID 
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(minBOPPolicyLocationOnBranch.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(minBOPPolicyLocationOnBranch.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) PC_BOPLocation2 
				ON minBOPPolicyLocationOnBranch.FixedID = PC_BOPLocation2.Location
					AND PC_BOPLocation2.BranchID = pcx_umbcost_jmic.BranchID				
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(PC_BOPLocation2.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(PC_BOPLocation2.ExpirationDate,perCost.PeriodEnd)
*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
			--State and Country
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState 
				ON costratedState.ID = pcx_umbcost_jmic.RatedState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
				ON primaryLocState.ID = CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
					END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState 
				ON taxLocState.ID = pcx_umbcost_jmic.TaxJurisdiction
/*			--Policy Period Cost Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPerEfflocation
				ON costPerEfflocation.BranchID = pcx_umbcost_jmic.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(costPerEfflocation.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(costPerEfflocation.ExpirationDate,perCost.PeriodEnd)	
				AND costPerEfflocation.StateInternal = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
				ON costPerEfflocationState.ID = costPerEfflocation.StateInternal
*/			
			--Coverage Eff Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
				ON covEffLoc.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.ID END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
				ON covEfflocState.ID = covEffloc.StateInternal
/*			--Rating Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME =  {partition_date})) ratingLocation 
				ON ratingLocation.ID = CASE 
					WHEN covEffLoc.ID IS NOT NULL
						AND covEfflocState.ID IS NOT NULL
						AND covEfflocState.ID = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN covEffLoc.ID
					WHEN costPerEfflocation.PublicID IS NOT NULL
						AND CostPerEfflocationState.ID = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN costPerEfflocation.ID
					ELSE - 1
					END
*/

			WHERE 1 = 1
			/**** TEST *****/
		--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)

		) FinTrans
		INNER JOIN UMBFinTrans AS sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN UMBFinTrans AS hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN UMBFinTrans AS hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN UMBFinTrans AS businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN UMBFinTrans AS locationRisk
			ON locationRisk.Key='LocationLevelRisk'

		WHERE 1=1
	--		AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1
	
) extractData