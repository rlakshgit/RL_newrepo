/*TEST*/
--DECLARE vpolicynumber STRING; DECLARE {partition_date} Timestamp;
--SET vpolicynumber = '55-000034'	--'55-000342'	--'55-011498'	--'55-001942'	--'55-000342'	--'55-000950'	--'55-001338'
	-- '55-014561',	'55-011405' , '55-005246', '55-009751' , '55-001046', '55-001338', '55-011255' , '55-010342'
--SET {partition_date} = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_policyperiod`);


/**** Kimberlite - Financial Transactions ********
		FinancialTransactionBOPDirect.sql 
			CONVERTED TO BIG QUERY
**************************************************
----------------------------------------------------------------------------------------------
 *****	Change Log	*****

	04/05/2021	DROBAK		Changed name to use BuildingPublicID like Risk Bldg extract
	04/05/2021	DROBAK		Commented out "Is" fields in Peaks, temps section
	06/22/2021	DROBAK		Misc field clean-up; add to final select IsPrimaryLocationBOPLocation, BOPLocationPublicID
	06/24/2021	DROBAK		Cov & Locn left joins; updated Unit Tests
	07/27/2021	DROBAK		Change so pc_policyperiod joins to pc_job opposed to perCost (cost level) table (aligns with IM); comment pc_policy
	07/28/2021	DROBAK		Changed ChargePatternCode to ChargePattern (to match other tables)
	08/06/2021	DROBAK		Added join condition to pc_policyline that matches DW join; extended join condition for Temp dates and cov tables
	08/19/2021	DROBAK		Remove use of minBOPPolicyLocationOnBranch; change RatingLocationPublicID code to work like IM; Fix some left joins for BQ
	09/01/2021	DROBAK		Change BuildingPublicID from 'None' to CAST(NULL AS STRING), where applicable (BQ only change)
	09/10/2021	DROBAK		Changed Data Type from Float64 to Decimal for AdjustedFullTermAmount
	09/28/2021	DROBAK		New Version for Date Logic: use transaction table dates in place of cost table; use <= instead of < for ExpirationDate
	10/04/2021	DROBAK		Round AdjustedFullTermAmount to zero
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes)

---------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
---------------------------------------------------------------------------------------------
	pc_businessownerscov.PublicID				AS	CoveragePublicID
	pcx_bopsublinecov_jmic.PublicID				AS	CoveragePublicID
	pc_boplocationcov.PublicID					AS	CoveragePublicID
	pcx_bopsubloccov_jmic.PublicID				AS	CoveragePublicID
	pc_bopbuildingcov.PublicID					AS	CoveragePublicID
---------------------------------------------------------------------------------------------
 *****  Unioned Sections  *****
 --------------------------------------------------------------------------------------------
	BusinessOwnersCov 				--Line
	BOPSubLineCov 					--Sub Line
	BOPLocationCov 					--Location
	BOPSubLocCov 					--Sub Location
	BOPBuildingCov 					--Building
	BOPOneTimeCredit 				--One Time Credit
	AdditionalInsuredCustomCoverage	--Additional Insured Custom Covg
	n/a								--Other No Covg
---------------------------------------------------------------------------------------------
/**********************************************************************************************************************************/
/*
CREATE OR REPLACE TABLE FinTrans
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO FinTrans
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256') 
			,('LineCode','BusinessOwnersLine')
			,('BusinessType', 'Direct')
			,('LineLevelCoverage','Line')
			,('SubLineLevelCoverage','SubLine')
			,('LocationLevelCoverage','Location')
			,('SubLocLevelCoverage','SubLoc')
			,('BuildingLevelCoverage', 'Building')
			,('OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC')
			,('AdditionalInsuredCustomCoverage','Additional_Insured_JMIC')
			,('NoCoverage','NoCoverage')
			,('LocationLevelRisk', 'BusinessOwnersLocation')
			,('BuildingLevelRisk', 'BusinessOwnersBuilding');
*/

WITH FinTrans AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode','BusinessOwnersLine' UNION ALL
  SELECT 'BusinessType', 'Direct' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
  SELECT 'OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC' UNION ALL
  SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
  SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
)

SELECT
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,BOPCoverageKey
	,RiskLocationKey
	,RiskBuildingKey
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
--INTO #FinTrans
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,CASE WHEN CoveragePublicID IS NOT NULL
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
		END AS BOPCoverageKey
		,CASE WHEN BOPLocationPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
		END AS RiskLocationKey
		,CASE WHEN BuildingPublicID IS NOT NULL
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BuildingPublicID,hashKeySeparator.Value,buildingRisk.Value)) 
		END AS RiskBuildingKey
		,businessTypeConfig.Value AS BusinessType
		,FinTrans.*
	FROM (

		/********************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																*/
		/********************************************************************************************************************************************/
		/********************************************************************************************************************************************/
		/*													L I N E    C O V G   																	*/
		/********************************************************************************************************************************************/
		SELECT
			pc_businessownerscov.PublicID AS CoveragePublicID
			,coverageLevelConfigBusinessOwnersLine.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
		/*	,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pc_bopcost.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pc_bopcost.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective */
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_boptransaction.Amount
				ELSE CASE 
					WHEN pc_bopcost.NumDaysInRatedTerm = 0
						THEN 0
						ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
					END
				END AS AdjustedFullTermAmount
			--Primary ID and Location; effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			--Effective Coverage and Locations (only coverage specific)
			,pc_businessownerscov.PublicID AS CoverageEffPublicID
			,COALESCE(pc_businessownerscov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date}) pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN(SELECT * FROM  `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.BusinessOwnersCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			--(1)ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
			--(1)AND pc_bopcost.BranchID = pc_policyline.BranchID
			--(2)ON pc_policyline.BranchID = perCost.ID
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans coverageLevelConfigBusinessOwnersLine
			ON coverageLevelConfigBusinessOwnersLine.Key = 'LineLevelCoverage' 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) pc_businessownerscov
			ON pc_businessownerscov.FixedID = pc_bopcost.BusinessOwnersCov
			AND pc_businessownerscov.BranchID = pc_bopcost.BranchID
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.ExpirationDate,perCost.PeriodEnd)	AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern`  costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup
		--PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation
			ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
			AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
			AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
	
		--BOP original Coverage Ref in BOP Cost
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) businessOwnersCovCost
			ON businessOwnersCovCost.ID = pc_bopcost.BusinessOwnersCov

		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

		--State and Country
		LEFT JOIN  `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN  `{project}.{pc_dataset}.pctl_state` primaryLocState
		 	ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END
			
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN  `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal

		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)

		WHERE 1 = 1


		UNION ALL

		/************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T														*/
		/************************************************************************************************************************************/
		/************************************************************************************************************************************/
		/*												 S U B   L I N E   C O V G   														*/
		/************************************************************************************************************************************/
		SELECT 
			pcx_bopsublinecov_jmic.PublicID	AS CoveragePublicID
			,coverageLevelConfigBOPSubLine.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE	AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
 
			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pc_boptransaction.CreateTime AS TrxnCreateTime
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_boptransaction.Amount
				ELSE CASE 
						WHEN pc_bopcost.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
					END
				END AS AdjustedFullTermAmount			
			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

			--Effective Coverage and Locations (only coverage specific)
			,pcx_bopsublinecov_jmic.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_bopsublinecov_jmic.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,Coalesce(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate
			,0 AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date}) pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date})  pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.BOPSubLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans coverageLevelConfigBOPSubLine
			ON coverageLevelConfigBOPSubLine.Key = 'SubLineLevelCoverage'	
			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_bopsublinecov_jmic
			ON pcx_bopsublinecov_jmic.FixedID = pc_bopcost.BOPSubLineCov
				AND pcx_bopsublinecov_jmic.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Cost's BOP Sub Line Cov's Sub Line
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) bopsublinecovsubline
			ON bopsublinecovsubline.FixedID = pcx_bopsublinecov_jmic.BOPSubLine
				AND bopsublinecovsubline.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,bopsublinecovsubline.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,bopsublinecovsubline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) bopSubLineCovCost
			ON bopSubLineCovCost.ID = pc_bopcost.BopSubLineCov

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date})  pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
	
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date})  pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
			ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
				AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
	
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_segment`  pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END
			
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  covEffLoc
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal


		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1


		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*												  L O C A T I O N   C O V G   																					*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pc_boplocationcov.PublicID	AS	CoveragePublicID
			,coverageLevelConfigLocation.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,bopLocationCovBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	ABS(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE	AS SegmentCode
			--,pc_bopcost.Basis AS CostBasis			
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pc_bopLocationCov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_bopLocationCov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_boptransaction.Amount
				ELSE CASE
						WHEN pc_bopcost.NumDaysInRatedTerm = 0
							THEN 0
							ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
					END
				END AS AdjustedFullTermAmount
						
			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

			--Effective Coverage and Locations (only coverage specific)
			,pc_boplocationcov.PublicID AS CoverageEffPublicID
			,COALESCE(pc_boplocationcov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date})  pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})   pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date})   pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})   pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.BOPLocationCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})   perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date})   pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN  `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date})   pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans coverageLevelConfigLocation
			ON coverageLevelConfigLocation.Key = 'LocationLevelCoverage'

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date}) pc_boplocationcov
			ON pc_boplocationcov.FixedID = pc_bopcost.BOPLocationCov
				AND pc_boplocationcov.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,pc_bopLocationCov.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,pc_bopLocationCov.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--LocationCov's BOP Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopLocationCovBOPLocation
			ON bopLocationCovBOPLocation.FixedID = pc_boplocationcov.BOPLocation
				AND bopLocationCovBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,bopLocationCovBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,bopLocationCovBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--LocationCov BOP Location's Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopLocationCovPolicyLocation
			ON bopLocationCovPolicyLocation.FixedID = bopLocationCovBOPLocation.Location
				AND bopLocationCovPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,bopLocationCovPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,bopLocationCovPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--BOP original Coverage Ref in BOP Cost
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date})  bopLocationCovCost
			ON bopLocationCovCost.ID = pc_bopcost.BopLocationCov
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date})  pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
		ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date})  pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopLocationCov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN  `{project}.{pc_dataset}.pctl_segment`  pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state`  costratedState 
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END		
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState 
			ON effectiveLocState.ID = bopLocationCovPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  covEffLoc
			ON covEffLoc.ID = COALESCE(bopLocationCovPolicyLocation.ID, costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal


		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1


		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*											 S U B 	L O C A T I O N   C O V G   																				*/
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_bopsubloccov_jmic.PublicID	AS	CoveragePublicID
			,coverageLevelConfigSubLoc.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,bopSubLocCovBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_boptransaction.Amount
				ELSE CASE
						WHEN pc_bopcost.NumDaysInRatedTerm = 0
						THEN 0
						ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
					END
				END AS AdjustedFullTermAmount
					
			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

			--Effective Coverage and Locations (only coverage specific)
			,pcx_bopsubloccov_jmic.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_bopsubloccov_jmic.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, effectiveLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date}) pc_boptransaction
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_boptransaction.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
				ON pc_boptransaction.BOPCost = pc_bopcost.ID
				AND pc_bopcost.BOPSubLocCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
				ON pc_bopcost.BranchID = perCost.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_policyline.BranchID = pc_bopcost.BranchID
				AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
			INNER JOIN  `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
				--AND pctl_policyline.TYPECODE='BusinessOwnersLine'
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON perCost.UWCompany = pc_uwcompany.ID
			INNER JOIN FinTrans lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN FinTrans coverageLevelConfigSubLoc
				ON coverageLevelConfigSubLoc.Key = 'SubLocLevelCoverage'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic`  WHERE _PARTITIONTIME = {partition_date}) pcx_bopsubloccov_jmic
				ON pcx_bopsubloccov_jmic.FixedID = pc_bopcost.BOPSubLocCov
					AND pcx_bopsubloccov_jmic.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopSubLocCov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopSubLocCov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloc_jmic`  WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovSubLoc
				ON bopSubLocCovSubLoc.FixedID = pcx_bopsubloccov_jmic.BOPsubloc
					AND bopSubLocCovSubLoc.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovSubLoc.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovSubLoc.ExpirationDate,perCost.PeriodEnd) AS DATE)
			--BOP Location Cov's BOP Location ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation`  WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovBOPLocation
				ON bopSubLocCovBOPLocation.FixedID = bopSubLocCovSubLoc.BOPLocation
					AND bopSubLocCovBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
			--BOP Location's Policy Location ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation`  WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovPolicyLocation 
				ON bopSubLocCovPolicyLocation.FixedID = bopSubLocCovBOPLocation.Location
					AND bopSubLocCovPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)		

			--BOP original Coverage Ref in BOP Cost
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date})  bopSubLocCovCost
				ON bopSubLocCovCost.ID = pc_bopcost.BopSubLocCov
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date})  pc_paymentplansummary
				ON perCost.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
				ON costChargePattern.ID = pc_bopcost.ChargePattern
			--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  costPolicyLocation
				ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)

			--Effective Dates Fields
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date})  pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)

			--Effective Fields's Primary Location Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

			--Ensure Effective Primary location matches the BOP's BOP location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopSubLocCov_jmic.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
	
			LEFT JOIN  `{project}.{pc_dataset}.pctl_segment`  pctl_segment
				ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

			--State and Country
			LEFT JOIN `{project}.{pc_dataset}.pctl_state`  costratedState
				ON costratedState.ID = pc_bopcost.RatedState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
				ON primaryLocState.ID = CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
					END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
				ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState
				ON effectiveLocState.ID = bopSubLocCovPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
				ON taxLocState.ID = pc_bopcost.TaxState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState 
				ON costPolicyLocState.ID = costPolicyLocation.StateInternal

			--Coverage Eff Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  covEffLoc
				ON covEffLoc.ID = COALESCE(bopSubLocCovPolicyLocation.ID, costPolicyLocation.ID, 
						CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
							THEN effectiveFieldsPrimaryPolicyLocation.ID
						END)		
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
				ON covEfflocState.ID = covEffloc.StateInternal


			/**** TEST *****/
			--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
			WHERE 1 = 1


		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*											  	B U I L D I N G   C O V G   																				    */
		/****************************************************************************************************************************************************************/
		SELECT 
			pc_bopbuildingcov.PublicID AS CoveragePublicID
			,coverageLevelConfigBopBldg.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,pc_boplocation.PublicID AS BOPLocationPublicID
			,pc_bopbuilding.PublicID AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate, DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_bopBuildingCov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_boptransaction.Amount
				ELSE CASE
						WHEN pc_bopcost.NumDaysInRatedTerm = 0
							THEN 0
						ELSE CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL) 	
					END
				END AS AdjustedFullTermAmount

			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

			--Effective Coverage and Locations (only coverage specific)
			,pc_bopbuildingcov.PublicID AS CoverageEffPublicID
			,COALESCE(pc_bopbuildingcov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,Coalesce(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, effectiveLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedStateCode
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank


		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date})  pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.BOPBuildingCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)	
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans coverageLevelConfigBopBldg
			ON coverageLevelConfigBopBldg.Key = 'BuildingLevelCoverage'

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov`  WHERE _PARTITIONTIME = {partition_date}) pc_bopbuildingcov
			ON pc_bopbuildingcov.FixedID = pc_bopcost.BOPBuildingCov
				AND pc_bopbuildingcov.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,pc_bopbuildingcov.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--BuildingCov's BOP Building
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding`  WHERE _PARTITIONTIME = {partition_date}) pc_bopbuilding
			ON pc_bopbuilding.FixedID = pc_bopbuildingcov.BOPBuilding
				AND pc_bopbuilding.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,pc_bopbuilding.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,pc_bopbuilding.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Building's BOP Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  pc_boplocation
			ON pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
				AND pc_boplocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,pc_boplocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,pc_boplocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Building BOP Location's Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
			ON pc_policylocation.FixedID = pc_boplocation.Location
				AND pc_policylocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd)	AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) bopBuilding
			ON bopBuilding.BranchID = pc_bopcost.BranchID
				AND bopBuilding.FixedID = pc_bopcost.BOPBuilding
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,bopBuilding.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,bopBuilding.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Building's BOP Location	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopBuildingBOPLocation
			ON bopBuildingBOPLocation.FixedID = bopBuilding.BOPLocation
				AND bopBuildingBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,bopBuildingBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,bopBuildingBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  bopBuildingPolicylocation
			ON bopBuildingPolicylocation.FixedID = bopBuildingBOPLocation.Location
				AND bopBuildingPolicylocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,bopBuildingPolicylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,bopBuildingPolicylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--BOP original Coverage Ref in BOP Cost
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date})  bopBuildingCovCost
			ON bopBuildingCovCost.ID = pc_bopcost.BopBuildingCov

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
			ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
				AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopBuildingCov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_segment`  pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  covEffLoc
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal


		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1


		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*											     O N E  T I M E   C O V G   																				    */
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_boponetimecredit_jmic_v2.PublicID	AS	CoveragePublicID
			,pcBOPOneTimeCreditCustomCov.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,bopOneTimeCreditBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionnPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE 	WHEN pc_bopcost.NumDaysInRatedTerm = 0
						THEN 0
					ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			--Effective Coverage and Locations (only coverage specific)
			,pcx_boponetimecredit_jmic_v2.PublicID AS CoverageEffPublicID
			,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPOneTimeCreditCustomCov.Value) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, effectiveLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date}) pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost 
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.BOPOneTimeCredit IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN  `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans pcBOPOneTimeCreditCustomCov 
			ON pcBOPOneTimeCreditCustomCov.Key = 'OneTimeCreditCustomCoverage'
			AND pcBOPOneTimeCreditCustomCov.Value = 'BOPOneTimeCredit_JMIC'

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boponetimecredit_jmic_v2` WHERE _PARTITIONTIME = {partition_date}) pcx_boponetimecredit_jmic_v2
			ON pcx_boponetimecredit_jmic_v2.FixedID = pc_bopcost.BOPOnetimeCredit
				AND pcx_boponetimecredit_jmic_v2.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_boponetimecredit_jmic_v2.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_boponetimecredit_jmic_v2.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopOneTimeCreditBOPLocation
			ON bopOneTimeCreditBOPLocation.FixedID = pcx_boponetimecredit_jmic_v2.BOPLocation
				AND bopOneTimeCreditBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(bopOneTimeCreditBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(bopOneTimeCreditBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--BOPOneTimeCredits BOP Location's Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopOneTimeCreditPolicyLocation
			ON bopOneTimeCreditPolicyLocation.FixedID = bopOneTimeCreditBOPLocation.Location
				AND bopOneTimeCreditPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(bopOneTimeCreditPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(bopOneTimeCreditPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_onetimecreditreason_jmic` bopOneTimeCreditReason
			ON bopOneTimeCreditReason.ID = pcx_boponetimecredit_jmic_v2.CreditReason						
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		--Charge Lookup
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date})  pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END		
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` effectiveLocState
			ON effectiveLocState.ID = bopOneTimeCreditPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  covEffLoc
			ON covEffLoc.ID = COALESCE(bopOneTimeCreditPolicyLocation.ID, costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal


		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1


		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*									A D D I T I O N A L  I N S U R E D  C U S T O M  C O V G 																    */
		/****************************************************************************************************************************************************************/
		
		SELECT 
			pc_boptransaction.PublicID	AS	CoveragePublicID
			,pcBOPAdditionalInsuredCustomCov.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE	WHEN pc_bopcost.NumDaysInRatedTerm = 0
						THEN 0
					ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			--Effective Coverage and Locations (only coverage specific)
			,'None' AS CoverageEffPublicID
			,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPAdditionalInsuredCustomCov.Value) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date})  pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND pc_bopcost.AdditionalInsured IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN  `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans pcBOPAdditionalInsuredCustomCov 
			ON pcBOPAdditionalInsuredCustomCov.Key = 'AdditionalInsuredCustomCoverage'
			AND pcBOPAdditionalInsuredCustomCov.Value = 'Additional_Insured_JMIC'
	
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup
		--PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END		
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal

		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1

		
		UNION ALL

		/****************************************************************************************************************************************************************/
		/*												B	O	P	     D I R E C T																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*												 O T H E R   N O  C O V G   																					*/
		/****************************************************************************************************************************************************************/
		SELECT 
			'None' AS CoveragePublicID
			,coverageLevelConfigOther.Value AS CoverageLevel
			,pc_boptransaction.PublicID AS TransactionPublicID
			,COALESCE(costPolicyLocation.PublicID, CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.PublicID
					END) AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_boptransaction.EffDate,perCost.PeriodEnd)  AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_boptransaction.Amount AS TransactionAmount
			,pc_boptransaction.PostedDate AS TransactionPostedDate
			,CAST(pc_boptransaction.WrittenDate as DATE) AS TransactionWrittenDate
			,CAST(pc_boptransaction.ToBeAccrued AS INT64) AS CanBeEarned
			,CAST(pc_boptransaction.EffDate as DATE) AS EffectiveDate
			,CAST(pc_boptransaction.ExpDate as DATE) AS ExpirationDate
			,CASE 
				WHEN pc_boptransaction.Amount = 0 OR pc_bopcost.NumDaysInRatedTerm = 0 THEN 0
				ELSE (	abs(pc_bopcost.ActualTermAmount) * --Full term 
						TIMESTAMP_DIFF(pc_boptransaction.EffDate, pc_boptransaction.ExpDate,DAY) / pc_bopcost.NumDaysInRatedTerm
						) * --rated factor 
					pc_boptransaction.Amount / ABS(pc_boptransaction.Amount) --Side 
				END AS PrecisionAmount
			,pctl_segment.TYPECODE AS SegmentCode
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			,costChargePattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

			,CASE WHEN pc_boptransaction.BranchID = pc_bopcost.BranchID THEN 1 ELSE 0 END AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,pc_policyperiod.PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			,CASE WHEN pc_bopcost.NumDaysInRatedTerm = 0
					THEN 0
					ELSE ROUND(CAST(pc_bopcost.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd,DAY) AS FLOAT64) / CAST(pc_bopcost.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount

			--Primary ID and Location; Effective and Primary Fields
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

			--Effective Coverage and Locations (only coverage specific)
			,'None' AS CoverageEffPublicID
			,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode

			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,primaryLocState.TYPECODE AS PrimaryLocationStateCode
			,'None' AS CoverageLocationEffLocationStateCode
			,taxLocState.TYPECODE AS TaxLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,perCost.EditEffectiveDate AS CostEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,ROW_NUMBER() OVER(PARTITION BY pc_boptransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_boptransaction` WHERE _PARTITIONTIME = {partition_date}) pc_boptransaction
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_boptransaction.BranchID = pc_policyperiod.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
			ON pc_boptransaction.BOPCost = pc_bopcost.ID
			AND -1=COALESCE(pc_bopcost.BOPLocationCov
			,pc_bopcost.BusinessOwnersCov
			,pc_bopcost.BOPBuildingCov
			,pc_bopcost.BOPSubLocCov
			,pc_bopcost.BOPSubLineCov
			,pc_bopcost.BOPOneTimeCredit
			,pc_bopcost.AdditionalInsured
			,-1)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON pc_bopcost.BranchID = perCost.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_bopcost.BranchID
			AND pc_policyline.FixedID = pc_bopcost.BusinessOwnersLine	--Added from DW code to prevent dupes
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pc_boptransaction.EffDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
			ON perCost.UWCompany = pc_uwcompany.ID
		--INNER JOIN `{project}.{pc_dataset}.pc_policy`  pc_policy
		--	ON pc_policy.Id = pc_policyperiod.PolicyId
		INNER JOIN FinTrans lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN FinTrans coverageLevelConfigOther
			ON coverageLevelConfigOther.Key = 'NoCoverage'
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
			ON perCost.ID = pc_paymentplansummary.PolicyPeriod
			AND pc_paymentplansummary.Retired = 0
		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` costChargePattern
			ON costChargePattern.ID = pc_bopcost.ChargePattern
		--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
		--Effective Dates Fields
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date})  pc_effectivedatedfields
			ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Effective Fields's Primary Location Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryPolicyLocation
			ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--Ensure Effective Primary location matches the BOP's BOP location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date})  effectiveFieldsPrimaryBOPLocation
			ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
				AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcost.EffectiveDate,perCost.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_segment`   pctl_segment
			ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)

		--State and Country
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState
			ON costratedState.ID = pc_bopcost.RatedState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState
			ON primaryLocState.ID = CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
				END
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState
			ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState
			ON taxLocState.ID = pc_bopcost.TaxState
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPolicyLocState
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal

		--Coverage Eff Policy Location
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc	 
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.ID
					END)
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState
			ON covEfflocState.ID = covEffloc.StateInternal


		/**** TEST *****/
		--WHERE pc_policyperiod.PolicyNumber = IFNULL(@policynumber, pc_policyperiod.PolicyNumber)
		WHERE 1 = 1

		) FinTrans
		INNER JOIN FinTrans sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN FinTrans hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN FinTrans hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN FinTrans businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN FinTrans locationRisk
			ON locationRisk.Key='LocationLevelRisk'
		INNER JOIN FinTrans buildingRisk
			ON buildingRisk.Key='BuildingLevelRisk'

		WHERE 1=1
		--	AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1
		

) extractData 
