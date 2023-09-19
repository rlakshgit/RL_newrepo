/*TEST*/
--DECLARE vpolicynumber STRING; DECLARE {partition_date} Timestamp;
--SET vpolicynumber = '55-006970';
--UMB, ceded
--'55-009622' --'55-000093' --55-006970
--55-001926		--lots of records
--55-011140 --55-002846
/**********************************************************************************************************************************/


/**** Kimberlite - Financial Transactions ********************
		FinancialTransactionUMBCeded.sql
			CONVERTED TO BIG QUERY
-------------------------------------------------------------------------------------------------------------------
	*****  Sections  *****
-------------------------------------------------------------------------------------------------------------------
		UmbrellaLineCov 			--Line
		No Covg						--No Coverage
-------------------------------------------------------------------------------------------------------------------

	*****  Change History  *****

	04/02/2021	DROBAK		Update No Coverage Section; standardize columns
	06/29/2021	DROBAK		Commented out "Is" fields in Peaks, temps section 
	06/29/2021	DROBAK		Add field IsTransactionSliceEffective; Cov & Locn left joins; updated Unit Tests
	09/03/2021	DROBAK		Change CoveragePublicID & LineCode from 'None' to CAST(NULL AS STRING) (BQ only change)
	10/12/2021	DROBAK		ROUND to zero AdjustedFullTermAmount
	10/28/2021	DROBAK		Remove use of minBOPPolicyLocationOnBranch - change BOPLocationPublicID (like CoverageUMB) & RatingLocationPublicID (like IM & BOP)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode); retool the syntax to look like other queries (no logic changes)

-------------------------------------------------------------------------------------------------------------------
	NOTE:  
		BigQuery UDF contained within

*******************************************************************************************************************/

WITH FinConfig AS
 (SELECT 'SourceSystem' as Key, 'GW' as Value UNION ALL
  SELECT 'HashKeySeparator', '_' UNION ALL
  SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
  SELECT 'LineCode', 'UmbrellaLine_JMIC' UNION ALL
  SELECT 'BusinessType', 'Ceded' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'CededCoverable', 'DefaultCov' UNION ALL
  SELECT 'LocationLevelRisk', 'BusinessOwnersLocation'  )
  
SELECT
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,UMBCoverageKey
	,RiskLocationKey
	,BusinessType
	,CededCoverable
	,CoveragePublicID
	,TransactionPublicID
	,JobNumber
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
	,CostPublicID
	,PolicyPeriodPublicID
	,RatingLocationPublicID
	,CedingRate
	,CededAmount
	,CededCommissions
	,CededTermAmount
	,RIAgreementType
	,RIAgreementNumber
	,CededID
	,CededAgreementID
	,RICoverageGroupID
	,RICoverageGroupType
	,LineCode
    ,DATE('{date}') as bq_load_date		
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,CASE WHEN CoveragePublicID IS NOT NULL
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
		END AS UMBCoverageKey
		,CASE WHEN BOPLocationPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,BOPLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
		END AS RiskLocationKey
		,businessTypeConfig.Value AS BusinessType
		,FinTrans.*
	FROM (
		/****************************************************************************************************************************************************************/
		/*												  U M B 	     C E D E D   																					*/
		/****************************************************************************************************************************************************************/
		/****************************************************************************************************************************************************************/
		/*													L I N E    C O V G   																					    */
		/****************************************************************************************************************************************************************/
		SELECT 
			pcx_umbrellalinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_umbcededpremiumtrans_jmic.PublicID AS TransactionPublicID
			--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS BOPLocationPublicID
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.PublicID
					ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				END AS BOPLocationPublicID		-- f/k/a CoverageLocationEffPublicID
			*/
			,IFNULL(pc_policyperiod.PeriodID,perCost.PeriodID) AS PolicyPeriodPeriodID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_umbcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_umbcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_umbcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			--,CASE WHEN pcx_umbcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			/*,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_umbcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_umbcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, pc_policyperiod.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			*/
			,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_umbcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_umbcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, pc_policyperiod.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			--,1 AS TrxnAccrued
			,CAST(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged	--not sent to billing			
			--,pcx_umbcededpremiumtrans_jmic.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			,pctl_segment.TYPECODE AS SegmentCode
			--,pcx_umbcost_jmic.Basis AS CostBasis
			--,pcx_umbcost_jmic.ActualBaseRate
			--,pcx_umbcost_jmic.ActualAdjRate
			,pcx_umbcost_jmic.ActualTermAmount
			,pcx_umbcost_jmic.ActualAmount
			,pctl_chargepattern.TYPECODE AS ChargePattern
			,pcx_umbcost_jmic.ChargeGroup --stored as a code, not ID. 
			,pcx_umbcost_jmic.ChargeSubGroup --stored as a code, not ID. 
			/*,CASE 
				WHEN pcx_umbcost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
				END AS CostEffectiveDate
			,CASE 
				WHEN pcx_umbcost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			--,1 AS Onset			
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
			--,pcx_umbcost_jmic.Subtype AS CostSubtype
			,perCost.PolicyNumber AS PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_umbcost_jmic.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_umbcededpremiumtrans_jmic.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_umbcost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			/*
				Notes:
				1) Full Term Premium = Ceded Premium prorated over the period start / end
				2) peaks and temps = na
				3) Normal Ceded Trxns 
			*/
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate), IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
					THEN CASE 
							WHEN IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
								AND IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
								THEN pcx_umbcededpremiumtrans_jmic.CededPremium
							ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) 
								* pcx_umbcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate)
								,IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
							END
				ELSE 0
				END AS AdjustedFullTermAmount
			--Primary ID and Location
			,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			,pcx_umbrellalinecov_jmic.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_umbrellalinecov_jmic.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CoverageEffPatternCode
			/*,CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.LocationNum
				ELSE effectiveFieldsPrimaryPolicyLocation.LocationNum
				END AS CoverageLocationEffNumber
			*/
			,COALESCE(umbrellalinecovCost.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CostCoverageCode
			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,'None' AS CoverageLocationEffLocationStateCode
			,perCost.EditEffectiveDate AS CostEditEffectiveDate
			--,perCost.EditEffectiveDate AS TrxnEditEffectiveDate
			--Peaks and Temp and SERP Related Data
			--,0 AS IsPeakOrTemp
			--,CASE WHEN umbrellalinecovCost.PatternCode IS NULL THEN 0 ELSE 1 END AS IsSupplementalCoverage
			--,0 AS IsAdditionalInsuredCoverage
			--,0 AS IsOneTimeCreditCoverage
			--,0 AS IsAdditionalInsuredCoverage
			--,0 AS IsOneTimeCreditCoverage			
			--,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1 END AS IsPrimaryLocationBOPLocation
			--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--Ceding Data
			,pcx_umbcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_umbcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_umbcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_umbcost_jmic.ActualTermAmount * pcx_umbcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_umbcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
	  		,ROW_NUMBER() OVER(PARTITION BY pcx_umbcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM `{project}.{pc_dataset}.pcx_umbcededpremiumtrans_jmic` pcx_umbcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcededpremiumjmic
				ON pcx_umbcededpremiumjmic.ID = pcx_umbcededpremiumtrans_jmic.UMBCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcost_jmic
				ON pcx_umbcededpremiumjmic.UMBCovCost_JMIC = pcx_umbcost_jmic.ID
				AND pcx_umbcost_jmic.UmbrellaLineCov IS NOT NULL

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
				ON pcx_umbcost_jmic.BranchID = perCost.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = perCost.JobID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON perCost.UWCompany = pc_uwcompany.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
				ON pc_policy.Id = perCost.PolicyId
			LEFT JOIN FinConfig CededCoverable 
				ON CededCoverable.Key = 'CededCoverable'
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pcx_umbcost_jmic.UMBLine = pc_policyline.FixedID
				AND pcx_umbcost_jmic.BranchID = pc_policyline.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			LEFT JOIN FinConfig lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			LEFT JOIN FinConfig coverageLevelConfig
				ON coverageLevelConfig.Key = 'LineLevelCoverage' 
			--Cost's UmbrellaLineCov
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbrellalinecov_jmic
				ON pcx_umbrellalinecov_jmic.FixedID = pcx_umbcost_jmic.UmbrellaLineCov
				AND pcx_umbrellalinecov_jmic.BranchID = pcx_umbcost_jmic.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_umbrellalinecov_jmic.ExpirationDate,perCost.PeriodEnd)	
			--Coverage from Cost
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) umbrellalinecovCost 
				ON umbrellalinecovCost.ID = pcx_umbcost_jmic.UmbrellaLineCov 
			--Cede Agreement
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_umbcededpremiumtrans_jmic.Agreement
			LEFT JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm 
				ON pc_policyterm.ID = pcx_umbcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
				ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
					AND pc_policyperiod.ModelDate < pcx_umbcededpremiumtrans_jmic.CalcTimestamp			
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup 
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID					
			/*There may be more than one coverage group; but within the same GL Payables category*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary 
				ON perCost.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pcx_umbcost_jmic.ChargePattern
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
			--Ensure Effective Primary location matches the BOP's BOP location (for UMB)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd)

/*			--Get the minimum location (number) for this line and branch
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) minBOPPolicyLocationOnBranch
				ON  minBOPPolicyLocationOnBranch.BranchID = pcx_umbcost_jmic.BranchID --pc_policyperiod.ID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(minBOPPolicyLocationOnBranch.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(minBOPPolicyLocationOnBranch.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation2 
				ON minBOPPolicyLocationOnBranch.FixedID = pc_boplocation2.Location
					AND pc_boplocation2.BranchID = pcx_umbcost_jmic.BranchID				
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_boplocation2.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_boplocation2.ExpirationDate,perCost.PeriodEnd)
*/	
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
			--State and Country
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState ON costratedState.ID = pcx_umbcost_jmic.RatedState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
				ON primaryLocState.ID = CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
					END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState ON taxLocState.ID = pcx_umbcost_jmic.TaxJurisdiction

/*			--Policy Period Cost Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPerEfflocation
				ON costPerEfflocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(costPerEfflocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(costPerEfflocation.ExpirationDate,perCost.PeriodEnd)	
					AND costPerEfflocation.StateInternal = Coalesce(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState ON costPerEfflocationState.ID = costPerEfflocation.StateInternal
*/			
			--Coverage Eff Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
				ON covEffLoc.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.ID END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState ON covEfflocState.ID = covEffloc.StateInternal

/*			--Rating Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
				ON ratingLocation.ID = CASE 
					WHEN covEffLoc.ID IS NOT NULL
						AND covEfflocState.ID IS NOT NULL
						AND covEfflocState.ID = Coalesce(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN covEffLoc.ID
					WHEN costPerEfflocation.PublicID IS NOT NULL
						AND CostPerEfflocationState.ID = Coalesce(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
						THEN costPerEfflocation.ID
					ELSE - 1
					END
*/			
			--WHERE pcx_umbcededpremiumtrans_jmic.CreateTime>@StartDate --all transactions that where strictly posted after last run's posted date. Posted date is the date when job completes

			WHERE 1 = 1
			AND pcx_umbcededpremiumtrans_jmic._PARTITIONTIME = {partition_date}

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
			CAST(NULL AS STRING) AS CoveragePublicID
			--'None' AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_umbcededpremiumtrans_jmic.PublicID AS TransactionPublicID
			--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS BOPLocationPublicID
			/*,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.PublicID
					ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				END AS BOPLocationPublicID		-- f/k/a CoverageLocationEffPublicID
			*/
			,IFNULL(pc_policyperiod.PeriodID,perCost.PeriodID) AS PolicyPeriodPeriodID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_umbcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			--,'None' AS LineCode
			,CAST(NULL AS STRING) AS LineCode
			,pcx_umbcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_umbcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			--,CASE WHEN pcx_umbcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			/*,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_umbcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_umbcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, pc_policyperiod.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			*/
			,`{project}.custom_functions.fn_GetMaxDate`
				(	`{project}.custom_functions.fn_GetMaxDate`(CAST(pcx_umbcededpremiumtrans_jmic.DatePosted AS DATE), CAST(pcx_umbcededpremiumtrans_jmic.EffectiveDate AS DATE))
					,CAST(IFNULL(pc_policyperiod.EditEffectiveDate, pc_policyperiod.EditEffectiveDate) AS DATE)
				) AS TransactionWrittenDate
			--,1 AS TrxnAccrued
			,CAST(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			--,0 AS TrxnCharged	--not sent to billing
			--,pcx_umbcededpremiumtrans_jmic.CededPremium AS PrecisionAmount
			,COALESCE(costRatedState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedStateCode
			,pctl_segment.TYPECODE AS SegmentCode
			--,pcx_umbcost_jmic.Basis AS CostBasis
			--,pcx_umbcost_jmic.ActualBaseRate
			--,pcx_umbcost_jmic.ActualAdjRate
			,pcx_umbcost_jmic.ActualTermAmount
			,pcx_umbcost_jmic.ActualAmount
			,pctl_chargepattern.TYPECODE
			,pcx_umbcost_jmic.ChargeGroup --stored as a code, not ID. 
			,pcx_umbcost_jmic.ChargeSubGroup --stored as a code, not ID. 
			/*,CASE 
				WHEN pcx_umbcost_jmic.EffectiveDate IS NOT NULL
					THEN CAST(CAST(pcx_umbcost_jmic.EffectiveDate AS STRING) AS DATE) -- CAST null eff / exp dates to Period start / end dates
				END AS CostEffectiveDate
			,CASE 
				WHEN pcx_umbcost_jmic.ExpirationDate IS NOT NULL
					THEN CAST(pcx_umbcost_jmic.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					ELSE CAST(perCost.PeriodEnd AS DATE)
				END AS CostExpirationDate
			*/
			--,1 AS Onset
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
			--,pcx_umbcost_jmic.Subtype AS CostSubtype
			,perCost.PolicyNumber AS PolicyNumber
			,perCost.PublicID AS PolicyPeriodPublicID
			,pcx_umbcost_jmic.PublicID AS CostPublicID
			--,'None' AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pcx_umbcededpremiumtrans_jmic.CreateTime AS TrxnCreateTime
			--,CAST(perCost.PeriodStart AS DATE) AS PeriodStart
			--,CAST(perCost.PeriodEnd AS DATE) AS PeriodEnd
			,pcx_umbcost_jmic.NumDaysInRatedTerm AS NumDaysInRatedTerm
			/*
				Notes:
				1) Full Term Premium = Ceded Premium prorated over the period start / end
				2) peaks and temps = na
				3) Normal Ceded Trxns 
			*/
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate), IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
					THEN CASE 
							WHEN IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate) = perCost.PeriodStart
								AND IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate) = perCost.PeriodEnd
								THEN pcx_umbcededpremiumtrans_jmic.CededPremium
							ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(perCost.PeriodStart, perCost.PeriodEnd, DAY) AS FLOAT64) 
								* pcx_umbcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_umbcededpremiumtrans_jmic.EffectiveDate, pcx_umbcededpremiumjmic.EffectiveDate)
								,IFNULL(pcx_umbcededpremiumtrans_jmic.ExpirationDate, pcx_umbcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
							END
				ELSE 0
				END AS AdjustedFullTermAmount
			--Primary ID and Location
			--Effective and Primary Fields
			,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			--Effective Coverage and Locations (only coverage specific)
			,pcx_umbrellalinecov_jmic.PublicID AS CoverageEffPublicID
			,COALESCE(pcx_umbrellalinecov_jmic.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CoverageEffPatternCode
			/*,CASE 
				WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
					THEN minBOPPolicyLocationOnBranch.LocationNum
				ELSE effectiveFieldsPrimaryPolicyLocation.LocationNum
				END AS CoverageLocationEffNumber
			*/
			,COALESCE(umbrellalinecovCost.PatternCode, pcx_umbcost_jmic.ChargeSubGroup, pcx_umbcost_jmic.ChargeGroup) AS CostCoverageCode
			--Peaks and Temp, SERP, ADDN Insured, OneTime
			--,ratingLocation.PublicID AS RatingLocationPublicID
			,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,'None' AS CoverageLocationEffLocationStateCode
			,perCost.EditEffectiveDate AS CostEditEffectiveDate
			--,perCost.EditEffectiveDate AS TrxnEditEffectiveDate
			--Peaks and Temp and SERP Related Data
			--,0 AS IsPeakOrTemp
			--,CASE WHEN umbrellalinecovCost.PatternCode IS NULL THEN 0 ELSE 1	END AS IsSupplementalCoverage
			--,0 AS IsAdditionalInsuredCoverage
			--,0 AS IsOneTimeCreditCoverage
			--,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1	END AS IsPrimaryLocationBOPLocation
			--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--Ceding Data
			,pcx_umbcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_umbcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_umbcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_umbcost_jmic.ActualTermAmount * pcx_umbcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_umbcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
	  		,ROW_NUMBER() OVER(PARTITION BY pcx_umbcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_umbcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM `{project}.{pc_dataset}.pcx_umbcededpremiumtrans_jmic` pcx_umbcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcededpremiumjmic
				ON pcx_umbcededpremiumjmic.ID = pcx_umbcededpremiumtrans_jmic.UMBCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbcost_jmic 
				ON pcx_umbcededpremiumjmic.UMBCovCost_JMIC = pcx_umbcost_jmic.ID
				AND -1=COALESCE(pcx_umbcost_jmic.UmbrellaLineCov,-1)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
				ON pcx_umbcost_jmic.BranchID = perCost.ID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = perCost.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON perCost.UWCompany = pc_uwcompany.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
				ON pc_policy.Id = perCost.PolicyId
			INNER JOIN FinConfig CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN FinConfig coverageLevelConfig ON coverageLevelConfig.Key = 'NoCoverage' 
			--Cost's UmbrellaLineCov
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_umbrellalinecov_jmic
				ON pcx_umbrellalinecov_jmic.FixedID = pcx_umbcost_jmic.UmbrellaLineCov
				AND pcx_umbrellalinecov_jmic.BranchID = pcx_umbcost_jmic.BranchID
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pcx_umbrellalinecov_jmic.EffectiveDate,perCost.PeriodStart)
				AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pcx_umbrellalinecov_jmic.ExpirationDate,perCost.PeriodEnd)	
			--Coverage from Cost
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_umbrellalinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) umbrellalinecovCost 
				ON umbrellalinecovCost.ID = pcx_umbcost_jmic.UmbrellaLineCov 
			--Cede Agreement
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_umbcededpremiumtrans_jmic.Agreement
			LEFT JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
				
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm 
				ON pc_policyterm.ID = pcx_umbcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_policyperiod.PolicyTermID = pc_policyterm.ID
					AND pc_policyperiod.ModelDate < pcx_umbcededpremiumtrans_jmic.CalcTimestamp							
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup 
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID

			/*There may be more than one coverage group; but within the same GL Payables category*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary 
				ON perCost.ID = pc_paymentplansummary.PolicyPeriod AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pcx_umbcost_jmic.ChargePattern
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
			--Ensure Effective Primary location matches the BOP's BOP location (for UMB)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,perCost.PeriodEnd)

/*			--Get the minimum location (number) for this line and branch
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) minBOPPolicyLocationOnBranch
				ON  minBOPPolicyLocationOnBranch.BranchID = pcx_umbcost_jmic.BranchID --pc_policyperiod.ID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(minBOPPolicyLocationOnBranch.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(minBOPPolicyLocationOnBranch.ExpirationDate,perCost.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation2 
				ON minBOPPolicyLocationOnBranch.FixedID = pc_boplocation2.Location
					AND pc_boplocation2.BranchID = pcx_umbcost_jmic.BranchID				
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(pc_boplocation2.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(pc_boplocation2.ExpirationDate,perCost.PeriodEnd)
*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, perCost.Segment)
			--State and Country
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState ON costratedState.ID = pcx_umbcost_jmic.RatedState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
				ON primaryLocState.ID = CASE 
					WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
					END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryPolicyLocState ON primaryPolicyLocState.ID = effectiveFieldsPrimaryPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` taxLocState ON taxLocState.ID = pcx_umbcost_jmic.TaxJurisdiction

/*			--Policy Period Cost Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPerEfflocation
				ON costPerEfflocation.BranchID = pcx_umbcost_jmic.BranchID
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) >= COALESCE(costPerEfflocation.EffectiveDate,perCost.PeriodStart)
					AND COALESCE(pcx_umbcost_jmic.EffectiveDate,perCost.EditEffectiveDate) < COALESCE(costPerEfflocation.ExpirationDate,perCost.PeriodEnd)	
					AND costPerEfflocation.StateInternal = COALESCE(pcx_umbcost_jmic.RatedState, taxLocState.ID, primaryLocState.ID)
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState ON costPerEfflocationState.ID = costPerEfflocation.StateInternal
*/
			--Coverage Eff Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc
				ON covEffLoc.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.ID END
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState ON covEfflocState.ID = covEffloc.StateInternal

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
			--WHERE pcx_umbcededpremiumtrans_jmic.CreateTime>@StartDate --all transactions that where strictly posted after last run's posted date. Posted date is the date when job completes
			
			WHERE 1 = 1
			AND pcx_umbcededpremiumtrans_jmic._PARTITIONTIME = {partition_date}

			/**** TEST *****/
		--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)


		) FinTrans
		INNER JOIN FinConfig AS sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN FinConfig AS hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN FinConfig AS hashAlgorithm
			ON hashAlgorithm.Key = 'HashAlgorithm'
		INNER JOIN FinConfig AS businessTypeConfig
			ON businessTypeConfig.Key = 'BusinessType'
		INNER JOIN FinConfig AS locationRisk
			ON locationRisk.Key='LocationLevelRisk'

		WHERE 1=1
	--		AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
			AND	TransactionPublicID IS NOT NULL
			AND TransactionRank = 1

) extractData
