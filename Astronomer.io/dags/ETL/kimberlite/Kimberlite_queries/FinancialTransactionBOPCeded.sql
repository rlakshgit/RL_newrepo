-- tag: FinancialTransactionBOPCeded - tag ends/
/**** Kimberlite - Financial Transactions ********************
		FinancialTransactionBOPCeded.sql
			CONVERTED TO BIG QUERY 
-------------------------------------------------------------------------------------------------------------------
	*****  Change History  *****

	10/01/2020	DROBAK		Init create
	02/22/2021	DROBAK		[1] - BOP DefaultCov - Line Cov -- added field for BOPLocationPublicID
							[2] - BOP BOPCov - Line Cov -- added field for BOPLocationPublicID
							[3] - Fixed join with transaction tables from >= and <= to just =
	03/30/2021	DROBAK		Output field cleanup; Finalized the FinancialTransactionKey
	04/05/2021	DROBAK		Changed name to use BuildingPublicID like Risk Bldg extract
	04/05/2021	DROBAK		Commented out "Is" fields in Peaks, temps section
	06/22/2021	DROBAK		Misc field clean-up; add to final select IsPrimaryLocationBOPLocation, BOPLocationPublicID
	06/23/2021	DROBAK		Add field IsTransactionSliceEffective; Cov & Locn left joins, updated Unit Tests
	08/19/2021	DROBAK		Remove use of minBOPPolicyLocationOnBranch - change RatingLocationPublicID code to work like IM
	08/20/2021	DROBAK		Replace field ChargePatternCode with ChargePattern (to match other tables)
	09/01/2021	DROBAK		Change CoveragePublicID, BuildingPublicID from 'None' to CAST(NULL AS STRING), where applicable (BQ only change)
	10/03/2021	DROBAK		Use new Transaction Date logic instead of Cost date table
	10/14/2021	DROBAK		Round AdjustedFullTermAmount
	11/01/2021	DROBAK		Fix TransactionWrittenDate logic to match DW (spSTG_FactPremiumWritten_Extract_GW)
	12/15/2021	DROBAK		BQ change since no subqueries allowed. For Line & Subline: get min Policy location when BOP Location is null (uses temp tbl) for Location Key
	12/21/2021	DROBAK		Correct RatingLocation & add Policy Period Cost Location (use same test case: trxnid = 323331 or  BranchID = 9647239 or pc_bopcededpremiumtransaction.PublicID = 'pc:323331')
							-- Corrected some join syntax specifically for BQ (shift _PARTITIONTIME from where clause to join)	
	01/27/2022	DROBAK		Correct TransactionWrittenDate to be DATE (not Datetime)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode)
	04/13/2023	DROBAK		Correction: remove hardcoded dates for testing

-------------------------------------------------------------------------------------------------------------------
		NOTE:  Substitute BigQuery UDF for GW_Reporting version
-------------------------------------------------------------------------------------------------------------------
		Questions:
			RIType uses gw_gl map tables - do we need replacement value & logic?
-------------------------------------------------------------------------------------------------------------------
*******************************************************************************************************************/

CREATE OR REPLACE TEMP TABLE FinTranBOPCedMaxLoc
AS
(
	SELECT MAX(pc_policylocation.Id) as MaxID
			,pc_bopcost.ID AS CostID
			,pc_policylocation.BranchID
			,pc_policylocation.StateInternal
	FROM 
	(
		(SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON pc_policylocation.BranchID = pc_bopcost.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_bopcost.BranchID = pc_policyperiod.ID
				AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= pc_policylocation.EffectiveDate or pc_policylocation.EffectiveDate is null) 
				AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < pc_policylocation.ExpirationDate or pc_policylocation.ExpirationDate is null)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
				ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)		
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
				ON taxLocState.ID = pc_bopcost.TaxState
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
				ON costPolicyLocState.ID = costPolicyLocation.StateInternal		 
			INNER JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
				ON pctl_state.id = pc_policylocation.stateinternal 
				AND pc_policylocation.StateInternal = COALESCE(pc_bopcost.RatedState,costPolicyLocState.ID,taxLocState.ID)

	)
		GROUP BY pc_bopcost.ID
				,pc_policylocation.BranchID
				,pc_policylocation.StateInternal
);

CREATE OR REPLACE TEMP TABLE FinTranBOPCedMinLoc
AS
(
    SELECT
        MIN(PC_POLICYLOCATION2.PublicId) As PublicId
        , PC_POLICYLOCATION2.BranchID AS PolLocBranchID
        , PC_BOPLOCATION2.BranchID AS BopLocBranchID
        , PC_POLICYLOCATION2.EffectiveDate AS PolLocEffectiveDate
        , PC_POLICYLOCATION2.ExpirationDate AS PolLocExpirationDate
        , PC_BOPLOCATION2.EffectiveDate AS BopLocEffectiveDate
        , PC_BOPLOCATION2.ExpirationDate AS BopLocExpirationDate
       
    FROM  
    (
        (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS PC_POLICYLOCATION2
        INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS PC_BOPLOCATION2
			ON PC_POLICYLOCATION2.FixedID = PC_BOPLOCATION2.Location
			--AND PC_POLICYLOCATION2.BranchID = 9647239
    	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost
            ON pc_bopcost.BranchID = PC_POLICYLOCATION2.BranchID
            AND pc_bopcost.BranchID = PC_BOPLOCATION2.BranchID
			AND pc_bopcost.BusinessOwnersCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium`  WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcededpremium
            ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
        INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcededpremiumtransaction
			ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
    )
    GROUP BY PC_POLICYLOCATION2.BranchID
        , PC_BOPLOCATION2.BranchID
		, PC_POLICYLOCATION2.EffectiveDate 
        , PC_POLICYLOCATION2.ExpirationDate
        , PC_BOPLOCATION2.EffectiveDate
        , PC_BOPLOCATION2.ExpirationDate
);

CREATE OR REPLACE TEMP TABLE FinTranBOPCovCedMinLoc
AS
(
    SELECT
        MIN(PC_POLICYLOCATION2.PublicId) As PublicId
        , PC_POLICYLOCATION2.BranchID AS PolLocBranchID
        , PC_BOPLOCATION2.BranchID AS BopLocBranchID
        , PC_POLICYLOCATION2.EffectiveDate AS PolLocEffectiveDate
        , PC_POLICYLOCATION2.ExpirationDate AS PolLocExpirationDate
        , PC_BOPLOCATION2.EffectiveDate AS BopLocEffectiveDate
        , PC_BOPLOCATION2.ExpirationDate AS BopLocExpirationDate
       
    FROM 
    (
              (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS PC_POLICYLOCATION2
                INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS PC_BOPLOCATION2
			    	ON PC_POLICYLOCATION2.FixedID = PC_BOPLOCATION2.Location
				    --AND PC_POLICYLOCATION2.BranchID = 9647239
    			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) AS pc_bopcost
                    ON pc_bopcost.BranchID = PC_POLICYLOCATION2.BranchID
                    AND pc_bopcost.BranchID = PC_BOPLOCATION2.BranchID
				    AND pc_bopcost.BusinessOwnersCov IS NOT NULL
         		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremium` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopcovcededpremium
                    ON pcx_bopcovcededpremium.BOPCovCost = pc_bopcost.ID
                INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremtransaction` WHERE _PARTITIONTIME = {partition_date}) AS pcx_bopcovcededpremtransaction
			    	ON pcx_bopcovcededpremium.ID = pcx_bopcovcededpremtransaction.BOPCovCededPremium
    )
    GROUP BY PC_POLICYLOCATION2.BranchID
        , PC_BOPLOCATION2.BranchID
        , PC_POLICYLOCATION2.EffectiveDate 
        , PC_POLICYLOCATION2.ExpirationDate
        , PC_BOPLOCATION2.EffectiveDate
        , PC_BOPLOCATION2.ExpirationDate
);

CREATE OR REPLACE TEMP TABLE FinTranBOPCed1 
AS
(
WITH FinConfigBOPCeded AS (
	SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
	SELECT 'LineCode','BusinessOwnersLine' UNION ALL
	SELECT 'BusinessType', 'Ceded' UNION ALL
	/*CoverageLevel Values */
	SELECT 'LineLevelCoverage','Line' UNION ALL
	SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
	SELECT 'LocationLevelCoverage','Location' UNION ALL
	SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
	SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
	SELECT 'OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC' UNION ALL
	SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
	SELECT 'NoCoverage','NoCoverage' UNION ALL
	/*Ceded Coverable Types */
	SELECT 'CededCoverable', 'DefaultCov' UNION ALL
	SELECT 'CededCoverableBOP', 'BOPCov' UNION ALL
	SELECT 'CededCoverableBOPBldg', 'BOPBuildingCov' UNION ALL
	/* Risk Key Values */
	SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
	SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
)

		/*--------------------------------------------
			BOP    Default Cov   CEDED
				  LINE   COVG
		---------------------------------------------*/
		SELECT 
			pc_businessownerscov.PublicID AS CoveragePublicID
			,coverageLevelConfigBusinessOwnersLine.Value AS CoverageLevel
			,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
			--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			,CASE WHEN effectiveFieldsPrimaryBOPLocation.PublicID IS NULL
					THEN minBOPPolicyLocationOnBranch.PublicID
				ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				END AS BOPLocationPublicID
			,CAST(NULL AS STRING) AS BuildingPublicID
			--,'None' AS BuildingPublicID
			,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
			,pc_job.JobNumber AS JobNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
			,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			/*
			Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
			Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
			[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
			the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
			appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
			until the end of peak and temp, when it will fully earn
			*/
			--,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
			--			,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATETIME) AS TransactionWrittenDate
			,cast( custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(cast(pc_bopcededpremiumtransaction.DatePosted as date),
					--peaks and temps
			    	CASE	WHEN (pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
							THEN DATE('1900-01-01')				
                        ELSE cast(pc_bopcededpremiumtransaction.EffectiveDate as date)
					END), IFNULL(CAST(per.EditEffectiveDate AS DATE), cast(pc_policyperiod.EditEffectiveDate as date))) as date) AS TransactionWrittenDate
			,1 AS TrxnAccrued
			,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
			,0 AS TrxnCharged --not sent to billing
			--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
			--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
			,pctl_segment.TYPECODE AS SegmentCode
			--,pc_bopcost.Basis AS CostBasis
			,pc_bopcost.ActualBaseRate
			,pc_bopcost.ActualAdjRate
			,pc_bopcost.ActualTermAmount
			,pc_bopcost.ActualAmount
			--,pc_bopcost.ChargePattern
			,pctl_chargepattern.TYPECODE AS ChargePattern
			,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
			,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
			,CASE 
				WHEN pc_bopcost.EffectiveDate IS NOT NULL
					THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
				END AS CostEffectiveDate
			,CASE 
				WHEN pc_bopcost.ExpirationDate IS NOT NULL
					THEN CAST(pc_bopcost.ExpirationDate AS DATE)
				ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
				END AS CostExpirationDate
			,1 AS Onset
			,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
					OR pc_bopcost.OverrideAdjRate IS NOT NULL
					OR pc_bopcost.OverrideTermAmount IS NOT NULL
					OR pc_bopcost.OverrideAmount IS NOT NULL
					THEN 1
				ELSE 0
				END AS CostOverridden
			,pc_bopcost.RateAmountType AS RateType
			,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,pc_bopcost.SubjectToReporting AS SubjectToReporting
			,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
					THEN 1 ELSE 0
				END AS IsReportingPolicy
			--,pc_bopcost.Subtype AS CostSubtypeCode
			,pc_policyperiod.PolicyNumber AS PolicyNumber
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_bopcost.PublicID AS CostPublicID
			--,pc_policyline.PublicID AS PolicyLinePublicID
			--,pc_policy.PublicId AS PolicyPublicId
			--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
			,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
			,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
			,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
			/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				*/
			,CASE --peaks and temps 
				WHEN (pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pc_bopcededpremiumtransaction.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
					WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
						THEN CASE 
								WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
									AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
									THEN pc_bopcededpremiumtransaction.CededPremium
								ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
								END
					ELSE 0
					END
				END AS AdjustedFullTermAmount

			--Primary ID and Location
			,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
			,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
			--Effective Coverage and Locations (only coverage specific)
			,pc_businessownerscov.PublicID AS CoverageEffPublicID
			,COALESCE(pc_businessownerscov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
			,COALESCE(businessOwnersCovCost.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode
			--Peaks and Temp, SERP, ADDN Insured, OneTime
			,ratingLocation.PublicID AS RatingLocationPublicID
			--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
			,'None' AS CoverageLocationEffLocationStateCode
			,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
			--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
			--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

			,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
				END AS IsPrimaryLocationBOPLocation
			
			--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			--Ceding Data
			,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
			,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
			,pc_bopcededpremiumtransaction.Commission AS CededCommissions
			,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pc_bopcededpremium.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pc_ricoveragegroup.ID AS RICoverageGroupID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
				ORDER BY 
					IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

	--select pc_policyperiod.policynumber
			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
				ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
				ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
				AND pc_bopcost.BusinessOwnersCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
				ON pc_bopcost.BranchID = pc_policyperiod.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN FinConfigBOPCeded CededCoverable 
				ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
				ON pc_policy.Id = pc_policyperiod.PolicyId
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
				ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
				AND pc_bopcost.BranchID = pc_policyline.BranchID
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
				ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN FinConfigBOPCeded lineConfig 
				ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN FinConfigBOPCeded coverageLevelConfigBusinessOwnersLine
				ON coverageLevelConfigBusinessOwnersLine.Key = 'LineLevelCoverage'
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date})  pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
				ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) pc_businessownerscov
				ON pc_businessownerscov.FixedID = pc_bopcost.BusinessOwnersCov
				AND pc_businessownerscov.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)	
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date})  pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			/*There may be more than one coverage group; but within the same GL Payables category*/
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
				AND pc_paymentplansummary.Retired = 0
			--Charge Lookup
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
			--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
				ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
			--Effective Dates Fields
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

			--Effective Fields's Primary Location Policy Location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
				ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
					AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

			--Ensure Effective Primary location matches the BOP's BOP location
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
				ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
				AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

			--Get the minimum location (number) for this line and branch		
            LEFT JOIN FinTranBOPCedMinLoc AS minBOPPolicyLocationOnBranch
                ON minBOPPolicyLocationOnBranch.PolLocBranchID = pc_bopcost.BranchID
				AND minBOPPolicyLocationOnBranch.BopLocBranchID = pc_bopcost.BranchID
				AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.PolLocEffectiveDate OR minBOPPolicyLocationOnBranch.PolLocEffectiveDate IS NULL)
				AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.PolLocExpirationDate OR minBOPPolicyLocationOnBranch.PolLocExpirationDate IS NULL)
				AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.BopLocEffectiveDate OR minBOPPolicyLocationOnBranch.BopLocEffectiveDate IS NULL)
                AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.BopLocExpirationDate OR minBOPPolicyLocationOnBranch.BopLocExpirationDate IS NULL)                      
		
			--BOP original Coverage Ref in BOP Cost
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) businessOwnersCovCost 
				ON businessOwnersCovCost.ID = pc_bopcost.BusinessOwnersCov
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
				ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp  
			LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment 
				ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)
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
				
			--Policy Period Cost Location
            LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
				ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
				AND costPerEfflocation.CostID = pc_bopcost.ID
                AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= effectiveFieldsPrimaryPolicyLocation.EffectiveDate or effectiveFieldsPrimaryPolicyLocation.EffectiveDate is null) 
				AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < effectiveFieldsPrimaryPolicyLocation.ExpirationDate or effectiveFieldsPrimaryPolicyLocation.ExpirationDate is null)
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
				ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

            --Rating Location
	        LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
				ON ratingLocation.ID = CASE 
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN costPerEfflocation.MaxID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN costPerEfflocation.MaxID
				ELSE - 1
				END

			WHERE 1 = 1
			/**** TEST *****/
		--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
        --AND pc_effectivedatedfields.BranchID=9647239


			UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*												 S U B   L I N E   C O V G   																					*/
			/****************************************************************************************************************************************************************/
			SELECT 
				pcx_bopsublinecov_jmic.PublicID AS CoveragePublicID
				,coverageLevelConfigBOPSubLine.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
			    ,CASE WHEN effectiveFieldsPrimaryBOPLocation.PublicID IS NULL
					    THEN minBOPPolicyLocationOnBranch.PublicID
				    ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
				    END AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pc_bopcededpremiumtransaction.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,0 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId
				--Other
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				,CASE --peaks and temps 
					WHEN (pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pc_bopcededpremiumtransaction.CededPremium
					ELSE --Normal Ceded Trxns 
						CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END 
					END AS AdjustedFullTermAmount

				--Primary ID and Location
				,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pcx_bopsublinecov_jmic.PublicID AS CoverageEffPublicID
				,COALESCE(pcx_bopsublinecov_jmic.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(pcx_bopsublinecov_jmic.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,'None' AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID
				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.BOPSubLineCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigBOPSubLine
					ON coverageLevelConfigBOPSubLine.Key = 'SubLineLevelCoverage'	
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_bopsublinecov_jmic
					ON pcx_bopsublinecov_jmic.FixedID = pc_bopcost.BOPSubLineCov
						AND pcx_bopsublinecov_jmic.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Cost's BOP Sub Line Cov's Sub Line
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) bopsublinecovsubline 
					ON bopsublinecovsubline.FixedID = pcx_bopsublinecov_jmic.BOPSubLine
						AND bopsublinecovsubline.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempFromDt_JMIC,bopsublinecovsubline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsublinecov_jmic.FinalPersistedTempToDt_JMIC,bopsublinecovsubline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) bopSubLineCovCost 
					ON bopSubLineCovCost.ID = pc_bopcost.BopSubLineCov					
	
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

                --Get the minimum location (number) for this line and branch		
                LEFT JOIN FinTranBOPCedMinLoc AS minBOPPolicyLocationOnBranch
                    ON minBOPPolicyLocationOnBranch.PolLocBranchID = pc_bopcost.BranchID
                    AND minBOPPolicyLocationOnBranch.BopLocBranchID = pc_bopcost.BranchID
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.PolLocEffectiveDate OR minBOPPolicyLocationOnBranch.PolLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.PolLocExpirationDate OR minBOPPolicyLocationOnBranch.PolLocExpirationDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.BopLocEffectiveDate OR minBOPPolicyLocationOnBranch.BopLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.BopLocExpirationDate OR minBOPPolicyLocationOnBranch.BopLocExpirationDate IS NULL)    

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp  	
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment 
					ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= effectiveFieldsPrimaryPolicyLocation.EffectiveDate or effectiveFieldsPrimaryPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < effectiveFieldsPrimaryPolicyLocation.ExpirationDate or effectiveFieldsPrimaryPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Coverage Eff Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
					ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
							CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
								THEN effectiveFieldsPrimaryPolicyLocation.ID
							END)
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
					ON covEfflocState.ID = covEffloc.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
				ON ratingLocation.ID = CASE 
					WHEN covEffLoc.ID IS NOT NULL
						AND covEfflocState.ID IS NOT NULL
						AND covEfflocState.ID = Coalesce(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
						THEN covEffLoc.ID
					WHEN costPerEfflocation.MaxID IS NOT NULL
						AND CostPerEfflocationState.ID = Coalesce(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
						THEN costPerEfflocation.MaxID
					ELSE - 1
					END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239
		
		UNION ALL
			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*												 L O C A T I O N   C O V G   																					*/
			/****************************************************************************************************************************************************************/
			SELECT 
				pc_boplocationcov.PublicID AS CoveragePublicID
				,coverageLevelConfigLocation.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,bopLocationCovBOPLocation.PublicID AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd)  AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate  AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId			
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/* Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */			
				,CASE --peaks and temps 
					WHEN (pc_boplocationcov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_boplocationcov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pc_bopcededpremiumtransaction.CededPremium
					ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pc_bopcededpremiumtransaction.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END
					END AS AdjustedFullTermAmount
				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pc_boplocationcov.PublicID AS CoverageEffPublicID
				,COALESCE(pc_boplocationcov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(bopLocationCovCost.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID
				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.BOPLocationCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigLocation
					ON coverageLevelConfigLocation.Key = 'LocationLevelCoverage'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
	
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date}) pc_boplocationcov
					ON pc_boplocationcov.FixedID = pc_bopcost.BOPLocationCov
					AND pc_boplocationcov.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,pc_boplocationcov.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,pc_boplocationcov.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--LocationCov's BOP Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopLocationCovBOPLocation 
					ON bopLocationCovBOPLocation.FixedID = pc_boplocationcov.BOPLocation
						AND bopLocationCovBOPLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,bopLocationCovBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,bopLocationCovBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--LocationCov BOP Location's Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopLocationCovPolicyLocation 
					ON bopLocationCovPolicyLocation.FixedID = bopLocationCovBOPLocation.Location
						AND bopLocationCovPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_boplocationcov.FinalPersistedTempFromDt_JMIC,bopLocationCovPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_boplocationcov.FinalPersistedTempToDt_JMIC,bopLocationCovPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BOP original Coverage Ref in BOP Cost
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocationcov` WHERE _PARTITIONTIME = {partition_date}) bopLocationCovCost 
					ON bopLocationCovCost.ID = pc_bopcost.BopLocationCov

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment 
					ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)
					
				--Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= bopLocationCovPolicyLocation.EffectiveDate or bopLocationCovPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < bopLocationCovPolicyLocation.ExpirationDate or bopLocationCovPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

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
					ON effectiveLocState.ID = bopLocationCovPolicyLocation.StateInternal
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

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID,taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID,taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*											 S U B 	L O C A T I O N   C O V G   																				*/
			/****************************************************************************************************************************************************************/
			SELECT 
				pcx_bopsubloccov_jmic.PublicID	AS	CoveragePublicID
				,coverageLevelConfigSubLoc.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,bopSubLocCovBOPLocation.PublicID AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				--,pc_bopcost.Basis AS CostBasis
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				--,pc_bopcost.SubjectToReporting AS SubjectToReporting
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId
				--Other
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */			
				,CASE --peaks and temps 
					WHEN (pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pc_bopcededpremiumtransaction.CededPremium
					ELSE --Normal Ceded Trxns 
						CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
							END
						END AS AdjustedFullTermAmount

				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pcx_bopsubloccov_jmic.PublicID AS CoverageEffPublicID
				,COALESCE(pcx_bopsubloccov_jmic.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(bopSubLocCovCost.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
						THEN 0 ELSE 1 END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID

				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.BOPSubLocCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigSubLoc
					ON coverageLevelConfigSubLoc.Key = 'SubLocLevelCoverage'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date})  pcx_bopsubloccov_jmic
					ON pcx_bopsubloccov_jmic.FixedID = pc_bopcost.BOPSubLocCov
					AND pcx_bopsubloccov_jmic.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,pcx_bopsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,pcx_bopsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovSubLoc 
					ON bopSubLocCovSubLoc.FixedID = pcx_bopsubloccov_jmic.BOPsubloc
					AND bopSubLocCovSubLoc.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovSubLoc.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovSubLoc.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BOP Location Cov's BOP Location ID
				LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovBOPLocation 
					ON bopSubLocCovBOPLocation.FixedID = bopSubLocCovSubLoc.BOPLocation
					AND bopSubLocCovBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BOP Location's Policy Location ID
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovPolicyLocation 
					ON bopSubLocCovPolicyLocation.FixedID = bopSubLocCovBOPLocation.Location
					AND bopSubLocCovPolicyLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempFromDt_JMIC,bopSubLocCovPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_bopsubloccov_jmic.FinalPersistedTempToDt_JMIC,bopSubLocCovPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--BOP original Coverage Ref in BOP Cost
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) bopSubLocCovCost  
					ON bopSubLocCovCost.ID = pc_bopcost.BopSubLocCov					
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
					ON effectiveLocState.ID = bopSubLocCovPolicyLocation.StateInternal
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
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= bopSubLocCovPolicyLocation.EffectiveDate or bopSubLocCovPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < bopSubLocCovPolicyLocation.ExpirationDate or bopSubLocCovPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID,taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID,taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239
			
);

CREATE OR REPLACE TEMP TABLE FinTranBOPCed2 
AS
(
WITH FinConfigBOPCeded AS (
	SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
	SELECT 'LineCode','BusinessOwnersLine' UNION ALL
	SELECT 'BusinessType', 'Ceded' UNION ALL
	/*CoverageLevel Values */
	SELECT 'LineLevelCoverage','Line' UNION ALL
	SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
	SELECT 'LocationLevelCoverage','Location' UNION ALL
	SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
	SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
	SELECT 'OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC' UNION ALL
	SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
	SELECT 'NoCoverage','NoCoverage' UNION ALL
	/*Ceded Coverable Types */
	SELECT 'CededCoverable', 'DefaultCov' UNION ALL
	SELECT 'CededCoverableBOP', 'BOPCov' UNION ALL
	SELECT 'CededCoverableBOPBldg', 'BOPBuildingCov' UNION ALL
	/* Risk Key Values */
	SELECT 'LocationLevelRisk', 'BusinessOwnersLocation' UNION ALL
	SELECT 'BuildingLevelRisk', 'BusinessOwnersBuilding'
)
			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*											  	B U I L D I N G   C O V G   																				    */
			/****************************************************************************************************************************************************************/
			SELECT 
				pc_bopbuildingcov.PublicID	AS	CoveragePublicID
				,coverageLevelConfigBopBldg.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,pc_boplocation.PublicID AS BOPLocationPublicID
				,pc_bopbuilding.PublicID AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd)  AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 
				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId		
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */			
				,CASE --peaks and temps 
					WHEN (pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_bopbuildingcov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pc_bopcededpremiumtransaction.CededPremium
					ELSE --Normal Ceded Trxns 
						CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END
					END AS AdjustedFullTermAmount
				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pc_bopbuildingcov.PublicID AS CoverageEffPublicID
				,COALESCE(pc_bopbuildingcov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(bopBuildingCovCost.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID
				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.BOPBuildingCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigBopBldg
					ON coverageLevelConfigBopBldg.Key = 'BuildingLevelCoverage'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date})  pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuildingcov
					ON pc_bopbuildingcov.FixedID = pc_bopcost.BOPBuildingCov
						AND pc_bopbuildingcov.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BuildingCov's BOP Building
				LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuilding
					ON pc_bopbuilding.FixedID = pc_bopbuildingcov.BOPBuilding
						AND pc_bopbuilding.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Building's BOP Location
				LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
					ON pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
						AND pc_boplocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)	 AS DATE)
				--Building BOP Location's Policy Location
				LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
					ON pc_policylocation.FixedID = pc_boplocation.Location
						AND pc_policylocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) bopBuilding
					ON bopBuilding.BranchID = pc_bopcost.BranchID
						AND bopBuilding.FixedID = pc_bopcost.BOPBuilding
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuilding.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuilding.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Building's BOP Location	
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopBuildingBOPLocation 
					ON bopBuildingBOPLocation.FixedID = bopBuilding.BOPLocation
						AND bopBuildingBOPLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuildingBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuildingBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopBuildingPolicylocation 
					ON bopBuildingPolicylocation.FixedID = bopBuildingBOPLocation.Location
						AND bopBuildingPolicylocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuildingPolicylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuildingPolicylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--BOP original Coverage Ref in BOP Cost
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) bopBuildingCovCost 
					ON bopBuildingCovCost.ID = pc_bopcost.BopBuildingCov
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype`  pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date})  pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
					ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
							CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
								THEN effectiveFieldsPrimaryPolicyLocation.ID
							END)
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
					ON covEfflocState.ID = covEffloc.StateInternal
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= bopBuildingPolicylocation.EffectiveDate or bopBuildingPolicylocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < bopBuildingPolicylocation.ExpirationDate or bopBuildingPolicylocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID,effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*											     O N E  T I M E   C O V G   																				    */
			/****************************************************************************************************************************************************************/
			SELECT 
				pcx_boponetimecredit_jmic_v2.PublicID AS CoveragePublicID
				,pcBOPOneTimeCreditCustomCov.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,bopOneTimeCreditBOPLocation.PublicID AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,Coalesce(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId		
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd  AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				, --peaks and temps = na
					 --Normal Ceded Trxns 
					 	CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END AS AdjustedFullTermAmount

				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

				--Effective Coverage and Locations (only coverage specific)
				,pcx_boponetimecredit_jmic_v2.PublicID AS CoverageEffPublicID
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPOneTimeCreditCustomCov.Value) AS CoverageEffPatternCode
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPOneTimeCreditCustomCov.Value) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,effectiveLocState.TYPECODE AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID
				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.BOPOneTimeCredit IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN(SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded pcBOPOneTimeCreditCustomCov 
					ON pcBOPOneTimeCreditCustomCov.Key = 'OneTimeCreditCustomCoverage'
					AND pcBOPOneTimeCreditCustomCov.Value = 'BOPOneTimeCredit_JMIC'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_boponetimecredit_jmic_v2` WHERE _PARTITIONTIME = {partition_date}) pcx_boponetimecredit_jmic_v2
					ON pcx_boponetimecredit_jmic_v2.FixedID = pc_bopcost.BOPOnetimeCredit
						AND pcx_boponetimecredit_jmic_v2.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pcx_boponetimecredit_jmic_v2.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pcx_boponetimecredit_jmic_v2.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopOneTimeCreditBOPLocation 
					ON bopOneTimeCreditBOPLocation.FixedID = pcx_boponetimecredit_jmic_v2.BOPLocation
						AND bopOneTimeCreditBOPLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(bopOneTimeCreditBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(bopOneTimeCreditBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BOPOneTimeCredits BOP Location's Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopOneTimeCreditPolicyLocation 
					ON bopOneTimeCreditPolicyLocation.FixedID = bopOneTimeCreditBOPLocation.Location
						AND bopOneTimeCreditPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(bopOneTimeCreditPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(bopOneTimeCreditPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN `{project}.{pc_dataset}.pctl_onetimecreditreason_jmic` bopOneTimeCreditReason 
					ON bopOneTimeCreditReason.ID = pcx_boponetimecredit_jmic_v2.CreditReason						
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
					ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
							CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
								THEN effectiveFieldsPrimaryPolicyLocation.ID
							END)
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
					ON covEfflocState.ID = covEffloc.StateInternal

                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= bopOneTimeCreditPolicyLocation.EffectiveDate or bopOneTimeCreditPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < bopOneTimeCreditPolicyLocation.ExpirationDate or bopOneTimeCreditPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*									A D D I T I O N A L  I N S U R E D  C U S T O M  C O V G 																    */
			/****************************************************************************************************************************************************************/
			SELECT 
				pc_bopcededpremiumtransaction.PublicID	AS	CoveragePublicID
				,pcBOPAdditionalInsuredCustomCov.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(COALESCE(pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_policyperiod.EditEffectiveDate) AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,Coalesce(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				--,pc_bopcost.SubjectToReporting AS SubjectToReporting
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId	
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart  AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				, --peaks and temps = na
					 --Normal Ceded Trxns 
					 	CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END AS AdjustedFullTermAmount

				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

				--Effective Coverage and Locations (only coverage specific)
				,'None' AS CoverageEffPublicID
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPAdditionalInsuredCustomCov.Value) AS CoverageEffPatternCode
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup, pcBOPAdditionalInsuredCustomCov.Value) AS CostCoverageCode
				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,'None' AS CoverageLocationEffLocationStateCode
				,Coalesce(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID

				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND pc_bopcost.AdditionalInsured IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date})  pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded pcBOPAdditionalInsuredCustomCov 
					ON pcBOPAdditionalInsuredCustomCov.Key = 'AdditionalInsuredCustomCoverage'
					AND pcBOPAdditionalInsuredCustomCov.Value = 'Additional_Insured_JMIC'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= effectiveFieldsPrimaryPolicyLocation.EffectiveDate or effectiveFieldsPrimaryPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < effectiveFieldsPrimaryPolicyLocation.ExpirationDate or effectiveFieldsPrimaryPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B	O	P	     C E D E D  																					*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*												 O T H E R   N O  C O V G   																					*/
			/****************************************************************************************************************************************************************/
			SELECT 
				--'NONE' AS	CoveragePublicID
				CAST(NULL AS STRING) AS CoveragePublicID
				,coverageLevelConfigOther.Value AS CoverageLevel
				,pc_bopcededpremiumtransaction.PublicID AS TransactionPublicID
				,COALESCE(costPolicyLocation.PublicID, CASE 
						WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
							THEN minBOPPolicyLocationOnBranch.PublicID
						ELSE effectiveFieldsPrimaryPolicyLocation.PublicID
						END) AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcededpremiumtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pc_bopcededpremiumtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pc_bopcededpremiumtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pc_bopcededpremiumtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pc_bopcededpremiumtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING)  AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pc_bopcededpremiumtransaction.ExpirationDate , pc_bopcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pc_bopcededpremiumtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				--,pc_bopcost.SubjectToReporting AS SubjectToReporting
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId		
				--,pc_bopcededpremiumtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				, --peaks and temps = na
					 --Normal Ceded Trxns
 					 	CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pc_bopcededpremiumtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pc_bopcededpremiumtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pc_bopcededpremiumtransaction.EffectiveDate, pc_bopcededpremium.EffectiveDate), IFNULL(pc_bopcededpremiumtransaction.ExpirationDate, pc_bopcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END AS AdjustedFullTermAmount
				--Primary ID and Location
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID

				--Effective Coverage and Locations (only coverage specific)
				,'None' AS CoverageEffPublicID
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode

				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,'None' AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,pc_policyperiod.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID

				--Ceding Data
				,pc_bopcededpremiumtransaction.CedingRate AS CedingRate
				,pc_bopcededpremiumtransaction.CededPremium AS CededAmount
				,pc_bopcededpremiumtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pc_bopcededpremiumtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pc_bopcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pc_bopcededpremiumtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremiumtransaction` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremiumtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcededpremium` WHERE _PARTITIONTIME = {partition_date}) pc_bopcededpremium
					ON pc_bopcededpremium.ID = pc_bopcededpremiumtransaction.BOPCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date}) pc_bopcost
					ON pc_bopcededpremium.BOPCost = pc_bopcost.ID
					AND -1=COALESCE(pc_bopcost.BOPLocationCov
					,pc_bopcost.BusinessOwnersCov
					,pc_bopcost.BOPBuildingCov
					,pc_bopcost.BOPSubLocCov
					,pc_bopcost.BOPSubLineCov
					,pc_bopcost.BOPOneTimeCredit
					,pc_bopcost.AdditionalInsured
					,-1)
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverable'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigOther
					ON coverageLevelConfigOther.Key = 'NoCoverage'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pc_bopcededpremiumtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)
				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcededpremiumtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

                --Get the minimum location (number) for this line and branch		
                LEFT JOIN FinTranBOPCedMinLoc AS minBOPPolicyLocationOnBranch
                    ON minBOPPolicyLocationOnBranch.PolLocBranchID = pc_bopcost.BranchID
                    AND minBOPPolicyLocationOnBranch.BopLocBranchID = pc_bopcost.BranchID
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.PolLocEffectiveDate OR minBOPPolicyLocationOnBranch.PolLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.PolLocExpirationDate OR minBOPPolicyLocationOnBranch.PolLocExpirationDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.BopLocEffectiveDate OR minBOPPolicyLocationOnBranch.BopLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.BopLocExpirationDate OR minBOPPolicyLocationOnBranch.BopLocExpirationDate IS NULL)    

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
					ON pc_policyterm.ID = pc_bopcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pc_bopcededpremiumtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

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
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= effectiveFieldsPrimaryPolicyLocation.EffectiveDate or effectiveFieldsPrimaryPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < effectiveFieldsPrimaryPolicyLocation.ExpirationDate or effectiveFieldsPrimaryPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END
				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL
			/****************************************************************************************************************************************************************/
			/*												B O P     BOP Cov    C E D E D      																			*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*		 											  L I N E    C O V G   																					    */
			/****************************************************************************************************************************************************************/
			SELECT 
				pc_businessownerscov.PublicID AS CoveragePublicID
				,coverageLevelConfigBusinessOwnersLine.Value AS CoverageLevel
				,pcx_bopcovcededpremtransaction.PublicID AS TransactionPublicID
				--,effectiveFieldsPrimaryBOPLocation.PublicID AS BOPLocationPublicID
				,COALESCE(costPolicyLocation.PublicID,
					CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL 
					THEN minBOPPolicyLocationOnBranch.PublicID 
					ELSE effectiveFieldsPrimaryPolicyLocation.PublicID END) AS BOPLocationPublicID
				,CAST(NULL AS STRING) AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pcx_bopcovcededpremtransaction.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pcx_bopcovcededpremtransaction.CededPremium AS TransactionAmount --the Ceded Premium
				,pcx_bopcovcededpremtransaction.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pcx_bopcovcededpremtransaction.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pcx_bopcovcededpremtransaction.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pcx_bopcovcededpremtransaction.EffectiveDate, pcx_bopcovcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pcx_bopcovcededpremtransaction.ExpirationDate , pcx_bopcovcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pcx_bopcovcededpremtransaction.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				--,pc_bopcost.SubjectToReporting AS SubjectToReporting
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId			
				--,pcx_bopcovcededpremtransaction.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				,CASE --peaks and temps 
					WHEN (pc_businessownerscov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_businessownerscov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pcx_bopcovcededpremtransaction.CededPremium
					ELSE --Normal Ceded Trxns 
						CASE 
							WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_bopcovcededpremtransaction.EffectiveDate, pcx_bopcovcededpremium.EffectiveDate), IFNULL(pcx_bopcovcededpremtransaction.ExpirationDate, pcx_bopcovcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pcx_bopcovcededpremtransaction.EffectiveDate, pcx_bopcovcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pcx_bopcovcededpremtransaction.ExpirationDate, pcx_bopcovcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pcx_bopcovcededpremtransaction.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_bopcovcededpremtransaction.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_bopcovcededpremtransaction.EffectiveDate, pcx_bopcovcededpremium.EffectiveDate), IFNULL(pcx_bopcovcededpremtransaction.ExpirationDate, pcx_bopcovcededpremium.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END
					END AS AdjustedFullTermAmount

				--Primary ID and Location
				--Effective and Primary Fields
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pc_businessownerscov.PublicID AS CoverageEffPublicID
				,COALESCE(pc_businessownerscov.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CoverageEffPatternCode
				,COALESCE(businessOwnersCovCost.PatternCode, pc_bopcost.ChargeSubGroup, pc_bopcost.ChargeGroup) AS CostCoverageCode

				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,'None' AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,per.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID

				--Ceding Data
				,pcx_bopcovcededpremtransaction.CedingRate AS CedingRate
				,pcx_bopcovcededpremtransaction.CededPremium AS CededAmount
				,pcx_bopcovcededpremtransaction.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pcx_bopcovcededpremtransaction.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pcx_bopcovcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable
				,ROW_NUMBER() OVER(PARTITION BY pcx_bopcovcededpremtransaction.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremtransaction` WHERE _PARTITIONTIME = {partition_date}) pcx_bopcovcededpremtransaction
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopcovcededpremium` WHERE _PARTITIONTIME = {partition_date}) pcx_bopcovcededpremium
					ON pcx_bopcovcededpremium.ID = pcx_bopcovcededpremtransaction.BOPCovCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pcx_bopcovcededpremium.BOPCovCost = pc_bopcost.ID
					AND pc_bopcost.BusinessOwnersCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverableBOP'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM  `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline`  pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigBusinessOwnersLine
					ON coverageLevelConfigBusinessOwnersLine.Key = 'LineLevelCoverage' 
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date})  pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pcx_bopcovcededpremtransaction.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) pc_businessownerscov
					ON pc_businessownerscov.FixedID = pc_bopcost.BusinessOwnersCov
					AND pc_businessownerscov.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_businessownerscov.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_businessownerscov.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)	

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm ON pc_policyterm.ID = pcx_bopcovcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pcx_bopcovcededpremtransaction.CalcTimestamp        
         
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)

				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_businessownerscov.FinalPersistedTempFromDt_JMIC,effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_bopcovcededpremtransaction.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_businessownerscov.FinalPersistedTempToDt_JMIC,effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--BOP original Coverage Ref in BOP Cost
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_businessownerscov` WHERE _PARTITIONTIME = {partition_date}) businessOwnersCovCost 
					ON businessOwnersCovCost.ID = pc_bopcost.BusinessOwnersCov

                --Get the minimum location (number) for this line and branch		
                LEFT JOIN FinTranBOPCovCedMinLoc AS minBOPPolicyLocationOnBranch
                    ON minBOPPolicyLocationOnBranch.PolLocBranchID = pc_bopcost.BranchID
                    AND minBOPPolicyLocationOnBranch.BopLocBranchID = pc_bopcost.BranchID
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.PolLocEffectiveDate OR minBOPPolicyLocationOnBranch.PolLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.PolLocExpirationDate OR minBOPPolicyLocationOnBranch.PolLocExpirationDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) >= minBOPPolicyLocationOnBranch.BopLocEffectiveDate OR minBOPPolicyLocationOnBranch.BopLocEffectiveDate IS NULL)
                    AND (COALESCE(pc_bopcost.EffectiveDate, pc_policyperiod.EditEffectiveDate) < minBOPPolicyLocationOnBranch.BopLocExpirationDate OR minBOPPolicyLocationOnBranch.BopLocExpirationDate IS NULL)    

				--LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm
				--	ON pc_policyterm.ID = pcx_bopcovcededpremium.PolicyTerm
				--LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				--	ON per.PolicyTermID = pc_policyterm.ID
				--	AND per.ModelDate < pcx_bopcovcededpremtransaction.CalcTimestamp 
				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

				--State and Country
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState 
					ON costratedState.ID = pc_bopcost.RatedState
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
					ON primaryLocState.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
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
					
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= effectiveFieldsPrimaryPolicyLocation.EffectiveDate or effectiveFieldsPrimaryPolicyLocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < effectiveFieldsPrimaryPolicyLocation.ExpirationDate or effectiveFieldsPrimaryPolicyLocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
			--	AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
            --AND pc_effectivedatedfields.BranchID=9647239

		UNION ALL

			/****************************************************************************************************************************************************************/
			/*												B O P     BOP Building Cov    C E D E D      																	*/
			/****************************************************************************************************************************************************************/
			/****************************************************************************************************************************************************************/
			/*		 											     L I N E    C O V G   																				    */
			/****************************************************************************************************************************************************************/
			SELECT 
				pc_bopbuildingcov.PublicID AS CoveragePublicID
				,coverageLevelConfigBusinessOwnersLine.Value AS CoverageLevel
				,pcx_bopbuildingcededpremtrans.PublicID AS TransactionPublicID
				,pc_boplocation.PublicID AS BOPLocationPublicID
				,pc_bopbuilding.PublicID AS BuildingPublicID
				,pc_policyperiod.PeriodID AS PolicyPeriodPeriodID
				,pc_job.JobNumber AS JobNumber
				,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) <  CAST(COALESCE(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
						THEN 1 ELSE 0 END 
					AS IsTransactionSliceEffective
				,pctl_policyline.TYPECODE AS LineCode
				,pcx_bopbuildingcededpremtrans.CededPremium AS TransactionAmount --the Ceded Premium
				,pcx_bopbuildingcededpremtrans.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
				,CASE WHEN pcx_bopbuildingcededpremtrans.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
				/*
				Logic Summary: Regular coverages are accounted for at a date that is greater of the change, posted, or effective date
				Peaks and temps behave differently, per JM requirements. They are accounted for at the time of renewal or change or submission
				[note that this does not mean they accrue over the entire term]. Unlike regular coverages, they accrue earnings over 
				the effective and expiration date of the temp / peak coverage. Because they are potentially accounted for prior to earnings, they will still 
				appear in the accrual summary tables with 0 earnings (aka the first month of written will have a max unearned premium), followed by 0 (accrual) each month 
				until the end of peak and temp, when it will fully earn
				*/
				,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pcx_bopbuildingcededpremtrans.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
				,1 AS TrxnAccrued
				,CAST(IFNULL(pcx_bopbuildingcededpremtrans.EffectiveDate, pcx_bopbuildingcededpremium.EffectiveDate) AS DATE) AS EffectiveDate
				,CAST(IFNULL(pcx_bopbuildingcededpremtrans.ExpirationDate , pcx_bopbuildingcededpremium.ExpirationDate) AS DATE) AS ExpirationDate
				,0 AS TrxnCharged --not sent to billing
				--,pcx_bopbuildingcededpremtrans.CededPremium AS PrecisionAmount
				--,COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState	--where the bop location was rated (denormalised located with /just the state)
				,pctl_segment.TYPECODE	AS SegmentCode
				,pc_bopcost.ActualBaseRate
				,pc_bopcost.ActualAdjRate
				,pc_bopcost.ActualTermAmount
				,pc_bopcost.ActualAmount
				--,pc_bopcost.ChargePattern
				,pctl_chargepattern.TYPECODE AS ChargePattern
				,pc_bopcost.ChargeGroup AS ChargeGroup --stored as a code, not ID. 
				,pc_bopcost.ChargeSubGroup AS ChargeSubGroup --stored as a code, not ID. 

				,CASE 
					WHEN pc_bopcost.EffectiveDate IS NOT NULL
						THEN CAST(pc_bopcost.EffectiveDate AS DATE) -- CAST null eff / exp dates to Period start / end dates
					END AS CostEffectiveDate
				,CASE 
					WHEN pc_bopcost.ExpirationDate IS NOT NULL
						THEN CAST(pc_bopcost.ExpirationDate AS DATE)
					ELSE CAST(pc_policyperiod.PeriodEnd AS DATE)
					END AS CostExpirationDate
				,1 AS Onset
				,CASE WHEN pc_bopcost.OverrideBaseRate IS NOT NULL
						OR pc_bopcost.OverrideAdjRate IS NOT NULL
						OR pc_bopcost.OverrideTermAmount IS NOT NULL
						OR pc_bopcost.OverrideAmount IS NOT NULL
						THEN 1
					ELSE 0
					END AS CostOverridden
				,pc_bopcost.RateAmountType AS RateType
				,CASE WHEN pc_bopcost.RateAmountType = 3 -- Tax or surcharge 
						THEN 0 ELSE 1
					END AS IsPremium
				,CASE WHEN pc_paymentplansummary.ReportingPatternCode IS NOT NULL
						THEN 1 ELSE 0
					END AS IsReportingPolicy
				--,pc_bopcost.Subtype AS CostSubtypeCode
				,pc_policyperiod.PolicyNumber AS PolicyNumber
				,pc_policyperiod.PublicID AS PolicyPeriodPublicID
				,pc_bopcost.PublicID AS CostPublicID
				--,pc_policyline.PublicID AS PolicyLinePublicID
				--,pc_policy.PublicId AS PolicyPublicId
				--,pcx_bopbuildingcededpremtrans.CreateTime AS TrxnCreateTime
				,CAST(pc_policyperiod.PeriodStart AS DATE) AS PeriodStart
				,CAST(pc_policyperiod.PeriodEnd  AS DATE) AS PeriodEnd
				,pc_bopcost.NumDaysInRatedTerm AS NumDaysInRatedTerm
				/*	Note: 1) Full Term Premium = Ceded Premium prorated over the period start / end
				 */
				,CASE --peaks and temps 
					WHEN (pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pc_bopbuildingcov.FinalPersistedTempToDt_JMIC IS NOT NULL) 
						THEN pcx_bopbuildingcededpremtrans.CededPremium
					ELSE --Normal Ceded Trxns 
						CASE 
							WHEN cast(TIMESTAMP_DIFF(IFNULL(pcx_bopbuildingcededpremtrans.EffectiveDate, pcx_bopbuildingcededpremium.EffectiveDate), IFNULL(pcx_bopbuildingcededpremtrans.ExpirationDate, pcx_bopbuildingcededpremium.ExpirationDate), DAY) AS FLOAT64) <> 0
								THEN CASE 
										WHEN IFNULL(pcx_bopbuildingcededpremtrans.EffectiveDate, pcx_bopbuildingcededpremium.EffectiveDate) = pc_policyperiod.PeriodStart
											AND IFNULL(pcx_bopbuildingcededpremtrans.ExpirationDate, pcx_bopbuildingcededpremium.ExpirationDate) = pc_policyperiod.PeriodEnd
											THEN pcx_bopbuildingcededpremtrans.CededPremium
										ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_bopbuildingcededpremtrans.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_bopbuildingcededpremtrans.EffectiveDate, pcx_bopbuildingcededpremium.EffectiveDate), IFNULL(pcx_bopbuildingcededpremtrans.ExpirationDate, pcx_bopbuildingcededpremium.ExpirationDate),DAY) AS FLOAT64) AS DECIMAL),0)
										END
							ELSE 0
						END
					END AS AdjustedFullTermAmount

				--Primary ID and Location
				--Effective and Primary Fields
				,pc_effectivedatedfields.PublicID  AS EffectiveDatedFieldsPublicID
				,effectiveFieldsPrimaryPolicyLocation.LocationNum AS PrimaryPolicyLocationNumber
				,effectiveFieldsPrimaryPolicyLocation.PublicID AS PrimaryPolicyLocationPublicID
				--Effective Coverage and Locations (only coverage specific)
				,pc_bopbuildingcov.PublicID AS CoverageEffPublicID
				,pc_bopbuildingcov.PatternCode AS CoverageEffPatternCode
				,bopBuildingCovCost.PatternCode AS CostCoverageCode

				--Peaks and Temp, SERP, ADDN Insured, OneTime
				,ratingLocation.PublicID AS RatingLocationPublicID
				--,effectiveFieldsPrimaryPolicyLocation.PublicID as RatingLocationPublicID
				,'None' AS CoverageLocationEffLocationStateCode
				,COALESCE(costRatedState.TYPECODE, costPolicyLocState.TYPECODE, taxLocState.TYPECODE, primaryLocState.TYPECODE)	AS	RatedState
				--,pc_policyperiod.EditEffectiveDate AS CostEditEffectiveDate
				--,per.EditEffectiveDate AS TrxnEditEffectiveDate

				,CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN 0 ELSE 1
					END AS IsPrimaryLocationBOPLocation
				
				--,minBOPPolicyLocationOnBranch.PublicID AS MinBOPPolicyLocationPublicID
				,pc_uwcompany.PublicID AS UWCompanyPublicID

				--Ceding Data
				,pcx_bopbuildingcededpremtrans.CedingRate AS CedingRate
				,pcx_bopbuildingcededpremtrans.CededPremium AS CededAmount
				,pcx_bopbuildingcededpremtrans.Commission AS CededCommissions
				,pc_bopcost.ActualTermAmount * pcx_bopbuildingcededpremtrans.CedingRate AS CededTermAmount
				,pctl_riagreement.TYPECODE AS RIAgreementType
				,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
				,pcx_bopbuildingcededpremium.ID AS CededID
				,pc_reinsuranceagreement.ID AS CededAgreementID
				,pc_ricoveragegroup.ID AS RICoverageGroupID
				,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
				,CededCoverable.Value AS CededCoverable

				,ROW_NUMBER() OVER(PARTITION BY pcx_bopbuildingcededpremtrans.ID
					ORDER BY 
						IFNULL(pc_bopcost.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
						,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
				) AS TransactionRank

		--select pc_policyperiod.policynumber
				FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_bopbuildingcededpremtrans` WHERE _PARTITIONTIME = {partition_date}) pcx_bopbuildingcededpremtrans
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_bopbuildingcededpremium` WHERE _PARTITIONTIME = {partition_date}) pcx_bopbuildingcededpremium
					ON pcx_bopbuildingcededpremium.ID = pcx_bopbuildingcededpremtrans.BOPBuildingCededPremium
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopcost` WHERE _PARTITIONTIME = {partition_date})  pc_bopcost
					ON pcx_bopbuildingcededpremium.BOPBuildingCovCost = pc_bopcost.ID
					AND pc_bopcost.BOPBuildingCov IS NOT NULL
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
					ON pc_bopcost.BranchID = pc_policyperiod.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
					ON pc_job.ID = pc_policyperiod.JobID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) pc_uwcompany
					ON pc_policyperiod.UWCompany = pc_uwcompany.ID
				INNER JOIN FinConfigBOPCeded CededCoverable 
					ON CededCoverable.Key = 'CededCoverableBOPBldg'
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policy` WHERE _PARTITIONTIME = {partition_date}) pc_policy
					ON pc_policy.Id = pc_policyperiod.PolicyId
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
					ON pc_bopcost.BusinessOwnersLine = pc_policyline.FixedID
					AND pc_bopcost.BranchID = pc_policyline.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
					ON pctl_policyline.ID = pc_policyline.Subtype
				INNER JOIN FinConfigBOPCeded lineConfig 
					ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
				INNER JOIN FinConfigBOPCeded coverageLevelConfigBusinessOwnersLine
					ON coverageLevelConfigBusinessOwnersLine.Key = 'LineLevelCoverage'
				--Cede Agreement
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date})  pc_reinsuranceagreement
					ON pc_reinsuranceagreement.ID = pcx_bopbuildingcededpremtrans.Agreement
				INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` pctl_riagreement
					ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuildingcov
					ON pc_bopbuildingcov.FixedID = pc_bopcost.BOPBuildingCov
						AND pc_bopbuildingcov.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_bopbuildingcov.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuildingcov.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--BuildingCov's BOP Building
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) pc_bopbuilding
					ON pc_bopbuilding.FixedID = pc_bopbuildingcov.BOPBuilding
						AND pc_bopbuilding.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_bopbuilding.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_bopbuilding.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Building's BOP Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) pc_boplocation
					ON pc_boplocation.FixedID = pc_bopbuilding.BOPLocation
						AND pc_boplocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Building BOP Location's Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
					ON pc_policylocation.FixedID = pc_boplocation.Location
						AND pc_policylocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuilding` WHERE _PARTITIONTIME = {partition_date}) bopBuilding
					ON bopBuilding.BranchID = pc_bopcost.BranchID
						AND bopBuilding.FixedID = pc_bopcost.BOPBuilding
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuilding.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuilding.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				--Building's BOP Location	
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) bopBuildingBOPLocation 
					ON bopBuildingBOPLocation.FixedID = bopBuilding.BOPLocation
						AND bopBuildingBOPLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuildingBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuildingBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) bopBuildingPolicylocation 
					ON bopBuildingPolicylocation.FixedID = bopBuildingBOPLocation.Location
						AND bopBuildingPolicylocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempFromDt_JMIC,bopBuildingPolicylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_bopbuildingcov.FinalPersistedTempToDt_JMIC,bopBuildingPolicylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--BOP original Coverage Ref in BOP Cost
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_bopbuildingcov` WHERE _PARTITIONTIME = {partition_date}) bopBuildingCovCost 
					ON bopBuildingCovCost.ID = pc_bopcost.BopBuildingCov
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) pc_ricoveragegroup
					ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
				/*There may be more than one coverage group; but within the same GL Payables category*/
				LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` pctl_ricoveragegrouptype
					ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType

				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) pc_policyterm 
					ON pc_policyterm.ID = pcx_bopbuildingcededpremium.PolicyTerm
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per 
					ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pcx_bopbuildingcededpremtrans.CalcTimestamp        
         
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
				--Charge Lookup
				LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern ON pctl_chargepattern.ID = pc_bopcost.ChargePattern
				--Cost Policy Location Lookup; PolLocation for BOP Additional Insured Coverage; BOPTaxPolicyLocation for all TAX charges 
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) costPolicyLocation 
					ON costPolicyLocation.ID = COALESCE(pc_bopcost.PolLocation, pc_bopcost.BOPTaxPolicyLocation)

				--Effective Dates Fields
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
					ON pc_effectivedatedfields.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Effective Fields's Primary Location Policy Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryPolicyLocation 
					ON effectiveFieldsPrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation
						AND effectiveFieldsPrimaryPolicyLocation.BranchID = pc_bopcost.BranchID
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
						AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				--Ensure Effective Primary location matches the BOP's BOP location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) effectiveFieldsPrimaryBOPLocation 
					ON effectiveFieldsPrimaryBOPLocation.Location = effectiveFieldsPrimaryPolicyLocation.FixedID
					AND effectiveFieldsPrimaryBOPLocation.BranchID = pc_bopcost.BranchID
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < CAST(COALESCE(effectiveFieldsPrimaryBOPLocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

				LEFT JOIN `{project}.{pc_dataset}.pctl_segment` pctl_segment 
					ON pctl_segment.Id = IFNULL(pc_policyperiod.Segment, pc_policyperiod.Segment)

				--State and Country
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` costratedState 
					ON costratedState.ID = pc_bopcost.RatedState
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` primaryLocState 
					ON primaryLocState.ID = CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL THEN effectiveFieldsPrimaryPolicyLocation.StateInternal
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
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) covEffLoc 
					ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, 
							CASE WHEN effectiveFieldsPrimaryBOPLocation.ID IS NULL
								THEN effectiveFieldsPrimaryPolicyLocation.ID
							END)
				LEFT JOIN `{project}.{pc_dataset}.pctl_state` covEfflocState 
					ON covEfflocState.ID = covEffloc.StateInternal
										
                --Policy Period Cost Location
                LEFT JOIN FinTranBOPCedMaxLoc AS costPerEfflocation
                    ON costPerEfflocation.BranchID = pc_bopcost.BranchID 
                    AND costPerEfflocation.CostID = pc_bopcost.ID
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= pc_policylocation.EffectiveDate or pc_policylocation.EffectiveDate is null) 
                    AND (COALESCE(pc_bopcost.EffectiveDate,pc_policyperiod.EditEffectiveDate) < pc_policylocation.ExpirationDate or pc_policylocation.ExpirationDate is null)
                LEFT JOIN `{project}.{pc_dataset}.pctl_state` costPerEfflocationState 
                    ON costPerEfflocationState.ID = costPerEfflocation.StateInternal

				--Rating Location
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) ratingLocation 
					ON ratingLocation.ID = CASE 
						WHEN covEffLoc.ID IS NOT NULL
							AND covEfflocState.ID IS NOT NULL
							AND covEfflocState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN covEffLoc.ID
						WHEN costPerEfflocation.MaxID IS NOT NULL
							AND CostPerEfflocationState.ID = COALESCE(pc_bopcost.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
							THEN costPerEfflocation.MaxID
						ELSE - 1
						END

				WHERE 1 = 1
				/**** TEST *****/
				--AND pc_policyperiod.PolicyNumber = IFNULL(vpolicynumber, pc_policyperiod.PolicyNumber)
                --AND pc_effectivedatedfields.BranchID=9647239

);

--CREATE OR REPLACE TABLE `{project}.{dest_dataset}.FinancialTransactionBOPCeded`
--AS
DELETE `{project}.{dest_dataset}.FinancialTransactionBOPCeded` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.FinancialTransactionBOPCeded`
	( 
		SourceSystem	
		,FinancialTransactionKey	
		,PolicyTransactionKey	
		,BOPCoverageKey	
		,RiskLocationKey	
		,RiskBuildingKey	
		,BusinessType	
		,CededCoverable	
		,CoveragePublicID	
		,TransactionPublicID
		,BOPLocationPublicID
		,IsPrimaryLocationBOPLocation
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
		,NumDaysInRatedTerm	
		,AdjustedFullTermAmount	
		,ActualTermAmount
		,ActualAmount
		,ChargePattern
		,ChargeGroup 
		,ChargeSubGroup
		,RateType
		,RatedState	
		,CedingRate	
		,CededAmount	
		,CededCommissions	
		,CededTermAmount	
		,RIAgreementType	
		,RIAgreementNumber	
		,CededID	
		,CededAgreementID
		,RICoverageGroupType	
		,RatingLocationPublicID	
		,CostPublicID	
		,PolicyPeriodPublicID
		,LineCode
		,bq_load_date
	)
	WITH FinConfigBOPCeded AS (
		SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
		SELECT 'HashKeySeparator','_' UNION ALL
		SELECT 'HashAlgorithm', 'SHA2_256' UNION ALL
		SELECT 'LineCode','BusinessOwnersLine' UNION ALL
		SELECT 'BusinessType', 'Ceded' UNION ALL
		/*CoverageLevel Values */
		SELECT 'LineLevelCoverage','Line' UNION ALL
		SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
		SELECT 'LocationLevelCoverage','Location' UNION ALL
		SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
		SELECT 'BuildingLevelCoverage', 'Building' UNION ALL
		SELECT 'OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC' UNION ALL
		SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
		SELECT 'NoCoverage','NoCoverage' UNION ALL
		/*Ceded Coverable Types */
		SELECT 'CededCoverable', 'DefaultCov' UNION ALL
		SELECT 'CededCoverableBOP', 'BOPCov' UNION ALL
		SELECT 'CededCoverableBOPBldg', 'BOPBuildingCov' UNION ALL
		/* Risk Key Values */
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
		,CededCoverable	
		,CoveragePublicID	
		,TransactionPublicID
		,BOPLocationPublicID
		,IsPrimaryLocationBOPLocation
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
		,NumDaysInRatedTerm	
		,AdjustedFullTermAmount	
		,ActualTermAmount
		,ActualAmount
		,ChargePattern
		,ChargeGroup 
		,ChargeSubGroup
		,RateType
		,RatedState	
		,CedingRate	
		,CededAmount	
		,CededCommissions	
		,CededTermAmount	
		,RIAgreementType	
		,RIAgreementNumber	
		,CededID	
		,CededAgreementID
		,RICoverageGroupType	
		,RatingLocationPublicID	
		,CostPublicID	
		,PolicyPeriodPublicID
		,LineCode
		,DATE('{date}') as bq_load_date	
	
	FROM (
		SELECT
			sourceConfig.Value AS SourceSystem
			,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessTypeConfig.Value,CededCoverable,LineCode)) AS FinancialTransactionKey
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
			(SELECT * FROM FinTranBOPCed1)
			UNION ALL
			(SELECT * FROM FinTranBOPCed2)
		) FinTrans
				INNER JOIN FinConfigBOPCeded AS sourceConfig
					ON sourceConfig.Key='SourceSystem'
				INNER JOIN FinConfigBOPCeded AS hashKeySeparator
					ON hashKeySeparator.Key='HashKeySeparator'
				INNER JOIN FinConfigBOPCeded AS hashAlgorithm
					ON hashAlgorithm.Key = 'HashAlgorithm'
				INNER JOIN FinConfigBOPCeded AS businessTypeConfig
					ON businessTypeConfig.Key = 'BusinessType'
				INNER JOIN FinConfigBOPCeded AS locationRisk
					ON locationRisk.Key='LocationLevelRisk'
				INNER JOIN FinConfigBOPCeded AS buildingRisk
					ON buildingRisk.Key='BuildingLevelRisk'

				WHERE 1=1
				--	AND	PolicyNumber = IFNULL(vpolicynumber, PolicyNumber)
					AND	TransactionPublicID IS NOT NULL
					AND TransactionRank = 1
                
	) extractData;

	DROP TABLE FinTranBOPCed1;
	DROP TABLE FinTranBOPCed2;
	DROP TABLE FinTranBOPCedMaxLoc;
	DROP TABLE FinTranBOPCedMinLoc;
	DROP TABLE FinTranBOPCovCedMinLoc;