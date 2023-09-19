/*TEST
DECLARE vpolicynumber STRING; DECLARE vPartition Timestamp;
--SET vpolicynumber = '55-000886';
--55-010060 --55-000886  --55-001370 --55-006536 --55-003682, 55-006536,  55-006550
--SET vPartition = '2020-11-30'; --(SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_bopcededpremium`);
SET vPartition = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic`);
*/

/**********************************************************************************************************************************/
/**** Kimberlite - Financial Transactions ********************
		FinancialTransactionIMCeded.sql
			Converted to Big Query
************************************************************************************************************************************
		This query will bring in all Instances of all types of financial transactions provided under the ILM product 
************************************************************************************************************************************
------------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/05/2021	DROBAK		Changed ILMCoverageKey to IMCoverageKey
	04/05/2021	DROBAK		Changed ILMLocationPublicID to IMLocationPublicID
	07/28/2021	DROBAK		Add field IsTransactionSliceEffective
	09/01/2021	DROBAK		Added join condition to pc_policyline that matches DW join; Extended join condition for Peak-Temp dates and Cov tables
	09/01/2021	DROBAK		Change CoveragePublicID, ILMStockPublicID & IMLocationPublicID from 'None' to CAST(NULL AS STRING), where applicable (BQ only change)
	09/07/2021	DROBAK		For Line and SubLine change to LEFT JOIN for pcx_ilmlocation_jmic
	10/12/2021	DROBAK		Use new Transaction Date logic in stead of cost table date; Round AdjustedFullTermAmount
	01/27/2022	DROBAK		Correct TransactionWrittenDate to be Date (not Datetime)
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode)

-------------------------------------------------------------------------------------------------------------------------------------
*/
/*
CREATE OR REPLACE TABLE IMCededFinancialsConfig
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO IMCededFinancialsConfig
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256')
			,('LineCode','ILMLine_JMIC')
			,('BusinessType', 'Ceded')
			--CoverageLevel Values
			,('LineLevelCoverage','Line')
			,('SubLineLevelCoverage','SubLine')
			,('LocationLevelCoverage','Location')
			,('SubLocLevelCoverage','SubLoc')
			,('StockLevelCoverage','Stock')
			,('SubStockLevelCoverage','SubStock')
			,('OnetimeCredit','OnetimeCredit')
			,('NoCoverage','NoCoverage')
			--Ceded Coverable Types
			,('CededCoverable', 'DefaultCov')
			,('CededCoverableStock', 'ILMStockCov')
			,('CededCoverableSubStock', 'ILMSubStockCov')
			,('CededCoverableSubLine', 'ILMSubLineCov')
			-- Risk Key Values
			,('LocationLevelRisk','ILMLocation')
			,('StockLevelRisk','ILMStock')	 		  
			,('OneTimeCreditCustomCoverage','ILMOneTimeCredit_JMIC')
			,('AdditionalInsuredCustomCoverage','Additional_Insured_JMIC');
*/

CREATE OR REPLACE TEMP TABLE FinTranIMCed1
as
(
	WITH imCededFinancials AS (
	  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
	  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
	  SELECT 'LineLevelCoverage','Line' UNION ALL
	  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
	  SELECT 'LocationLevelCoverage','Location' UNION ALL
	  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
	  SELECT 'StockLevelCoverage','Stock' UNION ALL
	  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
	  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
	  SELECT 'NoCoverage','NoCoverage' UNION ALL
	  SELECT 'LocationLevelRisk','ILMLocation' UNION ALL
	  SELECT 'StockLevelRisk','ILMStock' UNION ALL
	  SELECT 'OneTimeCreditCustomCoverage','ILMOneTimeCredit_JMIC' UNION ALL
	  SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
	  SELECT 'CededCoverable', 'DefaultCov' UNION ALL
	  SELECT 'CededCoverableStock', 'ILMStockCov' UNION ALL
	  SELECT 'CededCoverableSubStock', 'ILMSubStockCov' UNION ALL
	  SELECT 'CededCoverableSubLine', 'ILMSubLineCov' UNION ALL
	  SELECT 'BusinessType','Ceded'
	)
		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			LINE COV
		**********************************/
		SELECT 
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmlinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE)
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			--,'None' AS ILMStockPublicID
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
					END 
				END AS AdjustedFullTermAmount
			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,0 AS IsPeakOrTemp
			,CASE WHEN pcx_ilmlinecov_jmic.PatternCode IS NULL THEN 0 ELSE 1 END AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMLineCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE) 
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <=CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE) 
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'LineLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlinecov_jmic
				ON  pcx_ilmlinecov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlinecov_jmic.FixedID = pcx_ilmcost_jmic.ILMLineCov
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE) 
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE) 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
				ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE) 
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE) 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE) 
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE) 
			LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE) 
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE) 
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState

		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			
			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)
			
	UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			SUB LINE COV
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmsublinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubLineCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubLineCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) <= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublinecov_jmic
				ON  pcx_ilmsublinecov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsublinecov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLineCov
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubline_jmic
				ON  pcx_ilmsubline_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsubline_jmic.FixedID = pcx_ilmsublinecov_jmic.ILMSubLine
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubline_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
				ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/

		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			LOCATION COV
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmlocationcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmlocationcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmlocationcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmlocationcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmlocationcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMLocationCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'LocationLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocationcov_jmic
				ON  pcx_ilmlocationcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmlocationcov_jmic.FixedID = pcx_ilmcost_jmic.ILMLocationCov
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocationcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocationcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmlocation_jmic.FixedID = pcx_ilmlocationcov_jmic.ILMLocation
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
				AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern
				ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype
				ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState
				ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)
			
		UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			SUB LOCATION COV
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmsubloccov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID	--keep this or use line below??????
			--,effectiveFieldsPrimaryILMLocation.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmsubloccov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubloccov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubLocCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloccov_jmic
				ON  pcx_ilmsubloccov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsubloccov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLocCov
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloccov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubloccov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubloc_jmic
				ON  pcx_ilmsubloc_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsubloc_jmic.FixedID = pcx_ilmsubloccov_jmic.ILMsubloc
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloc_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmlocation_jmic.FixedID = pcx_ilmsubloc_jmic.ILMLocation
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
				AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)

			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			JEWELRY STOCK COV
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_jewelrystockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.JewelryStockCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'StockLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockcov_jmic
				ON  pcx_jewelrystockcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_jewelrystockcov_jmic.FixedID = pcx_ilmcost_jmic.JewelryStockCov
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
				ON  pcx_jewelrystock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_jewelrystock_jmic.FixedID = pcx_jewelrystockcov_jmic.JewelryStock
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
				AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			SUB STOCK COV
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmsubstockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'SubStockLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstockcov_jmic
				ON  pcx_ilmsubstockcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstockcov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubStockCov
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstock_jmic
				ON  pcx_ilmsubstock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstock_jmic.FixedID = pcx_ilmsubstockcov_jmic.ILMSubStock
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
				ON  pcx_jewelrystock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_jewelrystock_jmic.FixedID = pcx_ilmsubstock_jmic.JewelryStock
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
					AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)			
		
);

CREATE OR REPLACE TEMP TABLE FinTranIMCed2 
as
(
	WITH imCededFinancials AS (
	  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
	  SELECT 'HashKeySeparator','_' UNION ALL
	  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
	  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
	  SELECT 'LineLevelCoverage','Line' UNION ALL
	  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
	  SELECT 'LocationLevelCoverage','Location' UNION ALL
	  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
	  SELECT 'StockLevelCoverage','Stock' UNION ALL
	  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
	  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
	  SELECT 'NoCoverage','NoCoverage' UNION ALL
	  SELECT 'LocationLevelRisk','ILMLocation' UNION ALL
	  SELECT 'StockLevelRisk','ILMStock' UNION ALL
	  SELECT 'OneTimeCreditCustomCoverage','ILMOneTimeCredit_JMIC' UNION ALL
	  SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
	  SELECT 'CededCoverable', 'DefaultCov' UNION ALL
	  SELECT 'CededCoverableStock', 'ILMStockCov' UNION ALL
	  SELECT 'CededCoverableSubStock', 'ILMSubStockCov' UNION ALL
	  SELECT 'CededCoverableSubLine', 'ILMSubLineCov' UNION ALL
	  SELECT 'BusinessType','Ceded'
	)

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			ONE TIME CREDITS
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			,pcx_ilmonetimecredit_jmic_v2.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
						CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE))
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			--Normal Ceded Trxns 
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
					THEN CASE 
							WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
								AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
								THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
							ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
							END
				ELSE 0
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,0 AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmonetimecredit_jmic_v2.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'OnetimeCredit' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmonetimecredit_jmic_v2` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmonetimecredit_jmic_v2
				ON  pcx_ilmonetimecredit_jmic_v2.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmonetimecredit_jmic_v2.FixedID = pcx_ilmcost_jmic.ILMOneTimeCredit
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmonetimecredit_jmic_v2.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmonetimecredit_jmic_v2.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstockcov_jmic
				ON  pcx_ilmsubstockcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstockcov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubStockCov
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstock_jmic
				ON  pcx_ilmsubstock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstock_jmic.FixedID = pcx_ilmsubstockcov_jmic.ILMSubStock
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
				ON  pcx_jewelrystock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_jewelrystock_jmic.FixedID = pcx_ilmonetimecredit_jmic_v2.JewelryStock_JMIC
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
					AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
			--ILM OneTimeCredit Reason
			LEFT JOIN `{project}.{pc_dataset}.pctl_onetimecreditreason_jmic` AS pctl_onetimecreditreason_jmic 
				ON pctl_onetimecreditreason_jmic.ID = pcx_ilmonetimecredit_jmic_v2.CreditReason
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - DEFAULT COVERAGE
			NO COVERAGE
		**********************************/
		SELECT
			pcx_ilmcededpremiumtrans_jmic.PublicID as TransactionPublicID
			--,'None' AS CoveragePublicID
			,CAST(NULL AS STRING) AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			--,'None' AS IMLocationPublicID
			,CAST(NULL AS STRING) AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmcededpremiumtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmcededpremiumtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmcededpremiumtrans_jmic.DatePosted AS DATE), 
						CAST(pcx_ilmcededpremiumtrans_jmic.EffectiveDate AS DATE))
						,CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate , pcx_ilmcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			--Normal Ceded Trxns 
			,CASE 
				WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
					THEN CASE 
							WHEN IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
								AND IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
								THEN pcx_ilmcededpremiumtrans_jmic.CededPremium
							ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmcededpremiumtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmcededpremiumtrans_jmic.EffectiveDate, pcx_ilmcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmcededpremiumtrans_jmic.ExpirationDate, pcx_ilmcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
							END
				ELSE 0
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,0 AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmcededpremiumtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmcededpremiumtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmcededpremiumtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmcededpremiumtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmcededpremiumtrans_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcededpremiumjmic
				ON pcx_ilmcededpremiumjmic.ID = pcx_ilmcededpremiumtrans_jmic.ILMCededPremium_JMIC			
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmcededpremiumjmic.ILMCovCost_JMIC = pcx_ilmcost_jmic.ID 
				AND -1=COALESCE(pcx_ilmcost_jmic.ILMLineCov
				,pcx_ilmcost_jmic.ILMSubLineCov
				,pcx_ilmcost_jmic.ILMLocationCov
				,pcx_ilmcost_jmic.ILMSubLocCov
				,pcx_ilmcost_jmic.JewelryStockCov
				,pcx_ilmcost_jmic.ILMSubStockCov
				,pcx_ilmcost_jmic.ILMOneTimeCredit
				,-1)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverable'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'NoCoverage' 
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmlocation_jmic.FixedID = IFNULL(pcx_ilmcost_jmic.ILMLocation_JMIC,pcx_ilmcost_jmic.ILMTaxLocation_JMIC)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
					AND pc_policylocation.FixedID = COALESCE(pcx_ilmlocation_jmic.Location, pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmcededpremiumtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement
				ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype
				ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
					AND per.ModelDate < pcx_ilmcededpremiumtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
				ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmcededpremiumtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - STOCK COVERAGE
			LINE LEVEL
		**********************************/
		SELECT
			pcx_ilmstkcededpremtrans_jmic.PublicID as TransactionPublicID
			,pcx_jewelrystockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmstkcededpremtrans_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmstkcededpremtrans_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmstkcededpremtrans_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmstkcededpremtrans_jmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmstkcededpremtrans_jmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmstkcededpremtrans_jmic.EffectiveDate, pcx_ilmstockcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmstkcededpremtrans_jmic.ExpirationDate , pcx_ilmstockcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmstkcededpremtrans_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
					WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmstkcededpremtrans_jmic.EffectiveDate, pcx_ilmstockcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmstkcededpremtrans_jmic.ExpirationDate, pcx_ilmstockcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
						THEN CASE 
								WHEN IFNULL(pcx_ilmstkcededpremtrans_jmic.EffectiveDate, pcx_ilmstockcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
									AND IFNULL(pcx_ilmstkcededpremtrans_jmic.ExpirationDate, pcx_ilmstockcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
									THEN pcx_ilmstkcededpremtrans_jmic.CededPremium
								ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmstkcededpremtrans_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmstkcededpremtrans_jmic.EffectiveDate, pcx_ilmstockcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmstkcededpremtrans_jmic.ExpirationDate, pcx_ilmstockcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
								END
					ELSE 0
					END
				END AS AdjustedFullTermAmount

			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmstkcededpremtrans_jmic.CedingRate AS CedingRate
			,pcx_ilmstkcededpremtrans_jmic.CededPremium AS CededAmount
			,pcx_ilmstkcededpremtrans_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmstkcededpremtrans_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmstockcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmstkcededpremtrans_jmic.ID
				ORDER BY
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM 
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmstkcededpremtrans_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmstkcededpremtrans_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmstockcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmstockcededpremiumjmic
				ON pcx_ilmstockcededpremiumjmic.ID = pcx_ilmstkcededpremtrans_jmic.ILMStockCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmstockcededpremiumjmic.ILMStockCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.JewelryStockCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverableStock'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'LineLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystockcov_jmic
				ON  pcx_jewelrystockcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_jewelrystockcov_jmic.FixedID = pcx_ilmcost_jmic.JewelryStockCov
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
				ON  pcx_jewelrystock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_jewelrystock_jmic.FixedID = pcx_jewelrystockcov_jmic.JewelryStock
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
				AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmstkcededpremtrans_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm 
				ON pc_policyterm.ID = pcx_ilmstockcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmstkcededpremtrans_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmstkcededpremtrans_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - SUB STOCK COVERAGE
			LINE LEVEL
		**********************************/
		SELECT
			pcx_ilmsubstkcededpremtranjmic.PublicID as TransactionPublicID
			,pcx_ilmsubstockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmsubstkcededpremtranjmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmsubstkcededpremtranjmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmsubstkcededpremtranjmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmsubstkcededpremtranjmic.DatePosted AS DATE), 
							CASE /*is a temp, ignore accrual date*/
							WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
								THEN CAST('1900-01-01' AS DATE)
							ELSE CAST(pcx_ilmsubstkcededpremtranjmic.EffectiveDate AS DATE)
							END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmsubstkcededpremtranjmic.EffectiveDate, pcx_ilmsubstkcededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmsubstkcededpremtranjmic.ExpirationDate , pcx_ilmsubstkcededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			--,COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID) AS RatedState
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmsubstkcededpremtranjmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmsubstkcededpremtranjmic.EffectiveDate, pcx_ilmsubstkcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmsubstkcededpremtranjmic.ExpirationDate, pcx_ilmsubstkcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmsubstkcededpremtranjmic.EffectiveDate, pcx_ilmsubstkcededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmsubstkcededpremtranjmic.ExpirationDate, pcx_ilmsubstkcededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmsubstkcededpremtranjmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmsubstkcededpremtranjmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmsubstkcededpremtranjmic.EffectiveDate, pcx_ilmsubstkcededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmsubstkcededpremtranjmic.ExpirationDate, pcx_ilmsubstkcededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount
			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmsubstkcededpremtranjmic.CedingRate AS CedingRate
			,pcx_ilmsubstkcededpremtranjmic.CededPremium AS CededAmount
			,pcx_ilmsubstkcededpremtranjmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmsubstkcededpremtranjmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmsubstkcededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmsubstkcededpremtranjmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstkcededpremtranjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstkcededpremtranjmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstkcededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstkcededpremiumjmic
				ON pcx_ilmsubstkcededpremiumjmic.ID = pcx_ilmsubstkcededpremtranjmic.ILMSubStkCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmsubstkcededpremiumjmic.ILMSubStockCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverableSubStock'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'SubStockLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstockcov_jmic
				ON  pcx_ilmsubstockcov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstockcov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubStockCov
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubstockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubstock_jmic
				ON  pcx_ilmsubstock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmsubstock_jmic.FixedID = pcx_ilmsubstockcov_jmic.ILMSubStock
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jewelrystock_jmic
				ON  pcx_jewelrystock_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_jewelrystock_jmic.FixedID = pcx_ilmsubstock_jmic.JewelryStock
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmlocation_jmic.FixedID = pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pcx_ilmcost_jmic.BranchID
					AND pc_policylocation.FixedID = pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsubstockcov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmsubstkcededpremtranjmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement 
				ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmsubstkcededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmsubstkcededpremtranjmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmsubstkcededpremtranjmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/
		
		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)

	UNION ALL

		/**********************************
			ILM CEDED - SUB LINE COVERAGE
			SUB LINE LEVEL
		**********************************/
		SELECT 
			pcx_ilmsublncededpremtran_jmic.PublicID as TransactionPublicID
			,pcx_ilmsublinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_policyperiod.PublicID AS PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(pc_policyperiod.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(pc_policyperiod.EditEffectiveDate AS DATE) < CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			,pctl_policyline.TYPECODE AS LineCode
			,pcx_ilmsublncededpremtran_jmic.CededPremium AS TransactionAmount --the Ceded Premium
			,pcx_ilmsublncededpremtran_jmic.CreateTime AS TransactionPostedDate --use this becuase this has a timestamp. Could also use CalcTimestamp (if needed). 
			,CASE WHEN pcx_ilmsublncededpremtran_jmic.DateWritten IS NOT NULL THEN 1 ELSE 0 END AS TrxnWritten
			,CAST(CAST(custom_functions.fn_GetMaxDate(custom_functions.fn_GetMaxDate(CAST(pcx_ilmsublncededpremtran_jmic.DatePosted AS DATE), 
				CASE /*is a temp, ignore accrual date*/
				WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN CAST('1900-01-01' AS DATE)
				ELSE CAST(pcx_ilmsublncededpremtran_jmic.EffectiveDate AS DATE)
				END), CAST(pc_policyperiod.EditEffectiveDate AS DATE)) AS STRING) AS DATE) AS TransactionWrittenDate
			,CAST(IFNULL(pcx_ilmsublncededpremtran_jmic.EffectiveDate, pcx_ilmsublinecededpremiumjmic.EffectiveDate) AS DATE) AS EffectiveDate
			,CAST(IFNULL(pcx_ilmsublncededpremtran_jmic.ExpirationDate , pcx_ilmsublinecededpremiumjmic.ExpirationDate) AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup
			,pcx_ilmcost_jmic.ChargeSubGroup
			,pcx_ilmcost_jmic.RateAmountType AS RateType
			,CASE WHEN pcx_ilmcost_jmic.RateAmountType = 3 -- Tax or surcharge 
					THEN 0 ELSE 1
				END AS IsPremium
			,ratedState.TYPECODE AS RatedState
			,pc_uwcompany.PublicID AS UWCompanyPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodStart, 101)) AS PeriodStart
			--,CONVERT(DATETIME, CONVERT(CHAR(10), pc_policyperiod.PeriodEnd, 101)) AS PeriodEnd
			,CASE --peaks and temps 
				WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmsublncededpremtran_jmic.CededPremium
				ELSE --Normal Ceded Trxns 
					CASE 
						WHEN CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmsublncededpremtran_jmic.EffectiveDate, pcx_ilmsublinecededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmsublncededpremtran_jmic.ExpirationDate, pcx_ilmsublinecededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) <> 0
							THEN CASE 
									WHEN IFNULL(pcx_ilmsublncededpremtran_jmic.EffectiveDate, pcx_ilmsublinecededpremiumjmic.EffectiveDate) = pc_policyperiod.PeriodStart
										AND IFNULL(pcx_ilmsublncededpremtran_jmic.ExpirationDate, pcx_ilmsublinecededpremiumjmic.ExpirationDate) = pc_policyperiod.PeriodEnd
										THEN pcx_ilmsublncededpremtran_jmic.CededPremium
									ELSE ROUND(CAST(CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) * pcx_ilmsublncededpremtran_jmic.CededPremium / CAST(TIMESTAMP_DIFF(IFNULL(pcx_ilmsublncededpremtran_jmic.EffectiveDate, pcx_ilmsublinecededpremiumjmic.EffectiveDate), IFNULL(pcx_ilmsublncededpremtran_jmic.ExpirationDate, pcx_ilmsublinecededpremiumjmic.ExpirationDate), DAY) AS FLOAT64) AS DECIMAL),0)
									END
						ELSE 0
						END 
				END AS AdjustedFullTermAmount
			--Primary ID and Location;  Effective and Primary Fields
			--,pc_effectivedatedfields.PublicID AS EffectiveDatedFieldsPublicID
			,pc_policylocation.LocationNum AS PrimaryPolicyLocationNumber
			--,pc_policylocation.PublicID AS PrimaryPolicyLocationPublicID
			,pc_policylocation.PublicID as RatingLocationPublicID
			--Peaks and Temp and SERP Related Data
			,CASE WHEN (pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN 1 ELSE 0
				END AS IsPeakOrTemp
			,0 AS IsSupplementalCoverage
			--,minILMPolicyLocationOnBranch.PublicID AS MinILMPolicyLocationPublicID
			--Ceding Data
			,pcx_ilmsublncededpremtran_jmic.CedingRate AS CedingRate
			,pcx_ilmsublncededpremtran_jmic.CededPremium AS CededAmount
			,pcx_ilmsublncededpremtran_jmic.Commission AS CededCommissions
			,pcx_ilmcost_jmic.ActualTermAmount * pcx_ilmsublncededpremtran_jmic.CedingRate AS CededTermAmount
			,pctl_riagreement.TYPECODE AS RIAgreementType
			,pc_reinsuranceagreement.AgreementNumber AS RIAgreementNumber
			,pcx_ilmsublinecededpremiumjmic.ID AS CededID
			,pc_reinsuranceagreement.ID AS CededAgreementID
			,pctl_ricoveragegrouptype.TypeCode AS RICoverageGroupType
			,CededCoverable.Value AS CededCoverable
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmsublncededpremtran_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubLineCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublncededpremtran_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublncededpremtran_jmic
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecededpremiumjmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublinecededpremiumjmic
				ON pcx_ilmsublinecededpremiumjmic.ID = pcx_ilmsublncededpremtran_jmic.ILMSubLineCededPremium_JMIC
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmcost_jmic
				ON pcx_ilmsublinecededpremiumjmic.ILMSubLineCovCost_JMIC = pcx_ilmcost_jmic.ID
				AND pcx_ilmcost_jmic.ILMSubLineCov IS NOT NULL
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
				ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
				ON pc_job.ID = pc_policyperiod.JobID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
				ON pc_policyline.BranchID = pcx_ilmcost_jmic.BranchID
					AND pcx_ilmcost_jmic.ILMLine = pc_policyline.FixedID
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline ON pctl_policyline.ID = pc_policyline.Subtype
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
				ON pc_policyperiod.UWCompany = pc_uwcompany.ID
			INNER JOIN imCededFinancials CededCoverable ON CededCoverable.Key = 'CededCoverableSubLine'
			INNER JOIN imCededFinancials lineConfig ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			INNER JOIN imCededFinancials coverageLevelConfig ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsublinecov_jmic
				ON  pcx_ilmsublinecov_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsublinecov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLineCov
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsublinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsublinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubline_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmsubline_jmic
				ON  pcx_ilmsubline_jmic.BranchID = pcx_ilmcost_jmic.BranchID
				AND pcx_ilmsubline_jmic.FixedID = pcx_ilmsublinecov_jmic.ILMSubLine
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubline_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubline_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
				ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation
				ON  pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)>= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE)<= CAST(COALESCE(pcx_ilmsublinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) AS DATE)
			--Cede Agreement
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_reinsuranceagreement` WHERE _PARTITIONTIME = {partition_date}) AS pc_reinsuranceagreement
				ON pc_reinsuranceagreement.ID = pcx_ilmsublncededpremtran_jmic.Agreement
			INNER JOIN `{project}.{pc_dataset}.pctl_riagreement` AS pctl_riagreement ON pctl_riagreement.ID = pc_reinsuranceagreement.SubType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_ricoveragegroup` WHERE _PARTITIONTIME = {partition_date}) AS pc_ricoveragegroup
				ON pc_ricoveragegroup.Agreement = pc_reinsuranceagreement.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_ricoveragegrouptype` AS pctl_ricoveragegrouptype ON pctl_ricoveragegrouptype.ID = pc_ricoveragegroup.GroupType
			--look up ceded premiums transaction's branch id
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyterm` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyterm ON pc_policyterm.ID = pcx_ilmsublinecededpremiumjmic.PolicyTerm
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) per
				ON per.PolicyTermID = pc_policyterm.ID
				AND per.ModelDate < pcx_ilmsublncededpremtran_jmic.CalcTimestamp    
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_paymentplansummary` WHERE _PARTITIONTIME = {partition_date}) AS pc_paymentplansummary
					ON pc_policyperiod.ID = pc_paymentplansummary.PolicyPeriod
					AND pc_paymentplansummary.Retired = 0
			LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern	
			LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` AS pctl_rateamounttype ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS ratedState ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		/* Currently not used in Kimberlite
			--Get the minimum location (number) for this line and branch
			LEFT JOIN  PolicyCenter.dbo.pc_policylocation AS minILMPolicyLocationOnBranch
				ON  minILMPolicyLocationOnBranch.BranchID = pcx_ilmcost_jmic.BranchID
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(minILMPolicyLocationOnBranch.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(minILMPolicyLocationOnBranch.ExpirationDate,pc_policyperiod.PeriodEnd)
			INNER JOIN PolicyCenter.dbo.pcx_ilmlocation_jmic AS pcx_ilmlocation_jmic_min
				ON minILMPolicyLocationOnBranch.FixedID = pcx_ilmlocation_jmic_min.Location
					AND pcx_ilmlocation_jmic_min.BranchID = pcx_ilmcost_jmic.BranchID				
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) >= COALESCE(pcx_ilmlocation_jmic_min.EffectiveDate,pc_policyperiod.PeriodStart)
					AND CAST(COALESCE(pcx_ilmsublncededpremtran_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate) AS DATE) < COALESCE(pcx_ilmlocation_jmic_min.ExpirationDate,pc_policyperiod.PeriodEnd)
		*/

		--State And Country
			--LEFT JOIN PolicyCenter.dbo.pctl_state AS taxLocState on taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction

			WHERE 1 = 1
			--AND pc_PolicyPeriod.PolicyNumber = IFNULL(vpolicynumber, pc_PolicyPeriod.PolicyNumber)	
);

DELETE `{project}.{dest_dataset}.FinancialTransactionIMCeded` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.FinancialTransactionIMCeded` (
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,IMCoverageKey
	,RiskLocationKey
	,RiskStockKey
	,BusinessType
	,CededCoverable
	,CoveragePublicID
	,TransactionPublicID
	,IsTransactionSliceEffective
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

WITH imCededFinancials AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','ILMLine_JMIC' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'SubLineLevelCoverage','SubLine' UNION ALL
  SELECT 'LocationLevelCoverage','Location' UNION ALL
  SELECT 'SubLocLevelCoverage','SubLoc' UNION ALL
  SELECT 'StockLevelCoverage','Stock' UNION ALL
  SELECT 'SubStockLevelCoverage','SubStock' UNION ALL
  SELECT 'OnetimeCredit','OnetimeCredit' UNION ALL
  SELECT 'NoCoverage','NoCoverage' UNION ALL
  SELECT 'LocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'StockLevelRisk','ILMStock' UNION ALL
  SELECT 'OneTimeCreditCustomCoverage','ILMOneTimeCredit_JMIC' UNION ALL
  SELECT 'AdditionalInsuredCustomCoverage','Additional_Insured_JMIC' UNION ALL
  SELECT 'CededCoverable', 'DefaultCov' UNION ALL
  SELECT 'CededCoverableStock', 'ILMStockCov' UNION ALL
  SELECT 'CededCoverableSubStock', 'ILMSubStockCov' UNION ALL
  SELECT 'CededCoverableSubLine', 'ILMSubLineCov' UNION ALL
  SELECT 'BusinessType','Ceded'
)
SELECT 
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,IMCoverageKey
	,RiskLocationKey
	,RiskStockKey
	,BusinessType
	,CededCoverable
	,CoveragePublicID
	,TransactionPublicID
	,IsTransactionSliceEffective
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
--INTO #imFinancials
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,businessType.Value,CededCoverable,LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,CASE WHEN CoveragePublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel))
		END AS IMCoverageKey
		,CASE WHEN IMLocationPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
		END AS RiskLocationKey
		,CASE WHEN ILMStockPublicID IS NOT NULL 
			THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ILMStockPublicID,hashKeySeparator.Value,stockRisk.Value)) 			
		END AS RiskStockKey
		,businessType.Value as BusinessType
		,imFinancials.*		
	FROM (
        (SELECT * FROM FinTranIMCed1)
		UNION ALL
		(SELECT * FROM FinTranIMCed2)
	) imFinancials
	INNER JOIN imCededFinancials  sourceConfig ON sourceConfig.Key='SourceSystem'
	INNER JOIN imCededFinancials  hashKeySeparator ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN imCededFinancials  locationRisk ON locationRisk.Key='LocationLevelRisk'
	INNER JOIN imCededFinancials  stockRisk ON stockRisk.Key='StockLevelRisk'
	INNER JOIN imCededFinancials  hashingAlgo ON hashingAlgo.Key='HashingAlgorithm'
	INNER JOIN imCededFinancials  businessType ON businessType.Key='BusinessType'

	WHERE 1=1
		AND	TransactionPublicID IS NOT NULL
		AND TransactionRank = 1

) extractData;

DROP TABLE FinTranIMCed1;
DROP TABLE FinTranIMCed2;