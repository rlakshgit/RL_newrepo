-- tag: FinancialTransactionIMDirect - tag ends/

/**** Kimberlite - Financial Transactions ********
		FinancialTransactionIMDirect.sql
			CONVERTED TO BIG QUERY
**************************************************
--This query will bring in all Instances of all types of financial transactions provided under the ILM product. 
--------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/05/2021	DROBAK		Changed ILMCoverageKey to IMCoverageKey
	04/05/2021	DROBAK		Changed ILMLocationPublicID to IMLocationPublicID
	07/15/2021	DROBAK		Corrected PolicyPeriod Join to use pcx_ilmtransaction_jmic.BranchID
	07/21/2021	DROBAK		Added Peak-Temp Date condition for Coverage Table Inner Joins; Change from <= to < on ExpirationDate condition
	07/26/2021	DROBAK		Change joins for coverage and below to use perCost (cost level)
	08/04/2021	DROBAK		Add field IsTransactionSliceEffective
	08/06/2021	DROBAK		Added join condition to pc_policyline that matches DW join; extended join condition for Temp dates and cov tables
	09/01/2021	DROBAK		Change CoveragePublicID, ILMStockPublicID & IMLocationPublicID from 'None' to CAST(NULL AS STRING), where applicable (BQ only change)
	09/07/2021	DROBAK		For Line and SubLine change to LEFT JOIN for pcx_ilmlocation_jmic
	09/10/2021	DROBAK		Changeed Data Type from Float64 to Decimal for AdjustedFullTermAmount
	09/15/2021	DROBAK		Fixed error: replace pc_policyperiod with perCost for BranchID join in pc_policylocation (one occurrence)
	09/28/2021	DROBAK		New Version for Date Logic: use transaction table dates in place of cost table; use <= instead of < for ExpirationDate; LEFT join pc_policylocation
	10/04/2021	DROBAK		Round AdjustedFullTermAmount to zero
	11/03/2021	DROBAK		TransactionWrittenDate, EffectiveDate, ExpirationDate, CanBeEarned - Make it DATE not TIMESTAMP (compatible with other queries/tables in BQ too)
	02/25/2022	DROBAK		Fix missing Risk Location Keys with use of Temp table to define min IM Location
	03/08/2022	DROBAK		Use Temp table to get Minimum IM Location PublicID -- used in RiskLocationKey for sections: Line, SubLine, and NoCoverage
	06/01/2022	DROBAK		Add LineCode (Used to generate ProductCode)
	07/25/2022	DROBAK		Added back Delete/Insert Stmnt for DAG to execute properly (future: fix how this works by using BQ Operator)
	03/03/2023	DROBAK		Corrected RatingLocationPublicID by following DW code method; added/uses new temp table tmp_PolicyVersion_IM_PrimaryRatingLocation

--------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE imFinancials 
 (
		 Key STRING,
		 Value STRING
	);
	INSERT INTO imFinancials 
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashingAlgorithm','SHA2_256')
			,('LineCode','ILMLine_JMIC')
			,('LineLevelCoverage','Line')
			,('SubLineLevelCoverage','SubLine')
			,('LocationLevelCoverage','Location')
			,('SubLocLevelCoverage','SubLoc')
			,('StockLevelCoverage','Stock')
			,('SubStockLevelCoverage','SubStock')
			,('OnetimeCredit','OnetimeCredit')
			,('NoCoverage','NoCoverage')
			,('LocationLevelRisk','ILMLocation')
			,('StockLevelRisk','ILMStock')
			,('BusinessType','Direct');
*/

CREATE OR REPLACE TEMP TABLE tmp_PolicyVersion_IM_PrimaryRatingLocation
AS
(
	SELECT IM_PrimaryRatingLocation.*
	FROM
	(
		--This code is also used in RiskLocationIM, so the two table use SAME PublicID from SAME Table (pcx_ilmlocation_jmic)
		--NOTE: If this CTE is referenced more than once you must conert to a Temp or Perm Temp table for BQ
		SELECT 
			pc_policyperiod.ID								AS PolicyPeriodID
			,pc_policyperiod.EditEffectiveDate				AS SubEffectiveDate
			,pctl_policyline.TYPECODE						AS PolicyLineOfBusiness
			--This flag displays whether or not the LOB Location matches the PrimaryLocation
			,MAX(CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
			--If the Primary loc matches the LOB loc, use it, otherwise use the MIN location num's corresponding LOB Location
			,COALESCE(MIN(CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
					,MIN(pc_policylocation.LocationNum))	AS RatingLocationNum
			,CASE WHEN pcx_ilmlocation_jmic.Location = PrimaryPolicyLocation.FixedID THEN MAX(pcx_ilmlocation_jmic.ID) ELSE NULL END AS PrimaryIMLocationID
			,MAX(PrimaryPolicyLocation.ID)					AS PrimaryPolicyLocationID
		--select pc_policyperiod.id, pc_policyperiod.EditEffectiveDate, pc_policyperiod.PeriodStart, pc_policylocation.EffectiveDate
		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
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
				AND pctl_policyline.TYPECODE = 'ILMLine_JMIC'
			--Inland Marine Location
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
				AND pcx_ilmlocation_jmic.BranchID = pc_policylocation.BranchID
				AND pcx_ilmlocation_jmic.ILMLine = pc_policyline.FixedID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
			--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS PrimaryPolicyLocation
				ON PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation 
				AND PrimaryPolicyLocation.BranchID = pc_effectivedatedfields.BranchID 
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		--WHERE pc_policyperiod.PolicyNumber = ISNULL(@policynumber, pc_policyperiod.PolicyNumber)	--Testing
		GROUP BY
			pc_policyperiod.ID
			,pc_policyperiod.EditEffectiveDate
			,pctl_policyline.TYPECODE
			,pcx_ilmlocation_jmic.Location
			,PrimaryPolicyLocation.FixedID
	) IM_PrimaryRatingLocation
	WHERE IsPrimaryLocation = 'Y'
);

CREATE OR REPLACE TEMP TABLE tmp_min_policy_location
AS
(
	SELECT	LocRank.*
	FROM
	(	
		SELECT  
				pc_policylocation.PublicID
				,pc_policylocation.ID AS minIMPolicyLocationOnBranchID
				,pc_policylocation.StateInternal
				,PolicyNumber
				,pc_policylocation.BranchID AS PolicyLocationBranchID
				,pc_policylocation.EffectiveDate
				,pc_policylocation.ExpirationDate
				,ROW_NUMBER() OVER(PARTITION BY pc_policylocation.BranchID
								ORDER BY pc_policylocation.LocationNum, cost.ID ASC
				) AS TransactionRank
			--select top 1 pc_policylocation.LocationNum
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) cost 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=cost.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
			ON pc_policylocation.BranchID = cost.BranchID
			AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID
			AND pcx_ilmlocation_jmic.BranchID = cost.BranchID
			AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		WHERE 1=1
		ORDER BY pc_policylocation.LocationNum
	) LocRank
	WHERE 1=1
	AND TransactionRank = 1
);

CREATE OR REPLACE TEMP TABLE tmp_costPerEfflocation
AS
(
	SELECT effLocn.*
	FROM
	(
		SELECT 
				MAX(pc_policylocation.Id) AS PolicyLocationID
				,pc_policylocation.PublicID
				,pc_policylocation.BranchID AS PolicyLocationBranchID
				,pc_policylocation.StateInternal
				,pc_policylocation.EffectiveDate
				,pc_policylocation.ExpirationDate
				,ROW_NUMBER() OVER(PARTITION BY pc_policylocation.BranchID
								ORDER BY pc_policylocation.ID ASC
				) AS TransactionRank
		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) cost 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
				ON perCost.ID=cost.BranchID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
				ON pc_policylocation.BranchID = cost.BranchID
				AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(cost.EffectiveDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
			INNER JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state 
				ON pctl_state.id = pc_policylocation.stateinternal
		WHERE 1=1
			--AND perCost.PolicyNumber = @policyNumber
		GROUP BY pc_policylocation.Id
				,pc_policylocation.PublicID
				,pc_policylocation.BranchID
				,pc_policylocation.StateInternal
				,pc_policylocation.EffectiveDate
				,pc_policylocation.ExpirationDate
	) effLocn
	WHERE TransactionRank = 1
);

CREATE OR REPLACE TEMP TABLE imDirectFinancials
AS SELECT *
FROM (
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
  SELECT 'BusinessType','Direct'
);

/*
   CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_IMDirectFinancials`
    AS
    SELECT outerquery.*
	FROM
	(
*/
--DELETE `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.FinancialTransactionIMDirect` (
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,IMCoverageKey
	,RiskLocationKey
	,RiskStockKey
	,BusinessType
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
	,RatingLocationPublicID
	,CostPublicID
	,PolicyPeriodPublicID
	,LineCode
	,bq_load_date	
)


SELECT 
	SourceSystem
	,FinancialTransactionKey
	,PolicyTransactionKey
	,IMCoverageKey
	,RiskLocationKey
	,RiskStockKey
	,BusinessType
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
	,RatingLocationPublicID
	,CostPublicID
	,PolicyPeriodPublicID
	,LineCode
    ,DATE('{date}') as bq_load_date
    --,CURRENT_DATE() as bq_load_date

FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,hashKeySeparator.Value,businessType.Value, LineCode)) AS FinancialTransactionKey
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
		--LineLevelCoverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmlinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate AS DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps
				WHEN (pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank

		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'LineLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlinecov_jmic   
			ON  pcx_ilmlinecov_jmic.BranchID = perCost.ID
				AND pcx_ilmlinecov_jmic.FixedID = pcx_ilmcost_jmic.ILMLineCov
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlinecov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlinecov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--effFields
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields  
			ON   pc_effectivedatedfields.BranchID = perCost.ID
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--effectiveFieldsPrimaryPolicyLocation
		LEFT JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON  pc_policylocation.BranchID = perCost.ID
			AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--effectiveFieldsPrimaryILMLocation
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON  pcx_ilmlocation_jmic.BranchID = perCost.ID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		
		LEFT JOIN tmp_min_policy_location	--a BQ Temp Table
			ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			--AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
			--AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--3/3/2023
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, CASE
					WHEN pcx_ilmlocation_jmic.ID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE pc_policylocation.ID
					END)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation	--BQ temp table
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_min_policy_location.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlinecov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
					ELSE - 1
				END
	--END - new code 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState 
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
		
		UNION ALL

		--SubLineLevelCoverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			--,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubLineCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as RatingLocationPublicID
			--,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmSubLineCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubLineCov IS NOT NULL		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubLineCov_jmic  
			ON  pcx_ilmSubLineCov_jmic.BranchID = perCost.ID
				AND pcx_ilmSubLineCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLineCov
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmSubLineCov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmSubLineCov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields  
			ON   pc_effectivedatedfields.BranchID = perCost.ID
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation  
			ON  pc_policylocation.BranchID = perCost.ID
			AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic   
			ON  pcx_ilmlocation_jmic.BranchID = perCost.ID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		
		LEFT JOIN tmp_min_policy_location	--a BQ Temp Table
			ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--3/3/2023
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc
			ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, CASE
					WHEN pcx_ilmlocation_jmic.ID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE pc_policylocation.ID
					END)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation	--BQ temp table
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_min_policy_location.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLineCov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
					ELSE - 1
				END
	--END - new code 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern`  pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 

		--LocationLevelCoverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmLocationCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmLocationCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON  coverageLevelConfig.Key = 'LocationLevelCoverage' 
		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic 
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMLocationCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		--ilmLocationCov
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmLocationCov_jmic 
			ON  pcx_ilmLocationCov_jmic.BranchID = perCost.ID
				AND pcx_ilmLocationCov_jmic.FixedID = pcx_ilmcost_jmic.ILMLocationCov
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmLocationCov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmLocationCov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--ilmLocationCovILMLocation
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic 
			ON pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_ilmLocationCov_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--ilmLocationCovPolicyLocation
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation 
			ON pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--BEGIN - 3/3/2023
		--Join in the tmp_PolicyVersion_IM_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN tmp_PolicyVersion_IM_PrimaryRatingLocation AS tmp_PolicyVersion_IM_PrimaryRatingLocation
		ON tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation_primary
		ON pc_policylocation_primary.BranchID = tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation_primary.LocationNum = tmp_PolicyVersion_IM_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation_primary.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation_primary.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN tmp_min_policy_location
		ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc 
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, CASE 
					WHEN tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryIMLocationID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryPolicyLocationID
					END)
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmLocationCov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation 
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
				ELSE - 1
				END
	--END - 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState	
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 
		
		--SubLocLevelCoverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubLocCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmSubLocCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubLocCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubLocCov_jmic  
			ON  pcx_ilmSubLocCov_jmic.BranchID = perCost.ID
				AND pcx_ilmSubLocCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLocCov
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmSubLocCov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmSubLocCov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmsubloc_jmic   
			ON pcx_ilmsubloc_jmic.BranchID = perCost.ID
				AND pcx_ilmsubloc_jmic.FixedID=pcx_ilmSubLocCov_jmic.ILMSubLoc
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubloc_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubloc_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_ilmsubloc_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation  
			ON pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--BEGIN - 3/3/2023
		--Join in the tmp_PolicyVersion_IM_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN tmp_PolicyVersion_IM_PrimaryRatingLocation AS tmp_PolicyVersion_IM_PrimaryRatingLocation
		ON tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation_primary
		ON pc_policylocation_primary.BranchID = tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation_primary.LocationNum = tmp_PolicyVersion_IM_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation_primary.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation_primary.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN tmp_min_policy_location
		ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc 
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, CASE 
					WHEN tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryIMLocationID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryPolicyLocationID
					END)
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubLocCov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation 
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
				ELSE - 1
				END
	--END - 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
				
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)

		UNION ALL 
	
		--Stock Coverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_jewelrystockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.JewelryStockCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'StockLevelCoverage' 
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystockcov_jmic
			ON  pcx_jewelrystockcov_jmic.BranchID = perCost.ID
				AND pcx_jewelrystockcov_jmic.FixedID = pcx_ilmcost_jmic.JewelryStockCov	
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystockcov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystockcov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = perCost.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_jewelrystockcov_jmic.JewelryStock
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--BEGIN - 3/3/2023
		--Join in the tmp_PolicyVersion_IM_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN tmp_PolicyVersion_IM_PrimaryRatingLocation AS tmp_PolicyVersion_IM_PrimaryRatingLocation
		ON tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation_primary
		ON pc_policylocation_primary.BranchID = tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation_primary.LocationNum = tmp_PolicyVersion_IM_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation_primary.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation_primary.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN tmp_min_policy_location
		ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc 
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, CASE 
					WHEN tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryIMLocationID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryPolicyLocationID
					END)
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystockcov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation 
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
				ELSE - 1
				END
	--END - 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState

		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 
	
		--Sub Stock Level Coverage
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubStockCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE --peaks and temps 
				WHEN (pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC IS NOT NULL AND pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC IS NOT NULL) 
					THEN pcx_ilmtransaction_jmic.Amount
				ELSE CASE WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
							THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
						END
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmSubStockCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubStockLevelCoverage'
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic 
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubStockCov_jmic 
			ON  pcx_ilmSubStockCov_jmic.BranchID = perCost.ID
				AND pcx_ilmSubStockCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubStockCov
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmSubStockCov_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmSubStockCov_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
				--AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmSubStockCov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				--AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmSubStockCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmsubstock_jmic 
			ON pcx_ilmsubstock_jmic.BranchID = perCost.ID
				AND pcx_ilmsubstock_jmic.FixedID=pcx_ilmSubStockCov_jmic.ILMSubStock
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmsubstock_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmsubstock_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic  
			ON pcx_jewelrystock_jmic.BranchID = perCost.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_ilmsubstock_jmic.JewelryStock
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_jewelrystock_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_jewelrystock_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
			ON pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--BEGIN - 3/3/2023
		--Join in the tmp_PolicyVersion_IM_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN tmp_PolicyVersion_IM_PrimaryRatingLocation AS tmp_PolicyVersion_IM_PrimaryRatingLocation
		ON tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation_primary
		ON pc_policylocation_primary.BranchID = tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation_primary.LocationNum = tmp_PolicyVersion_IM_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation_primary.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation_primary.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN tmp_min_policy_location
		ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc 
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, CASE 
					WHEN tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryIMLocationID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryPolicyLocationID
					END)
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempFromDt_JMIC,tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmSubStockCov_jmic.FinalPersistedTempToDt_JMIC,tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation 
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
				ELSE - 1
				END
	--END - 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
	UNION ALL
	
	--One Time Credit
	SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmonetimecredit_jmic_v2.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE 	WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
					THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmonetimecredit_jmic_v2.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'OnetimeCredit'
		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMOneTimeCredit IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmonetimecredit_jmic_v2` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmonetimecredit_jmic_v2
			ON  pcx_ilmonetimecredit_jmic_v2.BranchID = perCost.ID
				AND pcx_ilmonetimecredit_jmic_v2.FixedID = pcx_ilmcost_jmic.ILMOneTimeCredit
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmonetimecredit_jmic_v2.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmonetimecredit_jmic_v2.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = perCost.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_ilmonetimecredit_jmic_v2.JewelryStock_JMIC
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_jewelrystock_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_jewelrystock_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,pc_policyperiod.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,pc_policyperiod.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
					AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

	--BEGIN - 3/3/2023
		--Join in the tmp_PolicyVersion_IM_PrimaryRatingLocation Table to map the Natural Key for RatingLocationKey		
		LEFT JOIN tmp_PolicyVersion_IM_PrimaryRatingLocation AS tmp_PolicyVersion_IM_PrimaryRatingLocation
		ON tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID = pc_policyperiod.ID
		AND tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate = pc_policyperiod.EditEffectiveDate
		--Join on policyLine but umbrella uses BOP's rating location so in the case of Umbrella, join on BOP
		AND ((tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = pctl_policyline.TYPECODE) 
			or 
			(pctl_policyline.TYPECODE = 'UmbrellaLine_JMIC' and tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyLineOfBusiness = 'BusinessOwnersLine')) 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation_primary
		ON pc_policylocation_primary.BranchID = tmp_PolicyVersion_IM_PrimaryRatingLocation.PolicyPeriodID  
		AND pc_policylocation_primary.LocationNum = tmp_PolicyVersion_IM_PrimaryRatingLocation.RatingLocationNum  
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation_primary.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(tmp_PolicyVersion_IM_PrimaryRatingLocation.SubEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation_primary.ExpirationDate,pc_policyperiod.PeriodEnd)
	
		LEFT JOIN tmp_min_policy_location
		ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
		AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
			ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc 
			ON covEffLoc.ID = COALESCE(pc_policylocation.ID, costPolicyLocation.ID, CASE 
					WHEN tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryIMLocationID IS NULL
						THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
					ELSE tmp_PolicyVersion_IM_PrimaryRatingLocation.PrimaryPolicyLocationID
					END)
	
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS effectiveLocState 
			ON effectiveLocState.ID = pc_policylocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
			ON costPolicyLocState.ID = costPolicyLocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
			ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
			ON primaryLocState.ID = CASE 
				WHEN pcx_ilmlocation_jmic.ID IS NULL
				THEN tmp_min_policy_location.StateInternal
				ELSE pc_policylocation.StateInternal
				END

		LEFT JOIN tmp_costPerEfflocation
			ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
			ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
			ON covEfflocState.ID = covEffloc.StateInternal
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation 
			ON ratingLocation.ID = CASE
				WHEN covEffLoc.ID IS NOT NULL
					AND covEfflocState.ID IS NOT NULL
					AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN covEffLoc.ID
				WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
					AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, effectiveLocState.ID, taxLocState.ID, primaryLocState.ID)
					THEN tmp_costPerEfflocation.PolicyLocationID
				ELSE - 1
				END
	--END - 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)

	UNION ALL

	--NoCoverage
	SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
 			--,IFNULL(pcx_ilmlocation_jmic.PublicID,pcx_ilmlocation_jmic_via_policy_location.PublicID) AS IMLocationPublicID
			--,'None' AS IMLocationPublicID
			--,CAST(NULL AS STRING) AS IMLocationPublicID
			,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS IMLocationPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,CASE	WHEN CAST(perCost.EditEffectiveDate AS DATE) >= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodStart) AS DATE)
					AND CAST(perCost.EditEffectiveDate AS DATE) <= CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.PeriodEnd) AS DATE)
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			/* Original
			,CASE	WHEN perCost.EditEffectiveDate >= COALESCE(pcx_ilmcost_jmic.EffectiveDate,perCost.PeriodStart)
					AND perCost.EditEffectiveDate <  COALESCE(pcx_ilmcost_jmic.ExpirationDate,perCost.PeriodEnd) 
					THEN 1 ELSE 0 END 
				AS IsTransactionSliceEffective
			*/
            ,pctl_policyline.TYPECODE AS LineCode			
			--,'None' AS CoveragePublicID
			,CAST(NULL AS STRING) AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,CAST(pcx_ilmtransaction_jmic.EffDate AS DATE) AS EffectiveDate
			,CAST(pcx_ilmtransaction_jmic.ExpDate AS DATE) AS ExpirationDate
			,CAST(NULL AS STRING) AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,CAST(pcx_ilmtransaction_jmic.WrittenDate as DATE) AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,CASE 	WHEN pcx_ilmcost_jmic.NumDaysInRatedTerm = 0
					THEN 0
						ELSE ROUND(CAST(pcx_ilmcost_jmic.ActualTermAmount * (CAST(TIMESTAMP_DIFF(pc_policyperiod.PeriodStart, pc_policyperiod.PeriodEnd, DAY) AS FLOAT64) / CAST(pcx_ilmcost_jmic.NumDaysInRatedTerm AS FLOAT64)) AS DECIMAL),0)
				END AS AdjustedFullTermAmount
			--,pcx_ilmcost_jmic.ActualBaseRate 
			--,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			--,pc_policylocation.PublicID as RatingLocationPublicID
			--,COALESCE(pcx_ilmlocation_jmic.PublicID, tmp_min_policy_location.PublicID, pc_policylocation.PublicID) AS PrevRatingLocationPublicID
			,ratingLocation.PublicID AS RatingLocationPublicID
			,pcx_ilmcost_jmic.PublicID AS CostPublicID
			,ROW_NUMBER() OVER(PARTITION BY pcx_ilmtransaction_jmic.ID
				ORDER BY 
					IFNULL(pcx_ilmcost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
					,IFNULL(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
-- 					,IFNULL(pcx_ilmlocation_jmic_via_policy_location.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
			) AS TransactionRank
		FROM (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmtransaction_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmtransaction_jmic
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			--ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
			ON pc_policyperiod.ID=pcx_ilmtransaction_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID 
			AND -1=COALESCE(pcx_ilmcost_jmic.ILMLineCov
			,pcx_ilmcost_jmic.ILMSubLineCov
			,pcx_ilmcost_jmic.ILMLocationCov
			,pcx_ilmcost_jmic.ILMSubLocCov
			,pcx_ilmcost_jmic.JewelryStockCov
			,pcx_ilmcost_jmic.ILMSubStockCov
			,pcx_ilmcost_jmic.ILMOneTimeCredit
			,-1)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) perCost
			ON perCost.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = perCost.ID
			AND pc_policyline.FixedID = pcx_ilmcost_jmic.ILMLine
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policyline.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policyline.ExpirationDate,perCost.PeriodEnd) AS DATE)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'NoCoverage' 

/*			
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic   
			ON  pcx_ilmlocation_jmic.BranchID = perCost.ID
			AND pcx_ilmlocation_jmic.Location = IFNULL(pcx_ilmcost_jmic.ILMLocation_JMIC,pcx_ilmcost_jmic.ILMTaxLocation_JMIC)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON  pc_policylocation.BranchID = perCost.ID
			--AND pc_policylocation.FixedID = COALESCE(pcx_ilmlocation_jmic.Location,pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
*/

		--BEGIN - new code 3/3/2023
			--effFields
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
				ON pc_effectivedatedfields.BranchID = perCost.ID
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_effectivedatedfields.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_effectivedatedfields.ExpirationDate,perCost.PeriodEnd) AS DATE)
			--effectiveFieldsPrimaryPolicyLocation
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_policylocation 
				ON  pc_policylocation.BranchID = perCost.ID
				AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pc_policylocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pc_policylocation.ExpirationDate,perCost.PeriodEnd) AS DATE)
			--effectiveFieldsPrimaryILMLocation
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) AS pcx_ilmlocation_jmic
				ON  pcx_ilmlocation_jmic.BranchID = perCost.ID
				AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(pcx_ilmlocation_jmic.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<=CAST(COALESCE(pcx_ilmlocation_jmic.ExpirationDate,perCost.PeriodEnd) AS DATE)
		--END - new code 3/3/2023

		LEFT JOIN tmp_min_policy_location	--a BQ Temp Table
			ON tmp_min_policy_location.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(tmp_min_policy_location.EffectiveDate,perCost.PeriodStart) AS DATE)
			AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(tmp_min_policy_location.ExpirationDate,perCost.PeriodEnd) AS DATE)

		--BEGIN - new code 3/3/2023
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS costPolicyLocation 
				ON costPolicyLocation.ID = COALESCE(pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS covEffLoc
				ON covEffLoc.ID = COALESCE(costPolicyLocation.ID, CASE
						WHEN pcx_ilmlocation_jmic.ID IS NULL
							THEN tmp_min_policy_location.minIMPolicyLocationOnBranchID
						ELSE pc_policylocation.ID
						END)

			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS covEfflocState	
				ON covEfflocState.ID = covEffloc.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPolicyLocState 
				ON costPolicyLocState.ID = costPolicyLocation.StateInternal
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS taxLocState 
				ON taxLocState.ID = pcx_ilmcost_jmic.TaxJurisdiction
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS primaryLocState 
				ON primaryLocState.ID = CASE 
					WHEN pcx_ilmlocation_jmic.ID IS NULL
					THEN tmp_min_policy_location.StateInternal
					ELSE pc_policylocation.StateInternal
					END

			LEFT JOIN tmp_costPerEfflocation
				ON tmp_costPerEfflocation.PolicyLocationBranchID = pcx_ilmcost_jmic.BranchID
				AND tmp_costPerEfflocation.StateInternal = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)>=CAST(COALESCE(tmp_costPerEfflocation.EffectiveDate,perCost.PeriodStart) AS DATE)
				AND CAST(COALESCE(pcx_ilmtransaction_jmic.EffDate,perCost.EditEffectiveDate) AS DATE)<CAST(COALESCE(tmp_costPerEfflocation.ExpirationDate,perCost.PeriodEnd) AS DATE)

			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS costPerEfflocationState 
				ON costPerEfflocationState.ID = tmp_costPerEfflocation.StateInternal
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) AS ratingLocation
				ON ratingLocation.ID = CASE
					WHEN covEffLoc.ID IS NOT NULL
						AND covEfflocState.ID IS NOT NULL
						AND covEfflocState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
						THEN covEffLoc.ID
					WHEN tmp_costPerEfflocation.PublicID IS NOT NULL
						AND CostPerEfflocationState.ID = COALESCE(pcx_ilmcost_jmic.RatedState, costPolicyLocState.ID, taxLocState.ID, primaryLocState.ID)
						THEN tmp_costPerEfflocation.PolicyLocationID
						ELSE - 1
					END
		--END - new code 3/3/2023

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)

	) imFinancials
	INNER JOIN imDirectFinancials  sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN imDirectFinancials  hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN imDirectFinancials  locationRisk
		ON locationRisk.Key='LocationLevelRisk'
	INNER JOIN imDirectFinancials  stockRisk
		ON stockRisk.Key='StockLevelRisk'
	INNER JOIN imDirectFinancials  hashingAlgo
		ON hashingAlgo.Key='HashingAlgorithm'
	INNER JOIN imDirectFinancials  businessType
		ON businessType.Key='BusinessType'

	WHERE 1=1
		AND TransactionPublicID IS NOT NULL
		AND TransactionRank = 1
		
) extractData 
