/*TEST*/
--DECLARE policynumber STRING;
--SET policyNumber = '55-001338'
--SET policyNumber = '55-014561' --provided by shannon. edge

/**************************************************************************************************************************************************************************
IM Financials - This query will bring in all Instances of all types of financial transactions provided under the ILM product. 
**************************************************************************************************************************************************************************
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/05/2021	DROBAK		Changed ILMCoverageKey to IMCoverageKey
	04/05/2021	DROBAK		Changed ILMLocationPublicID to IMLocationPublicID

--------------------------------------------------------------------------------------------
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

WITH imDirectFinancials AS (
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
	,JobNumber
	,PolicyNumber
	,CoverageLevel
	,EffectiveDate
	,ExpirationDate
	,TransactionAmount
	,TransactionPostedDate
	,TransactionWrittenDate
	,NumDaysInRatedTerm
	,ActualBaseRate 
	,ActualAdjRate 
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
    ,DATE('{date}') as bq_load_date	
	
--INTO #imFinancials
FROM (
	SELECT 
		sourceConfig.Value AS SourceSystem
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,TransactionPublicID,hashKeySeparator.Value,businessType.Value, LineCode)) AS FinancialTransactionKey
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID)) AS PolicyTransactionKey
		,CASE WHEN 
			CoveragePublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
			END AS IMCoverageKey
		,CASE WHEN 
			IMLocationPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,IMLocationPublicID,hashKeySeparator.Value,locationRisk.Value)) 
			END AS RiskLocationKey
		,CASE WHEN 
			ILMStockPublicID IS NOT NULL THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ILMStockPublicID,hashKeySeparator.Value,stockRisk.Value)) 
			END AS RiskStockKey
		,businessType.Value as BusinessType
		,imFinancials.*		
	FROM (
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmlinecov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,'None' AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'LineLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlinecov_jmic   
			ON  pcx_ilmlinecov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlinecov_jmic.FixedID = pcx_ilmcost_jmic.ILMLineCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlinecov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlinecov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields  
			ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN  (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON  pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState 
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
		
		UNION ALL
	
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubLineCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,'None' AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubLineCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE	
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubLineLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsublinecov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubLineCov_jmic  
			ON  pcx_ilmSubLineCov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmSubLineCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLineCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmSubLineCov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmSubLineCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields  
			ON   pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation  
			ON  pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = pc_effectivedatedfields.PrimaryLocation
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic   
			ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.Location = pc_policylocation.FixedID --potential blowout contained via ranks
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern`  pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 

		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmLocationCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,'None' AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic 
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMLocationCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON  coverageLevelConfig.Key = 'LocationLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocationcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmLocationCov_jmic 
			ON  pcx_ilmLocationCov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmLocationCov_jmic.FixedID = pcx_ilmcost_jmic.ILMLocationCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmLocationCov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmLocationCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic 
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_ilmLocationCov_jmic.ILMLocation
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
				
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 
	
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubLocCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,'None' AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubLocCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubLocLevelCoverage' 
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloccov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubLocCov_jmic  
			ON  pcx_ilmSubLocCov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmSubLocCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubLocCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmSubLocCov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmSubLocCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubloc_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmsubloc_jmic   
			ON pcx_ilmsubloc_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmsubloc_jmic.FixedID=pcx_ilmSubLocCov_jmic.ILMSubLoc
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmsubloc_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmsubloc_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_ilmsubloc_jmic.ILMLocation
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation  
			ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
				
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)

		UNION ALL 
	
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_jewelrystockcov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.JewelryStockCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'StockLevelCoverage' 
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
			
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystockcov_jmic
			ON  pcx_jewelrystockcov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_jewelrystockcov_jmic.FixedID = pcx_ilmcost_jmic.JewelryStockCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_jewelrystockcov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_jewelrystockcov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_jewelrystockcov_jmic.JewelryStock
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState

		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
		UNION ALL 
	
		SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmSubStockCov_jmic.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic 
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMSubStockCov IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'SubStockLevelCoverage' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstockcov_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmSubStockCov_jmic 
			ON  pcx_ilmSubStockCov_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmSubStockCov_jmic.FixedID = pcx_ilmcost_jmic.ILMSubStockCov
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmSubStockCov_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmSubStockCov_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmsubstock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmsubstock_jmic 
			ON pcx_ilmsubstock_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmsubstock_jmic.FixedID=pcx_ilmSubStockCov_jmic.ILMSubStock
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmsubstock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmsubstock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic  
			ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_ilmsubstock_jmic.JewelryStock
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic  
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
			ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)
	
	UNION ALL

	SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,pcx_ilmonetimecredit_jmic_v2.PublicID AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
			,pcx_ilmlocation_jmic.PublicID AS IMLocationPublicID
			,pcx_jewelrystock_jmic.PublicID AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmcost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmcost_jmic
			ON pcx_ilmtransaction_jmic.ILMCost_JMIC = pcx_ilmcost_jmic.ID
			AND pcx_ilmcost_jmic.ILMOneTimeCredit IS NOT NULL
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'OnetimeCredit' 
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmonetimecredit_jmic_v2` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmonetimecredit_jmic_v2
			ON  pcx_ilmonetimecredit_jmic_v2.BranchID = pc_policyperiod.ID
				AND pcx_ilmonetimecredit_jmic_v2.FixedID = pcx_ilmcost_jmic.ILMOneTimeCredit
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmonetimecredit_jmic_v2.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmonetimecredit_jmic_v2.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelrystock_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelrystock_jmic
			ON pcx_jewelrystock_jmic.BranchID = pc_policyperiod.ID
				AND pcx_jewelrystock_jmic.FixedID=pcx_ilmonetimecredit_jmic_v2.JewelryStock_JMIC
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_jewelrystock_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_jewelrystock_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic
			ON pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
				AND pcx_ilmlocation_jmic.FixedID=pcx_jewelrystock_jmic.ILMLocation
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON pc_policylocation.BranchID = pc_policyperiod.ID
				AND pc_policylocation.FixedID=pcx_ilmlocation_jmic.Location
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
			ON ratedState.ID = pcx_ilmcost_jmic.RatedState
		
		WHERE 1=1
		--AND PolicyNumber = IFNULL(@policynumber, PolicyNumber)

	UNION ALL

	SELECT 
			pcx_ilmtransaction_jmic.PublicID as TransactionPublicID
			,pc_policyperiod.PublicID as PolicyPeriodPublicID
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
            ,pctl_policyline.TYPECODE AS LineCode			
			,'None' AS CoveragePublicID
			,coverageLevelConfig.Value AS CoverageLevel
			,pcx_ilmtransaction_jmic.EffDate AS EffectiveDate
			,pcx_ilmtransaction_jmic.ExpDate AS ExpirationDate
-- 			,IFNULL(pcx_ilmlocation_jmic.PublicID,pcx_ilmlocation_jmic_via_policy_location.PublicID) AS IMLocationPublicID
			,'None' AS IMLocationPublicID
			,'None' AS ILMStockPublicID
			,pcx_ilmtransaction_jmic.Amount AS TransactionAmount
			,pcx_ilmtransaction_jmic.AmountBilling AS BillingAmount
			,pcx_ilmtransaction_jmic.ToBeAccrued AS CanBeEarned
			,pcx_ilmtransaction_jmic.PostedDate AS TransactionPostedDate
			,pcx_ilmtransaction_jmic.WrittenDate AS TransactionWrittenDate
			,pcx_ilmcost_jmic.NumDaysInRatedTerm
			,pcx_ilmcost_jmic.ActualBaseRate 
			,pcx_ilmcost_jmic.ActualAdjRate 
			,pcx_ilmcost_jmic.ActualTermAmount 
			,pcx_ilmcost_jmic.ActualAmount 
			,pctl_chargepattern.TYPECODE AS ChargePattern 
			,pcx_ilmcost_jmic.ChargeGroup 
			,pcx_ilmcost_jmic.ChargeSubGroup 
			,pctl_rateamounttype.TYPECODE AS RateType
			,ratedState.TYPECODE AS RatedState
			,pc_policylocation.PublicID as RatingLocationPublicID
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
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod 
			ON pc_policyperiod.ID=pcx_ilmcost_jmic.BranchID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline  
			ON pc_policyline.BranchID = pc_policyperiod.ID
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
				AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline 
			ON pctl_policyline.ID = pc_policyline.Subtype
		INNER JOIN imDirectFinancials  lineConfig 
			ON lineConfig.Key = 'LineCode' AND lineConfig.Value=pctl_policyline.TYPECODE
		INNER JOIN imDirectFinancials  coverageLevelConfig
			ON coverageLevelConfig.Key = 'NoCoverage' 
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic   
			ON  pcx_ilmlocation_jmic.BranchID = pc_policyperiod.ID
			AND pcx_ilmlocation_jmic.Location = IFNULL(pcx_ilmcost_jmic.ILMLocation_JMIC,pcx_ilmcost_jmic.ILMTaxLocation_JMIC)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation 
			ON  pc_policylocation.BranchID = pc_policyperiod.ID
			AND pc_policylocation.FixedID = COALESCE(pcx_ilmlocation_jmic.Location,pcx_ilmcost_jmic.ILMMinPremPolicyLocation, pcx_ilmcost_jmic.ILMTaxPolicyLocation)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
 	--	LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_ilmlocation_jmic_via_policy_location` WHERE _PARTITIONTIME = {partition_date}) pcx_ilmlocation_jmic_via_policy_location   
 	--		ON  pcx_ilmlocation_jmic_via_policy_location.BranchID = pc_policyperiod.ID
 	--		AND pcx_ilmlocation_jmic_via_policy_location.Location = pc_policylocation.FixedID --potential blowout contained via ranks
 	--		AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)>=COALESCE(pcx_ilmlocation_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
 	--		AND COALESCE(pcx_ilmcost_jmic.EffectiveDate,pc_policyperiod.EditEffectiveDate)<=COALESCE(pcx_ilmlocation_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_chargepattern` pctl_chargepattern 
			ON pctl_chargepattern.ID = pcx_ilmcost_jmic.ChargePattern
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_rateamounttype` pctl_rateamounttype  
			ON pctl_rateamounttype.ID = pcx_ilmcost_jmic.RateAmountType
		LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` ratedState
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
