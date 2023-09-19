/*****************************************
  KIMBERLITE EXTRACT
    PolicyTransactionProduct
		BigQuery Converted
******************************************/
/*
----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

    07/28/2020  SLJ		    Initial create
	05/23/2022  DROBAK      Add logic to determine Product (remove redundant InsuranceProductCode, PolicyLine); 
							PolicyTransactionProductKey changed from InsuranceProductCode to PolicyLineCode
	05/31/2022	DROBAK		Add OfferingCode (to this table at Kimberlite layer)

----------------------------------------------------------------------------------------------------------------------------------
 */	

WITH ConfigPolicyTransaction AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256'
)


SELECT 
	ConfigSource.Value AS SourceSystem
	--SK For PK [<Source>_<PolicyPeriodPublicID>_<InsuranceProductCode>]
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, PolicyPeriodPublicID,ConfigHashSep.Value, PolicyLineCode)) AS PolicyTransactionProductKey
	--SK For FK [<Source>_<PolicyPeriodPublicID>]
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, PolicyPeriodPublicID)) AS PolicyTransactionKey
	,PolicyProduct.PolicyPeriodPublicId
	,PolicyProduct.PolicyLineCode
	,PolicyProduct.OfferingCode
    ,DATE('{date}') as bq_load_date			

FROM (
	SELECT 
		pc_policyperiod.PublicID									AS PolicyPeriodPublicId
		--,pc_policyline.PatternCode								AS InsuranceProductCode
		,pctl_policyline.TypeCode									AS PolicyLineCode
		--,pctl_policyline.NAME										AS PolicyLine
		,pc_effectivedatedfields.OfferingCode						AS OfferingCode
		
	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod

		INNER JOIN `{project}.{pc_dataset}.pc_job` AS pc_job
		  ON pc_job.ID = pc_policyperiod.JobID
		INNER JOIN `{project}.{pc_dataset}.pc_policyline` AS pc_policyline
		  ON pc_policyline.BranchID = pc_policyperiod.ID
		  AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		  AND pc_policyperiod.EditEffectiveDate <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		  ON pctl_policyline.ID = pc_policyline.SubType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) AS pc_effectivedatedfields
			ON  pc_effectivedatedfields.BranchID = pc_policyperiod.ID
			AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)

	WHERE	1 = 1
	AND pc_policyperiod._PARTITIONTIME = {partition_date}
	AND pc_job._PARTITIONTIME = {partition_date}
	AND pc_policyline._PARTITIONTIME = {partition_date}
	--AND PolicyNumber IN ('55-003822', 'P000000083', '24-053914')
  
)PolicyProduct

	INNER JOIN ConfigPolicyTransaction AS ConfigSource
	ON ConfigSource.Key='SourceSystem'

	INNER JOIN ConfigPolicyTransaction AS ConfigHashSep
	ON ConfigHashSep.Key='HashKeySeparator'

	INNER JOIN ConfigPolicyTransaction AS ConfigHashAlgo
	ON ConfigHashAlgo.Key='HashingAlgorithm'
