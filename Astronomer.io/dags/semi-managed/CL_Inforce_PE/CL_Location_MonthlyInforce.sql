--#StandardSQL
--CREATE OR REPLACE TABLE
--  `semi-managed-reporting.core_insurance_cl.nonpromoted_CL_Location_MonthlyInforcePremium`
--  (AsOfDate DATE
--  ,PolicyNumber STRING
--  ,ProductLine STRING
--  ,LOBCode STRING
--  ,PolicyType STRING
--  ,Country STRING
--  ,InforcePolicyPeriodID INTEGER
--  ,ASL INTEGER
--  ,LocationNumber INTEGER
--  ,RolexIndicator STRING
--  ,LocationPremium NUMERIC)
--PARTITION BY
--  DATE_TRUNC(AsOfDate, MONTH) 
--AS
SELECT
	DATE(earned.AsOfDate) AS AsOfDate
	,trxn.PolicyNumber
	,'CL' AS ProductLine
	,CASE trxn.PolicyLineCode
		WHEN 'ILM' THEN REPLACE(eff.OfferingCode, 'P', '')
		ELSE trxn.PolicyLineCode
		END AS LOBCode
    ,eff.OfferingCode AS PolicyType
	,CASE WHEN trxn.CompanyCode = 'JMCN' THEN 'CAN' ELSE 'USA' END AS Country
	,earned.InforcePolicyPeriodID  
	,trxn.AnnualStmntLine AS ASL
	,CASE
		WHEN trxn.PolicyLineCode = 'BOP' THEN bop_loc.LocationNum
		WHEN trxn.PolicyLineCode = 'ILM' THEN ilm_loc.LocationNum
		WHEN trxn.PolicyLineCode = 'UMB' THEN umb_loc.LocationNum
		ELSE trxn.PrimaryPolicyLocationNumber
		END AS LocationNumber
	,COALESCE(rolex.RolexIndicator, 'NonRolex') AS RolexIndicator
	,SUM(earned.InforcePremium) AS LocationPremium

FROM        `{project}.{dataset_ref_pe_pc}.pcrt_earn_summ` AS earned
INNER JOIN 	`{project}.{dataset_ref_pe_pc}.pcrt_trxn_summ` AS trxn
        ON  earned.ExtTrxnID = trxn.ID
        AND trxn.PolicyLineCode NOT IN ('PJ', 'JPA')
		AND trxn.IsCeded = false
INNER JOIN  `{project}.{dataset_ref_pc_current}.pc_policyperiod` AS pperiod
        ON  earned.InforcePolicyPeriodID = pperiod.ID
        AND DATE(pperiod._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
INNER JOIN  `{project}.{dataset_ref_pc_current}.pc_job` AS job
        ON  pperiod.JobID = job.ID
        AND job.Subtype <> 2
        AND DATE(job._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- OfferingCode provides PolicyType
INNER JOIN 	`{project}.{dataset_ref_pc_current}.pc_effectivedatedfields` AS eff
		ON	trxn.EffectiveDatedFieldsPublicID = eff.PublicID
        AND DATE(eff._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
LEFT JOIN   `semi-managed-reporting.core_insurance_cl.wip_rolex_accounts` AS rolex
		ON  trxn.PolicyNumber = rolex.PolicyNumber
-- Location attributes by Product
FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pc_policylocation` AS bop_loc
		ON	earned.InforceBOPPolicyLocationPublicID = bop_loc.PublicID
        AND DATE(bop_loc._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pc_policylocation` AS ilm_loc
		ON	earned.InforceILMPolicyLocationPublicID = ilm_loc.PublicID
        AND DATE(ilm_loc._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
FULL OUTER JOIN	`{project}.{dataset_ref_pc_current}.pc_policylocation` AS umb_loc
		ON	earned.InforceUMBPolicyLocationPublicID = umb_loc.PublicID
        AND DATE(umb_loc._PARTITIONDATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

WHERE	DATE(earned.AsOfDate) > LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 6 YEAR), YEAR)

GROUP BY
	earned.AsOfDate
	,trxn.PolicyNumber
	,trxn.PolicyLineCode
    ,eff.OfferingCode
	,trxn.CompanyCode
	,earned.InforcePolicyPeriodID
	,trxn.AnnualStmntLine
	,bop_loc.LocationNum
    ,ilm_loc.LocationNum
	,umb_loc.LocationNum
	,ilm_loc.SegmentationCode_JMIC
    ,trxn.PrimaryPolicyLocationNumber
	,COALESCE(rolex.RolexIndicator, 'NonRolex')