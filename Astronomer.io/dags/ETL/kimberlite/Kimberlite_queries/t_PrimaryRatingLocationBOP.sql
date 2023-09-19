-- tag: t_PrimaryRatingLocationBOP - tag ends/
/**** Kimberlite - Financial Transactions ********
		t_PrimaryRatingLocationBOP.sql 
			CONVERTED TO BIG QUERY
**************************************************

	NOTE: 
		Runs BEFORE Kimberlite Core DAG tasks execute. Loads to Core data set.
	PURPOSE:
		This table is used as a reference table for several extract queries (there is an IM equivalent version).
		It is to be loaded in the beginning of the DAG process run, before Kimberlite Core tables.
		Each extract query contains fail-safe code to verify table's existence for that day, and if not exist,
		will rebuild it.
	RELATED QUERIES:
		RiskLocationBusinessOwners, CoverageBOP, CoverageUMB, ClaimFinancialTransactionLineBOPCeded, ClaimFinancialTransactionLineBOPDirect
		,FinancialTransactionBOPDirect, FinancialTransactionBOPCeded

----------------------------------------------------------------------------------------------
 *****	Change Log	*****

	04/26/2023	DROBAK		Init

----------------------------------------------------------------------------------------------
*/

--CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite_core.t_PrimaryRatingLocationBOP`
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP`
AS SELECT LOBPrimary.* FROM
(
/*
INSERT INTO `{project}.{dest_dataset}.t_PrimaryRatingLocationBOP`
(
    PolicyPeriodID
    ,EditEffectiveDate
    ,PolicyLineOfBusiness
    --,IsPrimaryLocation
    ,RatingLocationNum
);
*/
SELECT
    pc_policyperiod.ID AS PolicyPeriodID
    ,pc_policyperiod.EditEffectiveDate AS EditEffectiveDate
    ,pctl_policyline.TYPECODE AS PolicyLineOfBusiness
    ,MAX(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN 'Y' ELSE 'N' END) AS IsPrimaryLocation
    --1. If the Primary loc matches the LOB loc, use it
    --2. Else use Coverage Effective Location (Same as RatingLocation and may be different than the min or primary)
    --3. Otherwise use the MIN location num's corresponding LOB Location
    ,COALESCE(MIN(CASE WHEN pc_boplocation.Location = PrimaryPolicyLocation.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
        ,MIN(CASE WHEN pc_boplocation.BOPLine = pc_policyline.FixedID THEN pc_policylocation.LocationNum ELSE NULL END)
        ,MIN(pc_policylocation.LocationNum)) AS RatingLocationNum

FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod
	--Blow out to include all policy locations for policy version / date segment
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) pc_policylocation
		ON pc_policyperiod.ID = pc_policylocation.BranchID
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policylocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policylocation.ExpirationDate,pc_policyperiod.PeriodEnd)
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
		ON pc_policyperiod.ID = pc_policyline.BranchID
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pctl_policyline`) pctl_policyline
		ON pc_policyline.SubType = pctl_policyline.ID			
		AND pctl_policyline.TYPECODE = 'BusinessOwnersLine'

	--BOP Location  
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_boplocation` WHERE _PARTITIONTIME = {partition_date}) AS pc_boplocation
		ON pc_boplocation.Location = pc_policylocation.FixedID
		AND pc_boplocation.BranchID = pc_policylocation.BranchID
		AND pc_boplocation.BOPLine = pc_policyline.FixedID
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_boplocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_boplocation.ExpirationDate,pc_policyperiod.PeriodEnd)
	--PolicyLine uses PrimaryLocation (captured in EffectiveDatedFields table) for "Revisioned" address; use to get state/jurisdiction  
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_effectivedatedfields` WHERE _PARTITIONTIME = {partition_date}) pc_effectivedatedfields
		ON pc_effectivedatedfields.BranchID = pc_policyperiod.ID  
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(pc_effectivedatedfields.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(pc_effectivedatedfields.ExpirationDate,pc_policyperiod.PeriodEnd)
	LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policylocation` WHERE _PARTITIONTIME = {partition_date}) PrimaryPolicyLocation
		ON PrimaryPolicyLocation.FixedID = pc_effectivedatedfields.PrimaryLocation 
		AND PrimaryPolicyLocation.BranchID = pc_effectivedatedfields.BranchID 
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) >= COALESCE(PrimaryPolicyLocation.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pc_policyperiod.EditEffectiveDate,pc_policyperiod.PeriodStart) < COALESCE(PrimaryPolicyLocation.ExpirationDate,pc_policyperiod.PeriodEnd)
 
GROUP BY
	pc_policyperiod.ID
	,pc_policyperiod.EditEffectiveDate
	,pctl_policyline.TYPECODE

) LOBPrimary;