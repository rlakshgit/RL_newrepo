-- tag: ClaimStatus - tag ends/
/*****************************************
	Kimberlite Extract Query
	    ClaimStatus.sql
			BQ Converted
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
-- Extracts Claim Status dimension data from the GW system 
-- ClaimStatus is completely rebuilt each run [in DWH]
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	10/26/2022	DROBAK		Initial build
	02/15/2023	DROBAK		Change to underlying table cc_history to remove table partitioning

-------------------------------------------------------------------------------------------------------------------
	-- Jinja Template --
	{project}.{dest_dataset} = ref_kimberlite

-------------------------------------------------------------------------------------------------------------------
	-- DWH References --
	sp_helptext bi_stage.spSTG_FactClaimStatus_Extract_GW
	bi_stage.spSTG_FactClaim_Extract_GW 
-------------------------------------------------------------------------------------------------------------------
*/
--Create Temp Table
	CREATE OR REPLACE TEMPORARY TABLE `_SESSION.Tmp_Claims`
	( 
		SourceSystem				STRING,
		UWCompanyPublicID			STRING,
		ClaimTransactionKey			BYTES, 
		ccHistoryID					INT64,
		ClaimPublicId				STRING,
		ClaimNumber					STRING,
		ClaimStatus					STRING,
		ClosedOutcomeDesc			STRING,
		EnteredDate					TIMESTAMP,
		--EntryType					STRING, 
		RowNumber					INT64, 
		IsTransactionSliceEffective INT64
	) AS

--Insert into Temp Table

	WITH ClaimStatusConfig AS 
	(
		SELECT 'SourceSystem'as Key,'GW' as Value UNION ALL
		SELECT 'HashKeySeparator','_' UNION ALL
		SELECT 'HashAlgorithm','SHA2_256'
	)
	SELECT
		sourceConfig.Value AS SourceSystem			--natural key
		,COALESCE(pc_uwcompany.PublicID, gw_policytype_company_map.uwCompanyPublicID) AS UWCompanyPublicID
		,CASE WHEN cc_claim.PublicId IS NOT NULL THEN
			SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,cc_claim.PublicId))
		END												AS ClaimTransactionKey
		,cc_history.ID									AS ccHistoryID
		,cc_claim.PublicID								AS ClaimPublicId
		,cc_claim.ClaimNumber 
		,cctl_historytype.Name							AS ClaimStatus
		--Only get Closed Outcome for closed history type codes
		,CASE WHEN cctl_historytype.ID = 11 THEN cctl_claimclosedoutcometype.name ELSE NULL END AS ClosedOutcomeDesc
		,cc_history.EventTimestamp	AS EnteredDate
		--,'C' AS EntryType 			
		,ROW_NUMBER() OVER ( PARTITION BY cc_claim.PublicID  
					ORDER BY cc_claim.PublicID ,cc_history.EventTimestamp 
		) AS RowNumber 		
		--excludes certain claims from being processed/used downstream
		,CASE WHEN COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') NOT IN ('FedNat', 'TWICO') 
				THEN 1 ELSE 0 END						AS IsTransactionSliceEffective
	FROM 
		(SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
		INNER JOIN `{project}.{cc_dataset}.cc_history` AS cc_history
			ON (cc_history.ClaimID  = cc_claim.ID )
		INNER JOIN `{project}.{cc_dataset}.cctl_historytype` AS cctl_historytype
			ON (cc_history.Type  = cctl_historytype.ID )	
		INNER JOIN ClaimStatusConfig AS sourceConfig ON sourceConfig.Key='SourceSystem'
		INNER JOIN ClaimStatusConfig AS hashKeySeparator ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN ClaimStatusConfig AS hashAlgorithm ON hashAlgorithm.Key = 'HashAlgorithm'

		LEFT JOIN `{project}.{cc_dataset}.cctl_claimclosedoutcometype` AS cctl_claimclosedoutcometype
			ON (cc_claim.ClosedOutcome  = cctl_claimclosedoutcometype.ID )
		LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy
			ON cc_policy.id = cc_claim.PolicyID
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			ON cc_policy.PC_PeriodPublicId_JMIC = pc_policyperiod.PublicID 
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
			ON pc_policyperiod.UWCompany = pc_uwcompany.ID
		--If the join to Policy Center's PolicyPeriod table above fails, use Claim Center's Policy Type
		--table to derive the company
		LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype
			ON cc_policy.PolicyType = cctl_policytype.ID
		--Mapping table to derive the company from policy type
		LEFT JOIN `{project}.{pe_cc_dataset}.gw_policytype_company_map` AS gw_policytype_company_map
			ON cctl_policytype.Name = gw_policytype_company_map.PolicyType
		LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype
			ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
						
	WHERE 1=1 
		AND	cctl_historytype.TYPECODE IN ('closed','opened','reopened')
		AND cc_history.ExposureID IS NULL
		AND cc_history.MatterID IS NULL  -- exclude Matters also (lawsuits)
	;

--Compare to get EndingDate values
--Insert into final Kimberlite (Building Block) Table

	--INSERT INTO `qa-edl.B_QA_ref_kimberlite.ClaimStatus`
	INSERT INTO `{project}.{dest_dataset}.ClaimStatus`
		(SourceSystem
		,UWCompanyPublicID
		,ClaimTransactionKey 
		,ccHistoryID
		,ClaimPublicID
		,ClaimNumber
		,ClaimStatus 
		,ClosedOutcomeDesc
		,EnteredDate 
		,EndingDate
		,IsTransactionSliceEffective
		,bq_load_date 
		)
	SELECT DISTINCT
		cc_claim.SourceSystem			--natural key
		,cc_claim.UWCompanyPublicID
		,cc_claim.ClaimTransactionKey
		,cc_claim.ccHistoryID			--natural key
		,cc_claim.ClaimPublicID
		,cc_claim.ClaimNumber
		,cc_claim.ClaimStatus
		,cc_claim.ClosedOutcomeDesc
		,cc_claim.EnteredDate
		,IFNULL(DATETIME_ADD(cc_history.EnteredDate, INTERVAL -1 SECOND), `{project}.custom_functions.fn_GetDefaultMaxTimeStamp`()) AS EndingDate
		,cc_claim.IsTransactionSliceEffective
		,CURRENT_DATE() AS bq_load_date	--i.e., CreatedDate
	FROM `_SESSION.Tmp_Claims` AS cc_claim 
	LEFT JOIN `_SESSION.Tmp_Claims` AS cc_history
		ON (cc_claim.ClaimPublicID = cc_history.ClaimPublicID)
		AND cc_claim.RowNumber = cc_history.RowNumber -1;
