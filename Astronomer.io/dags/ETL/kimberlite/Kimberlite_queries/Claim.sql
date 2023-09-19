-- tag: Claim - tag ends/
/*****************************************
  KIMBERLITE EXTRACT
    Claim.sql
		Converted to BigQuery
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	03/21/2023	DROBAK		Initial build - Split into 2: Kimberlite core and a Building Block

-------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_Claim`
AS SELECT overallselect.*
FROM (
		---with ClaimAttribute
		---etc code
--) overallselect
*/

INSERT INTO `{project}.{dest_dataset}.Claim`
(
	SourceSystem
	,ClaimKey
	,PolicyTransactionKey
	,PolicyNumber
	,ClaimPublicId
	,ClaimID
	,ClaimPolicyPublicId
	,PolicyPeriodPublicID
	,ClaimNumber
	,ClosedOutcomeCode
	,ClosedOutcomeDesc
	,CloseDate
	,LossDate
	,ReportedDate
	,CreatedDate
	,MadeDate
	,ClaimStatusName
	,ClaimSource
	,HowReportedChannel
	,LossTypeName
	,LossCauseName
	,LossCauseTypecode
	,ClaimPolicyTypecode
	,SecondaryCauseOfLossCode
	,SecondaryCauseOfLossDesc
	,ClaimDescription
	,LossLocationID
	,ClaimEvent
	,CatastropheType
	,LitigationStatus
	,IsClaimantRepresented
	,ClaimType
	,IsVerified
	,SurveyPreference
	,InvolesRolexWatches
	,IsVerifiedManually
	,CoverageInQuestion
	,SIUStatus
	,BaseSIUScore
	,ReferredtoSIUTeam
	,Flagged
	,AssignedUserID
	,TravelLossLocationID
	,UWCompanyCode
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,IsTransactionSliceEffective
	,bq_load_date
)

		WITH ClaimTransConfig AS (
		  SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
		  SELECT 'HashKeySeparator','_' UNION ALL
		  SELECT 'HashingAlgorithm', 'SHA2_256'
		)
		
		SELECT 
			SourceSystem
			,ClaimKey
			,PolicyTransactionKey
			,PolicyNumber
			,ClaimPublicId
			,ClaimID
			,ClaimPolicyPublicId
			,PolicyPeriodPublicID
			,ClaimNumber
			,ClosedOutcomeCode
			,ClosedOutcomeDesc
			,CloseDate
			,LossDate
			,ReportedDate
			,CreatedDate
			,MadeDate
			,ClaimStatusName
			,ClaimSource
			,HowReportedChannel
			,LossTypeName
			,LossCauseName
			,LossCauseTypecode
			,ClaimPolicyTypecode
			,SecondaryCauseOfLossCode
			,SecondaryCauseOfLossDesc
			,ClaimDescription
			,LossLocationID
			,ClaimEvent
			,CatastropheType
			,LitigationStatus
			,IsClaimantRepresented
			,ClaimType
			,IsVerified
			,SurveyPreference
			,InvolesRolexWatches
			,IsVerifiedManually
			,CoverageInQuestion
			,SIUStatus
			,BaseSIUScore
			,ReferredtoSIUTeam
			,Flagged
			,AssignedUserID
			,TravelLossLocationID
			,UWCompanyCode
			,IsClaimForLegacyPolicy
			,LegacyPolicyNumber
			,LegacyClaimNumber
			,IsTransactionSliceEffective
			,bq_load_date

		FROM (
				SELECT 
					ConfigSource.Value AS SourceSystem			--natural key
					--SK For PK [<Source>_<PolicyPeriodPublicID>]
					,CASE WHEN ClaimPublicId IS NOT NULL 
							THEN SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, ClaimPublicId)) 
						END AS ClaimKey
					--SK For FK [<Source>_<PolicyPeriodPublicID>]
					,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
							THEN SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value,PolicyPeriodPublicID)) 
						END AS PolicyTransactionKey
					,ClaimAttribute.*	
					,DATE('{date}') AS bq_load_date
					--,CURRENT_DATE() AS bq_load_date

				FROM (
						SELECT 
							cc_policy.PolicyNumber														AS PolicyNumber
							,cc_claim.PublicId															AS ClaimPublicId			--natural key
							,cc_claim.ID																AS ClaimID
							,pc_policyperiod.PublicID													AS PolicyPeriodPublicID
							,cc_policy.PublicId															AS ClaimPolicyPublicId
							,cc_claim.ClaimNumber														AS ClaimNumber
							,cc_claim.ClosedOutcome														AS ClosedOutcomeCode
							,cctl_claimclosedoutcometype.NAME											AS ClosedOutcomeDesc
							,cc_claim.CloseDate															AS CloseDate
							,cc_claim.LossDate															AS LossDate
							,cc_claim.ReportedDate														AS ReportedDate
							,cc_claim.CreateTime														AS CreatedDate
							,cc_claim.ClaimsMadeDate_JMIC												AS MadeDate
							,cctl_losscause.TYPECODE													AS LossCauseTypecode
							,cctl_losstype.TYPECODE														AS LossTypeTypecode
							,cctl_claimstate.NAME														AS ClaimStatusName
							,cctl_claimsource.Name														AS ClaimSource				--NEW 1/18/23
							,cctl_howreportedtype.DESCRIPTION											AS HowReportedChannel		--NEW 1/10/23
							,cctl_losstype.NAME															AS LossTypeName
							,cctl_losscause.NAME														AS LossCauseName
							,cctl_policytype.TYPECODE													AS ClaimPolicyTypecode
							,secondaryLossCause.TYPECODE												AS SecondaryCauseOfLossCode
							,secondaryLossCause.DESCRIPTION												AS SecondaryCauseOfLossDesc
							,cc_claim.Description														AS ClaimDescription
							,cc_claim.LossLocationID													AS LossLocationID
							,cc_catastrophe.Name														AS ClaimEvent
							,cctl_catastrophetype.DESCRIPTION											AS CatastropheType
							,cctl_litigationStatus.DESCRIPTION											AS LitigationStatus
							,IFNULL(CAST(cc_claim.IsRepresented_JMIC AS INT64),0)						AS IsClaimantRepresented
							,CASE WHEN CAST(cc_claim.IncidentReport AS INT64) = 1 THEN 'Incident' 
								  WHEN CAST(cc_claim.IncidentReport AS INT64) = 0 THEN 'Claim' 
								END																		AS ClaimType
							,COALESCE(CAST(cc_policy.Verified AS INT64),0)								AS IsVerified
							,cctl_survey_jmic.NAME														AS SurveyPreference
							,CAST(cc_claim.DoesInvolveRolexWatches_JM AS INT64)							AS InvolesRolexWatches
							,CAST(cc_policy.VerifiedByCC_JMIC AS INT64)									AS IsVerifiedManually
							,cc_claim.CoverageInQuestion												AS CoverageInQuestion
							,cctl_siustatus.Description													AS SIUStatus
							,cc_claim.SIScore															AS BaseSIUScore
							,cctl_yesno.Description														AS ReferredtoSIUTeam
							,cctl_flaggedtype.Description												AS Flagged
							,cc_claim.AssignedUserID													AS AssignedUserID
							,cc_claim.TravelLossLocationID_JM											AS TravelLossLocationID
							,cctl_underwritingcompanytype.TYPECODE										AS UWCompanyCode
				
			--****************Legacy Claims Fields******************************		

							,CAST(cc_claim.isClaimForLegacyPolicy_JMIC AS INT64)						AS IsClaimForLegacyPolicy
							,cc_claim.LegacyPolicyNumber_JMIC											AS LegacyPolicyNumber
							,cc_claim.LegacyClaimNumber_JMIC											AS LegacyClaimNumber

			--****************Legacy Claims Fields******************************		
			
							,CASE WHEN COALESCE(cctl_underwritingcompanytype.TYPECODE, '?') 
									NOT IN ('FedNat', 'TWICO') --excludes claims from being processed
								THEN 1 ELSE 0 END														AS IsTransactionSliceEffective

						FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim

						LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_policy` WHERE _PARTITIONTIME = {partition_date}) AS cc_policy
							ON cc_policy.ID = cc_claim.PolicyID
						LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
							ON pc_policyperiod.PublicID =  cc_policy.PC_PeriodPublicId_JMIC	
						LEFT JOIN `{project}.{cc_dataset}.cctl_claimclosedoutcometype` AS cctl_claimclosedoutcometype
							ON cc_claim.ClosedOutcome = cctl_claimclosedoutcometype.ID
						LEFT JOIN `{project}.{cc_dataset}.cctl_siustatus` AS cctl_siustatus
							ON cctl_siustatus.Id = cc_claim.SIUStatus
						LEFT JOIN `{project}.{cc_dataset}.cctl_yesno` AS cctl_yesno
							ON cctl_yesno.ID = cc_claim.SIEscalateSIU
						LEFT JOIN `{project}.{cc_dataset}.cctl_flaggedtype` AS cctl_flaggedtype
							ON cctl_flaggedtype.ID = cc_claim.Flagged
						LEFT JOIN `{project}.{cc_dataset}.cctl_losscause` AS cctl_losscause
							ON cctl_losscause.ID = cc_claim.LossCause
						LEFT JOIN `{project}.{cc_dataset}.cctl_losscause` AS secondaryLossCause 
							ON secondaryLossCause.ID = cc_claim.SecondaryLossCause_JMIC		
						LEFT JOIN `{project}.{cc_dataset}.cctl_losstype` AS cctl_losstype
							ON cctl_losstype.ID = cc_claim.LossType

						LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_catastrophe` WHERE _PARTITIONTIME = {partition_date}) AS cc_catastrophe
							ON cc_catastrophe.id =  cc_claim.CatastropheID
						LEFT JOIN `{project}.{cc_dataset}.cctl_catastrophetype` AS cctl_catastrophetype  
							ON cc_catastrophe.Type = cctl_catastrophetype.ID 
						LEFT JOIN  `{project}.{cc_dataset}.cctl_claimstate` AS cctl_claimstate
							ON cctl_claimstate.ID = cc_claim.State
						LEFT JOIN `{project}.{cc_dataset}.cctl_underwritingcompanytype` AS cctl_underwritingcompanytype
							ON cc_policy.UnderwritingCo = cctl_underwritingcompanytype.ID
						LEFT JOIN `{project}.{cc_dataset}.cctl_policytype` AS cctl_policytype
							ON cc_policy.PolicyType = cctl_policytype.ID
						LEFT JOIN `{project}.{cc_dataset}.cctl_survey_jmic` AS cctl_survey_jmic
							ON cc_claim.SurveyType_JMIC = cctl_survey_jmic.ID	
						LEFT JOIN `{project}.{cc_dataset}.cctl_litigationstatus` AS cctl_litigationstatus
							ON cctl_litigationStatus.ID = cc_claim.LitigationStatus
						LEFT JOIN `{project}.{cc_dataset}.cctl_howreportedtype` AS cctl_howreportedtype ON cctl_howreportedtype.ID = cc_claim.HowReported			
						LEFT JOIN `{project}.{cc_dataset}.cctl_claimsource`  AS cctl_claimsource ON cc_claim.claimsource = cctl_claimsource.ID

				) ClaimAttribute

				INNER JOIN ClaimTransConfig  AS ConfigSource
				ON ConfigSource.Key='SourceSystem'

				INNER JOIN ClaimTransConfig  AS ConfigHashSep
				ON ConfigHashSep.Key='HashKeySeparator'

				INNER JOIN ClaimTransConfig AS ConfigHashAlgo
				ON ConfigHashAlgo.Key='HashingAlgorithm'

				WHERE 1=1

		) extractData 

--) overallselect
