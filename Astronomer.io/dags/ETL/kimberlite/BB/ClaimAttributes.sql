-- tag: ClaimAttributes - tag ends/
/*****************************************
  KIMBERLITE EXTRACT
    ClaimAttributes.sql
		Converted to BigQuery
******************************************/
/*
-------------------------------------------------------------------------------------------------------------------
	-- Change History --

	03/21/2023	DROBAK		Initial build
	05/05/2023	DROBAK		Updates to Insert Stmt

-------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ClaimAttributes`
AS SELECT overallselect.*
FROM (
		---with ClaimAttribute
		---etc code
--) overallselect
*/
CREATE OR REPLACE TEMP TABLE ClaimTransContactsPivot AS 
(
	SELECT ID, TYPECODE, ClaimantContactPublicID
	FROM
		(	SELECT cc_claim.ID, ClaimantContactPublicID, ClaimContactRoles.TYPECODE
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim 
			LEFT JOIN 
				(	SELECT cc_claimcontact.ClaimID, cc_contact.PublicID AS ClaimantContactPublicID, cctl_contactrole.TYPECODE
					FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claimcontact` WHERE _PARTITIONTIME = {partition_date}) AS cc_claimcontact
						INNER JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_claimcontactrole` WHERE _PARTITIONTIME = {partition_date}) AS cc_claimcontactrole
							ON cc_claimcontactrole.ClaimContactID = cc_claimcontact.ID
						INNER JOIN `{project}.{cc_dataset}.cctl_contactrole` AS cctl_contactrole
							ON cc_claimcontactrole.Role = cctl_contactrole.ID
							AND cctl_contactrole.TYPECODE IN ( 'reporter', 'supervisor', 'LawEnfcAgcy', 'claimant')
						LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact
							ON cc_claimcontact.ContactID = cc_contact.ID
					GROUP BY cc_claimcontact.ClaimID, cc_contact.PublicID, cctl_contactrole.TYPECODE
				) AS ClaimContactRoles
			ON ClaimContactRoles.ClaimID = cc_claim.ID
			--WHERE Claim.ID in (74538)
			--Seed values to get all columns needed for select into Kimberlite
			UNION ALL
			SELECT 0 AS ID, NULL AS ClaimantContactPublicID, 'reporter' AS TYPECODE UNION ALL
			SELECT 0 AS ID, NULL AS ClaimantContactPublicID, 'supervisor' AS TYPECODE UNION ALL
			SELECT 0 AS ID, NULL AS ClaimantContactPublicID, 'LawEnfcAgcy' AS TYPECODE UNION ALL
			SELECT 0 AS ID, NULL AS ClaimantContactPublicID, 'claimant' AS TYPECODE
		) AS claimroles
);
CALL `{project}.custom_functions.sp_pivot`(
  'ClaimTransContactsPivot' # source table
  ,'{project}.{core_dataset}.t_ClaimTransContacts_Pivot' # destination table
  , ['ID'] # row_ids
  , 'TYPECODE'-- # pivot_col_name
  , 'ClaimantContactPublicID'--# pivot_col_value
  , 12	--# max_columns
  , 'MAX' --# aggregation
  , '' --# optional_limit
);
DROP table ClaimTransContactsPivot;


INSERT INTO `{project}.{dest_dataset}.ClaimAttributes` (
	SourceSystem
	,ClaimKey
	,PolicyTransactionKey
	,PolicyNumber
	,ClaimPublicId
	,ClaimID
	,PolicyPeriodPublicID
	,ClaimPolicyPublicId
	,ClaimNumber
	,ClosedOutcomeCode
	,ClosedOutcomeDesc
	,CloseDate
	,LossDate
	,ReportedDate
	,CreatedDate
	,MadeDate
	,TrialDate
	,LastOpenDate
	,LastCloseDate
	,CauseOfLoss
	,CauseOfLossGroup
	,ClaimStatusName
	,ClaimSource			--NEW 1/18/23
	,HowReportedChannel		--NEW 1/10/23
	,LossTypeName
	,LossCauseName
	,LossCauseTypecode
	,ClaimPolicyTypecode
	,SecondaryCauseOfLossCode
	,SecondaryCauseOfLossDesc
	,ClaimDescription
	,LossCounty
	,LossCity
	,LossPostalCode
	,LossStateCode
	,LossCountryCode
	,ExaminerFullName
	,ExaminerLoginID
    ,ExaminerPublicID
	,ReportedByFullName
    ,ReportedByLoginID
	,ReportedByPublicID
	,ClaimEvent
	,CatastropheType				--NEW 1/18/23
	,ClaimLawSuit
	,ClaimCourtType
	,LitigationStatus
	,IsClaimantRepresented
	,IsAverageReserveExpense		--New 1/18/23
	,IsAverageReserveIndemnity		--New 1/18/23
	,ClaimType
	,IsVerified
	,SurveyPreference
	,ReportToJSAandJVC
	,InvolesRolexWatches
	,IsVerifiedManually
	,CoverageInQuestion
	,SIUStatus
	,SIUScore
	,ReferredtoSIUTeam
	,Flagged
	,FirstLitigationStartDate
	,TravelLossLocationCity
	,TravelLossLocationStateCode
	,TravelLossLocationCountryCode
	,ClaimantContactPublicID
	--,SupervisorContactPublicID		--Never populated thus far in GW
	,LawEnfcAgcyContactPublicID
	,IsClaimForLegacyPolicy
	,LegacyPolicyNumber
	,LegacyClaimNumber
	,bq_load_date
)

		    WITH CTE_SIUquestionScore AS (
			SELECT 
				cc_claim.ClaimNumber
				,cc_claim.ID
				,SUM(cc_questionchoice.Score) SIUquestionScore 
			FROM (SELECT * FROM `{project}.{cc_dataset}.cc_claim` WHERE _PARTITIONTIME = {partition_date}) AS cc_claim
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_siuanswerset` WHERE _PARTITIONTIME = {partition_date}) AS cc_siuanswerset 
					ON cc_siuanswerset.ClaimID = cc_claim.ID
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_answerset` WHERE _PARTITIONTIME = {partition_date}) AS cc_answerset 
					ON cc_answerset.ID = cc_siuanswerset.AnswerSetID
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_answer` WHERE _PARTITIONTIME = {partition_date}) AS cc_answer 
					ON cc_answer.AnswerSetID = cc_answerset.ID
				LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_questionchoice` WHERE _PARTITIONTIME = {partition_date}) AS cc_questionchoice 
					ON cc_questionchoice.ID = cc_answer.ChoiceAnswerID
			WHERE cc_questionchoice.Score IS NOT NULL   --added to reduce query cost
			GROUP BY cc_claim.ClaimNumber, cc_claim.ID
			)
			,ClaimTransConfig AS (
				SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
				SELECT 'HashKeySeparator','_' UNION ALL
				SELECT 'HashingAlgorithm', 'SHA2_256'
			)

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
							PolicyNumber
							,ClaimPublicId			--natural key
							,Claim.ClaimID
							,PolicyPeriodPublicID
							,ClaimPolicyPublicId
							,Claim.ClaimNumber
							,ClosedOutcomeCode
							,ClosedOutcomeDesc
							,CloseDate
							,LossDate
							,ReportedDate
							,CreatedDate
							,MadeDate
							,MaxClaimMatters.TrialDate													AS TrialDate
							,lastOpenedDate.maxLastOpenDate												AS LastOpenDate
							,lastClosedDate.maxLastClosedDate											AS LastCloseDate
							,COALESCE(CauseOfLossDesc.ConformedCauseOfLossDesc, LossCauseTypecode)		AS CauseOfLoss
							,COALESCE(CauseOfLossDesc.LossGroup, LossTypeName)							AS CauseOfLossGroup
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
							,Address.County																	AS LossCounty
							,Address.City																	AS LossCity
							,Address.PostalCode																AS LossPostalCode
							,Address.StateCode																AS LossStateCode
							,Address.CountryCode															AS LossCountryCode
							,CASE WHEN ContactExaminer.ContactID IS NOT NULL
								THEN COALESCE(ContactExaminer.FullName,CONCAT(COALESCE
									(CONCAT(ContactExaminer.FirstName,' '),'') 
									,COALESCE(ContactExaminer.LastName,'')))
								ELSE '' END																	AS ExaminerFullName
							,ContactExaminer.UserName														AS ExaminerLoginID
							,ContactExaminer.ContactPublicID												AS ExaminerPublicID
								-- Name for commercial, first and last for personal
							,CASE WHEN t_ClaimTransContacts_Pivot.Reporter IS NOT NULL
								THEN COALESCE(ContactReporter.FullName,CONCAT(COALESCE
									(CONCAT(ContactReporter.FirstName,' '),'') 
									,COALESCE(ContactReporter.LastName,'')))
								ELSE '' END																	AS ReportedByFullName
							,ContactReporter.UserName														AS ReportedByLoginID
							,ContactReporter.ContactPublicID												AS ReportedByPublicID
							,ClaimEvent
							,CatastropheType
							--Just need true/false indicator
							,CASE WHEN MaxClaimMatters.MatterName IS NOT NULL 
								THEN 1 ELSE 0 END														AS ClaimLawSuit
							,MaxClaimMatters.CourtName													AS ClaimCourtType
							,LitigationStatus
							,IsClaimantRepresented
							,cAvgReserveExpense.IsAverageReserveExpense					                AS IsAverageReserveExpense
							,cAvgReserveIndemnity.IsAverageReserveIndemnity								AS IsAverageReserveIndemnity  
							,ClaimType
							,IsVerified
							,SurveyPreference
							,CauseOfLossDesc.ReportToJSAandJVC											AS ReportToJSAandJVC
							,InvolesRolexWatches
							,IsVerifiedManually
							,CoverageInQuestion
							,SIUStatus
							,COALESCE(CTE_SIUquestionScore.SIUquestionScore,0) + 
									COALESCE(BaseSIUScore,0)											AS SIUScore
							,ReferredtoSIUTeam
							,Flagged
							,Litigations.LitigationStartDate                                            AS FirstLitigationStartDate
							,TravelLossaddr.City														AS TravelLossLocationCity
							,TravelLossaddr.StateCode													AS TravelLossLocationStateCode
							,TravelLossaddr.CountryCode													AS TravelLossLocationCountryCode
							,t_ClaimTransContacts_Pivot.claimant										AS ClaimantContactPublicID
							--,t_ClaimTransContacts_Pivot.Supervisor									AS SupervisorContactPublicID
							,t_ClaimTransContacts_Pivot.LawEnfcAgcy										AS LawEnfcAgcyContactPublicID
				
					--****************Legacy Claims Fields********************		

							,IsClaimForLegacyPolicy
							,LegacyPolicyNumber
							,LegacyClaimNumber

					--****************Legacy Claims Fields********************		
			

					FROM (SELECT * FROM `{project}.{core_dataset}.Claim` WHERE CAST(bq_load_date AS TIMESTAMP) = {partition_date}) AS Claim
						LEFT JOIN `{project}.{core_dataset}.t_ClaimTransContacts_Pivot` AS t_ClaimTransContacts_Pivot
							ON Claim.ClaimID = t_ClaimTransContacts_Pivot.ID
						LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE CAST(bq_load_date AS TIMESTAMP) = {partition_date}) AS Address
							ON AddressID = Claim.LossLocationID
							AND Address.SourceCenter = 'CC'

						LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_user` WHERE _PARTITIONTIME = {partition_date}) AS assignedToUser 
							ON assignedToUser.ID = Claim.AssignedUserID
						LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Contact` WHERE CAST(bq_load_date AS TIMESTAMP) = {partition_date}) AS ContactExaminer
							ON ContactExaminer.ContactID = assignedToUser.ContactID
							AND ContactExaminer.SourceCenter = 'CC'

						LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Contact` WHERE CAST(bq_load_date AS TIMESTAMP) = {partition_date}) AS ContactReporter
							ON ContactReporter.ContactPublicID = t_ClaimTransContacts_Pivot.Reporter
							AND ContactReporter.SourceCenter = 'CC'

				        LEFT JOIN (
									SELECT cc_matter.NAME AS MatterName
											, cc_matter.TrialDate
											, cctl_mattercourttype.NAME AS CourtName, cc_matter.ClaimID
											, ROW_NUMBER() OVER(PARTITION BY cc_matter.ClaimID
													ORDER BY cc_matter.UpdateTime DESC
											) AS TransactionRank
									FROM (SELECT * FROM `{project}.{cc_dataset}.cc_matter` WHERE _PARTITIONTIME = {partition_date}) AS cc_matter
										LEFT JOIN `{project}.{cc_dataset}.cctl_mattercourttype` AS cctl_mattercourttype
											ON cctl_mattercourttype.ID = cc_matter.CourtType
							) MaxClaimMatters
								ON MaxClaimMatters.ClaimID = Claim.ClaimID AND TransactionRank=1

				        LEFT JOIN 
							(   SELECT   MAX(cc_history.EventTimestamp) AS maxLastOpenDate
										, cc_history.ClaimID
								FROM `{project}.{cc_dataset}.cc_history` AS cc_history
									INNER JOIN `{project}.{cc_dataset}.cctl_historytype` AS cctl_historytype
										ON cc_history.Type = cctl_historytype.ID
											AND cctl_historytype.TYPECODE IN ('opened', 'reopened')                   
											AND cc_history.ExposureID IS NULL
											AND cc_history.MatterID IS NULL
								GROUP BY cc_history.ClaimID
							) AS lastOpenedDate      
								ON lastOpenedDate.ClaimID = Claim.ClaimID

						LEFT JOIN 
							(   SELECT  MAX(cc_history.EventTimestamp) AS maxLastClosedDate
										,cc_history.ClaimID
								FROM `{project}.{cc_dataset}.cc_history` AS cc_history
									INNER JOIN `{project}.{cc_dataset}.cctl_historytype` AS cctl_historytype
										ON cc_history.Type = cctl_historytype.ID
											AND cctl_historytype.TYPECODE = 'closed'
											AND cc_history.ExposureID IS NULL
											AND cc_history.MatterID IS NULL
								GROUP BY cc_history.ClaimID
							) AS lastClosedDate
								ON lastClosedDate.ClaimID = Claim.ClaimID

						LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.CauseOfLossDesc`) AS CauseOfLossDesc
							ON CauseOfLossDesc.ProductLine = CASE WHEN Claim.ClaimPolicyTypecode in ('JMIC_PL', 'JPAPersonalArticles') THEN 'PL' ELSE 'CL' END
							AND CauseOfLossDesc.GWLossCauseTypeCode = Claim.LossCauseTypecode 
							AND CauseOfLossDesc.IsActive = 1
							AND GWLossCauseTypecode IS NOT NULL

						LEFT JOIN 
							( SELECT cc_reserveline.ClaimID, COALESCE(CAST(cc_reserveline.IsAverageReserveSource_jmic AS INT64), 1) AS IsAverageReserveExpense
									,ROW_NUMBER() OVER(PARTITION BY cc_reserveline.ClaimID ORDER BY cc_reserveline.UpdateTime DESC ) AS ReserveTransactionRank                 
							  FROM (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline
								INNER JOIN `{project}.{cc_dataset}.cctl_costtype` AS cctl_costtype 
								  ON cctl_costtype.ID = cc_reserveline.CostType
							  WHERE cctl_costtype.TYPECODE = 'claimcost'
							) AS cAvgReserveExpense
								ON cAvgReserveExpense.ClaimID = Claim.ClaimID AND ReserveTransactionRank=1 

				        LEFT JOIN 
							( SELECT cc_reserveline.ClaimID, COALESCE(CAST(cc_reserveline.IsAverageReserveSource_jmic AS INT64), 1) IsAverageReserveIndemnity
									, ROW_NUMBER() OVER(PARTITION BY cc_reserveline.ClaimID ORDER BY cc_reserveline.UpdateTime DESC ) AS IndemnityTransactionRank                   
							  FROM (SELECT * FROM `{project}.{cc_dataset}.cc_reserveline` WHERE _PARTITIONTIME = {partition_date}) AS cc_reserveline
								INNER JOIN `{project}.{cc_dataset}.cctl_costtype` AS cctl_costtype 
								  ON cctl_costtype.ID = cc_reserveline.CostType
							  WHERE cctl_costtype.TYPECODE = 'expense_jmic'
							) AS cAvgReserveIndemnity
								ON cAvgReserveIndemnity.ClaimID = Claim.ClaimID AND IndemnityTransactionRank=1

				        LEFT JOIN (
								SELECT 
									cc_matter.ClaimID
									,cc_matter.AssignmentDate AS LitigationStartDate
									,ROW_NUMBER() OVER(PARTITION BY cc_matter.ClaimID ORDER BY cc_matter.AssignmentDate) AS LitigationIndex
								FROM (SELECT * FROM `{project}.{cc_dataset}.cc_matter` WHERE _PARTITIONTIME = {partition_date}) AS cc_matter
							) AS Litigations 
								ON Litigations.ClaimID = Claim.ClaimID AND Litigations.LitigationIndex = 1

						LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE CAST(bq_load_date AS TIMESTAMP) = {partition_date}) AS TravelLossaddr
							ON TravelLossaddr.AddressID = Claim.TravelLossLocationID
							AND TravelLossaddr.SourceCenter = 'CC'
												
						LEFT JOIN CTE_SIUquestionScore ON Claim.ClaimID = CTE_SIUquestionScore.ID

					WHERE 1 = 1
					AND IsTransactionSliceEffective = 1
       
				) ClaimAttribute

				INNER JOIN ClaimTransConfig  AS ConfigSource
				ON ConfigSource.Key='SourceSystem'

				INNER JOIN ClaimTransConfig  AS ConfigHashSep
				ON ConfigHashSep.Key='HashKeySeparator'

				INNER JOIN ClaimTransConfig AS ConfigHashAlgo
				ON ConfigHashAlgo.Key='HashingAlgorithm'

				WHERE 1=1

		--) extractData
