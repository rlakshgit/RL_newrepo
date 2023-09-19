-- tag: ProducerUser - tag ends/
/****************************************************************************************************************
	Kimberlite Core Extract
		ProducerUser.sql
			Big Query converted
	
	Descr:
		This table holds producer users key and link to Agency.

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	05/22/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
sp_helptext '[bi_stage].[spSTG_DimUserAgency_Extract_GW]'
sp_helptext '[bi_stage].[spSTG_DimUserAgency_Transform_Lkup_EffectiveDateKey]'
sp_helptext 'bi_stage.spDDS_DimUserAgency_Upsert'
--select * from DW_DDS.bi_dds.DimAgency where AgencyCode = '900'
*******************************************************************************************************************
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite_core.dar_ProducerUser`
AS SELECT outerselect.*
FROM (
*/

INSERT INTO `{project}.{dest_dataset}.ProducerUser` 
(
	SourceSystem
	,ProducerUserKey
	,UserKey
	,AgencyKey
	,ProducerCode
	,ProducerCodePublicID
	,AgentUserID
	,ExternalUserName
	,bq_load_date
)

WITH Config AS (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
)

SELECT
	sourceConfig.Value AS SourceSystem
	,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,COALESCE(AgentUserID,ExternalUserName),IFNULL(ProducerCodePublicID,''))) AS ProducerUserKey
	,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,COALESCE(AgentUserID,ExternalUserName)))			AS UserKey
	,CASE WHEN ProducerCodePublicID IS NOT NULL
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ProducerCodePublicID))
		END																										AS AgencyKey	--ProducerCodeOfRecordKey
	,ProducerCode
	,ProducerCodePublicID
	,AgentUserID
	,ExternalUserName
	,DATE('{date}') AS bq_load_date
	--,CURRENT_DATE() AS bq_load_date

FROM
(
	SELECT	ProducerCodePublicID
			,ProducerCode
			,AgentUserID
			,ExternalUserName
	FROM (
		SELECT DISTINCT
			pc_producercode.PublicID AS ProducerCodePublicID
			, pc_producercode.Code AS ProducerCode
			, AgentUserId_JMIC AS AgentUserID
			, CAST(NULL AS STRING) AS ExternalUserName
			, ROW_NUMBER() OVER (PARTITION BY AgentUserID_JMIC 
								ORDER BY AgentFirstName_JM DESC
										,AgentLastName_JM DESC
										,PeriodStart DESC
										,pc_policyperiod.CreateTime DESC) AS PrimaryUserRecord

		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
			LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
				ON pc_policyperiod.ProducerCodeOfRecordID = pc_producercode.ID

		WHERE 1 = 1 
			AND AgentUserId_JMIC IS NOT NULL
			--and AgentUserId_JMIC = 'xlunhx@apptest.jminsure.com'
			--and pc_producercode.PublicID = 'JMPC:586700'

	) innerselect
	WHERE innerselect.PrimaryUserRecord = 1
	
	UNION ALL

	SELECT DISTINCT
		pc_producercode.PublicID AS ProducerCodePublicID
		, pc_producercode.Code AS ProducerCode
		, CAST(NULL AS STRING) AS AgentUserID
		, pc_credential.UserName AS ExternalUserName

	FROM 
		(SELECT * FROM `{project}.{pc_dataset}.pc_user` WHERE _PARTITIONTIME = {partition_date}) AS pc_user
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_credential` WHERE _PARTITIONTIME = {partition_date}) AS pc_credential
			ON pc_credential.ID = pc_user.CredentialID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_userproducercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_userproducercode
			ON pc_userproducercode.UserID = pc_user.ID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS pc_producercode
			ON pc_producercode.id = pc_userproducercode.ProducerCodeID
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_role` WHERE _PARTITIONTIME = {partition_date}) AS pc_role
			ON pc_role.Id = pc_userproducercode.RoleID
	WHERE 1 = 1 
		AND pc_credential.UserName IS NOT NULL									
		AND pc_user.ExternalUser = true
		--and pc_producercode.PublicID = 'JMPC:877100'

)DimProducerUser

INNER JOIN Config AS sourceConfig
	ON sourceConfig.Key='SourceSystem'
INNER JOIN Config AS hashKeySeparator
	ON hashKeySeparator.Key='HashKeySeparator'
INNER JOIN Config AS hashingAlgo
	ON hashingAlgo.Key='HashAlgorithm'

--)outerselect