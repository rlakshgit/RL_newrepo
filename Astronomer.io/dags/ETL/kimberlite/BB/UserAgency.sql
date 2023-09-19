-- tag: UserAgency - tag ends/
/****************************************************************************************************************
	Kimberlite Building Block Extract
		UserAgency.sql
			Big Query converted
	
	Descr:
		This table holds user and agency keys along with effective dates.

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	05/16/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
sp_helptext '[bi_stage].[spSTG_DimUserAgency_Extract_GW]'
sp_helptext '[bi_stage].[spSTG_DimUserAgency_Transform_Lkup_EffectiveDateKey]'
--select * from DW_DDS.bi_dds.DimAgency where AgencyCode = '900'
*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_UserAgency`
AS SELECT outerselect.*
FROM (
*/

INSERT INTO `{project}.{dest_dataset}.UserAgency` 
(
	SourceSystem
	,UserAgencyKey
	,UserKey
	,AgencyKey
	,RowEffectiveDate
	,RowExpirationDate
	,IsCurrentRow
	,UserAgencyID
	,ProducerCodePublicID
	,ProducerCode
	,OrgCode
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
	,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,UserAgencyID)) AS UserAgencyKey
	,UserKey
	,AgencyKey
	,RowEffectiveDate
	,RowExpirationDate
	,IsCurrentRow
	,UserAgencyID
	,ProducerCodePublicID
	,ProducerCode
	,OrgCode
	,AgentUserID
	,ExternalUserName
	,DATE('{date}') AS bq_load_date
	--,CURRENT_DATE() AS bq_load_date

FROM
(
	SELECT DISTINCT
  		ProducerUser.UserKey
		, ProducerUser.AgencyKey
		, ProducerUser.ProducerCodePublicID AS ProducerCodePublicID
		, LTRIM(RTRIM(agency.AgencySubCode)) AS ProducerCode
		, LTRIM(RTRIM(agency.AgencyMasterCode)) AS OrgCode
		, ProducerUser.AgentUserID AS AgentUserID
		, CAST(NULL AS STRING) AS ExternalUserName
		, IFNULL(LTRIM(RTRIM(agency.AgencySubCode)),'') || ProducerUser.AgentUserID AS UserAgencyID
		, DATE('1900-01-01') AS RowEffectiveDate
		, -1 As RowExpirationDate
		, 1 AS IsCurrentRow

	FROM
		`{project}.{core_dataset}.ProducerUser` AS ProducerUser
		LEFT OUTER JOIN `{project}.{core_dataset}.Agency` AS Agency
			ON Agency.AgencySubCode = ProducerUser.ProducerCode
	WHERE 1 = 1 
		AND ProducerUser.AgentUserID IS NOT NULL

	UNION ALL

	SELECT DISTINCT
  		ProducerUser.UserKey
		, ProducerUser.AgencyKey
		, ProducerUser.ProducerCodePublicID AS ProducerCodePublicID
		, LTRIM(RTRIM(Agency.AgencySubCode)) AS ProducerCode
		, LTRIM(RTRIM(Agency.AgencyMasterCode)) AS OrgCode
		, CAST(NULL AS STRING) AS AgentUserID
		, ProducerUser.ExternalUserName AS ExternalUserName
		, IFNULL(LTRIM(RTRIM(Agency.AgencySubCode)),'') || ProducerUser.ExternalUserName AS UserAgencyID
		, DATE('1900-01-01') AS RowEffectiveDate
		, -1 As RowExpirationDate
		, 1 AS IsCurrentRow

	FROM 
		`{project}.{core_dataset}.ProducerUser` AS ProducerUser
		LEFT OUTER JOIN `{project}.{core_dataset}.Agency` AS Agency
			ON Agency.AgencySubCode = ProducerUser.ProducerCode
	WHERE 1 = 1 
		AND ProducerUser.ExternalUserName IS NOT NULL	

)DimUserAgency

INNER JOIN Config AS sourceConfig
	ON sourceConfig.Key='SourceSystem'
INNER JOIN Config AS hashKeySeparator
	ON hashKeySeparator.Key='HashKeySeparator'
INNER JOIN Config AS hashingAlgo
	ON hashingAlgo.Key='HashAlgorithm'

--)outerselect
;