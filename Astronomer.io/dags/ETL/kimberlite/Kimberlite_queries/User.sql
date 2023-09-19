-- tag: User - tag ends/
/************************************************************
	Kimberlite Extract Query
		User.sql
			Big Query Converted
	Descr:
		This table holds User info. This functions as a raw table
		at the Kimberlite core layer level.

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/07/2023	DROBAK		Init
	05/16/2023	DROBAK		Added sections for PC users and ADLDSUsers; added parameter: {ad_bief_dataset} = ref_ad_bief_src_current
	06/07/2023	DROBAK		Added final select and left join with AD View within this extract query rather than use a core layer view

------------------------------------------------------------------------------------------------------------------
--select * from bi_dds.DimUser where LoginID = 'Admin'
--DW Sources--
USE DW_STAGE;
sp_helptext '[bi_stage].[spSTG_DimUser_Extract_GW]'
[bi_stage].[spSTG_DimUser_Extract_ADLDS]
[bi_stage].[spSTG_DimUser_Extract_AD]
*******************************************************************************************************************/

CREATE TEMP TABLE temp_Users
(
  SourceSystem STRING,
  UserKeyStub STRING,
  LoginID STRING,
  FullName STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  Title STRING,
  TelephoneNumber STRING,
  Email STRING,
  DistinguishedName STRING,
  Department STRING,
  AgentUserID STRING,
  ExternalUserName STRING,
  adldsObjectGUID STRING
);

  INSERT INTO temp_Users
  SELECT outerselect.*
    FROM (

        WITH CTE_DistinctUsers AS
        (
          SELECT innerselect.*
          FROM (
            SELECT
				'GW' AS SourceSystem
				,PublicID AS UserKeyStub
                ,AgentUserID_JMIC
                ,AgentFirstName_JM 
                ,AgentLastName_JM
                ,AgentEmailAddress_JM
                ,AgentPhone_JM
                --AgentUserID_JMIC is our key, but this key can have multiple records.
                --Prioritize records based on
                --1. If the record has name values
                --2. The most recent policy effective date
                --3. The most recent written record
                ,ROW_NUMBER() OVER (PARTITION BY AgentUserID_JMIC 
                          ORDER BY AgentFirstName_JM DESC
                              ,AgentLastName_JM DESC
                              ,PeriodStart DESC
                              ,CreateTime DESC) AS PrimaryUserRecord
							  
			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod
            WHERE 1 = 1 
            AND	pc_policyperiod.AgentUserID_JMIC IS NOT NULL
            --AND AgentUserID_JMIC = 'scolligan'

          ) innerselect
          WHERE innerselect.PrimaryUserRecord = 1
        )

        SELECT
          SourceSystem
          ,DimUser.UserKeyStub
          ,DimUser.LoginID 
          ,DimUser.FullName
          ,DimUser.FirstName 
          ,DimUser.LastName
          ,DimUser.MiddleName
          ,DimUser.Title
          ,DimUser.TelephoneNumber
          ,DimUser.Email
          ,DimUser.DistinguishedName
          ,DimUser.Department
          ,DimUser.AgentUserID
          ,DimUser.ExternalUserName
          ,DimUser.adldsObjectGUID

        FROM (
              SELECT DISTINCT
			  'GW' 																					AS SourceSystem
			  ,CTE_DistinctUsers.UserKeyStub														AS UserKeyStub
              ,CTE_DistinctUsers.AgentUserID_JMIC													AS LoginID
              ,CONCAT(CTE_DistinctUsers.AgentFirstName_JM, ' ', CTE_DistinctUsers.AgentLastName_JM)	AS FullName
              ,CTE_DistinctUsers.AgentFirstName_JM													AS FirstName
              ,CTE_DistinctUsers.AgentLastName_JM													AS LastName
              ,CAST(NULL AS STRING)																	AS MiddleName
              ,CAST(NULL AS STRING)																	AS Title
              ,CTE_DistinctUsers.AgentPhone_JM														AS TelephoneNumber
              ,COALESCE(CTE_DistinctUsers.AgentEmailAddress_JM,CTE_DistinctUsers.AgentUserID_JMIC)	AS Email
              ,CAST(NULL AS STRING)																	AS DistinguishedName
              ,CAST(NULL AS STRING)																	AS Department
              ,CTE_DistinctUsers.AgentUserID_JMIC													AS AgentUserID
              ,CAST(NULL AS STRING)																	AS ExternalUserName
              ,CAST(NULL AS STRING)																	AS adldsObjectGUID

            FROM 
              CTE_DistinctUsers AS CTE_DistinctUsers

            UNION ALL

            --find the mismatched data type
            SELECT DISTINCT
			  'GW' 																					AS SourceSystem
			  ,pc_credential.PublicID																AS UserKeyStub
              ,pc_credential.UserName																AS LoginID
              ,IFNULL(IFNULL(pc_contact.FirstName,'') || ' ' || IFNULL(pc_contact.LastName,''),'')	AS FullName
              ,pc_contact.FirstName																	AS FirstName
              ,pc_contact.LastName																	AS LastName
              ,pc_contact.MiddleName																AS MiddleName
              ,pc_user.JobTitle																		AS Title
              ,COALESCE(pc_contact.WorkPhone,pc_contact.HomePhone)									AS TelephoneNumber
              ,pc_contact.EmailAddress1																AS Email
              ,CAST(NULL AS STRING)																	AS DistinguishedName
              ,pc_user.Department																	AS Department
              ,CAST(NULL AS STRING)																	AS AgentUserID
              ,pc_credential.UserName																AS ExternalUserName
              ,CAST(NULL AS STRING)																	AS adldsObjectGUID

            FROM 
			(SELECT * FROM `{project}.{pc_dataset}.pc_user` WHERE _PARTITIONTIME = {partition_date}) AS pc_user
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_credential` WHERE _PARTITIONTIME = {partition_date}) AS pc_credential 
				ON pc_credential.id=pc_user.CredentialID 
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact
				ON pc_contact.Id=pc_user.ContactID
		WHERE 1=1
			AND pc_user.ExternalUser = true
			AND IFNULL(pc_credential.UserName,'') <> ''
			AND pc_credential.Retired = 0

		UNION ALL
          
            SELECT DISTINCT
			  'ADLDS' 																			AS SourceSystem
			  , CAST(NULL AS STRING)															AS UserKeyStub
              , adldsUsers.cn																	AS LoginID
              , IFNULL(NULLIF(IFNULL(NULLIF(adldsUsers.givenName,''),'') || IFNULL(' ' || 
                NULLIF(adldsUsers.middleName,''),'') || IFNULL(' ' || 
                NULLIF(adldsUsers.sn,''),''),''),'?')											AS FullName
              , adldsUsers.givenName															AS FirstName
              , adldsUsers.sn																	AS LastName
              , adldsUsers.middleName															AS MiddleName
              , CAST(NULL AS STRING)															AS Title
              , adldsUsers.telephoneNumber														AS TelephoneNumber
              , adldsUsers.mail																	AS Email
              , adldsUsers.distinguishedName													AS DistinguishedName
              , CAST(NULL AS STRING)															AS Department
              , CAST(NULL AS STRING)															AS AgentUserID
              , CAST(NULL AS STRING)															AS ExternalUserName
              , adldsUsers.objectGUID															AS adldsObjectGUID
                  
            FROM
			(SELECT * FROM `{project}.{ad_bief_dataset}.ADLDSUsers` WHERE _PARTITIONTIME = {partition_date}) AS adldsUsers
		WHERE 1 = 1

	) AS DimUser
	
) outerselect;

/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite_core.dar_User`
AS SELECT finalselect.*
FROM (
*/
--Final select and insert from temp_users plus any new records in v_ADView
INSERT INTO `{project}.{dest_dataset}.User` 
(
	SourceSystem
	,UserKey
	,LoginID 
	,FullName
	,FirstName 
	,LastName
	,MiddleName
	,Title
	,TelephoneNumber
	,Email
	,DistinguishedName
	,Department
	,AgentUserID
	,ExternalUserName
	,adldsObjectGUID
	,UserKeyStub
	,bq_load_date
)

  WITH Config AS (
    --SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
    SELECT 'HashKeySeparator' AS Key,'_' AS Value UNION ALL
    SELECT 'HashAlgorithm', 'SHA2_256'
  )

  SELECT
    SourceSystem
	,SHA256(CONCAT(SourceSystem,hashKeySeparator.Value,LoginID,hashKeySeparator.Value,IFNULL(TotalUsers.LoginID,'')))	AS UserKey
    ,TotalUsers.LoginID 
    ,TotalUsers.FullName
    ,TotalUsers.FirstName 
    ,TotalUsers.LastName
    ,TotalUsers.MiddleName
    ,TotalUsers.Title
    ,TotalUsers.TelephoneNumber
    ,TotalUsers.Email
    ,TotalUsers.DistinguishedName
    ,TotalUsers.Department
    ,TotalUsers.AgentUserID
    ,TotalUsers.ExternalUserName
    ,TotalUsers.adldsObjectGUID
    ,TotalUsers.UserKeyStub
    ,DATE('{date}') AS bq_load_date
    --,CURRENT_DATE() AS bq_load_date

  FROM 
  (
    SELECT
      SourceSystem
      ,UserKeyStub
      ,LoginID 
      ,FullName
      ,FirstName 
      ,LastName
      ,MiddleName
      ,Title
      ,TelephoneNumber
      ,Email
      ,DistinguishedName
      ,Department
      ,AgentUserID
      ,ExternalUserName
      ,adldsObjectGUID
    FROM
      temp_Users

    UNION ALL

    SELECT DISTINCT
      ADView.SourceSystem
      ,CAST(NULL AS STRING)			AS UserKeyStub
      ,ADView.sAMAccountName		AS LoginID
      ,ADView.name					AS FullName
      ,ADView.givenName				AS FistName
      ,ADView.sn					AS LastName
      ,ADView.middleName			AS MiddleName
      ,ADView.title					AS Title
      ,ADView.telephoneNumber		AS TelephoneNumber
      ,ADView.mail					AS Email
      ,ADView.distinguishedName		AS DistinguishedName
      ,ADView.department			AS Department
      ,CAST(NULL AS STRING)			AS AgentUserID
      ,CAST(NULL AS STRING)			AS ExternalUserName
      ,ADView.objectGUID			AS adObjectGUID
    
    FROM
      (SELECT "AD" AS SourceSystem, * FROM `{project}.{ad_bief_dataset}.v_ADView` WHERE _PARTITIONTIME = {partition_date}) AS ADView
      LEFT OUTER JOIN temp_Users AS temp_Users
        ON ADView.manager = temp_Users.DistinguishedName
    WHERE ADView.cn NOT LIKE 'svc-%'	--filter out known service accounts

  ) AS TotalUsers

  INNER JOIN Config AS hashKeySeparator
    ON hashKeySeparator.Key='HashKeySeparator'
  INNER JOIN Config AS hashingAlgo
    ON hashingAlgo.Key='HashAlgorithm'

--) finalselect