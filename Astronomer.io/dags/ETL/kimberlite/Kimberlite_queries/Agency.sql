-- tag: Agency - tag ends/
/****************************************************************************************************************
	Kimberlite Extract
		Agency.sql
			Big Query converted
	
	Descr:
		This table holds Agency codes and attributes

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	05/15/2023	DROBAK		Init
	06/08/2023	DROBAK		Add SeniorPreferredUnderwriterID,PreferredUnderwriterID

------------------------------------------------------------------------------------------------------------------
[bi_stage].[spSTG_DimAgency_Extract_GW]
--select * from DW_DDS.bi_dds.DimAgency where AgencyCode = '900'
*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite_core.dar_Agency`
AS SELECT outerselect.*
FROM (
*/

INSERT INTO `{project}.{dest_dataset}.Agency` 
(
	SourceSystem
	,AgencyKey
	,AgencySubCode
	,ProducerCodePublicID
	,PCOrganizationPublicID
	,AgencyMasterCode
	,AgencyMasterName
	,AgencyCode
	,AgencyName
	,AgencyDescription
	,AgencyAddress1
	,AgencyAddress2
	,AgencyCity
	,AgencyStateCode
	,AgencyCounty
	,AgencyPostalCode
	,AgencyMailingAddress1
	,AgencyMailingAddress2
	,AgencyMailingCity
	,AgencyMailingState
	,AgencyMailingPostalCode
	,AgencyPrimaryPhoneNumber
	,AgencyPrimaryEmailAddress
	,AgencySubName
	,AgencySubGroupType
	,AgencySubActivationDate
	,AgencySubTerminationDate
	,AgencySubPrimaryWritingState
	,AgencySubStatus
	,IsServiceCenter
	,OriginalContractDate
	,OriginalApptDate
	,SeniorPreferredUnderwriterID
	,PreferredUnderwriterID
	,IsStrategicPartner
	,Retired
	,bq_load_date
)

WITH Config AS (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
)

SELECT
	sourceConfig.Value AS SourceSystem
	,CASE WHEN ProducerCodePublicID IS NOT NULL
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,ProducerCodePublicID))
		END																					AS AgencyKey	--ProducerCodeOfRecordKey
	,AgencySubCode
	,ProducerCodePublicID
	,PCOrganizationPublicID
	,AgencyMasterCode
	,AgencyMasterName
	,AgencyCode
	,AgencyName
	,AgencyDescription
	,AgencyAddress1
	,AgencyAddress2
	,AgencyCity
	,AgencyStateCode
	,AgencyCounty
	,AgencyPostalCode
	,AgencyMailingAddress1
	,AgencyMailingAddress2
	,AgencyMailingCity
	,AgencyMailingState
	,AgencyMailingPostalCode
	,AgencyPrimaryPhoneNumber
	,AgencyEmailAddress1 AS AgencyPrimaryEmailAddress
	,AgencySubName
	,AgencySubGroupType
	,AgencySubActivationDate
	,AgencySubTerminationDate
	,AgencySubPrimaryWritingState
	,AgencySubStatus
	,IsServiceCenter
	,OriginalContractDate
	,OriginalApptDate
	,SeniorPreferredUnderwriterID
	,PreferredUnderwriterID
	,IsStrategicPartner
	,Retired
	--,AgencyType
	,DATE('{date}') AS bq_load_date
	--,CURRENT_DATE() AS bq_load_date

FROM 
(
	SELECT
		-- Natural Keys
		COALESCE(AgencyDetails.AgencySubCode,producercodes.Code)				AS AgencySubCode
		,COALESCE(AgencyDetails.ProducerCodePublicID,producercodes.PublicID)	AS ProducerCodePublicID
		,CAST(NULL AS STRING)													AS PCOrganizationPublicID
		-- Core Fields
		,AgencyDetails.AgencyMasterCode			AS AgencyMasterCode
		,AgencyDetails.AgencyMasterName			AS AgencyMasterName
		,AgencyDetails.AgencyCode				AS AgencyCode
		,AgencyDetails.AgencyName				AS AgencyName
		,AgencyDetails.AgencyDescription		AS AgencyDescription
		,AgencyDetails.AgencyAddress1			AS AgencyAddress1
		,AgencyDetails.AgencyAddress2			AS AgencyAddress2
		,AgencyDetails.AgencyCity				AS AgencyCity
		,AgencyDetails.AgencyStateCode			AS AgencyStateCode
		,AgencyDetails.AgencyCounty				AS AgencyCounty
		,AgencyDetails.AgencyPostalCode			AS AgencyPostalCode
		,AgencyDetails.AgencyMailingAddress1	AS AgencyMailingAddress1
		,AgencyDetails.AgencyMailingAddress2	AS AgencyMailingAddress2
		,AgencyDetails.AgencyMailingCity		AS AgencyMailingCity
		,AgencyDetails.AgencyMailingState		AS AgencyMailingState
		,AgencyDetails.AgencyMailingPostalCode	AS AgencyMailingPostalCode
		,AgencyDetails.AgencyPrimaryPhoneNumber	AS AgencyPrimaryPhoneNumber
		,AgencyDetails.AgencyEmailAddress1		AS AgencyEmailAddress1

		,COALESCE(AgencyDetails.AgencySubName,producercodes.SubagencyName_JMIC)								AS AgencySubName
		,AgencyDetails.AgencySubGroupType																	AS AgencySubGroupType
		,COALESCE(AgencyDetails.AgencySubActivationDate,custom_functions.fn_GetDefaultMinDateTime())		AS AgencySubActivationDate 
		,COALESCE(AgencyDetails.AgencySubTerminationDate,custom_functions.fn_GetDefaultMaxDateTime())		AS AgencySubTerminationDate
		,AgencyDetails.AgencySubPrimaryWritingState															AS AgencySubPrimaryWritingState
		,AgencyDetails.AgencySubStatus																		AS AgencySubStatus
		,COALESCE(AgencyDetails.IsServiceCenter,producercodes.Participate_ServiceCenter_JM)					AS IsServiceCenter
		,COALESCE(AgencyDetails.OriginalContractDate,NULL)													AS OriginalContractDate
		,COALESCE(AgencyDetails.OriginalApptDate,NULL)														AS OriginalApptDate
		,COALESCE(AgencyDetails.SeniorPreferredUnderwriterID, producercodes.SeniorPreferredUnderwriterID)	AS SeniorPreferredUnderwriterID
		,COALESCE(AgencyDetails.PreferredUnderwriterID, producercodes.PreferredUnderwriterID)				AS PreferredUnderwriterID
		,COALESCE(AgencyDetails.IsStrategicPartner,0)														AS IsStrategicPartner
		,COALESCE(AgencyDetails.Retired,0) 																	AS Retired
		--,AgencyType = ''

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date}) AS producercodes
	LEFT OUTER JOIN
	(
		SELECT AgencySubCode
			,ProducerCodePublicID
			,AgencyMasterCode
			,AgencyMasterName 
			,AgencyCode 
			,AgencyName
			,AgencyDescription
			,AgencyAddress1 
			,AgencyAddress2 
			,AgencyCity 
			,AgencyStateCode 
			,AgencyCounty 
			,AgencyPostalCode
			,AgencyMailingAddress1 
			,AgencyMailingAddress2 
			,AgencyMailingCity
			,AgencyMailingState
			,AgencyMailingPostalCode
			,AgencyPrimaryPhoneNumber
			,AgencyEmailAddress1
			,AgencySubName 
			,AgencySubGroupType 
			,AgencySubActivationDate
			,AgencySubTerminationDate 
			,AgencySubPrimaryWritingState 
			,AgencySubStatus
			,IsServiceCenter
			,OriginalContractDate
			,OriginalApptDate
			,SeniorPreferredUnderwriterID
			,PreferredUnderwriterID
			,IsStrategicPartner
			,Retired

 		FROM (
			SELECT 
				producercode.Code						AS AgencySubCode
				,producercode.PublicID					AS ProducerCodePublicID
				,pc_organization.MasterAgencyCode_JMIC	AS AgencyMasterCode
				,pc_organization.Name					AS AgencyMasterName
				,AgencyGroup.Code_JMIC					AS AgencyCode
				,AgencyGroup.Name						AS AgencyName
				,AgencyGroup.description				AS AgencyDescription
				,ProducerAddress.AddressLine1			AS AgencyAddress1
				,ProducerAddress.AddressLine2			AS AgencyAddress2
				,ProducerAddress.City					AS AgencyCity
				,CASE WHEN ProducerState.TYPECODE IS Null -- Safty net to get unknown state from country
					THEN 
						CASE WHEN ProducerCountry.TYPECODE = 'CA' THEN 'XC'
							WHEN ProducerCountry.TYPECODE = 'US' THEN 'XU'
							ELSE NULL
							END
					ELSE ProducerState.TYPECODE
					END						AS AgencyStateCode
				,ProducerAddress.County		AS AgencyCounty
				,ProducerAddress.PostalCode	AS AgencyPostalCode
				,CAST(NULL AS STRING)		AS AgencyMailingAddress1
				,CAST(NULL AS STRING)		AS AgencyMailingAddress2
				,CAST(NULL AS STRING)		AS AgencyMailingCity
				,CAST(NULL AS STRING)		AS AgencyMailingState
				,CAST(NULL AS STRING)		AS AgencyMailingPostalCode
				,CAST(NULL AS STRING)		AS AgencyPrimaryPhoneNumber
				,CAST(NULL AS STRING)		AS AgencyEmailAddress1
							
				,producercode.SubagencyName_JMIC					AS AgencySubName
				,AgencyGroupType.NAME								AS AgencySubGroupType
				,custom_functions.fn_GetDefaultMinDateTime()		AS AgencySubActivationDate	--qa-edl.custom_functions.fn_GetDefaultMinDatetime
				,producercode.TerminationDate						AS TerminationDate
				,custom_functions.fn_GetDefaultMaxDateTime()		AS AgencySubTerminationDate--qa-edl.custom_functions.fn_GetDefaultMaxDatetime
				,ProducerState.TYPECODE								AS AgencySubPrimaryWritingState
				,ProducerStatus.Name								AS AgencySubStatus
				,producercode.Participate_ServiceCenter_JM			AS IsServiceCenter
				,AgencyGroup.OriginalContractDate_JMIC				AS OriginalContractDate
				,AgencyGroup.OriginalApptDate_JMIC					AS OriginalApptDate
				,producercode.SeniorPreferredUnderwriterID			AS SeniorPreferredUnderwriterID
				,producercode.PreferredUnderwriterID				AS PreferredUnderwriterID
				,CASE WHEN t_lkup_StrategicPartners.AgencyMasterName IS NOT NULL 
						THEN 1 ELSE 0 END							AS IsStrategicPartner
				,producercode.Retired AS Retired

				,ROW_NUMBER() OVER(PARTITION BY producercode.Code
						ORDER BY ProducerStatus.Name
								,producercode.CreateTime DESC 
								,AgencyGroupType.Typecode)			AS RowNbr

			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_producercode` WHERE _PARTITIONTIME = {partition_date})  AS producercode

			INNER JOIN `{project}.{pc_dataset}.pctl_producerstatus` AS ProducerStatus
				ON producercode.ProducerStatus = ProducerStatus.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date})  AS ProducerAddress
				ON producercode.AddressID = ProducerAddress.ID
			LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS ProducerState
				ON ProducerAddress.State = ProducerState.ID
			LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_country` AS ProducerCountry -- Safty net to get unknown state from country
				on ProducerAddress.Country = ProducerCountry.ID

			-- AGENCY
			-- Get Group for Agency level
			-- NOTE: pc_producercode.Code = 'DIRD' does not have a corresponding Agency record (pctl_grouptype.ID = 10002); all others do
			-- NOTE: changed to LEFT joins to accomodate old/lost producer codes from Financial tables that do not have a Group mapping in GW
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_groupproducercode` WHERE _PARTITIONTIME = {partition_date})  AS AgencyGroupMapping
				ON producercode.ID = AgencyGroupMapping.ProducerCodeID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_group` WHERE _PARTITIONTIME = {partition_date}) AS AgencyGroup   
				ON AgencyGroupMapping.GroupID = AgencyGroup.ID
				AND producercode.OrganizationID = AgencyGroup.OrganizationID
			LEFT JOIN `{project}.{pc_dataset}.pctl_grouptype` AS AgencyGroupType
				ON AgencyGroup.GroupType = AgencyGroupType.ID

			-- MASTER AGENCY
			-- "Organization" represents MasterAgency; use Business Type rather than Group Type
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_organization` WHERE _PARTITIONTIME = {partition_date}) AS pc_organization
				ON producercode.OrganizationID = pc_organization.ID
			INNER JOIN `{project}.{pc_dataset}.pctl_businesstype` AS pctl_businesstype
				ON pc_organization.Type = pctl_businesstype.ID
			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date})  AS pc_contact
				ON pc_contact.ID = pc_organization.ContactID

			--Strategic Partner
			LEFT JOIN `{project}.{dest_dataset}.t_lkup_StrategicPartners` AS t_lkup_StrategicPartners
				ON t_lkup_StrategicPartners.AgencyMasterCode = pc_organization.MasterAgencyCode_JMIC

		)Agency
		WHERE RowNbr = 1

	)AgencyDetails
		ON producercodes.Code = AgencyDetails.AgencySubCode
		AND producercodes.PublicID = ProducerCodePublicID
	--WHERE producercodes.Retired = 0  --The DELETE option is only used when created in error, not to inactivate

	UNION ALL

	SELECT
		-- Natural Keys
		 MasterAgencyCode_JMIC							AS AgencySubCode
		,NULL											AS ProducerCodePublicID
		,pc_organization.PublicID						AS PCOrganizationPublicID
		-- Core Fields
		,pc_organization.MasterAgencyCode_JMIC			AS AgencyMasterCode
		,pc_organization.Name							AS AgencyMasterName
		,pc_organization.MasterAgencyCode_JMIC			AS AgencyCode
		,pc_organization.Name							AS AgencyName
		,NULL											AS AgencyDescription
		,ProducerAddress.AddressLine1					AS AgencyAddress1
		,ProducerAddress.AddressLine2					AS AgencyAddress2
		,ProducerAddress.City							AS AgencyCity
		,CASE WHEN ProducerState.TYPECODE IS NULL -- Safty net to get unknown state from country
			THEN 
				CASE WHEN ProducerCountry.TYPECODE = 'CA' THEN 'XC'
					WHEN ProducerCountry.TYPECODE = 'US' THEN 'XU'
					ELSE NULL
				END
			ELSE ProducerState.TYPECODE
			END												AS AgencyStateCode
		,ProducerAddress.County								AS AgencyCounty
		,ProducerAddress.PostalCode							AS AgencyPostalCode
		,MailingAddress.AddressLine1						AS AgencyMailingAddress1
		,MailingAddress.AddressLine2						AS AgencyMailingAddress2
		,MailingAddress.City								AS AgencyMailingCity
		,MailingAddressState.TYPECODE						AS AgencyMailingState
		,MailingAddress.PostalCode							AS AgencyMailingPostalCode
		,CASE WHEN pc_contact.PrimaryPhone = 1 
			THEN pc_contact.WorkPhone 
			ELSE NULL END									AS AgencyPrimaryPhoneNumber
		,pc_contact.EmailAddress1							AS AgencyEmailAddress1

		,pc_organization.Name											AS AgencySubName
		,'Organization'													AS AgencySubGroupType
		,custom_functions.fn_GetDefaultMinDateTime()					AS AgencySubActivationDate--'1990-01-01 00:00:00.0000000'
		,custom_functions.fn_GetDefaultMaxDateTime()					AS AgencySubTerminationDate--'9999-12-31'
		,COALESCE(PrimaryAddressState.TYPECODE,ProducerState.TYPECODE)	AS AgencySubPrimaryWritingState
		,ProducerStatus.Name											AS AgencySubStatus
		,false															AS IsServiceCenter
		,NULL															AS OriginalContractDate
		,NULL															AS OriginalApptDate
		,NULL															AS SeniorPreferredUnderwriterID
		,NULL															AS PreferredUnderwriterID
		,CASE WHEN t_lkup_StrategicPartners.AgencyMasterName IS NOT NULL 
				THEN 1 ELSE 0 END										AS IsStrategicPartner
		,pc_organization.Retired										AS Retired
		--,AgencyType = 0

	--   select *
	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_organization` WHERE _PARTITIONTIME = {partition_date}) AS pc_organization
	INNER JOIN `{project}.{pc_dataset}.pctl_businesstype` AS pctl_businesstype
		ON pctl_businesstype.ID = pc_organization.type	
		AND pctl_businesstype.Name = 'Master Agency' --added to filter out dupe of B50 (had Insurer and Master Agency)
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact
		ON pc_contact.ID = pc_organization.ContactID
	INNER JOIN `{project}.{pc_dataset}.pctl_producerstatus` AS ProducerStatus
		ON pc_organization.ProducerStatus = ProducerStatus.ID
	INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS ProducerAddress
		ON pc_contact.primaryAddressID = ProducerAddress.ID
	LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS ProducerState
		ON ProducerAddress.State = ProducerState.ID
	LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_country` AS ProducerCountry -- Safty net to get unknown state from country
		on ProducerAddress.Country = ProducerCountry.ID
	LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS PrimaryAddress
		ON pc_contact.PrimaryAddressID = PrimaryAddress.ID
	LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS PrimaryAddressState
		ON PrimaryAddress.State = PrimaryAddressState.ID
	LEFT OUTER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS MailingAddress
		ON pc_contact.MailingAddress_JMIC = MailingAddress.ID
	LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_state` AS MailingAddressState
		ON MailingAddress.State = MailingAddressState.ID

	--Strategic Partner
	LEFT JOIN `{project}.{dest_dataset}.t_lkup_StrategicPartners` AS t_lkup_StrategicPartners
		ON t_lkup_StrategicPartners.AgencyMasterCode = pc_organization.MasterAgencyCode_JMIC
		--AND t_lkup_StrategicPartners.Category='StrategicPartner'

	--WHERE pc_organization.Retired = 0

) innerselect

INNER JOIN Config AS sourceConfig
	ON sourceConfig.Key='SourceSystem'
INNER JOIN Config AS hashKeySeparator
	ON hashKeySeparator.Key='HashKeySeparator'
INNER JOIN Config AS hashingAlgo
	ON hashingAlgo.Key='HashAlgorithm'

--)outerselect