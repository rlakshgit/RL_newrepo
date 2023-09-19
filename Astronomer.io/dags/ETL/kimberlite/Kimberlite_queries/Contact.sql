-- tag: Contact - tag ends/
/****************************************************************************************************************
	Kimberlite Extract
		Contact.sql
			Converted to BigQuery

	Desc:	Contains all contacts from the four GW centers, plus key attributes.

	NOTES:	Need to confirm Key here connects to other Account/Contact tables; then how do the BB layer tables connect 
			to the "contact" fields in Core or BB tables (should it be PublicID or Hashed Key?)
			Need to run in BQ
-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/13/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--DWH Sources
--bi_stage.spSTG_DimContact_Extract_PC
--bi_stage.spSTG_DimContact_Extract_BC

*******************************************************************************************************************/

CREATE TEMP TABLE Config 
AS SELECT *
FROM (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
);

/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_Contact`
AS SELECT outerselect.*
FROM (
--) outerselect
*/
INSERT INTO `{project}.{dest_dataset}.Contact` (
	SourceSystem
	,ContactKey
	,SourceCenter
	,ContactPublicID
	,ContactID
	,PrimaryAddressID
	,MailingAddressID
	,Retired
	,ContactType
	,FullName
	,FirstName
	,MiddleName
	,LastName
	,Prefix
	,Suffix
	,Gender
	,PrimaryPhoneType
	,WorkPhone
	,HomePhone
	,CellPhone
	,MobilePhone
	,OtherPhoneOne
	,EmailAddress1
	,EmailAddress2
	,WebAddress
	,DateOfBirth
	,ContactForSurvey
	,RecMarketingMaterial
	,LongName
	,Occupation
	,MaritalStatus
	,LinkID
	,PlatformOrgName
	,PlatformOrgId
	,JBTRating
	,UserName
	,Obfuscated
	,bq_load_date
)

WITH CTE_JBTRating AS
(
	SELECT  ContactID, JBTRating_JMIC				
			,ROW_NUMBER() OVER(PARTITION BY ContactID
					ORDER BY LastUpdatedDate DESC
				) AS TransactionRank
	FROM `{project}.{pc_dataset}.pcx_Relationship_JMIC` AS pcx_Relationship_JMIC
	WHERE 1=1
	AND _PARTITIONTIME = {partition_date}
	--AND pcx_Relationship_JMIC.ContactID = 153910
)

SELECT
	Contacts.*
	,DATE('{date}') AS bq_load_date		
	-- ,CURRENT_DATE() AS bq_load_date

FROM (
		SELECT
			sourceConfig.Value AS SourceSystem
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,pc_contact.PublicID,hashKeySeparator.Value,'PC'))	AS ContactKey
			,'PC'													AS SourceCenter
			,pc_contact.PublicID									AS ContactPublicID
			,pc_contact.ID											AS ContactID
			,pc_contact.PrimaryAddressID							AS PrimaryAddressID
			,FirstMailingAddress.MailingAddressID					AS MailingAddressID
			,pc_contact.Retired										AS Retired
			,pctl_contact.TYPECODE									AS ContactType
			,pc_contact.Name										AS FullName
			,pc_contact.FirstName									AS FirstName
			,pc_contact.MiddleName									AS MiddleName
			,pc_contact.LastName									AS LastName
			,pc_contact.Prefix										AS Prefix
			,pc_contact.Suffix										AS Suffix
			,pc_contact.Gender										AS Gender
			,pctl_primaryphonetype.TYPECODE							AS PrimaryPhoneType
			,pc_contact.WorkPhone									AS WorkPhone
			,pc_contact.HomePhone									AS HomePhone
			,pc_contact.CellPhone									AS CellPhone
			,pc_contact.MobilePhone_JMIC							AS MobilePhone
			,pc_contact.OtherPhoneOne_JMIC							AS OtherPhoneOne
			,pc_contact.EmailAddress1								AS EmailAddress1
			,pc_contact.EmailAddress2								AS EmailAddress2
			,pc_contact.Website_JMIC								AS WebAddress
			,CAST(pc_contact.DateOfBirth AS DATETIME)				AS DateOfBirth
			,CAST(pc_contact.ContactForSurvey_JMIC_PL AS INT)		AS ContactForSurvey
			,CAST(pc_contact.RecMaterketingMaterial_JMIC_PL AS INT)	AS RecMarketingMaterial
			,pc_contact.LongName_JMIC								AS LongName
			,pc_contact.Occupation									AS Occupation
			,pctl_maritalstatus.Name								AS MaritalStatus
			,CAST(NULL AS STRING)									AS LinkID
			,CAST(NULL AS STRING)									AS PlatformOrgName
			,CAST(NULL AS STRING)									AS PlatformOrgId
			,CTE_JBTRating.JBTRating_JMIC							AS JBTRating	--Applies only to Jeweler Roles
			,pc_credential.UserName									AS UserName
			,CAST(pc_contact.ObfuscatedInternal AS INT64)			AS Obfuscated
			
		FROM
			(SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact

			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	pc_contact.ID AS ContactID
						,MAX(pc_address_mailing.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact
					LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contactaddress` WHERE _PARTITIONTIME = {partition_date}) AS pc_contactaddress
						ON pc_contact.ID = pc_contactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS pc_address_mailing
						ON  pc_contactaddress.AddressID = pc_address_mailing.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{pc_dataset}.pctl_addresstype` AS pctl_addresstype
						ON pc_address_mailing.AddressType = pctl_addresstype.ID
						AND pctl_addresstype.TYPECODE = 'Mailing_JMIC'
				WHERE 1=1
					--AND pc_contact.ID = 520
				GROUP BY pc_contact.ID
			) FirstMailingAddress
				ON pc_contact.ID = FirstMailingAddress.ContactID

			LEFT JOIN `{project}.{pc_dataset}.pctl_contact` AS pctl_contact
				ON pc_contact.Subtype = pctl_contact.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_primaryphonetype` AS pctl_primaryphonetype
				ON pc_contact.PrimaryPhone = pctl_primaryphonetype.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_maritalstatus` AS pctl_maritalstatus
				ON pc_contact.MaritalStatus = pctl_maritalstatus.ID
			--LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_Relationship_JMIC` WHERE _PARTITIONTIME = {partition_date}) AS pcx_Relationship_JMIC
			--	ON pcx_Relationship_JMIC.ContactID = pc_contact.ID
			LEFT JOIN CTE_JBTRating AS CTE_JBTRating
				ON CTE_JBTRating.ContactID = pc_contact.ID
				AND CTE_JBTRating.TransactionRank = 1
			--Get UserName for Userid lookup 
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_user` WHERE _PARTITIONTIME = {partition_date}) AS pc_user
				ON pc_contact.ID = pc_user.ContactID
			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_credential` WHERE _PARTITIONTIME = {partition_date}) AS pc_credential  
				ON pc_credential.ID = pc_user.CredentialID

		INNER JOIN Config sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN Config hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN Config hashingAlgo
			ON hashingAlgo.Key='HashAlgorithm'

		WHERE 1=1

	UNION ALL

	---BC---
		SELECT
			sourceConfig.Value AS SourceSystem
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,bc_contact.PublicID,hashKeySeparator.Value,'BC'))	AS ContactKey
			,'BC'													AS SourceCenter
			,bc_contact.PublicID									AS ContactPublicID
			,bc_contact.ID											AS ContactID
			,bc_contact.PrimaryAddressID							AS PrimaryAddressID
			,FirstMailingAddress.MailingAddressID					AS MailingAddressID
			,bc_contact.Retired										AS Retired
			,bctl_contact.TYPECODE									AS ContactType
			,bc_contact.Name										AS FullName
			,bc_contact.FirstName									AS FirstName
			,bc_contact.MiddleName									AS MiddleName
			,bc_contact.LastName									AS LastName
			,bc_contact.Prefix										AS Prefix
			,bc_contact.Suffix										AS Suffix
			,bc_contact.Gender										AS Gender
			,bctl_primaryphonetype.TYPECODE							AS PrimaryPhoneType
			,bc_contact.WorkPhone									AS WorkPhone
			,bc_contact.HomePhone									AS HomePhone
			,bc_contact.CellPhone									AS CellPhone
			,CAST(NULL AS STRING)									AS MobilePhone
			,CAST(NULL AS STRING)									AS OtherPhoneOne
			,bc_contact.EmailAddress1								AS EmailAddress1
			,bc_contact.EmailAddress2								AS EmailAddress2
			,CAST(NULL AS STRING)									AS WebAddress
			,CAST(bc_contact.DateOfBirth AS DATETIME)				AS DateOfBirth
			,CAST(NULL AS INT)										AS ContactForSurvey
			,CAST(NULL AS INT)										AS RecMarketingMaterial
			,CAST(NULL AS STRING)									AS LongName
			,bc_contact.Occupation									AS Occupation
			,bctl_maritalstatus.Name								AS MaritalStatus
			,CAST(NULL AS STRING)									AS LinkID
			,CAST(NULL AS STRING)									AS PlatformOrgName
			,CAST(NULL AS STRING)									AS PlatformOrgId
			,CAST(NULL AS STRING)									AS JBTRating	--Applies only to Jeweler Roles
			,bc_credential.UserName									AS UserName
			,CAST(bc_contact.ObfuscatedInternal AS INT64)			AS Obfuscated
			
		FROM (SELECT * FROM `{project}.{bc_dataset}.bc_contact` WHERE _PARTITIONTIME = {partition_date}) AS bc_contact

			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	bc_contact.ID AS ContactID
						,MAX(bc_address_mailing.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{bc_dataset}.bc_contact` WHERE _PARTITIONTIME = {partition_date}) AS bc_contact
					LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_contactaddress` WHERE _PARTITIONTIME = {partition_date}) AS bc_contactaddress
						ON bc_contact.ID = bc_contactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address_mailing
						ON  bc_contactaddress.AddressID = bc_address_mailing.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{bc_dataset}.bctl_addresstype` AS bctl_addresstype
						ON bc_address_mailing.AddressType = bctl_addresstype.ID
						AND bctl_addresstype.TYPECODE = 'Mailing_JMIC'
				WHERE 1=1
				GROUP BY bc_contact.ID
			) FirstMailingAddress
				ON bc_contact.ID = FirstMailingAddress.ContactID

			LEFT JOIN `{project}.{bc_dataset}.bctl_contact` AS bctl_contact
				ON bc_contact.Subtype = bctl_contact.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_primaryphonetype` AS bctl_primaryphonetype
				ON bc_contact.PrimaryPhone = bctl_primaryphonetype.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_maritalstatus` AS bctl_maritalstatus
				ON bc_contact.MaritalStatus = bctl_maritalstatus.ID

			--Get UserName for Userid lookup 
			LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_user` WHERE bq_load_date = date({partition_date})) AS bc_user  
				ON bc_contact.ID = bc_user.ContactID  
			LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_credential` WHERE bq_load_date = date({partition_date})) AS bc_credential
				ON bc_credential.ID = bc_user.ContactID

		INNER JOIN Config sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN Config hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN Config hashingAlgo
			ON hashingAlgo.Key='HashAlgorithm'

		WHERE 1=1

	UNION ALL

	---CC---
		SELECT
			sourceConfig.Value AS SourceSystem
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,cc_contact.PublicID,hashKeySeparator.Value,'CC'))	AS ContactKey
			,'CC'													AS SourceCenter
			,cc_contact.PublicID									AS ContactPublicID
			,cc_contact.ID											AS ContactID
			,cc_contact.PrimaryAddressID							AS PrimaryAddressID
			,FirstMailingAddress.MailingAddressID					AS MailingAddressID
			,cc_contact.Retired										AS Retired
			,cctl_contact.TYPECODE									AS ContactType
			,cc_contact.Name										AS FullName
			,cc_contact.FirstName									AS FirstName
			,cc_contact.MiddleName									AS MiddleName
			,cc_contact.LastName									AS LastName
			,cc_contact.Prefix										AS Prefix
			,cc_contact.Suffix										AS Suffix
			,cc_contact.Gender										AS Gender
			,cctl_primaryphonetype.TYPECODE							AS PrimaryPhoneType
			,cc_contact.WorkPhone									AS WorkPhone
			,cc_contact.HomePhone									AS HomePhone
			,cc_contact.CellPhone									AS CellPhone
			,CAST(NULL AS STRING)									AS MobilePhone
			,CAST(NULL AS STRING)									AS OtherPhoneOne
			,cc_contact.EmailAddress1								AS EmailAddress1
			,cc_contact.EmailAddress2								AS EmailAddress2
			,CAST(NULL AS STRING)									AS WebAddress
			,CAST(cc_contact.DateOfBirth AS DATETIME)				AS DateOfBirth
			,CAST(NULL AS INT)										AS ContactForSurvey
			,CAST(NULL AS INT)										AS RecMarketingMaterial
			,CAST(NULL AS STRING)									AS LongName
			,cc_contact.Occupation									AS Occupation
			,cctl_maritalstatus.Name								AS MaritalStatus
			,CAST(NULL AS STRING)									AS LinkID
			,CAST(NULL AS STRING)									AS PlatformOrgName
			,CAST(NULL AS STRING)									AS PlatformOrgId
			,CAST(NULL AS STRING)									AS JBTRating	--Applies only to Jeweler Roles
			,cc_credential.UserName									AS UserName
			,CAST(cc_contact.ObfuscatedInternal AS INT64)			AS Obfuscated

		FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact

			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	cc_contact.ID AS ContactID
						,MAX(cc_address_mailing.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact
					LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contactaddress` WHERE _PARTITIONTIME = {partition_date}) AS cc_contactaddress
						ON cc_contact.ID = cc_contactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_mailing
						ON  cc_contactaddress.AddressID = cc_address_mailing.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{cc_dataset}.cctl_addresstype` AS cctl_addresstype
						ON cc_address_mailing.AddressType = cctl_addresstype.ID
						AND cctl_addresstype.TYPECODE = 'Mailing_JMIC'
				WHERE 1=1
				GROUP BY cc_contact.ID
			) FirstMailingAddress
				ON cc_contact.ID = FirstMailingAddress.ContactID

			LEFT JOIN `{project}.{cc_dataset}.cctl_contact` AS cctl_contact
				ON cc_contact.Subtype = cctl_contact.ID					
			LEFT JOIN `{project}.{cc_dataset}.cctl_primaryphonetype` AS cctl_primaryphonetype
				ON cc_contact.PrimaryPhone = cctl_primaryphonetype.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_maritalstatus` AS cctl_maritalstatus
					ON cc_contact.MaritalStatus = cctl_maritalstatus.ID
			--Get UserName for Userid lookup 
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_user` WHERE bq_load_date = date({partition_date})) AS cc_user
				ON cc_contact.ID = cc_user.ContactID  
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_credential` WHERE bq_load_date = date({partition_date})) AS cc_credential  
				ON cc_credential.ID = cc_user.CredentialID

		INNER JOIN Config sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN Config hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN Config hashingAlgo
			ON hashingAlgo.Key='HashAlgorithm'

		WHERE 1=1

	UNION ALL

	---AB---
		SELECT
			sourceConfig.Value AS SourceSystem
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,ab_abcontact.PublicID,hashKeySeparator.Value,'CM'))	AS ContactKey
			,'CM'													AS SourceCenter
			,ab_abcontact.PublicID									AS ContactPublicID
			,ab_abcontact.ID										AS ContactID
			,ab_abcontact.PrimaryAddressID							AS PrimaryAddressID
			,FirstMailingAddress.MailingAddressID					AS MailingAddressID
			,ab_abcontact.Retired									AS Retired
			,abtl_abcontact.TYPECODE								AS ContactType
			,ab_abcontact.Name										AS FullName
			,ab_abcontact.FirstName									AS FirstName
			,ab_abcontact.MiddleName								AS MiddleName
			,ab_abcontact.LastName									AS LastName
			,ab_abcontact.Prefix									AS Prefix
			,ab_abcontact.Suffix									AS Suffix
			,ab_abcontact.Gender									AS Gender
			,abtl_primaryphonetype.TYPECODE							AS PrimaryPhoneType
			,ab_abcontact.WorkPhone									AS WorkPhone
			,ab_abcontact.HomePhone									AS HomePhone
			,ab_abcontact.CellPhone									AS CellPhone
			,CAST(NULL AS STRING)									AS MobilePhone
			,CAST(NULL AS STRING)									AS OtherPhoneOne
			,ab_abcontact.EmailAddress1								AS EmailAddress1
			,ab_abcontact.EmailAddress2								AS EmailAddress2
			,ab_abcontact.Website_JMIC								AS WebAddress
			,CAST(ab_abcontact.DateOfBirth AS DATETIME)				AS DateOfBirth
			,CAST(ab_abcontact.ContactForSurvey_JMIC AS INT)		AS ContactForSurvey
			,CAST(ab_abcontact.RecMarketingMaterial_JMIC AS INT)	AS RecMarketingMaterial
			,ab_abcontact.LongName_JMIC								AS LongName
			,ab_abcontact.Occupation								AS Occupation
			,abtl_maritalstatus.Name								AS MaritalStatus
			,ab_abcontact.LinkID									AS LinkID
			,ab_abcontact.PlatformOrgName_JM						AS PlatformOrgName
			,ab_abcontact.PlatformOrgId_JM							AS PlatformOrgId
			,CAST(NULL AS STRING)									AS JBTRating	--Applies only to Jeweler Roles
			,ab_credential.UserName									AS UserName
			,CAST(NULL AS INT64)									AS Obfuscated
			
		FROM (SELECT * FROM `{project}.{cm_dataset}.ab_abcontact` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontact

			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	ab_abcontact.ID AS ContactID
						,MAX(ab_address_mailing.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{cm_dataset}.ab_abcontact` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontact
					LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_abcontactaddress` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontactaddress
						ON ab_abcontact.ID = ab_abcontactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address_mailing
						ON  ab_abcontactaddress.AddressID = ab_address_mailing.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{cm_dataset}.abtl_addresstype` AS abtl_addresstype
						ON ab_address_mailing.AddressType = abtl_addresstype.ID
						AND abtl_addresstype.TYPECODE = 'Mailing_JMIC'
				WHERE 1=1
				GROUP BY ab_abcontact.ID
			) FirstMailingAddress
				ON ab_abcontact.ID = FirstMailingAddress.ContactID

			LEFT JOIN `{project}.{cm_dataset}.abtl_abcontact` AS abtl_abcontact
				ON ab_abcontact.Subtype = abtl_abcontact.ID	
			LEFT JOIN `{project}.{cm_dataset}.abtl_primaryphonetype` AS abtl_primaryphonetype
				ON ab_abcontact.PrimaryPhone = abtl_primaryphonetype.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_maritalstatus` AS abtl_maritalstatus
				ON ab_abcontact.MaritalStatus = abtl_maritalstatus.ID

			--Get UserName for Userid lookup 
			LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_user` WHERE bq_load_date = date({partition_date})) AS ab_user
				ON ab_abcontact.ID = ab_user.ContactID
			LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_credential` WHERE bq_load_date = date({partition_date})) AS ab_credential  
				ON ab_credential.ID = ab_user.CredentialID

		INNER JOIN Config sourceConfig
			ON sourceConfig.Key='SourceSystem'
		INNER JOIN Config hashKeySeparator
			ON hashKeySeparator.Key='HashKeySeparator'
		INNER JOIN Config hashingAlgo
			ON hashingAlgo.Key='HashAlgorithm'

		WHERE 1=1

	) Contacts

--) outerselect
