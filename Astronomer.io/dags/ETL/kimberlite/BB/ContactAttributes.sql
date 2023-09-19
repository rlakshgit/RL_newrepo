-- tag: ContactAttributes - tag ends/
/****************************************************************************************************************
	Building Block Extract
		ContactAttributes.sql
			Converted to BigQuery

	DESC:	Contains contact attributes for primary and mailing addresses. Built from Kimberlite core contact and address tables.

	NOTES:	Pulls from Kimberlite tables: Contact and Address (need to design/build)

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/09/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--DWH Sources
--sp_helptext 'bi_stage.spSTG_DimContact_Extract_PC'
--bi_stage.spSTG_DimContact_Extract_BC

--select * from bi_dds.DimContact where ContactKey IN (3146294811, 3146294781)
--Select * from (SELECT * FROM `{project}.{pc_dataset}.pc_contact order by CreateTime desc
*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_ContactAttributes`
AS SELECT outerselect.*
FROM (
) outerselect
*/
INSERT INTO `{project}.{dest_dataset}.ContactAttributes` (
	SourceSystem
	--,ContactAddressKey
	,ContactKey
	,SourceCenter
	,ContactPublicID
	,AddressPublicID
	,PrimaryAddressID
	,MailingAddressID
	,ContactType
	,IsContactRetired
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
	,IsPrimaryAddressRetired
	--,PrimaryAddressType
	,PrimaryAddressStatusCode
	,PrimaryAddressCode
	,PrimaryAddressTypeCode
	,PrimaryAddressLine1
	,PrimaryAddressLine2
	,PrimaryAddressLine3
	,PrimaryAddressCity
	,PrimaryAddressCounty
	,PrimaryAddressStateCode
	,PrimaryAddressPostalCode
	,PrimaryAddressCountryCode
	,PrimaryAddressFIPSCode
	,IsMailingAddressRetired
	--,MailingAddressType
	,MailingAddressStatusCode
	,MailingAddressCode
	,MailingAddressLine
	,MailingAddressLine2
	,MailingAddressLine3
	,MailingAddressCity
	,MailingAddressCounty
	,MailingAddressStateCode
	,MailingAddressPostalCode
	,MailingAddressCountryCode
	,LinkID
	,PlatformOrgName
	,PlatformOrgId
	,JBTRating
	,Username
	,IsServiceAccount
	,Obfuscated
	,bq_load_date
)

SELECT innerselect.* FROM 
	(
	SELECT
			Contact.SourceSystem
			--,COALESCE(address_primary.AddressKey,address_mailing.AddressKey) AS ContactAddressKey
			,ContactKey
			,Contact.SourceCenter
			,ContactPublicID
			,COALESCE(address_primary.AddressPublicID,address_mailing.AddressPublicID) AS AddressPublicID
			,PrimaryAddressID
			,MailingAddressID
			,ContactType
			,CASE WHEN Contact.Retired = 0 THEN 0 ELSE 1 END		AS IsContactRetired
			,COALESCE(Contact.FullName,CONCAT(COALESCE
				(CONCAT(Contact.FirstName,' '),'')
				,COALESCE(Contact.LastName,'')))					AS FullName
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

			--Primary Address Info
			,CASE WHEN address_primary.Retired = 0 THEN 0
				ELSE 1 END											AS IsPrimaryAddressRetired
			--,pctl_addresstype_primary.NAME							AS PrimaryAddressType
			,address_primary.AddressStatusCode						AS PrimaryAddressStatusCode
			,address_primary.AddressCode							AS PrimaryAddressCode
			,address_primary.AddressTypeCode						AS PrimaryAddressTypeCode
			,address_primary.AddressLine1							AS PrimaryAddressLine1
			,address_primary.AddressLine2							AS PrimaryAddressLine2
			,address_primary.AddressLine3							AS PrimaryAddressLine3
			,address_primary.City									AS PrimaryAddressCity
			,address_primary.County									AS PrimaryAddressCounty
			,address_primary.StateCode								AS PrimaryAddressStateCode
			,address_primary.PostalCode								AS PrimaryAddressPostalCode
			,address_primary.CountryCode							AS PrimaryAddressCountryCode
			,address_primary.FIPSCode								AS PrimaryAddressFIPSCode
			--,CASE pctl_country_primary.TYPECODE
			--		WHEN 'US' THEN 'United States'
			--		WHEN 'CA' THEN 'Canada'
			--		ELSE pctl_country_primary.NAME
			--	END													AS PrimaryAddressCountry

		--Mailing Address (default to smae source as DWH as first option)
			--,pctl_addresstype_mailing.NAME							AS MailingAddressType
			,CASE WHEN address_mailing.Retired = 0 THEN 0
				ELSE 1 END											AS IsMailingAddressRetired
			,address_mailing.AddressStatusCode						AS MailingAddressStatusCode
			,address_mailing.AddressCode							AS MailingAddressCode
			,address_mailing.AddressLine1							AS MailingAddressLine
			,address_mailing.AddressLine2							AS MailingAddressLine2
			,address_mailing.AddressLine3							AS MailingAddressLine3
			,address_mailing.City									AS MailingAddressCity
			,address_mailing.County									AS MailingAddressCounty
			,address_mailing.StateCode								AS MailingAddressStateCode
			,address_mailing.PostalCode								AS MailingAddressPostalCode
			,address_mailing.CountryCode							AS MailingAddressCountryCode

			,Contact.LinkID
			,Contact.PlatformOrgName
			,Contact.PlatformOrgId
			,Contact.JBTRating
			,LOWER(UserName)											AS Username
			,CASE WHEN UserName IS NULL THEN 0 
				WHEN Username IN ('svc%','Renewal%','batch%','Sys%')
				THEN 1 ELSE 0 END										AS IsServiceAccount
			,Contact.Obfuscated											AS Obfuscated
			,DATE('{date}')												AS bq_load_date
			--,CURRENT_DATE() AS bq_load_date

	FROM
			(SELECT * FROM `{project}.{core_dataset}.Contact` WHERE bq_load_date = DATE({partition_date})) AS Contact
			--GROUP 1: Primary Address
			LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date})) AS address_primary
				ON Contact.PrimaryAddressID = address_primary.AddressID
				AND Contact.SourceCenter = address_primary.SourceCenter
			LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.Address` WHERE bq_load_date = DATE({partition_date})) AS address_mailing
				ON Contact.MailingAddressID = address_mailing.AddressID
				AND Contact.SourceCenter = address_mailing.SourceCenter
				--AND address_mailing.AddressTypeCode = 'Mailing_JMIC'
	) innerselect
	WHERE COALESCE(PrimaryAddressID,MailingAddressID) IS NOT NULL
-- ) outerselect
/*
DQ Checks

SELECT  ContactKey
FROM `qa-edl.B_QA_ref_kimberlite.dar_ContactAttributes`
GROUP BY ContactKey
HAVING COUNT(ContactKey) > 1

*/

/* This code belongs in Kimberlite Address


			--GROUP 1: Primary Address									
			LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address_primary
				ON bc_contact.PrimaryAddressID = bc_address_primary.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_addresstype` AS bctl_addresstype					
				ON bc_address_primary.AddressType = bctl_addresstype.ID	
			LEFT JOIN `{project}.{bc_dataset}.bctl_state` AS bctl_state_primary
				ON bc_address_primary.State = bctl_state_primary.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_country` AS bctl_country_primary
				ON bc_address_primary.Country = bctl_country_primary.ID

			

			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	bc_contact.ID AS ContactID
						,MAX(bc_address.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{bc_dataset}.bc_contact` WHERE _PARTITIONTIME = {partition_date}) AS bc_contact
					LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_contactaddress` WHERE _PARTITIONTIME = {partition_date}) AS bc_contactaddress
						ON bc_contact.ID = bc_contactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address
						ON bc_contactaddress.AddressID = bc_address.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{bc_dataset}.bctl_addresstype` AS bctl_addresstype
						ON bc_address.AddressType = bctl_addresstype.ID
						AND bctl_addresstype.TYPECODE = 'Mailing_JMIC'
				GROUP BY bc_contact.ID
			) FirstMailingAddress
				ON bc_contact.ID = FirstMailingAddress.ContactID
			LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address_mailing_1
				ON FirstMailingAddress.MailingAddressID = bc_address_mailing_1.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_state` AS bctl_state_mailing_1
				ON bc_address_mailing_1.State = bctl_state_mailing_1.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_country` AS bctl_country_mailing_1
				ON bc_address_mailing_1.Country = bctl_country_mailing_1.ID	


				
			--GROUP 1: Primary Address									
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_primary
				ON cc_contact.PrimaryAddressID = cc_address_primary.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_addresstype` AS cctl_addresstype					
				ON cc_address_primary.AddressType = cctl_addresstype.ID	
			LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_primary
				ON cc_address_primary.State = cctl_state_primary.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS cctl_country_primary
				ON cc_address_primary.Country = cctl_country_primary.ID

		
			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	cc_contact.ID AS ContactID
						,MAX(cc_address.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{cc_dataset}.cc_contact` WHERE _PARTITIONTIME = {partition_date}) AS cc_contact
					LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_contactaddress` WHERE _PARTITIONTIME = {partition_date}) AS cc_contactaddress
						ON cc_contact.ID = cc_contactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
						ON cc_contactaddress.AddressID = cc_address.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{cc_dataset}.cctl_addresstype` AS cctl_addresstype
						ON cc_address.AddressType = cctl_addresstype.ID
						AND cctl_addresstype.TYPECODE = 'Mailing_JMIC'
				GROUP BY cc_contact.ID
			) FirstMailingAddress
				ON cc_contact.ID = FirstMailingAddress.ContactID
			LEFT JOIN (SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address_mailing_1
				ON FirstMailingAddress.MailingAddressID = cc_address_mailing_1.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state_mailing_1
				ON cc_address_mailing_1.State = cctl_state_mailing_1.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS cctl_country_mailing_1
				ON cc_address_mailing_1.Country = cctl_country_mailing_1.ID

		
			--GROUP 1: Primary Address									
			LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address_primary
				ON ab_abcontact.PrimaryAddressID = ab_address_primary.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_addresstype` AS abtl_addresstype					
				ON ab_address_primary.AddressType = abtl_addresstype.ID	
			LEFT JOIN `{project}.{cm_dataset}.abtl_state` AS abtl_state_primary
				ON ab_address_primary.State = abtl_state_primary.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_country` AS abtl_country_primary
				ON ab_address_primary.Country = abtl_country_primary.ID

		
			--GROUP 1: Same as DWH, Get mailing Address Info -- Mailing_JMIC
			LEFT JOIN (
				SELECT	ab_abcontact.ID AS ContactID
						,MAX(ab_address.ID) AS MailingAddressID
				FROM (SELECT * FROM `{project}.{cm_dataset}.ab_abcontact` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontact
					LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_abcontactaddress` WHERE _PARTITIONTIME = {partition_date}) AS ab_abcontactaddress
						ON ab_abcontact.ID = ab_abcontactaddress.ContactID
					LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address
						ON ab_abcontactaddress.AddressID = ab_address.ID --May reference via contactaddress intersect table
					INNER JOIN `{project}.{cm_dataset}.abtl_addresstype` AS abtl_addresstype
						ON ab_address.AddressType = abtl_addresstype.ID
						AND abtl_addresstype.TYPECODE = 'Mailing_JMIC'
				GROUP BY ab_abcontact.ID
			) FirstMailingAddress
				ON ab_abcontact.ID = FirstMailingAddress.ContactID
			LEFT JOIN (SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address_mailing_1
				ON FirstMailingAddress.MailingAddressID = ab_address_mailing_1.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_state` AS abtl_state_mailing_1
				ON ab_address_mailing_1.State = abtl_state_mailing_1.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_country` AS abtl_country_mailing_1
				ON ab_address_mailing_1.Country = abtl_country_mailing_1.ID


 

*/