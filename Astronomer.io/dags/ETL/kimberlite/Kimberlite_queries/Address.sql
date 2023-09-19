-- tag: Address - tag ends/
/****************************************************************************************************************
	Kimberlite Extract
		Address.sql
			Converted to BigQuery

	DESC:	Contains all addresses and key attributes from all four GW centers.
	NOTES:	Need to confirm Key here connects to other Account/Contact tables; then how do the BB layer tables connect to the "contact" fields in Core or BB tables (should it be PublicID or Hashed Key?

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/13/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--DWH Sources
--bi_stage.spSTG_DimContact_Extract_PC
--bi_stage.spSTG_DimContact_Extract_BC
--CC & AB

*******************************************************************************************************************/
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_Address`
AS SELECT outerselect.*
FROM (
--) outerselect
*/
INSERT INTO `{project}.{dest_dataset}.Address` (
	SourceSystem
	,AddressKey
	,SourceCenter
	,AddressPublicID
	,AddressID
	,AddressCode
	,AddressTypeCode
	,AddressLine1
	,AddressLine2
	,AddressLine3
	,City
	,County
	,StateCode
	,PostalCode
	,CountryCode
	,FIPSCode
	,AddressStatusCode
	,Retired
	,IsObfuscated
	,bq_load_date
)

WITH Config AS (
	SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
	SELECT 'HashKeySeparator','_' UNION ALL
	SELECT 'HashAlgorithm', 'SHA2_256'
)
	SELECT
		sourceConfig.Value															AS SourceSystem
		,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,AddressPublicID,hashKeySeparator.Value,SourceCenter))	AS AddressKey
		,SourceCenter
		,AddressPublicID
		,AddressID
		,AddressCode
		,AddressTypeCode
		,AddressLine1
		,AddressLine2
		,AddressLine3
		,City
		,County
		,StateCode
		,PostalCode
		,CountryCode
		,FIPSCode
		,AddressStatusCode
		,Retired
		,CAST(IsObfuscated As INT64)													AS IsObfuscated
		,DATE('{date}')																AS bq_load_date
		--,CURRENT_DATE() AS bq_load_date
	FROM 
	(
		SELECT
			'PC'								AS SourceCenter
			,pc_address.PublicID				AS AddressPublicID
			,pc_address.ID						AS AddressID
			,pctl_address.TYPECODE				AS AddressCode
			,pctl_addresstype.TYPECODE			AS AddressTypeCode
			,AddressLine1						AS AddressLine1
			,AddressLine2						AS AddressLine2
			,AddressLine3						AS AddressLine3
			,City								AS City
			,County								AS County
			,pctl_state.TYPECODE				AS StateCode
			,PostalCode							AS PostalCode
			,pctl_country.TYPECODE				AS CountryCode
			,pc_address.FIPSCode_JMIC			AS FIPSCode
			,pctl_addressstatus_jmic.TYPECODE	AS AddressStatusCode
			,pc_address.Retired					AS Retired
			--,CASE WHEN pc_address.Retired = 0 THEN 0 ELSE 1 END		AS IsRetired		--Move to BB
			,pc_address.ObfuscatedInternal		AS IsObfuscated

		FROM 
			(SELECT * FROM `{project}.{pc_dataset}.pc_address` WHERE _PARTITIONTIME = {partition_date}) AS pc_address
			INNER JOIN `{project}.{pc_dataset}.pctl_address` AS pctl_address
				ON pctl_address.ID = pc_address.Subtype
			LEFT JOIN `{project}.{pc_dataset}.pctl_addresstype` AS pctl_addresstype
				ON pc_address.AddressType = pctl_addresstype.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_state` AS pctl_state
				ON pc_address.State = pctl_state.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_country` AS pctl_country
				ON pc_address.Country = pctl_country.ID
			LEFT JOIN `{project}.{pc_dataset}.pctl_addressstatus_jmic` AS pctl_addressstatus_jmic
				ON pctl_addressstatus_jmic.ID = pc_address.AddressStatus_JMIC
	
	UNION ALL

		SELECT
			'BC'								AS SourceCenter
			,bc_address.PublicID				AS AddressPublicID
			,bc_address.ID						AS AddressID
			,bctl_address.TYPECODE				AS AddressCode
			,bctl_addresstype.TYPECODE			AS AddressTypeCode
			,AddressLine1						AS AddressLine1
			,AddressLine2						AS AddressLine2
			,AddressLine3						AS AddressLine3
			,City								AS City
			,County								AS County
			,bctl_state.TYPECODE				AS StateCode
			,PostalCode							AS PostalCode
			,bctl_country.TYPECODE				AS CountryCode
			,bc_address.FIPSCode_JMIC			AS FIPSCode
			,bctl_addressstatus_jmic.TYPECODE	AS AddressStatusCode
			,bc_address.Retired					AS Retired
			--,CASE WHEN bc_address.Retired = 0 THEN 0 ELSE 1 END		AS IsRetired		--Move to BB
			,bc_address.ObfuscatedInternal		AS IsObfuscated

		FROM 
			(SELECT * FROM `{project}.{bc_dataset}.bc_address` WHERE _PARTITIONTIME = {partition_date}) AS bc_address
			INNER JOIN `{project}.{bc_dataset}.bctl_address` AS bctl_address
				ON bctl_address.ID = bc_address.Subtype
			LEFT JOIN `{project}.{bc_dataset}.bctl_addresstype` AS bctl_addresstype
				ON bc_address.AddressType = bctl_addresstype.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_state` AS bctl_state
				ON bc_address.State = bctl_state.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_country` AS bctl_country
				ON bc_address.Country = bctl_country.ID
			LEFT JOIN `{project}.{bc_dataset}.bctl_addressstatus_jmic` AS bctl_addressstatus_jmic
				ON bctl_addressstatus_jmic.ID = bc_address.AddressStatus_JMIC

	UNION ALL

		SELECT
			'CC'								AS SourceCenter
			,cc_address.PublicID				AS AddressPublicID
			,cc_address.ID						AS AddressID
			,cctl_address.TYPECODE				AS AddressCode
			,cctl_addresstype.TYPECODE			AS AddressTypeCode
			,AddressLine1						AS AddressLine1
			,AddressLine2						AS AddressLine2
			,AddressLine3						AS AddressLine3
			,City								AS City
			,County								AS County
			,cctl_state.TYPECODE				AS StateCode
			,PostalCode							AS PostalCode
			,cctl_country.TYPECODE				AS CountryCode
			,cc_address.FIPSCode_JMIC			AS FIPSCode
			,cctl_addressstatus_jmic.TYPECODE	AS AddressStatusCode
			,cc_address.Retired					AS Retired
			--,CASE WHEN cc_address.Retired = 0 THEN 0 ELSE 1 END		AS IsRetired		--Move to BB
			,cc_address.ObfuscatedInternal		AS IsObfuscated

		FROM 
			(SELECT * FROM `{project}.{cc_dataset}.cc_address` WHERE _PARTITIONTIME = {partition_date}) AS cc_address
			INNER JOIN `{project}.{cc_dataset}.cctl_address` AS cctl_address
				ON cctl_address.ID = cc_address.Subtype
			LEFT JOIN `{project}.{cc_dataset}.cctl_addresstype` AS cctl_addresstype
				ON cc_address.AddressType = cctl_addresstype.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_state` AS cctl_state
				ON cc_address.State = cctl_state.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_country` AS cctl_country
				ON cc_address.Country = cctl_country.ID
			LEFT JOIN `{project}.{cc_dataset}.cctl_addressstatus_jmic` AS cctl_addressstatus_jmic
				ON cctl_addressstatus_jmic.ID = cc_address.AddressStatus_JMIC
				
	UNION ALL

		SELECT
			'CM'								AS SourceCenter
			,ab_address.PublicID				AS AddressPublicID
			,ab_address.ID						AS AddressID
			,abtl_address.TYPECODE				AS AddressCode
			,abtl_addresstype.TYPECODE			AS AddressTypeCode
			,AddressLine1						AS AddressLine1
			,AddressLine2						AS AddressLine2
			,AddressLine3						AS AddressLine3
			,City								AS City
			,County								AS County
			,abtl_state.TYPECODE				AS StateCode
			,PostalCode							AS PostalCode
			,abtl_country.TYPECODE				AS CountryCode
			,ab_address.FIPSCode_JMIC			AS FIPSCode
			,abtl_addressstatus_jmic.TYPECODE	AS AddressStatusCode
			,ab_address.Retired					AS Retired
			--,CASE WHEN ab_address.Retired = 0 THEN 0 ELSE 1 END		AS IsRetired		--Move to BB
			,ab_address.ObfuscatedInternal		AS IsObfuscated

		FROM 
			(SELECT * FROM `{project}.{cm_dataset}.ab_address` WHERE _PARTITIONTIME = {partition_date}) AS ab_address
			INNER JOIN `{project}.{cm_dataset}.abtl_address` AS abtl_address
				ON abtl_address.ID = ab_address.Subtype
			LEFT JOIN `{project}.{cm_dataset}.abtl_addresstype` AS abtl_addresstype
				ON ab_address.AddressType = abtl_addresstype.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_state` AS abtl_state
				ON ab_address.State = abtl_state.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_country` AS abtl_country
				ON ab_address.Country = abtl_country.ID
			LEFT JOIN `{project}.{cm_dataset}.abtl_addressstatus_jmic` AS abtl_addressstatus_jmic
				ON abtl_addressstatus_jmic.ID = ab_address.AddressStatus_JMIC

	) Addresslist

	INNER JOIN Config sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN Config hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN Config hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'
		
--) outerselect