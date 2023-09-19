-- tag: AccountContactRole - tag ends/
/****************************************************************************************************************
	Kimberlite Extract
		AccountContactRole.sql
			BigQuery converted

	Descr: This table holds each unique AccountContactRole from the following GW systems: PC, BC
			Accounts do not exist in CC or AB (Contact Mgr)

-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	03/13/2023	DROBAK		Init

------------------------------------------------------------------------------------------------------------------
--select * from bi_dds.DimAccount where AccountNumber = '3000072802'
*******************************************************************************************************************/
-- Account Contact Roles --
/*
CREATE OR REPLACE TABLE `qa-edl.B_QA_ref_kimberlite.dar_AccountContactRole`
AS SELECT outerselect.*
FROM (
*/
INSERT INTO `{project}.{dest_dataset}.AccountContactRole` (
	SourceSystem
	,AccountContactRoleKey
	,AccountKey
	,ContactKey
	,SourceCenter
	,AccountContactRolePublicID
	,AccountPublicID
	,ContactPublicID
	,AccountID
	,IsAccountActive
	,RetiredAccount
	,AccountContactRoleType
	,AccountContactRoleName
	,LastUpdateTime
	,bq_load_date
)
	WITH Config AS (
		SELECT 'SourceSystem' AS Key,'GW' AS Value UNION ALL
		SELECT 'HashKeySeparator','_' UNION ALL
		SELECT 'HashAlgorithm', 'SHA2_256'
	)

	SELECT 
			sourceConfig.Value AS SourceSystem
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,AccountContactRolePublicID))	AS AccountContactRoleKey
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,AccountPublicID))	AS AccountKey
			,SHA256 (CONCAT(sourceConfig.Value,hashKeySeparator.Value,ContactPublicID,hashKeySeparator.Value,PC_BC_AccountContact.SourceCenter)) AS ContactKey
			,PC_BC_AccountContact.SourceCenter
			,PC_BC_AccountContact.AccountContactRolePublicID
			,PC_BC_AccountContact.AccountPublicID
			,PC_BC_AccountContact.ContactPublicID
			,PC_BC_AccountContact.AccountID
			--,PC_BC_AccountContact.ContactID
			,PC_BC_AccountContact.IsAccountActive
			,PC_BC_AccountContact.RetiredAccount
			,PC_BC_AccountContact.AccountContactRoleType
			,PC_BC_AccountContact.AccountContactRoleName
			,PC_BC_AccountContact.LastUpdateTime
			,DATE('{date}') AS bq_load_date
			--,CURRENT_DATE() AS bq_load_date	
	
	FROM (
			SELECT
				'PC'									AS SourceCenter
				,pc_accountcontact.Account				AS AccountID
				--,pc_accountcontact.Contact				AS ContactID
				,pc_account.PublicID					AS AccountPublicID
				,pc_contact.PublicID					AS ContactPublicID
				,CAST(pc_accountcontact.Active AS INT)	AS IsAccountActive
				,pc_accountcontact.Retired				AS RetiredAccount
				,pc_accountcontactrole.PublicID			AS AccountContactRolePublicID
				--,pc_accountcontactrole.AccountContact
				--,pctl_accountcontactrole.ID
				,pctl_accountcontactrole.TYPECODE		AS AccountContactRoleType
				,pctl_accountcontactrole.NAME			AS AccountContactRoleName
				,pc_accountcontact.LastUpdateTime		AS LastUpdateTime

			FROM (SELECT * FROM `{project}.{pc_dataset}.pc_accountcontact` WHERE _PARTITIONTIME = {partition_date}) AS pc_accountcontact
				LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_accountcontactrole` WHERE _PARTITIONTIME = {partition_date}) AS pc_accountcontactrole
					ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact	
				LEFT JOIN `{project}.{pc_dataset}.pctl_accountcontactrole` AS pctl_accountcontactrole
					ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
						ON pc_account.ID = pc_accountcontact.Account
				INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pc_contact
					ON pc_contact.ID = pc_accountcontact.Contact

			WHERE  1=1

		UNION ALL

			SELECT
				'BC'									AS SourceCenter
				,bc_accountcontact.AccountID			AS AccountID
				--,bc_accountcontact.ContactID			AS ContactID
				,bc_account.PublicID					AS AccountPublicID
				,bc_contact.PublicID					AS ContactPublicID
				,CAST(NULL AS INT)						AS IsAccountActive
				,bc_accountcontact.Retired				AS RetiredAccount
				,bc_accountcontactrole.PublicID			AS AccountContactRolePublicID
				,bctl_accountrole.TYPECODE				AS AccountContactRoleType
				,bctl_accountrole.NAME					AS AccountContactRoleName
				,bc_accountcontact.UpdateTime			AS LastUpdateTime
		
		--select bc_accountcontactrole.*
			FROM (SELECT * FROM `{project}.{bc_dataset}.bc_accountcontact` WHERE _PARTITIONTIME = {partition_date}) AS bc_accountcontact
				LEFT JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_accountcontactrole` WHERE _PARTITIONTIME = {partition_date}) AS bc_accountcontactrole
					ON bc_accountcontact.ID = bc_accountcontactrole.AccountContactID	
				LEFT JOIN `{project}.{bc_dataset}.bctl_accountrole` AS bctl_accountrole
					ON bc_accountcontactrole.Role = bctl_accountrole.ID
				INNER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_account` WHERE _PARTITIONTIME = {partition_date}) AS bc_account
					ON bc_account.ID = bc_accountcontact.AccountID
				INNER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_contact` WHERE _PARTITIONTIME = {partition_date}) AS bc_contact
					ON bc_contact.ID = bc_accountcontact.ContactID

			WHERE  1=1			

	) AS PC_BC_AccountContact

	INNER JOIN Config sourceConfig
		ON sourceConfig.Key='SourceSystem'
	INNER JOIN Config hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'
	INNER JOIN Config hashingAlgo
		ON hashingAlgo.Key='HashAlgorithm'

--) outerselect
