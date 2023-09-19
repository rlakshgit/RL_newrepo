WITH Dispositions AS (
    SELECT distinct
      dispositionId
      ,dispositionName
    FROM `prod-edl.ref_nice.t_skills_dispositions_skills_detail`
    GROUP BY
      dispositionId
      ,dispositionName
), AccountHolders AS (
--There is something off on this query - Running this code for account 3000004851 in GW only gives one record. Here in Big Query it gives two.
    SELECT DISTINCT
                pc_contact.PublicId
                ,pc_account.AccountNumber
                ,pctl_accountcontactrole.NAME AccountContactRole
                ,pctl_contact.Name ContactType
                ,pc_contact.AccountHolderCount
                ,pc_contact.firstname AS FirstName
                ,pc_contact.lastname AS LastName
                ,pc_contact.DateOfBirth
                ,pc_contact.CellPhone
                ,pc_contact.HomePhone
                ,pc_contact.MobilePhone_JMIC
                ,pc_contact.FaxPhone
                ,pc_contact.OtherPhoneOne_JMIC
                ,pc_contact.WorkPhone
  FROM `prod-edl.ref_pc_current.pc_account` pc_account
  inner join `prod-edl.ref_pc_current.pc_accountcontact` pc_accountcontact                ON pc_accountcontact.Account = pc_account.id
  inner join `prod-edl.ref_pc_current.pc_acctholderedge` pc_acctholderedge                ON pc_acctholderedge.OwnerID = pc_account.ID
  inner join `prod-edl.ref_pc_current.pc_contact` pc_contact                              ON pc_contact.ID = pc_acctholderedge.ForeignEntityID
  inner join `prod-edl.ref_pc_current.pc_accountcontactrole` pc_accountcontactrole        ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
  inner join `prod-edl.ref_pc_current.pctl_accountcontactrole` pctl_accountcontactrole    ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
  left outer join `prod-edl.ref_pc_current.pctl_contact` pctl_contact                     ON pc_contact.Subtype = pctl_contact.ID

  WHERE 1=1
  AND pctl_accountcontactrole.NAME = 'AccountHolder'
  AND pctl_contact.NAME = 'Person'
  AND DATE(pc_account._PARTITIONTIME) =  DATE('{{ ds }}')
  AND DATE(pc_acctholderedge._PARTITIONTIME) =  DATE('{{ ds }}')
  AND DATE(pc_contact._PARTITIONTIME) =  DATE('{{ ds }}')
  AND DATE(pc_accountcontact._PARTITIONTIME) =  DATE('{{ ds }}')
  AND DATE(pc_accountcontactrole._PARTITIONTIME) =  DATE('{{ ds }}')
  AND DATE(pc_contact._PARTITIONTIME) =  DATE('{{ ds }}')
  AND AccountHolderCount > 0

)
,CTE AS (
  SELECT
  base_api_contactId
  ,t_contact_details.masterContactId
  ,REPLACE(REPLACE(contactStart, 'T', ' '), 'Z', ' ') AS contactStart
  ,fromAddr
  ,toAddr
  ,teamName
  ,campaignName
  ,pointOfContactName
  ,skillName
  ,t_contact_details.firstName 	AS AgentFirstName
  ,t_contact_details.lastName	  AS AgentLastName
  ,REGEXP_EXTRACT(dispositionNotes, r"\b3[0-9]{9}\b") AS accountNumber
  --,dispositionNotes
   ,CASE WHEN transferIndicatorName= 'None' THEN NULL END AS transferIndicatorName
  ,mediaTypeName
  ,abandonSeconds
  ,abandoned
  ,isOutbound
  ,isRefused
  ,isShortAbandon
  ,isTakeover
  ,serviceLevelFlag
  ,primary.dispositionName      AS primaryDispositionName
  ,secondary.dispositionName    AS secondaryDispositionName

  FROM `prod-edl.ref_nice.t_contact_details` t_contact_details

  LEFT JOIN Dispositions primary        ON primary.dispositionId = t_contact_details.primaryDispositionId
  LEFT JOIN Dispositions secondary      ON secondary.dispositionId = t_contact_details.secondaryDispositionId
  WHERE DATE(_PARTITIONTIME) = DATE('{{ ds }}')
  AND ( LEFT(campaignName, 2) = 'PL' OR campaignName = 'Partner_Platform' )
)

SELECT
  base_api_contactId
  ,CASE WHEN fromAddr IN (CellPhone, HomePhone, MobilePhone_JMIC, FaxPhone, OtherPhoneOne_JMIC, WorkPhone) THEN AccountHolders.PublicId ELSE NULL END  AS contactId
 --,masterContactId
  ,contactStart
  ,fromAddr
  ,toAddr  -- For purposes of sending out sample data
  ,teamName
  ,campaignName
  ,pointOfContactName
  ,skillName
  , AgentFirstName firstName
  , AgentLastName lastName
  , CTE.accountNumber accountNumber
  , transferIndicatorName
  ,mediaTypeName
  ,abandonSeconds
  ,abandoned
  ,isOutbound
  ,isRefused
  ,isShortAbandon
  ,isTakeover
  ,serviceLevelFlag
  , primaryDispositionName
  , secondaryDispositionName

FROM CTE
LEFT JOIN AccountHolders              ON AccountHolders.AccountNumber  = CTE.accountNumber
ORDER BY fromAddr, ContactStart