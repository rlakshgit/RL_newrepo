with TrxnClaimContactRoleSubset as
(
   SELECT cc_transaction_inner.ClaimContactID, cctl_contactrole.TYPECODE, cctl_contactrole.NAME
  ,ROW_NUMBER() OVER ( PARTITION BY cc_transaction_inner.ClaimContactID ORDER BY cctl_contactrole.TYPECODE) rownumber
  FROM `{project}.{dataset}.cc_transaction` cc_transaction_inner
  INNER JOIN (select * from `{project}.{dataset}.cc_claimcontactrole`  WHERE DATE(_PARTITIONTIME) = '{date}') cc_claimcontactrole
  ON cc_transaction_inner.ClaimContactID = cc_claimcontactrole.ClaimContactID
  INNER JOIN `{project}.{dataset}.cctl_contactrole` cctl_contactrole
  ON cc_claimcontactrole.Role = cctl_contactrole.ID
  AND cctl_contactrole.TYPECODE in ('recoverypayer', 'checkpayee', 'recoveryonbehalfof') and cc_transaction_inner.bq_load_date='{date}'
),
TrxnClaimContactRoleSubsetCeded as
(
   SELECT cc_exposure_inner.ClaimantDenormID, cctl_contactrole.TYPECODE, cctl_contactrole.NAME
  ,ROW_NUMBER() OVER ( PARTITION BY cc_exposure_inner.ClaimantDenormID ORDER BY cctl_contactrole.TYPECODE) rownumber
  FROM `{project}.{dataset}.cc_exposure` cc_exposure_inner
  INNER JOIN (select * from `{project}.{dataset}.cc_claimcontactrole`  WHERE DATE(_PARTITIONTIME) = '{date}') cc_claimcontactrole
  ON cc_exposure_inner.ClaimantDenormID = cc_claimcontactrole.ClaimContactID
  INNER JOIN `{project}.{dataset}.cctl_contactrole` cctl_contactrole
  ON cc_claimcontactrole.Role = cctl_contactrole.ID
  AND cctl_contactrole.TYPECODE in ('recoverypayer', 'checkpayee', 'recoveryonbehalfof') and cc_exposure_inner.bq_load_date ='{date}'
),
cte_inner as
(
  SELECT
  cc_claim.claimnumber,
  cc_contact.FirstName ,
  cc_contact.LastName ,
  checkvendor.name ,
  checkvendor.FirstName ,
  checkVendor.LastName ,
  CASE WHEN tlt.TYPECODE IN('Payment', 'Recovery')
  THEN CASE WHEN c.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
  THEN CONCAT(COALESCE(cc_contact.FirstName,''),' ', COALESCE(cc_contact.LastName,''))
  ELSE COALESCE(checkvendor.name, CONCAT(COALESCE(checkvendor.FirstName,'') ,' ',COALESCE(checkVendor.LastName,'')))--get directly from check
  END ELSE NULL
  END AS ClaimPaymentContactFullName,
  CASE WHEN tlt.TYPECODE IN('Payment', 'Recovery')
  THEN CASE WHEN c.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
  THEN cc_contact.PublicID  --ID whould have never worked.
  ELSE checkvendor.PublicID
  END ELSE NULL
  END AS ClaimPaymentContactPublicID
  FROM `{project}.{dataset}.cc_transaction` cc_transaction
  LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_transactionlineitem` WHERE DATE(_PARTITIONTIME) ='{date}')  trslnit
  ON cc_transaction.ID = trslnit.TransactionID
  LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_claim` WHERE DATE(_PARTITIONTIME) = '{date}')  cc_claim
  ON cc_claim.ID = cc_transaction.ClaimID
  LEFT OUTER JOIN `{project}.{dataset}.cctl_transaction` tlt
  ON cc_transaction.SubType = tlt.ID
  LEFT OUTER JOIN  (select * from `{project}.{dataset}.cc_contact` WHERE DATE(_PARTITIONTIME) = '{date}') cc_contact
  ON cc_contact.ID = cc_transaction.ClaimContactID
  LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_check` WHERE DATE(_PARTITIONTIME) = '{date}') cc_check
  ON cc_check.ID = cc_transaction.CheckID
  LEFT OUTER JOIN (
  SELECT cccontact.Name, cccontact.PublicID, cccontact.FirstName, cccontact.LastName, cccontact.AddressBookUID, cccontact.ID
  ,ROW_NUMBER() OVER(PARTITION BY cccontact.AddressBookUID ORDER BY cccontact.ID DESC) rownumber
  FROM `{project}.{dataset}.cc_contact` cccontact  WHERE DATE(_PARTITIONTIME) = '{date}' ) checkvendor
  ON checkvendor.AddressBookUID = cc_check.VendorID_JMIC AND checkvendor.rownumber = 1
  LEFT OUTER JOIN (
  select * from TrxnClaimContactRoleSubset where rownumber = 1
  )c
  ON cc_transaction.ClaimContactID = c.ClaimContactID
  WHERE cc_transaction.bq_load_date  = '{date}'
),
cte_inner_ceded as
(
  SELECT
  CASE WHEN cctl_transaction.TYPECODE IN('Payment', 'Recovery')
    THEN CASE WHEN c.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
    THEN CONCAT(COALESCE(cc_contact.FirstName,''),' ', COALESCE(cc_contact.LastName,''))
    END ELSE NULL
  END AS ClaimPaymentContactFullName,
  CASE WHEN cctl_transaction.TYPECODE IN('Payment', 'Recovery')
    THEN CASE WHEN c.TYPECODE IN ('recoverypayer', 'checkpayee', 'recoveryonbehalfof')
    THEN cc_contact.PublicID  --ID whould have never worked.
    END ELSE NULL
  END AS ClaimPaymentContactPublicID
  FROM `{project}.{dataset}.cc_ritransaction` cc_ritransaction
  INNER JOIN (select * from `{project}.{dataset}.cc_claim` WHERE DATE(_PARTITIONTIME) = '{date}')  cc_claim ON cc_claim.ID = cc_ritransaction.ClaimID
  INNER JOIN (select * from `{project}.{dataset}.cc_policy` WHERE DATE(_PARTITIONTIME) = '{date}')  cc_policy ON cc_policy.ID = cc_claim.PolicyID
  LEFT OUTER JOIN `{project}.{dataset}.cctl_transaction` cctl_transaction ON cc_ritransaction.SubType = cctl_transaction.ID
  LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_exposure` WHERE DATE(_PARTITIONTIME) = '{date}')  cc_exposure ON cc_exposure.ID = cc_ritransaction.ExposureID
  LEFT OUTER JOIN  (select * from `{project}.{dataset}.cc_contact` WHERE DATE(_PARTITIONTIME) = '{date}') cc_contact ON cc_contact.ID = cc_exposure.ClaimantDenormID
  LEFT OUTER JOIN (select * from TrxnClaimContactRoleSubsetCeded where rownumber = 1)c  on  cc_exposure.ClaimantDenormID = c.ClaimantDenormID
  WHERE cc_ritransaction.bq_load_date  = '{date}'
) ,
direct_transaction
as
(select distinct
CAST(ClaimPayment.ClaimPaymentContactPublicID as STRING) ClaimPaymentContactPublicID
,CASE WHEN (ClaimPayment.ClaimPaymentContactFullName IS NULL OR ClaimPayment.ClaimPaymentContactFullName = ' ')
THEN CAST(initcap(cc_contact.NameDenorm) as STRING) ELSE CAST(initcap(ClaimPayment.ClaimPaymentContactFullName) as STRING) END AS ClaimPaymentContactFullName
,CAST(cctl_addresstype.NAME as STRING) AS AddressType
,CAST(cctl_contact.NAME as STRING) AS ContactType
,CAST(cc_address.AddressLine1 as STRING)AddressLine1
,CAST(cc_address.AddressLine2 as STRING)AddressLine2
,CAST(cc_address.PostalCode as STRING) PostalCode
,CASE WHEN cc_address.PostalCode LIKE '%-%' THEN CAST(LEFT(cc_address.PostalCode, 5) as STRING) ELSE CAST(cc_address.PostalCode as STRING) END AS Zip
,CASE WHEN cc_address.PostalCode LIKE '%-%' THEN CAST(RIGHT(cc_address.PostalCode, 4) as STRING) ELSE NULL END AS ZipExt
,CAST(cc_address.city as STRING) AS City
,CAST(cctl_state.NAME as STRING) AS State
,CAST(cc_address.county as STRING) AS County
,CAST(cctl_country.NAME as STRING) AS Country
,DATE("{{{{ ds }}}}") as bq_load_date
from cte_inner  ClaimPayment
INNER JOIN (select * from `{project}.{dataset}.cc_contact` WHERE DATE(_PARTITIONTIME) ='{date}')cc_contact ON cc_contact.PublicID = ClaimPayment.ClaimPaymentContactPublicID
LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_address` WHERE DATE(_PARTITIONTIME) = '{date}')cc_address on cc_address.ID =cc_contact.PrimaryAddressID
LEFT OUTER JOIN `{project}.{dataset}.cctl_addresstype` cctl_addresstype on cctl_addresstype.id = cc_address.AddressType
LEFT OUTER JOIN `{project}.{dataset}.cctl_contact` cctl_contact  ON cc_contact.Subtype = cctl_contact.ID
LEFT OUTER JOIN `{project}.{dataset}.cctl_state` cctl_state on cctl_state.id = cc_address.State
LEFT OUTER JOIN `{project}.{dataset}.cctl_country` cctl_country on cctl_country.ID = cc_address.Country
WHERE 1=1
),
ceded_transaction
as
(select distinct
CAST(ClaimPaymentCeded.ClaimPaymentContactPublicID as STRING) ClaimPaymentContactPublicID
,CASE WHEN (ClaimPaymentCeded.ClaimPaymentContactFullName IS NULL OR ClaimPaymentCeded.ClaimPaymentContactFullName = ' ')
THEN CAST(initcap(cc_contact.NameDenorm) as STRING) ELSE CAST(initcap(ClaimPaymentCeded.ClaimPaymentContactFullName) as STRING) END AS ClaimPaymentContactFullName
,CAST(cctl_addresstype.NAME as STRING) AS AddressType
,CAST(cctl_contact.NAME as STRING) AS ContactType
,CAST(cc_address.AddressLine1 as STRING)AddressLine1
,CAST(cc_address.AddressLine2 as STRING)AddressLine2
,CAST(cc_address.PostalCode as STRING) PostalCode
,CASE WHEN cc_address.PostalCode LIKE '%-%' THEN CAST(LEFT(cc_address.PostalCode, 5) as STRING) ELSE CAST(cc_address.PostalCode as STRING) END AS Zip
,CASE WHEN cc_address.PostalCode LIKE '%-%' THEN CAST(RIGHT(cc_address.PostalCode, 4) as STRING) ELSE NULL END AS ZipExt
,CAST(cc_address.city as STRING) AS City
,CAST(cctl_state.NAME as STRING) AS State
,CAST(cc_address.county as STRING) AS County
,CAST(cctl_country.NAME as STRING) AS Country
,DATE("{{{{ ds }}}}") as bq_load_date
from cte_inner_ceded  ClaimPaymentCeded
INNER JOIN (select * from `{project}.{dataset}.cc_contact` WHERE DATE(_PARTITIONTIME) ='{date}')cc_contact ON cc_contact.PublicID = ClaimPaymentCeded.ClaimPaymentContactPublicID
LEFT OUTER JOIN (select * from `{project}.{dataset}.cc_address` WHERE DATE(_PARTITIONTIME) = '{date}')cc_address on cc_address.ID =cc_contact.PrimaryAddressID
LEFT OUTER JOIN `{project}.{dataset}.cctl_addresstype` cctl_addresstype on cctl_addresstype.id = cc_address.AddressType
LEFT OUTER JOIN `{project}.{dataset}.cctl_contact` cctl_contact  ON cc_contact.Subtype = cctl_contact.ID
LEFT OUTER JOIN `{project}.{dataset}.cctl_state` cctl_state on cctl_state.id = cc_address.State
LEFT OUTER JOIN `{project}.{dataset}.cctl_country` cctl_country on cctl_country.ID = cc_address.Country
WHERE 1=1
)

select * from direct_transaction
union Distinct
select * from ceded_transaction