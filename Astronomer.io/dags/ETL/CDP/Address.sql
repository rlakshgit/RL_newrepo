/* Query for Customer Data Feed
I think we want to include county as well. We do so much with that.
v2 - add mailing address as type
v3 - [X] Remove description - what is that?
v4 - [X] Remove address type
v5 - 
[X] - Remove dependency on trxn summary table
[X] - Pull in mailing address logic from DW (3 sources) to match customer feed

20201113 Needs:
[ ] - Filter to transactions that are PL only (unless 'person' works... need to investigate)
*/

SELECT 
	AddressID
	,AddressLine1
	,AddressLine2
	,City
	,Zip
	,ZipExt
	,State
	,Country
	,County
FROM (
	--Get Primary Addresses
	SELECT --DISTINCT TOP 1000
		pc_address.PublicID							AS AddressID
		,pc_address.AddressLine1
		,pc_address.AddressLine2
		,pc_address.City
		,CASE WHEN pc_address.PostalCode LIKE '%-%' THEN LEFT(pc_address.PostalCode, 5) ELSE pc_address.PostalCode END AS Zip
		,CASE WHEN pc_address.PostalCode LIKE '%-%' THEN RIGHT(pc_address.PostalCode, 4) ELSE NULL END AS ZipExt
		,pctl_state.NAME							AS State
		,pctl_country.NAME							AS Country
		,pc_address.County
	
	FROM {prefix}{pc_dataset}.pc_contact 
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact  ON pc_contact.ID = pc_accountcontact.Contact
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_address WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_address ON pc_address.ID = pc_contact.PrimaryAddressID
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_state ON pctl_state.ID = pc_address.State
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_country ON pctl_country.ID = pc_address.Country
 
	WHERE 1=1
	AND	pctl_contact.ID = 2					--Person
	AND pctl_accountcontactrole.ID = 16		--NamedInsured /*14 = Locarted With, 2 = AccountHolder */
	AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
    AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    -- AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
   -- AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
	--AND pc_contact.ID = 79327

	UNION DISTINCT  

	-- GROUP 2
	--Get Mailing Addresses
	SELECT --DISTINCT TOP 1000 
		mailing.PublicID								AS AddressID
		,mailing.AddressLine1
		,mailing.AddressLine2
		,mailing.City
		,CASE WHEN mailing.PostalCode LIKE '%-%' THEN LEFT(mailing.PostalCode, 5) ELSE mailing.PostalCode END AS Zip
		,CASE WHEN mailing.PostalCode LIKE '%-%' THEN RIGHT(mailing.PostalCode, 4) ELSE NULL END AS ZipExt
		,pctl_state.NAME							AS State
		,pctl_country.NAME							AS Country
		,mailing.County
	
	FROM {prefix}{pc_dataset}.pc_contact 
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
	INNER JOIN {prefix}{pc_dataset}.pc_address mailing ON mailing.ID = pc_contact.MailingAddress_JMIC
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_state ON pctl_state.ID = mailing.State
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_country	ON pctl_country.ID = mailing.Country
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
 
	WHERE 1=1
	AND	pctl_contact.ID = 2					--Person
	AND pctl_accountcontactrole.ID = 16		--NamedInsured /*14 = Locarted With, 2 = AccountHolder */
	AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
    AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
   -- AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    AND DATE(mailing._PARTITIONTIME) = DATE('{date}')
	--AND pc_contact.ID = 79327


	UNION DISTINCT 

	-- GROUP 1
	-- Get Mailing Address Info - Mailing_FFR_JMIC
	(SELECT DISTINCT  
		PC_ADDRESS_MAILING_1.PublicID								AS AddressID
		,PC_ADDRESS_MAILING_1.AddressLine1
		,PC_ADDRESS_MAILING_1.AddressLine2
		,PC_ADDRESS_MAILING_1.City
		,CASE WHEN PC_ADDRESS_MAILING_1.PostalCode LIKE '%-%' THEN LEFT(PC_ADDRESS_MAILING_1.PostalCode, 5) ELSE PC_ADDRESS_MAILING_1.PostalCode END AS Zip
		,CASE WHEN PC_ADDRESS_MAILING_1.PostalCode LIKE '%-%' THEN RIGHT(PC_ADDRESS_MAILING_1.PostalCode, 4) ELSE NULL END AS ZipExt
		,PCTL_STATE_MAILING_1.NAME					AS State
	--	,[FirstlkpMailingAddressGeogState] = PCTL_STATE_MAILING_1.TYPECODE
		,CASE PCTL_COUNTRY_MAILING_1.TYPECODE
			WHEN 'US' THEN 'United States'
			WHEN 'CA' THEN 'Canada'
			ELSE PCTL_COUNTRY_MAILING_1.NAME
		END AS Country
		,PC_ADDRESS_MAILING_1.County

	FROM {prefix}{pc_dataset}.pc_contact 
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
	LEFT OUTER JOIN (	SELECT pc_contact.ID ContactID
							, MAX(PC_ADDRESS.ID) as MailingAddressID
						FROM {prefix}{pc_dataset}.pc_contact 
							LEFT OUTER JOIN {prefix}{pc_dataset}.pc_contactaddress 
								on pc_contact.ID = pc_contactaddress.ContactID
                AND DATE(pc_contactaddress._PARTITIONTIME) = DATE('{date}')
                AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
                
							INNER JOIN {prefix}{pc_dataset}.pc_address 
								on pc_contactaddress.AddressID = pc_address.ID
                AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
                AND DATE(pc_contactaddress._PARTITIONTIME) = DATE('{date}')
                
							INNER JOIN {prefix}{pc_dataset}.pctl_addresstype 
								on PC_ADDRESS.AddressType = pctl_addresstype.ID
								and pctl_addresstype.TYPECODE = 'mailing_ffr_JMIC'
                AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
						GROUP BY pc_contact.ID
					) FirstMailingAddress
						ON pc_contact.ID = FirstMailingAddress.ContactID
					LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_address WHERE DATE(_PARTITIONTIME) = DATE('{date}')) PC_ADDRESS_MAILING_1
						ON FirstMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_1.ID
					LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_state PCTL_STATE_MAILING_1
						ON PC_ADDRESS_MAILING_1.State = PCTL_STATE_MAILING_1.ID
					LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_country PCTL_COUNTRY_MAILING_1
						ON PC_ADDRESS_MAILING_1.Country = PCTL_COUNTRY_MAILING_1.ID	

	WHERE 1=1
	AND	pctl_contact.ID = 2					--Person
	AND pctl_accountcontactrole.ID = 16		--NamedInsured /*14 = Locarted With, 2 = AccountHolder */
	AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
    AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}'))
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    --AND DATE(PC_ADDRESS_MAILING_1._PARTITIONTIME) = DATE('{date}'))
  
  --LIMIT 1000)
	--AND pc_contact.ID = 79327

	UNION DISTINCT 

	-- GROUP 3
	--Get mailing Address Info -- Mailing_JMIC
	SELECT --DISTINCT TOP 1000 
		PC_ADDRESS_MAILING_3.PublicID								AS AddressID
		,PC_ADDRESS_MAILING_3.AddressLine1
		,PC_ADDRESS_MAILING_3.AddressLine2
		,PC_ADDRESS_MAILING_3.City
		,CASE WHEN PC_ADDRESS_MAILING_3.PostalCode LIKE '%-%' THEN LEFT(PC_ADDRESS_MAILING_3.PostalCode, 5) ELSE PC_ADDRESS_MAILING_3.PostalCode END AS Zip
		,CASE WHEN PC_ADDRESS_MAILING_3.PostalCode LIKE '%-%' THEN RIGHT(PC_ADDRESS_MAILING_3.PostalCode, 4) ELSE NULL END AS ZipExt
		,PCTL_STATE_MAILING_3.NAME					AS State
	--	,[FirstlkpMailingAddressGeogState] = PC_ADDRESS_MAILING_3.TYPECODE
		,CASE PCTL_COUNTRY_MAILING_3.TYPECODE
			WHEN 'US' THEN 'United States'
			WHEN 'CA' THEN 'Canada'
			ELSE PCTL_COUNTRY_MAILING_3.NAME
		END AS Country
		,PC_ADDRESS_MAILING_3.County

	FROM {prefix}{pc_dataset}.pc_contact 
	LEFT OUTER JOIN  (select * from {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
	LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
	LEFT OUTER JOIN (	SELECT pc_contact.ID ContactID
							, MAX(PC_ADDRESS.ID) as MailingAddressID
						FROM {prefix}{pc_dataset}.pc_contact 
							LEFT OUTER JOIN {prefix}{pc_dataset}.pc_contactaddress 
								on pc_contact.ID = pc_contactaddress.ContactID
                AND DATE(pc_contactaddress._PARTITIONTIME) = DATE('{date}')
                AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
                
							LEFT OUTER JOIN {prefix}{pc_dataset}.pc_address 
								on pc_contactaddress.AddressID = pc_address.ID --May reference via contactaddress intersect table
                AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
                AND DATE(pc_contactaddress._PARTITIONTIME) = DATE('{date}')
							
              INNER JOIN {prefix}{pc_dataset}.pctl_addresstype 
								on PC_ADDRESS.AddressType = pctl_addresstype.ID
								and pctl_addresstype.TYPECODE = 'Mailing_JMIC'
                AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
						GROUP BY pc_contact.ID
					) ThirdMailingAddress
						on pc_contact.ID = ThirdMailingAddress.ContactID
					LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_address WHERE DATE(_PARTITIONTIME) = DATE('{date}')) PC_ADDRESS_MAILING_3
						ON ThirdMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_3.ID
					LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_state PCTL_STATE_MAILING_3 
						ON PC_ADDRESS_MAILING_3.State = PCTL_STATE_MAILING_3.ID
					LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_country PCTL_COUNTRY_MAILING_3 
						ON PC_ADDRESS_MAILING_3.Country = PCTL_COUNTRY_MAILING_3.ID	

	WHERE 1=1
	AND	pctl_contact.ID = 2					--Person
	AND pctl_accountcontactrole.ID = 16		--NamedInsured /*14 = Locarted With, 2 = AccountHolder */
	AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
	--AND pc_contact.ID = 79327
  AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    --AND DATE(PC_ADDRESS_MAILING_3._PARTITIONTIME) = DATE('{date}')
) AddressData
WHERE AddressData.AddressID IS NOT NULL