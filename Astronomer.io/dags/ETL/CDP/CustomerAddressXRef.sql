/* CustomerAddressXRef Query for Customer Data Feed

v2 - adding mailing as part of Xref

20201113 Needs:
[X] - Looks like mailing address almost always only used for CL / Agencies, not people, although we have 3 records. This doesn't match experience with Data Warehouse. Investigate how DW gets mailing address, mirror logic here.

v4
[X] - Pull in mailing address logic from DW (3 sources) (2,222,035)

*/

SELECT
	SourceCustomerNumber
	,AddressID
	,AddressType
FROM (
		SELECT
			pc_contact.PublicID								AS SourceCustomerNumber,
			pc_address.PublicID								AS AddressID,
			'Primary'										AS AddressType

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact  ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN(SELECT * FROM {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		INNER JOIN {prefix}{pc_dataset}.pc_address ON pc_address.ID = pc_contact.PrimaryAddressID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID

		WHERE 1=1
		AND	pctl_contact.ID = 2							--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
        AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
		--AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
		--AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
		--AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
        AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')



		--AND pc_contact.ID = 2319 --79327


		UNION Distinct

		-- GROUP 2
		-- Get Mailing Addresses
		(SELECT DISTINCT
			pc_contact.PublicID								AS SourceCustomerNumber,
			pc_address.PublicID								AS AddressID,
			'Mailing'										AS AddressType

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		INNER JOIN {prefix}{pc_dataset}.pc_address ON pc_address.ID = pc_contact.MailingAddress_JMIC
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID

		WHERE 1=1
		AND	pctl_contact.ID = 2							--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
    AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')

    AND DATE(pc_address._PARTITIONTIME) = DATE('{date}'))
    --LIMIT 1000)

		--AND pc_contact.ID = 2319 --79327

		UNION Distinct

		-- GROUP 1
		-- Get Mailing Address Info - Mailing_FFR_JMIC
		(SELECT
			pc_contact.PublicID								AS SourceCustomerNumber,
			FirstMailingAddress.PublicID					AS AddressID,
			'Mailing'										AS AddressType

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (SELECT * FROM {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		INNER JOIN (SELECT pc_contact.ID ContactID, pc_address.PublicID
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
								on pc_address.AddressType = pctl_addresstype.ID
								and pctl_addresstype.TYPECODE = 'mailing_ffr_JMIC'
                AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
						GROUP BY pc_contact.ID, pc_address.PublicID
					) FirstMailingAddress
						ON pc_contact.ID = FirstMailingAddress.ContactID
					LEFT OUTER JOIN (Select * from  {prefix}{pc_dataset}.pc_address WHERE DATE(_PARTITIONTIME) = DATE('{date}')) PC_ADDRESS_MAILING_1
						ON FirstMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_1.ID


		WHERE 1=1
		AND	pctl_contact.ID = 2					--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
       AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}'))


		UNION Distinct

		-- GROUP 3
		-- Get mailing Address Info -- Mailing_JMIC
		SELECT
			pc_contact.PublicID								AS SourceCustomerNumber,
			ThirdMailingAddress.PublicID					AS AddressID,
			'Mailing'										AS AddressType

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontact WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_account WHERE DATE(_PARTITIONTIME) = DATE('{date}')) pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontactrole WHERE DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		INNER JOIN (	SELECT pc_contact.ID ContactID, pc_address.PublicID
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
									on pc_address.AddressType = pctl_addresstype.ID
									and pctl_addresstype.TYPECODE = 'Mailing_JMIC'
                  AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
							GROUP BY pc_contact.ID, pc_address.PublicID
						) ThirdMailingAddress
							on pc_contact.ID = ThirdMailingAddress.ContactID
						LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_address WHERE DATE(_PARTITIONTIME) = DATE('{date}'))PC_ADDRESS_MAILING_3
							ON ThirdMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_3.ID

		WHERE 1=1
		AND	pctl_contact.ID = 2						--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)	--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')
    AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')



	) CustomerData
WHERE CustomerData.AddressID IS NOT NULL