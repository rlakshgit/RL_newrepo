/* Query for Customer Data Feed

v6 - [X] Add allow contact for marketing

20201113 Needs:
[X] - Query optimization, maybe complete refactoring?? Extremely slow.

v8:
[X] - Pull in mailing address logic from DW (3 sources)  (2,106,586)
[X] - Added LocatedWith contact role to the query pull (2,116,660)

20201116 Needs:
[ ] - Dupes when linking to policyterm on PolicyNumber, using SELECT DISTINCT as a shortcut. Remove policyTerm dependency?
[ ] - Refactor to start with TransactionItem and only pull contacts that are associated as primary insured or wearer

*/
SELECT
	SourceCustomerNumber
	,FirstName
	,MiddleName
	,LastName
	,PrimaryEmailAddress
	,Gender
	,PrimaryPhoneNumber
	,PrimaryPhoneType
	,WorkPhone
	,CellPhone
	,HomePhone
	,MobilePhone
	,OtherPhone
	,BirthMonth
	,BirthYear
	,BirthDay
	,BirthDate
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(DateCreated as DATETIME)), ' UTC') as DateCreated
	,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(DateModfied as DATETIME)), ' UTC') as DateModified
	,AllowContactForMarketing
FROM (
	SELECT --TOP 1000
		pc_contact.Publicid AS SourceCustomerNumber
		,FirstName
		,MiddleName
		,LastName
		,COALESCE(EmailAddress1, EmailAddress2)							AS PrimaryEmailAddress
		,pctl_gendertype.NAME											AS Gender
		,COALESCE(
			CASE phone.TYPECODE
				WHEN 'work' THEN pc_contact.WorkPhone
				WHEN 'home'	THEN pc_contact.HomePhone
				WHEN 'mobile' THEN
					CASE pctl_contact.TYPECODE
						WHEN 'Person' THEN pc_contact.CellPhone
						WHEN 'Company' THEN pc_contact.MobilePhone_JMIC
						ELSE COALESCE(pc_contact.CellPhone, pc_contact.MobilePhone_JMIC)
					END
				WHEN 'Other_JMIC' THEN pc_contact.OtherPhoneOne_JMIC
			END, pc_contact.WorkPhone, pc_contact.HomePhone, pc_contact.CellPhone, pc_contact.OtherPhoneOne_JMIC) AS PrimaryPhoneNumber
		,phone.NAME PrimaryPhoneType
		,WorkPhone
		,CellPhone
		,HomePhone
		,MobilePhone_JMIC												AS MobilePhone
		,OtherPhoneOne_JMIC												AS OtherPhone
		,extract(month from DateOfBirth)												AS BirthMonth
		,extract(year from DateOfBirth)												AS BirthYear
		,extract(day from DateOfBirth)												AS BirthDay
		,CAST(DateOfBirth AS DATE)										AS BirthDate
		,pc_contact.CreateTime											AS DateCreated
		,pc_contact.UpdateTime											AS DateModfied
		,CASE pc_contact.RecMaterketingMaterial_JMIC_PL
			WHEN True THEN 'Yes'
			WHEN False THEN 'No'
		ELSE 'Unknown' END												AS AllowContactForMarketing
	-- SELECT COUNT(*)
		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontact Where DATE(_PARTITIONTIME) = DATE('{date}')) pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account Where DATE(_PARTITIONTIME) = DATE('{date}')) pc_account  ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		INNER JOIN {prefix}{pc_dataset}.pc_address ON pc_address.ID = pc_contact.PrimaryAddressID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_gendertype ON pctl_gendertype.ID = pc_contact.Gender
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_primaryphonetype phone ON phone.ID = pc_contact.PrimaryPhone

		WHERE 1=1
		AND	pctl_contact.ID = 2					--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')	/* how else to get rid of garbage ? */
		AND DATE(pc_contact._PARTITIONTIME) = DATE(DATE('{date}'))
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE(DATE('{date}'))
    --AND DATE(pc_account._PARTITIONTIME) = DATE(DATE('{date}'))
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE(DATE('{date}'))
      AND DATE(pc_address._PARTITIONTIME) = DATE(DATE('{date}'))
		--AND pc_contact.ID = 2319 --79327
		--AND COALESCE(pc_contact.Retired,0) = 0	/* not sure if we include this; if we join to Policy then no */

	UNION DISTINCT

		-- GROUP 2
		--Get Mailing Addresses
		SELECT --TOP 1000
		pc_contact.Publicid AS SourceCustomerNumber
		,FirstName
		,MiddleName
		,LastName
		,COALESCE(EmailAddress1, EmailAddress2)							AS PrimaryEmailAddress
		,pctl_gendertype.NAME											AS Gender
		,COALESCE(
			CASE phone.TYPECODE
				WHEN 'work' THEN pc_contact.WorkPhone
				WHEN 'home'	THEN pc_contact.HomePhone
				WHEN 'mobile' THEN
					CASE pctl_contact.TYPECODE
						WHEN 'Person' THEN pc_contact.CellPhone
						WHEN 'Company' THEN pc_contact.MobilePhone_JMIC
						ELSE COALESCE(pc_contact.CellPhone, pc_contact.MobilePhone_JMIC)
					END
				WHEN 'Other_JMIC' THEN pc_contact.OtherPhoneOne_JMIC
			END, pc_contact.WorkPhone, pc_contact.HomePhone, pc_contact.CellPhone, pc_contact.OtherPhoneOne_JMIC) AS PrimaryPhoneNumber
		,phone.NAME PrimaryPhoneType
		,WorkPhone
		,CellPhone
		,HomePhone
		,MobilePhone_JMIC												AS MobilePhone
		,OtherPhoneOne_JMIC												AS OtherPhone
		,extract(month from DateOfBirth)												AS BirthMonth
		,extract(year from DateOfBirth)												AS BirthYear
		,extract(day from DateOfBirth)												AS BirthDay
		,CAST(DateOfBirth AS DATE)										AS BirthDate
		,pc_contact.CreateTime											AS DateCreated
		,pc_contact.UpdateTime											AS DateModfied
		,CASE pc_contact.RecMaterketingMaterial_JMIC_PL
			WHEN True THEN 'Yes'
			WHEN False THEN 'No'
		ELSE 'Unknown' END												AS AllowContactForMarketing

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (select * from  {prefix}{pc_dataset}.pc_accountcontact Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact  ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (select * from  {prefix}{pc_dataset}.pc_account Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (select * from  {prefix}{pc_dataset}.pc_accountcontactrole Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		INNER JOIN {prefix}{pc_dataset}.pc_address ON pc_address.ID = pc_contact.MailingAddress_JMIC
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_gendertype ON pctl_gendertype.ID = pc_contact.Gender
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_primaryphonetype phone ON phone.ID = pc_contact.PrimaryPhone

		WHERE 1=1
		AND	pctl_contact.ID = 2					--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')	/* how else to get rid of garbage ? */
		 AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
		--AND pc_contact.ID = 2319 --79327
		--AND COALESCE(pc_contact.Retired,0) = 0	/* not sure if we include this; if we join to Policy then no */

	UNION DISTINCT

		-- GROUP 1
		-- Get Mailing Address Info - Mailing_FFR_JMIC
		SELECT --TOP 1000
		pc_contact.Publicid AS SourceCustomerNumber
		,FirstName
		,MiddleName
		,LastName
		,COALESCE(EmailAddress1, EmailAddress2)							AS PrimaryEmailAddress
		,pctl_gendertype.NAME											AS Gender
		,COALESCE(
			CASE phone.TYPECODE
				WHEN 'work' THEN pc_contact.WorkPhone
				WHEN 'home'	THEN pc_contact.HomePhone
				WHEN 'mobile' THEN
					CASE pctl_contact.TYPECODE
						WHEN 'Person' THEN pc_contact.CellPhone
						WHEN 'Company' THEN pc_contact.MobilePhone_JMIC
						ELSE COALESCE(pc_contact.CellPhone, pc_contact.MobilePhone_JMIC)
					END
				WHEN 'Other_JMIC' THEN pc_contact.OtherPhoneOne_JMIC
			END, pc_contact.WorkPhone, pc_contact.HomePhone, pc_contact.CellPhone, pc_contact.OtherPhoneOne_JMIC) AS PrimaryPhoneNumber
		,phone.NAME PrimaryPhoneType
		,WorkPhone
		,CellPhone
		,HomePhone
		,MobilePhone_JMIC												AS MobilePhone
		,OtherPhoneOne_JMIC												AS OtherPhone
		,extract(month from DateOfBirth)												AS BirthMonth
		,extract(year from DateOfBirth)												AS BirthYear
		,extract(day from DateOfBirth)												AS BirthDay
		,CAST(DateOfBirth AS DATE)										AS BirthDate
		,pc_contact.CreateTime											AS DateCreated
		,pc_contact.UpdateTime											AS DateModfied
		,CASE pc_contact.RecMaterketingMaterial_JMIC_PL
			WHEN True THEN 'Yes'
			WHEN False THEN 'No'
		ELSE 'Unknown' END												AS AllowContactForMarketing

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontact Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontact  ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_account Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_account  ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontactrole Where DATE(_PARTITIONTIME) = DATE('{date}'))pc_accountcontactrole  ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_gendertype ON pctl_gendertype.ID = pc_contact.Gender
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_primaryphonetype phone ON phone.ID = pc_contact.PrimaryPhone
		INNER JOIN (	SELECT pc_contact.ID ContactID
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
					LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_address Where DATE(_PARTITIONTIME) = DATE('{date}')) PC_ADDRESS_MAILING_1
						ON FirstMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_1.ID

		WHERE 1=1
		AND	pctl_contact.ID = 2					--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')	/* how else to get rid of garbage ? */
		AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    --AND DATE(PC_ADDRESS_MAILING_1 ._PARTITIONTIME) = DATE('{date}')
		--AND pc_contact.ID = 2319 --79327 and FirstMailingAddress.MailingAddressID = 4949295
		--AND COALESCE(pc_contact.Retired,0) = 0	/* not sure if we include this; if we join to Policy then no */

	UNION DISTINCT

		-- GROUP 3
		-- Get mailing Address Info -- Mailing_JMIC
		SELECT --TOP 1000
		pc_contact.Publicid AS SourceCustomerNumber
		,FirstName
		,MiddleName
		,LastName
		,COALESCE(EmailAddress1, EmailAddress2)							AS PrimaryEmailAddress
		,pctl_gendertype.NAME											AS Gender
		,COALESCE(
			CASE phone.TYPECODE
				WHEN 'work' THEN pc_contact.WorkPhone
				WHEN 'home'	THEN pc_contact.HomePhone
				WHEN 'mobile' THEN
					CASE pctl_contact.TYPECODE
						WHEN 'Person' THEN pc_contact.CellPhone
						WHEN 'Company' THEN pc_contact.MobilePhone_JMIC
						ELSE COALESCE(pc_contact.CellPhone, pc_contact.MobilePhone_JMIC)
					END
				WHEN 'Other_JMIC' THEN pc_contact.OtherPhoneOne_JMIC
			END, pc_contact.WorkPhone, pc_contact.HomePhone, pc_contact.CellPhone, pc_contact.OtherPhoneOne_JMIC) AS PrimaryPhoneNumber
		,phone.NAME PrimaryPhoneType
		,WorkPhone
		,CellPhone
		,HomePhone
		,MobilePhone_JMIC												AS MobilePhone
		,OtherPhoneOne_JMIC												AS OtherPhone
		,extract(month from DateOfBirth)												AS BirthMonth
		,extract(year from DateOfBirth)												AS BirthYear
		,extract(day from DateOfBirth)												AS BirthDay
		,CAST(DateOfBirth AS DATE)										AS BirthDate
		,pc_contact.CreateTime											AS DateCreated
		,pc_contact.UpdateTime											AS DateModfied
		,CASE pc_contact.RecMaterketingMaterial_JMIC_PL
			WHEN True THEN 'Yes'
			WHEN False THEN 'No'
		ELSE 'Unknown' END												AS AllowContactForMarketing

		FROM {prefix}{pc_dataset}.pc_contact
		LEFT OUTER JOIN (Select * from {prefix}{pc_dataset}.pc_accountcontact Where DATE(_PARTITIONTIME) = DATE('{date}')) pc_accountcontact ON pc_contact.ID = pc_accountcontact.Contact
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_account Where DATE(_PARTITIONTIME) = DATE('{date}')) pc_account ON pc_account.ID = pc_accountcontact.Account
		LEFT OUTER JOIN (select * from {prefix}{pc_dataset}.pc_accountcontactrole Where DATE(_PARTITIONTIME) = DATE('{date}')) pc_accountcontactrole ON pc_accountcontact.ID = pc_accountcontactrole.AccountContact
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_accountcontactrole ON pc_accountcontactrole.Subtype = pctl_accountcontactrole.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_gendertype ON pctl_gendertype.ID = pc_contact.Gender
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_contact ON pc_contact.Subtype = pctl_contact.ID
		LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_primaryphonetype phone ON phone.ID = pc_contact.PrimaryPhone
		INNER JOIN (	SELECT pc_contact.ID ContactID
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
						LEFT OUTER JOIN  (select * from {prefix}{pc_dataset}.pc_address Where DATE(_PARTITIONTIME) = DATE('{date}')) PC_ADDRESS_MAILING_3
							ON ThirdMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_3.ID

		WHERE 1=1
		AND	pctl_contact.ID = 2					--Person /* 1 = Company; 7 = User Contact; 10= ProducerContact_JMIC */
		AND pctl_accountcontactrole.ID IN (16, 14)		--NamedInsured /*14 = Located With, 2 = AccountHolder */
		AND (pc_contact.PublicID not LIKE 'system%' AND pc_contact.PublicID not like 'default%')	/* how else to get rid of garbage ? */
		AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontact._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_account._PARTITIONTIME) = DATE('{date}')
    --AND DATE(pc_accountcontactrole._PARTITIONTIME) = DATE('{date}')
    --AND DATE(PC_ADDRESS_MAILING_3 ._PARTITIONTIME) = DATE('{date}')
		--AND pc_contact.ID = 2319 --79327
		--AND COALESCE(pc_contact.Retired,0) = 0	/* not sure if we include this; if we join to Policy then no */
	) CustomerData
