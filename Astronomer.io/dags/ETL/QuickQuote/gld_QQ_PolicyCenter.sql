#StandardSQL
SELECT
  job.CreateTime AS QuoteCreateTimestamp
  ,job.CreateUserID AS QuoteCreateUserID
  ,pol.ProductCode AS QuoteProductType
  ,biz_src.NAME AS SourceOfBusiness
  ,agency.code as ProducerCode
  ,agency.Description as ProducerCodeDescription
  ,agency.MasterAgencyCode
  ,agency.MasterAgencyName
  ,agency.AgencyCode
  ,agency.AgencyName
  ,pperiod.UpdateTime AS QuoteUpdateTimestamp
  ,user.ContactID AS QuoteContactID
  ,contact.FirstName AS QuoteFirstName
  ,contact.LastName	AS QuoteLastName
  ,job.QuoteType AS QuoteType
  ,quote_type.TYPECODE AS QuoteTypeCode
  ,job.Subtype AS QuoteSubtype
  ,pperiod.Status AS PolicyPeriodStatus
  ,pp_status.TYPECODE AS PolicyStatusDescription
  ,job.QuickQuoteConversionDate_JM AS QuoteConversionTimestamp
  ,job.QuickQuoteConversionUser_JM AS QuoteConversionUserJM
  ,job.JobNumber AS QuoteJobNumber
  ,job.PolicyID	AS QuotePolicyID
  ,pol.AccountID AS QuoteAccountID
  ,account.AccountNumber AS QuoteAccountNumber
  ,pperiod.JobID AS QuoteJobID
  ,pperiod.TotalCostRPT AS QuoteTotalCostRPT
  ,contact.DateOfBirth as DateOfBirth
  ,QuoteConversionContact.FirstName AS QuoteConversionUserFirstName
  ,QuoteConversionContact.LastName AS QuoteConversionUserLastName
  ,pperiod.TotalSchedPremiumRPT_JMIC as TotalSchedPremiumRPT_JMIC

  ,CAST('{date}' as DATE) as bq_load_date

FROM
  `{project}.{base_dataset}.pc_job` AS job

INNER JOIN
  (select * from `{project}.{base_dataset}.pc_policy`  where DATE(_PARTITIONTIME) = '{date}') AS pol
  ON job.PolicyID = pol.ID

INNER JOIN
  (select * from `{project}.{base_dataset}.pc_policyperiod` where DATE(_PARTITIONTIME) = '{date}') AS pperiod
  ON job.ID = pperiod.JobID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pc_user` where DATE(_PARTITIONTIME) = '{date}') AS user
  ON job.CreateUserID = user.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pc_contact` where DATE(_PARTITIONTIME) = '{date}') AS contact
  ON user.ContactID = contact.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pctl_quotetype`) AS quote_type
  ON job.QuoteType = quote_type.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pctl_policyperiodstatus`) AS pp_status
  ON pperiod.Status = pp_status.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pc_account` where DATE(_PARTITIONTIME) = '{date}') AS account
  ON pol.AccountId = account.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pctl_source_jmic_pl` ) AS biz_src
  ON account.Source_JMIC_PL = biz_src.ID

LEFT JOIN
  (select * from `{project}.{base_dataset}.pc_user` where DATE(_PARTITIONTIME) = '{date}') AS QuoteConversionuser
  ON job.QuickQuoteConversionUser_JM = QuoteConversionuser.ID

  LEFT JOIN
  (select * from `{project}.{base_dataset}.pc_contact` where DATE(_PARTITIONTIME) = '{date}') AS QuoteConversionContact
  ON QuoteConversionuser.ContactID = QuoteConversionContact.ID


LEFT JOIN
	(
		SELECT
			producer.ID
			,producer.code
			,producer.Description
			,org.MasterAgencyCode_JMIC AS MasterAgencyCode
			,org.Name AS MasterAgencyName
			,agency.Code_JMIC AS AgencyCode
			,agency.Name AS AgencyName

		FROM `{project}.{base_dataset}.pc_producercode` AS producer
		INNER JOIN (select * from `{project}.{base_dataset}.pc_organization` where DATE(_PARTITIONTIME) = '{date}') AS org
			ON producer.OrganizationID = org.ID
		INNER JOIN (select * from `{project}.{base_dataset}.pc_groupproducercode` where DATE(_PARTITIONTIME) = '{date}') AS gpc
			ON producer.ID = gpc.ProducerCodeID
		INNER JOIN (select * from `{project}.{base_dataset}.pc_group` where DATE(_PARTITIONTIME) = '{date}') AS agency
			ON	gpc.GroupID = agency.ID
			AND producer.OrganizationID = agency.OrganizationID
			-- GroupType = 'agency_jmic'
			AND agency.GroupType = 10002
		WHERE DATE(producer._PARTITIONTIME) = '{date}'
) AS agency
  ON pol.ProducerCodeOfServiceID = agency.ID

WHERE
  CAST(job._PARTITIONTIME AS date) = '{date}'


GROUP BY
  job.CreateTime
  ,job.CreateUserID
  ,pol.ProductCode
  ,biz_src.NAME
  ,agency.code
  ,agency.Description
  ,agency.MasterAgencyCode
  ,agency.MasterAgencyName
  ,agency.AgencyCode
  ,agency.AgencyName
  ,pperiod.UpdateTime
  ,user.ContactID
  ,contact.FirstName
  ,contact.LastName
  ,job.QuoteType
  ,quote_type.TYPECODE
  ,job.Subtype
  ,pperiod.Status
  ,pp_status.TYPECODE
  ,job.QuickQuoteConversionDate_JM
  ,job.QuickQuoteConversionUser_JM
  ,job.JobNumber
  ,job.PolicyID
  ,pol.AccountID
  ,account.AccountNumber
  ,pperiod.JobID
  ,pperiod.TotalCostRPT
  ,contact.DateOfBirth
  ,QuoteConversionContact.FirstName
  ,QuoteConversionContact.LastName
  ,pperiod.TotalSchedPremiumRPT_JMIC

