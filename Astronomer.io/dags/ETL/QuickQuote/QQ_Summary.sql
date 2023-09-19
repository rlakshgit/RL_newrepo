SELECT
  qq_pc.SourceOfBusiness
  ,qq_pc.ProducerCode
  ,qq_pc.ProducerCodeDescription
  ,qq_pc.MasterAgencyCode
  ,qq_pc.MasterAgencyName
  ,qq_pc.AgencyCode
  ,qq_pc.AgencyName
  ,qq_pc.QuoteAccountNumber
  ,qq_pc.QuoteJobNumber
  ,qq_pc.QuoteFirstName
	,qq_pc.QuoteLastName
	,qq_pc.QuoteCreateUserID
	,qq_pc.QuoteCreateTimestamp
	,qq_pc.QuoteUpdateTimestamp
  ,qq_pc.QuoteType
  ,qq_pc.QuoteTypeCode
  ,qq_ratabase.PolicyState
  ,qq_ratabase.ItemType
  ,qq_ratabase.ItemSubtype
  ,qq_ratabase.ItemGender
  ,qq_ratabase.ItemValue
  ,qq_ratabase.ItemDeductible
  ,qq_ratabase.ItemPremium
  ,qq_ratabase.ItemCount
  ,qq_ratabase.PolicyValue
  ,qq_ratabase.PolicyPremium
  ,qq_ratabase.CreditScore
  ,COUNT(*) OVER (PARTITION BY qq_pc.QuoteTypeCode) AS TotalCountByQuoteType
  ,COUNT(*) OVER (PARTITION BY qq_pc.QuoteJobNumber) AS TotalQuotesByJobNumber
  ,AVG(qq_ratabase.PolicyPremium) OVER (PARTITION BY qq_pc.QuoteTypeCode) AS AveragePolicyPremiumByQuoteType
  ,CAST('{date}' as DATE) as bq_load_date
  
FROM
  `{project}.{base_dataset}.t_quickquote_policycenter` AS qq_pc

LEFT JOIN
  (SELECT * from `{project}.{base_dataset}.t_quickquote_ratabase` where DATE(_PARTITIONTIME) = '{date}') AS qq_ratabase
  ON qq_pc.QuoteJobNumber = qq_ratabase.JobNumber 

WHERE
     qq_pc.QuoteProductType = 'JPAPersonalArticles'
  AND qq_pc.QuoteTypeCode IS NOT NULL
  AND CAST(qq_pc._PARTITIONTIME AS date) = '{date}'
  
GROUP BY
  qq_pc.SourceOfBusiness
  ,qq_pc.ProducerCode
  ,qq_pc.ProducerCodeDescription
  ,qq_pc.MasterAgencyCode
  ,qq_pc.MasterAgencyName
  ,qq_pc.AgencyCode
  ,qq_pc.AgencyName
  ,qq_pc.QuoteAccountNumber
  ,qq_pc.QuoteJobNumber
  ,qq_pc.QuoteFirstName
  ,qq_pc.QuoteLastName
  ,qq_pc.QuoteCreateUserID
  ,qq_pc.QuoteCreateTimestamp
  ,qq_pc.QuoteUpdateTimestamp
  ,qq_pc.QuoteType
  ,qq_pc.QuoteTypeCode
  ,qq_ratabase.PolicyState
  ,qq_ratabase.ItemType
  ,qq_ratabase.ItemSubtype
  ,qq_ratabase.ItemGender
  ,qq_ratabase.ItemValue
  ,qq_ratabase.ItemDeductible
  ,qq_ratabase.ItemPremium
  ,qq_ratabase.ItemCount
  ,qq_ratabase.PolicyValue
  ,qq_ratabase.PolicyPremium
  ,qq_ratabase.CreditScore
