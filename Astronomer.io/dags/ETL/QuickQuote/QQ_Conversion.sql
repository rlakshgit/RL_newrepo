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
  ,qq_pc.QuoteConversionUserJM
  ,qq_pc.QuoteConversionUserLastName
  ,qq_pc.QuoteConversionUserFirstName
  --,qq_pc.DateOfBirth
  ,CAST(PARSE_DATE('%m/%d/%Y',qq_ratabase.DateOfBirth) AS TIMESTAMP) DateOfBirth
  ,CAST(PARSE_DATE('%m/%d/%Y',qq_ratabase.PolicyEffectiveDate) AS TIMESTAMP) PolicyEffectiveDate
  ,SAFE_CAST(TRUNC(DATE_DIFF(CAST(PARSE_DATE('%m/%d/%Y',qq_ratabase.PolicyEffectiveDate) AS TIMESTAMP), CAST(PARSE_DATE('%m/%d/%Y',qq_ratabase.DateOfBirth) AS TIMESTAMP), DAY) / 365.25) AS int64) AS Age
  ,qq_pc.TotalSchedPremiumRPT_JMIC
  ,qq_pc.QuoteConversionTimestamp
  ,qq_pc.PolicyStatusDescription
  ,qq_pc.QuoteTypeCode
  ,qq_ratabase.PolicyPremium
  ,qq_pc.QuoteTotalCostRPT
  ,qq_pc.QuoteUpdateTimestamp

  ,CAST('{date}' as DATE) as bq_load_date

FROM
  `{project}.{base_dataset}.t_quickquote_policycenter` AS qq_pc

LEFT JOIN
  (SELECT * from `{project}.{base_dataset}.t_quickquote_ratabase` where DATE(_PARTITIONTIME) = '{date}') AS qq_ratabase
  ON qq_pc.QuoteJobNumber = qq_ratabase.JobNumber

WHERE
      qq_pc.PolicyPeriodStatus = 9
  AND qq_pc.QuoteConversionTimestamp IS NOT NULL
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
  ,qq_pc.QuoteConversionUserJM
  ,qq_pc.QuoteConversionUserLastName
  ,qq_pc.QuoteConversionUserFirstName
  ,qq_ratabase.DateOfBirth
  ,qq_ratabase.PolicyEffectiveDate
  ,qq_pc.TotalSchedPremiumRPT_JMIC
  ,qq_pc.QuoteConversionTimestamp
  ,qq_pc.PolicyStatusDescription
  ,qq_pc.QuoteTypeCode
  ,qq_ratabase.PolicyPremium
  ,qq_pc.QuoteTotalCostRPT
  ,qq_pc.QuoteUpdateTimestamp

