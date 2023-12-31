CREATE TABLE if not exists `{project}.{dataset}.pl_items_monthly_inforce`
(
  DateKey INT64,,
  InforceDate DATE,
  QuarterNumber INT64,
  AccountNumber STRING,
  PolicyNumber STRING,
  PolicyOrigEffDate INT64,
  PolicyEffectiveDate INT64,
  PolicyExpirationDate INT64,
  SourceOfBusiness STRING,
  AccountSegment STRING,
  LOBProductLineCode STRING,
  LOBCode STRING,
  BusinessTypeDesc STRING,
  AutoPayIndicator BOOL,
  PaymentMethod STRING,
  AnnualStatementLine STRING,
  ASLDescription STRING,
  AgencyMasterCode STRING,
  AgencyMasterName STRING,
  AgencyCode STRING,
  AgencyName STRING,
  InforceStateCode STRING,
  InforceStateDesc STRING,
  InforceRegion STRING,
  InforceCountryCode STRING,
  InforceCountryDesc STRING,
  LOBStateCode STRING,
  LOBStateDesc STRING,
  LOBRegion STRING,
  LOBCountryCode STRING,
  LOBCountryDesc STRING,
  PrimaryRatingCity STRING,
  PrimaryRatingCounty STRING,
  PrimaryRatingPostalCode STRING,
  PrimaryInsured STRING,
  ItemNumber INT64,
  ItemDescription STRING,
  ItemBrand STRING,
  ItemClassDescription STRING,
  ItemLimit INT64,
  ItemStateCode STRING,
  ItemStateDesc STRING,
  ItemRegion STRING,
  ItemCountryCode STRING,
  ItemCountryDesc STRING,
  PremiumInforce NUMERIC,
  bq_load_date	DATE
)
PARTITION BY InforceDate;