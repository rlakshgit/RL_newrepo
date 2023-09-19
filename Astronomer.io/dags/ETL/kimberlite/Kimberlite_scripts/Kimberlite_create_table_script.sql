#############################################################################################
####Creating Table Claim ************
CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.Claim` 
(
  SourceSystem STRING,
  ClaimKey BYTES,
  PolicyTransactionKey BYTES,
  PolicyNumber STRING,
  ClaimPublicId STRING,
  ClaimID INT64,
  ClaimPolicyPublicId STRING,
  PolicyPeriodPublicID STRING,
  ClaimNumber STRING,
  ClosedOutcomeCode INT64,
  ClosedOutcomeDesc STRING,
  CloseDate TIMESTAMP,
  LossDate TIMESTAMP,
  ReportedDate TIMESTAMP,
  CreatedDate TIMESTAMP,
  MadeDate TIMESTAMP,
  ClaimStatusName STRING,
  ClaimSource STRING,
  HowReportedChannel STRING,
  LossTypeName STRING,
  LossCauseName STRING,
  LossCauseTypecode STRING,
  ClaimPolicyTypecode STRING,
  SecondaryCauseOfLossCode STRING,
  SecondaryCauseOfLossDesc STRING,
  ClaimDescription STRING,
  LossLocationID INT64,
  ClaimEvent STRING,
  CatastropheType STRING,
  LitigationStatus STRING,
  IsClaimantRepresented INT64,
  ClaimType STRING,
  IsVerified INT64,
  SurveyPreference STRING,
  InvolesRolexWatches INT64,
  IsVerifiedManually INT64,
  CoverageInQuestion BOOL,
  SIUStatus STRING,
  BaseSIUScore INT64,
  ReferredtoSIUTeam STRING,
  Flagged STRING,
  AssignedUserID INT64,
  TravelLossLocationID INT64,
  UWCompanyCode STRING,
  IsClaimForLegacyPolicy INT64,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  IsTransactionSliceEffective INT64,
  bq_load_date DATE
)
	PARTITION BY
		bq_load_date
	OPTIONS(
      description="Claims and core attributes, plus foreign keys"
   );




#############################################################################################
####Creating Table ContactAttributes ************
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ContactAttributes`
(
  SourceSystem STRING,
  ContactKey BYTES,
  SourceCenter STRING,
  ContactPublicID STRING,
  AddressPublicID STRING,
  PrimaryAddressID INT64,
  MailingAddressID INT64,
  ContactType STRING,
  IsContactRetired INT64,
  FullName STRING,
  FirstName STRING,
  MiddleName STRING,
  LastName STRING,
  Prefix INT64,
  Suffix INT64,
  Gender INT64,
  PrimaryPhoneType STRING,
  WorkPhone STRING,
  HomePhone STRING,
  CellPhone STRING,
  MobilePhone STRING,
  OtherPhoneOne STRING,
  EmailAddress1 STRING,
  EmailAddress2 STRING,
  WebAddress STRING,
  DateOfBirth DATETIME,
  ContactForSurvey INT64,
  RecMarketingMaterial INT64,
  LongName STRING,
  Occupation STRING,
  MaritalStatus STRING,
  IsPrimaryAddressRetired INT64,
  PrimaryAddressStatusCode STRING,
  PrimaryAddressCode STRING,
  PrimaryAddressTypeCode STRING,
  PrimaryAddressLine1 STRING,
  PrimaryAddressLine2 STRING,
  PrimaryAddressLine3 STRING,
  PrimaryAddressCity STRING,
  PrimaryAddressCounty STRING,
  PrimaryAddressStateCode STRING,
  PrimaryAddressPostalCode STRING,
  PrimaryAddressCountryCode STRING,
  PrimaryAddressFIPSCode STRING,
  IsMailingAddressRetired INT64,
  MailingAddressStatusCode STRING,
  MailingAddressCode STRING,
  MailingAddressLine STRING,
  MailingAddressLine2 STRING,
  MailingAddressLine3 STRING,
  MailingAddressCity STRING,
  MailingAddressCounty STRING,
  MailingAddressStateCode STRING,
  MailingAddressPostalCode STRING,
  MailingAddressCountryCode STRING,
  LinkID STRING,
  PlatformOrgName STRING,
  PlatformOrgId STRING,
  JBTRating STRING,
  Username STRING,
  IsServiceAccount INT64,
  Obfuscated INT64,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains contact attributes for primary and mailing addresses. Built from Kimberlite Core contact and address tables."
);

#############################################################################################
####Creating Table AccountContactRoleAttributes ************
CREATE TABLE  IF NOT EXISTS `{project}.{dataset}.AccountContactRoleAttributes`
(
  SourceSystem STRING,
  AccountContactRoleKey BYTES,
  AccountKey BYTES,
  AccountPublicID STRING,
  AccountNumber STRING,
  AccountCreatedDate TIMESTAMP,
  AccountOrigInceptionDate TIMESTAMP,
  AccountDistributionSource STRING,
  ApplicationTakenBy STRING,
  AccountStatus STRING,
  IsJewelerAccount INT64,
  ReferringJewelerPublicID STRING,
  IsSpecialAccount INT64,
  AccountOrgType STRING,
  ProxyReceivedDate TIMESTAMP,
  IsAccountResolicit INT64,
  AccountResolicitDate TIMESTAMP,
  IsPaperlessDelivery INT64,
  PaperlessDeliveryStatusDate TIMESTAMP,
  PreferredCommunicationMethodCode STRING,
  RetiredDateTime TIMESTAMP,
  BillingAccountType STRING,
  AutoPayIndicator INT64,
  PaymentMethod STRING,
  CreditCardExpDate TIMESTAMP,
  SourceCenter STRING,
  AccountContactRolePublicID STRING,
  IsAccountActive INT64,
  RetiredAccount INT64,
  AccountContactRoleType STRING,
  AccountContactRoleName STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="All Accounts and their Contact Roles, using data from Kimberlite Core Account and AccountContactRole tables."
);

#############################################################################################
####Creating Table AccountAttributes ************
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.AccountAttributes`
(
  SourceSystem STRING,
  AccountKey BYTES,
  SourceCenter STRING,
  AccountHolderContactKey BYTES,
  InsuredContactKey BYTES,
  AccountHolderContactPublicID STRING,
  ReferringJewelerPublicID STRING,
  InsuredContactPublicID STRING,
  AccountID INT64,
  AccountPublicID STRING,
  AccountNumber STRING,
  AccountCreatedDate TIMESTAMP,
  AccountOrigInceptionDate TIMESTAMP,
  AccountDistributionSource STRING,
  ApplicationTakenBy STRING,
  AccountStatus STRING,
  IsJewelerAccount INT64,
  IsSpecialAccount INT64,
  AccountOrgType STRING,
  ProxyReceivedDate TIMESTAMP,
  IsAccountResolicit INT64,
  AccountResolicitDate TIMESTAMP,
  IsPaperlessDelivery INT64,
  PaperlessDeliveryStatusDate TIMESTAMP,
  PreferredCommunicationMethodCode STRING,
  RetiredDateTime TIMESTAMP,
  BillingAccountType STRING,
  AutoPayIndicator INT64,
  PaymentMethod STRING,
  CreditCardExpDate TIMESTAMP,
  JBTRating STRING,
  PrimaryAddressTypeCode STRING,
  PrimaryAddressLine1 STRING,
  PrimaryAddressLine2 STRING,
  PrimaryAddressLine3 STRING,
  PrimaryAddressCity STRING,
  PrimaryAddressCounty STRING,
  PrimaryAddressStateCode STRING,
  PrimaryAddressPostalCode STRING,
  PrimaryAddressCountryCode STRING,
  MailingAddressLine STRING,
  MailingAddressLine2 STRING,
  MailingAddressLine3 STRING,
  MailingAddressCity STRING,
  MailingAddressCounty STRING,
  MailingAddressStateCode STRING,
  MailingAddressPostalCode STRING,
  MailingAddressCountryCode STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
     description="Contains a reportable Account table with a link (AccountHolderContactKey) to the AccountHolder info found in the ContactAttributes table (via ContactKey). The scope is Policy Center Account IDs. The Roles included are: pctl_accountcontactrole.Typecode IN ('OwnerOfficer', 'InsuredContactACR_JMIC',	'AccountHolder')"
);

#############################################################################################
####Creating Table Contact ************
CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.Contact`
(
  SourceSystem STRING,
  ContactKey BYTES,
  SourceCenter STRING,
  ContactPublicID STRING,
  ContactID INT64,
  PrimaryAddressID INT64,
  MailingAddressID INT64,
  Retired INT64,
  ContactType STRING,
  FullName STRING,
  FirstName STRING,
  MiddleName STRING,
  LastName STRING,
  Prefix INT64,
  Suffix INT64,
  Gender INT64,
  PrimaryPhoneType STRING,
  WorkPhone STRING,
  HomePhone STRING,
  CellPhone STRING,
  MobilePhone STRING,
  OtherPhoneOne STRING,
  EmailAddress1 STRING,
  EmailAddress2 STRING,
  WebAddress STRING,
  DateOfBirth DATETIME,
  ContactForSurvey INT64,
  RecMarketingMaterial INT64,
  LongName STRING,
  Occupation STRING,
  MaritalStatus STRING,
  LinkID STRING,
  PlatformOrgName STRING,
  PlatformOrgId STRING,
  JBTRating STRING,
  UserName STRING,
  Obfuscated INT64,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains all contacts from the four GW centers, plus key attributes."
);

#############################################################################################
####Creating Table `Address` ************
CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.Address`
(
  SourceSystem STRING,
  AddressKey BYTES,
  SourceCenter STRING,
  AddressPublicID STRING,
  AddressID INT64,
  AddressCode STRING,
  AddressTypeCode STRING,
  AddressLine1 STRING,
  AddressLine2 STRING,
  AddressLine3 STRING,
  City STRING,
  County STRING,
  StateCode STRING,
  PostalCode STRING,
  CountryCode STRING,
  FIPSCode STRING,
  AddressStatusCode STRING,
  Retired INT64,
  IsObfuscated INT64,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains all addresses and key attributes from all four GW centers."
);

#############################################################################################
####Creating Table Account ************
CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.Account`
(
  SourceSystem STRING,
  AccountKey BYTES,
  SourceCenter STRING,
  AccountPublicID STRING,
  AccountID INT64,
  AccountNumber STRING,
  AccountCreatedDate TIMESTAMP,
  AccountOrigInceptionDate TIMESTAMP,
  AccountDistributionSource STRING,
  ApplicationTakenBy STRING,
  AccountStatus STRING,
  IsJewelerAccount INT64,
  ReferringJewelerPublicID STRING,
  IsSpecialAccount INT64,
  AccountOrgType STRING,
  ProxyReceivedDate TIMESTAMP,
  IsAccountResolicit INT64,
  AccountResolicitDate TIMESTAMP,
  IsPaperlessDelivery INT64,
  PaperlessDeliveryStatusDate TIMESTAMP,
  PreferredCommunicationMethodCode STRING,
  RetiredDateTime TIMESTAMP,
  BillingAccountType STRING,
  AutoPayIndicator INT64,
  PaymentMethod STRING,
  CreditCardExpDate TIMESTAMP,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains Account, type, and status info mainly from GW PC and BC (without overlap). BC also provides limited billing account info such as autopay, paymentmethod, etc."
);

#############################################################################################
####Creating Table AccountContactRole ************
CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.AccountContactRole`
(
  SourceSystem STRING,
  AccountContactRoleKey BYTES,
  AccountKey BYTES,
  ContactKey BYTES,
  SourceCenter STRING,
  AccountContactRolePublicID STRING,
  AccountPublicID STRING,
  ContactPublicID STRING,
  AccountID INT64,
  IsAccountActive INT64,
  RetiredAccount INT64,
  AccountContactRoleType STRING,
  AccountContactRoleName STRING,
  LastUpdateTime TIMESTAMP,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains each unique AccountContactRole from GW PC & BC. Accounts do not exist in CC or CM (Contact Mgr)"
);


#############################################################################################
####Creating Table ClaimStatus ************

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimStatus`
(SourceSystem                   STRING
,UWCompanyPublicID              STRING
,ClaimTransactionKey            BYTES
,ccHistoryID                    INTEGER
,ClaimPublicID                  STRING
,ClaimNumber                    STRING
,ClaimStatus                    STRING
,ClosedOutcomeDesc              STRING
,EnteredDate                    TIMESTAMP
,EndingDate                     TIMESTAMP
,IsTransactionSliceEffective    INTEGER
--,CreatedDate                    DATE
,bq_load_date					DATE 
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Rebuilt daily to hold status history by ClaimNumber",
      labels=[("ref_kimberlite_buildingblock","")]
  );

#############################################################################################
####Creating Table ClaimPADirect ************

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimPADirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  PAJewelryCoverageKey BYTES,
  RiskPAJewelryKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  PAJewelryPublicID STRING,
  ccPAJewelryPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  JewelryArticleNumber INT64,
  ArticleTypeCode STRING,
  ArticleGenderCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  IsErodingReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveLossRecovery NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveDCCExpenseRecovery NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimReserveAOExpenseRecovery NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  ClaimLossRecovery NUMERIC,
  ClaimRecoveryDCCExpense NUMERIC,
  ClaimRecoveryAOExpense NUMERIC,
  ClaimRecoveryType STRING,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  ReserveLineCategoryCode STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  ClaimPaymentContactFullName STRING,
  ClaimPaymentContactIsJeweler INT64,
  ClaimPaymentContactIsJMJeweler INT64,
  ClaimPaymentContactPublicID STRING,
  ItemState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicId STRING,
  ItemAddressPublicId STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicId STRING,
  PolicyAddressPublicId STRING,
  bq_load_date DATE
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PA Direct claims attributes and metrics at Financial Transaction Line Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);


#############################################################################################
####Creating Table ClaimFinancialTransactionLinePADirect ************

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLinePADirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  PAJewelryCoverageKey BYTES,
  RiskPAJewelryKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicID STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  ccCoveragePublicID STRING,
  CoverageLevel STRING,
  PAJewelryPublicID STRING,
  ccPAJewelryPublicID STRING,
  ClaimPolicyPublicID STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  CoverageCode STRING,
  JewelryArticleNumber INT64,
  ArticleTypeCode STRING,
  ArticleGenderCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  DoesNotErodeReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  TransactionAmount NUMERIC,
  LineCode STRING,
  LOBCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionsSubmittedPrior INT64,
  ClaimRecoveryType STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  IsTransactionSliceEffective INT64,
  ClaimContactID INT64,
  UWCompanyPublicID STRING,
  ReserveLineCategoryCode STRING,
  ReserveLinePublicID STRING,
  ExposurePublicID STRING,
  IncidentPublicID STRING,
  ProducerPublicID STRING,
  ProducerCode STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicID STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  InsuredID INT64,
  LocatedWith INT64,
  LossLocationID INT64,
  VendorID STRING,
  DefaultSegment STRING,
  peSegment STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PA Direct claims at financial line key level ",
      labels=[("ref_kimberlite","claim")]
);

#############################################################################################
####Creating Table ClaimPJCeded ###########

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimPJCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  ItemCoverageKey BYTES,
  RiskJewelryItemKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  ItemPublicId STRING,
  ccItemPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  ItemNumber INT64,
  ItemClassCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  IsErodingReserves INT64,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  ItemState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicId STRING,
  ItemAddressPublicId STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicId STRING,
  PolicyAddressPublicId STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PJ Ceded claims attributes and metrics at Financial Transaction Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);


#############################################################################################
####Creating Table ClaimPJDirect ###########

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimPJDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  ItemCoverageKey BYTES,
  RiskJewelryItemKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  ItemPublicId STRING,
  ccItemPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  ItemNumber INT64,
  ItemClassCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  IsErodingReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveLossRecovery NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveDCCExpenseRecovery NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimReserveAOExpenseRecovery NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  ClaimLossRecovery NUMERIC,
  ClaimRecoveryDCCExpense NUMERIC,
  ClaimRecoveryAOExpense NUMERIC,
  ClaimRecoveryType STRING,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  ReserveLineCategoryCode STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  ClaimPaymentContactFullName STRING,
  ClaimPaymentContactIsJeweler INT64,
  ClaimPaymentContactIsJMJeweler INT64,
  ClaimPaymentContactPublicID STRING,
  ItemState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicId STRING,
  ItemAddressPublicId STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicId STRING,
  PolicyAddressPublicId STRING,
  bq_load_date DATE
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PJ Direct claims attributes and metrics at Financial Transaction Line Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);



#############################################################################################
####Creating Table ClaimFinancialTransactionLinePJCeded ###########

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLinePJCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  ItemCoverageKey BYTES,
  RiskJewelryItemKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  ccCoveragePublicID STRING,
  CoverageLevel STRING,
  ClaimPolicyPublicId STRING,
  ItemPublicId STRING,
  ccItemPublicID STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  CoverageCode STRING,
  ItemNumber INT64,
  ItemClassCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionAmount NUMERIC,
  LineCode STRING,
  LOBCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  IsClaimForLegacyPolicy BOOL,
  TransactionChangeType STRING,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  IsTransactionSliceEffective INT64,
  UWCompanyPublicID STRING,
  ReserveLinePublicID STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  ProducerPublicID STRING,
  ProducerCode STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicId STRING,
  PolicyAddressStateId INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  InsuredID INT64,
  LocatedWith INT64,
  LossLocationID INT64,
  DefaultSegment STRING,
  peSegment STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PJ Ceded claims at financial key level ",
      labels=[("ref_kimberlite","claim")]
);


#############################################################################################
####Creating Table ClaimFinancialTransactionLinePJDirect ###########

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLinePJDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  ItemCoverageKey BYTES,
  RiskJewelryItemKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  ccCoveragePublicID STRING,
  ClaimPolicyPublicId STRING,
  ItemPublicId STRING,
  ccItemPublicID STRING,
  CoverageLevel STRING,
  CoverageCode STRING,
  IsTransactionSliceEffective INT64,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  AccountNumber STRING,
  ItemNumber INT64,
  ItemClassCode STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  DoesNotErodeReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  TransactionAmount NUMERIC,
  LineCode STRING,
  LOBCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionsSubmittedPrior INT64,
  ClaimRecoveryType STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ClaimContactID INT64,
  UWCompanyPublicID STRING,
  ReserveLineCategoryCode STRING,
  ReserveLinePublicID STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  ProducerPublicID STRING,
  ProducerCode STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicId STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  InsuredID INT64,
  LocatedWith INT64,
  LossLocationID INT64,
  VendorID STRING,
  DefaultSegment STRING,
  peSegment STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="PJ Direct claims at financial line key level ",
      labels=[("ref_kimberlite","claim")]
);


#############################################################################################
####Creating Table PolicyTransactionProduct ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.PolicyTransactionProduct`
	(    
		SourceSystem	STRING,
		PolicyTransactionProductKey	BYTES,		
		PolicyTransactionKey	BYTES,
		PolicyPeriodPublicID	STRING,
		PolicyLineCode			STRING,
		OfferingCode			STRING ,
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date; 
   
#############################################################################################
####Creating Table RiskBuilding ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskBuilding`
	(    
		SourceSystem	STRING	,	
		RiskBuildingKey	BYTES	,	
		RiskLocationKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		BuildingPublicID	STRING	,	
		LocationPublicID	STRING	,	
		LocationLevelRisk	STRING	,	
		BuildingLevelRisk	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		EffectiveDate	TIMESTAMP	,	
		ExpirationDate	TIMESTAMP	,	
		IsPrimaryBuildingLocation STRING,
		BuildingProtectionClassCode STRING,
		PrimaryRatingBuildingFixedID INT64,
		JobNumber STRING,
		BuildingNumber	INT64	,	
		BuildingDescription	STRING	,	
		LocationClassPredominantBldgOccupancyCode	STRING	,	
		LocationClassPredominantBldgOccupancyClass	STRING	,	
		LocationClassInsuredBusinessCode	STRING	,	
		LocationClassInsuredBusinessClass	STRING	,	
		LocationEffectiveDate	TIMESTAMP	,	
		LocationExpirationDate	TIMESTAMP	,	
		BldgCodeEffectivenessGrade	STRING	,	
		BldgLimitToValuePercent	STRING	,	
		BPPLimitToValuePercent	STRING	,	
		IsTheftExclusion	INT64	,	
		IsBrandsAndLabels	INT64	,	
		IsWindOrHail	INT64	,	
		PublicUtilities	STRING	,	
		BuildingClass	STRING	,	
		EQTerritory	STRING	,	
		PremiumBasisAmount	INT64	,	
		ConstructionCode	STRING	,	
		ConstructionType	STRING	,	
		ConstructionYearBuilt	INT64	,	
		NumberOfFloors	INT64	,	
		TotalBuildngAreaSQFT	INT64	,	
		AreaOccupiedSQFT	INT64	,	
		Basement	INT64	,	
		IsBasementFinished	INT64	,	
		RoofMaterial	STRING	,	
		SmokeDetectors	INT64	,	
		PercentSprinklered	STRING	,	
		HeatingYear	INT64	,	
		PlumbingYear	INT64	,	
		RoofingYear	INT64	,	
		WiringYear	INT64	,	
		LastBldgInspectionDate	TIMESTAMP	,	
		LastBldgValidationDate	TIMESTAMP	,	
		PercentOccupied	INT64	,	
		AdjacentOccupancies	STRING	,	
		SharedPremises	INT64	,	
		PremisesSharedWith	STRING	,	
		BldgHandlePawnPropOtherJwlry	INT64	,	
		BldgNatlPawnAssociation	INT64	,	
		BldgMemberOtherPawnAssociations	STRING	,	
		BldgHavePawnLocalLicense	INT64	,	
		BldgHavePawnStateLicense	INT64	,	
		BldgHavePawnFederalLicense	INT64	,	
		BldgPawnLicenseAreYouBounded	INT64	,	
		BldgTotalAnnualSales	INT64	,	
		BldgSalesPawnPercent	NUMERIC	,	
		BldgSalesRetailPercent	NUMERIC	,	
		BldgSalesCheckCashingPercent	NUMERIC	,	
		BldgSalesGunsAndAmmunitionPercent	NUMERIC	,	
		BldgSalesAutoPawnPercent	NUMERIC	,	
		BldgSalesTitlePawnPercent	NUMERIC	,	
		BldgSalesOtherPercent	NUMERIC	,	
		BldgSalesOtherPercentDescription	STRING	,	
		BldgAvgDailyAmtOfNonJewelryPawnInventory	NUMERIC	,	
		BldgReportTransactionsRealTime	INT64	,	
		BldgReportTransactionsDaily	INT64	,	
		BldgReportTransactionsWeekly	INT64	,	
		BldgReportTransactionsOther	INT64	,	
		BldgReportTransactionsOtherDesc	STRING	,	
		BldgLoanPossessOrSellFireArms	INT64	,	
		BldgFedFirearmsDealerLicense	INT64	,	
		BldgTypesFedFirearmsLicenses	STRING	,	
		BldgFirearmsDisplayLockAndKey	INT64	,	
		BldgHowSecureLongGuns	STRING	,	
		BldgHowSecureOtherFirearms	STRING	,	
		BldgShootingRangeOnPrem	INT64	,	
		BldgHowSafeguardAmmoGunPowder	STRING	,	
		BuildingAddlCommentsAboutBusiness	STRING	,	
		FixedBuildingRank	INT64	,	
		IsTransactionSliceEffective	INT64	,	
		bq_load_date	DATE		
	)
   PARTITION BY
   bq_load_date;     
 
#############################################################################################
####Creating Table PolicyTransaction ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.PolicyTransaction`
	(   	
		SourceSystem	STRING,
		PolicyTransactionKey	BYTES,
		AccountNumber	STRING,
		PolicyNumber	STRING,
		LegacyPolicyNumber	STRING,
		Segment	STRING,
		JobNumber	STRING,
		PolicyPeriodPublicID	STRING,
		TranType	STRING,
		PeriodEffDate	TIMESTAMP,
		PeriodEndDate	TIMESTAMP,
		TransEffDate	TIMESTAMP,
		WrittenDate	TIMESTAMP,
		SubmissionDate	TIMESTAMP,
		JobCloseDate	TIMESTAMP,
		DeclineDate	TIMESTAMP,
		TermNumber	INT64,
		ModelNumber	INT64,
		TransactionStatus	STRING,
		TransactionCostRPT	NUMERIC,
		TotalCostRPT	NUMERIC,
		EstimatedPremium	NUMERIC,
		TotalMinimumPremiumRPT	NUMERIC,
		TotalMinimumPremiumRPTft	NUMERIC,
		TotalPremiumRPT	NUMERIC,
		TotalSchedPremiumRPT	NUMERIC,
		TotalUnschedPremiumRPT	NUMERIC,
		NotTakenReason	STRING,
		NotTakenExplanation	STRING,
		PolicyChangeReason	STRING,
		CancelSource	STRING,
		CancelType	STRING,
		CancelReason	STRING,
		CancelReasonDescription	STRING,
		CancelEffectiveDate	TIMESTAMP,
		ReinstReason	STRING,
		RewriteType	STRING,
		RenewalCode	STRING,
		PreRenewalDirection	STRING,
		NonRenewReason	STRING,
		NonRenewExplanation	STRING,
		IsConditionalRenewal	INT64,
		IsStraightThrough	INT64,	
		QuoteServiceID	STRING,
        bq_load_date	DATE,
        OriginalPolicyEffDate TIMESTAMP,
		PolicyTermPublicID STRING,
		OfferingCode STRING
	)
   PARTITION BY
   bq_load_date;	
   
#############################################################################################
####Creating Table RiskJewelryItem ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskJewelryItem`
	(    
		SourceSystem	STRING,
		RiskJewelryItemKey	BYTES,
		PolicyTransactionKey	BYTES,
		FixedItemRank	INT64,
		IsTransactionSliceEffective	INT64,
		PolicyNumber	STRING,
		JobNumber	STRING,
		ItemPublicID	STRING,
		ItemFixedID	INT64,
		PolicyPeriodPublicID	STRING,
		RiskLevel	STRING,
		ItemNumber	INT64,
		ItemEffectiveDate	TIMESTAMP,
		ItemExpirationDate	TIMESTAMP,
		ItemDescription	STRING,
		ItemClassOtherDescText	STRING,
		ItemBrand	STRING,
		ItemStyle	STRING,
		ItemStyleOtherDescText	STRING,
		ItemInitialValue	INT64,
		ItemDescriptionDate	TIMESTAMP,
		ItemAppraisalReceived	STRING,
		ItemAppraisalDocType	STRING,
		ItemAppraisalViewEntireDoc	INT64,
		ItemIVADate	TIMESTAMP,
		ItemIVAPercentage	NUMERIC,
		ItemHasIVAApplied	INT64,
		ItemUseInitialLimit	INT64,
		ItemPremiumDiffForIVA	INT64,
		ItemJewelerAppraiser	STRING,
		ItemValuationType	STRING,
		ItemBankVault	STRING,
		ItemDamage	INT64,
		ItemDamagaeDescText	STRING,
		ItemStored	STRING,
		ItemPLSafe	INT64,
		ItemSafe	INT64,
		ItemExpressDescText	STRING,
		ItemExpressDescIsAppraisal	INT64,
		IsItemInactive	INT64,
		InactiveReason	INT64,
		CostPublicID	INT64,
		bq_load_date	DATE		
    )
   PARTITION BY
   bq_load_date;		
      
#############################################################################################
####Creating Table RiskJewelryArticle ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskPAJewelry`
	(  
		SourceSystem	STRING,
		RiskPAJewelryKey	BYTES,
		PolicyTransactionKey	BYTES,
		FixedArticleRank	INT64,
		PolicyPeriodPublicID	STRING,
		PolicyLinePublicID	STRING,
		PAJewelryPublicID	STRING,
		JewelryArticleFixedID	INT64,
		JobNumber	STRING,
		RiskLevel	STRING,
		JewelryArticleNumber	INT64,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTransactionSliceEffective	INT64,
		ArticleType	STRING,
		ArticleSubType	STRING,
		ArticleGender	STRING,
		IsWearableTech	INT64,
		ArticleBrand	STRING,
		ArticleStyle	STRING,
		InitialValue	INT64,
		IsFullDescOverridden	INT64,
		FullDescription	STRING,
		IsAppraisalRequested	INT64,
		IsAppraisalReceived	INT64,
		AppraisalDate	TIMESTAMP,
		InspectionDate	TIMESTAMP,
		IsIVADeclined	INT64,
		IVADate	TIMESTAMP,
		IsIVAApplied	INT64,
		IVAPercentage	NUMERIC,
		ValuationType	STRING,
		IsDamaged	INT64,
		DamageType	STRING,
		IsInactive	INT64,
		InactiveReason	INT64,
		ArticleStored	STRING,
		SafeDetails	INT64,
		TimeOutOfVault	STRING,
		HasCarePlan	INT64,
		CarePlanExpirationDate	TIMESTAMP,
		CarePlanID	STRING,
		ArticleHowAcquired	STRING,
		DurationWithOtherInsurer	STRING,
		ArticleYearAcquired	INT64,
		bq_load_date	DATE
    )
   PARTITION BY
   bq_load_date;	

#############################################################################################
####Creating Table RiskJewelryItemFeatures ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskJewelryItemFeature`
	(
		SourceSystem	STRING	,	
		RiskItemFeatureKey	BYTES	,	
		RiskJewelryItemKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		FixedFeatureRank	INT64	,	
		ItemFeaturePublicID	STRING	,	
		ItemFeatureDetailPublicID	STRING	,	
		ItemFeatureDetailFixedID	INT64	,	
		ItemPublicID	STRING	,	
		ItemFixedID	INT64	,	
		PolicyPeriodPublicID	STRING	,	
		JewelryItemNumber	INT64	,	
		RiskLevel	STRING	,	
		FeatureType	STRING	,	
		FeatureDetailType	STRING	,	
		IsTransactionSliceEffective	INT64	,	
		NumberOfCntrStones	INT64	,	
		CntrStoneWeight	NUMERIC	,	
		CntrStoneMilliMeter	STRING	,	
		CntrStoneCut	STRING	,	
		CntrStoneCutOtherDesc	STRING	,	
		CntrStoneType	STRING	,	
		CntrStoneOtherDesc	STRING	,	
		CntrStonePearlType	STRING	,	
		ColorofCntrStone	STRING	,	
		CntrStoneClarity	STRING	,	
		CntrStoneClarityEnhancedType	STRING	,	
		CntrGemCert	STRING	,	
		NumberOfSideStones	INT64	,	
		SideStoneWeight	NUMERIC	,	
		SideStoneMilliMeter	STRING	,	
		SideStoneCut	STRING	,	
		SideStoneCutOtherDesc	STRING	,	
		SideStoneType	STRING	,	
		SideStoneOtherDesc	STRING	,	
		SideStonePearlType	STRING	,	
		ColorofSideStone	STRING	,	
		SideStoneClarity	STRING	,	
		SideStoneClarityEnhancedType	STRING	,	
		SideGemCert	STRING	,	
		CertNo	STRING	,	
		GramsOrDWT	NUMERIC	,	
		Length	NUMERIC	,	
		MilliMeter	STRING	,	
		ModelNo	STRING	,	
		MountingType	STRING	,	
		MountingOtherDesc	STRING	,	
		DescOfOther	STRING	,	
		NumberOfPearls	INT64	,	
		PearlType	STRING	,	
		PearlTypeOtherDesc	STRING	,	
		PreOwned	INT64	,	
		SerialNo	STRING	,	
		WatchMountingType	INT64	,	
		WatchMountingOtherDesc	STRING	,	
		FeatureNotes	STRING	,	
		bq_load_date	DATE	,
		JobNumber STRING
    )
   PARTITION BY
   bq_load_date;		   
 

#############################################################################################
####Creating Table RiskPAJewelryFeatureDetail ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskPAJewelryFeature`
	(
		SourceSystem	STRING	,	
		RiskPAJewelryFeatureKey	BYTES	,	
		RiskPAJewelryKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		FixedFeatureRank	INT64	,	
		PAJewelryFeaturePublicID	STRING	,	
		PAJewelryFeatureFixedID	INT64	,	
		RiskLevel	STRING	,	
		PAJewelryPublicID	STRING	,	
		PAJewelryFixedID	INT64	,	
		PolicyPeriodPublicID	STRING	,	
		JewelryArticleNumber	INT64	,	
		PolicyNumber	STRING	,	
		JobNumber	STRING	,	
		EffectiveDate	TIMESTAMP	,	
		ExpirationDate	TIMESTAMP	,	
		IsTransactionSliceEffective	INT64	,	
		FeatureCode	INT64	,	
		FeatureType	STRING	,	
		FeatureDetailType	STRING	,	
		NumberOfCenterStones	INT64	,	
		CenterStoneWeight	NUMERIC	,	
		CenterStoneMilliMeter	STRING	,	
		CenterStoneCut	STRING	,	
		CenterStoneCutOtherDesc	STRING	,	
		CenterStoneType	STRING	,	
		CenterStoneOtherDesc	STRING	,	
		CenterStonePearlType	STRING	,	
		ColorofCenterStone	STRING	,	
		CenterStoneClarity	STRING	,	
		CenterStoneClarityEnhancedType	STRING	,	
		CenterStoneGradingReport	STRING	,	
		NumberOfSideStones	INT64	,	
		SideStoneWeight	NUMERIC	,	
		SideStoneMilliMeter	STRING	,	
		SideStoneCut	STRING	,	
		SideStoneCutOtherDesc	STRING	,	
		SideStoneType	STRING	,	
		SideStoneOtherDesc	STRING	,	
		SideStonePearlType	STRING	,	
		ColorofSideStone	STRING	,	
		SideStoneClarity	STRING	,	
		SideStoneClarityEnhancedType	STRING	,	
		SideStoneGradingReport	STRING	,	
		GradingReportNumber	STRING	,	
		MaterialType	STRING	,	
		GramsOrDWT	NUMERIC	,	
		Length	NUMERIC	,	
		MilliMeter	STRING	,	
		ModelNumber	STRING	,	
		MountingType	STRING	,	
		MountingOtherDesc	STRING	,	
		NumberOfPearls	INT64	,	
		PearlType	STRING	,	
		PearlTypeOtherDesc	STRING	,	
		PreOwned	INT64	,	
		SerialNumber	STRING	,	
		WatchMountingType	INT64	,	
		WatchMountingOtherDesc	STRING	,	
		DescOfOther	STRING	,	
		FeatureNotes	STRING	,	
		bq_load_date	DATE
    )
   PARTITION BY
   bq_load_date;	   
 

#############################################################################################
####Creating Table JewelryItemCoverage ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.CoverageJewelryItem`
	(    
		ItemCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskJewelryItemKey	BYTES,
		SourceSystem	STRING,
		PolicyPeriodPublicID	STRING,
		JobNumber	STRING,
		PolicyNumber	STRING,
		PolicyLinePublicID	STRING,
		ItemPublicID	STRING,
		CoveragePublicID	STRING,
		CoverageTypeCode	STRING,
		CoverageLevel	STRING,
		CoverageNumber	INT64,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		ItemLimit	NUMERIC,
		ItemDeductible	NUMERIC,
		ItemValue	NUMERIC,
		ItemAnnualPremium	NUMERIC,
		CoverageCode	STRING,
		CostPublicID	STRING,
		IsTransactionSliceEffective	INT64,
		FixedCoverageInBranchRank	INT64,
		CoverageFixedID	INT64,
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date;     
   
#############################################################################################
####Creating Table PersonalArticleCoverage ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.CoveragePAJewelry`
	(    
		SourceSystem	STRING	,	
		PAJewelryCoverageKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		RiskPAJewelryKey	BYTES	,	
		CoveragePublicID	STRING	,	
		CoverageLevel	STRING	,	
		JobNumber	STRING	,	
		PAJewelryPublicID	STRING	,
		PAJewelryCoverageFixedID INT64,
		PolicyPeriodPublicID	STRING	,	
		CoverageTypeCode	STRING	,	
		PolicyNumber	STRING	,	
		CoverageNumber	INT64	,	
		EffectiveDate	TIMESTAMP	,	
		ExpirationDate	TIMESTAMP	,	
		IsTempCoverage	INT64	,	
		ItemLimit	NUMERIC	,	
		ItemDeductible	NUMERIC	,	
		ItemValue	NUMERIC	,	
		ItemAnnualPremium	NUMERIC	,	
		CoverageCode	STRING	,	
		CostPublicID	STRING	,	
		FixedCoverageInBranchRank	INT64	,	
		IsTransactionSliceEffective	INT64	,	
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date;      

#############################################################################################
####Creating Table BOPCoverage ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.CoverageBOP`
	(   
		SourceSystem	STRING,
		BOPCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskLocationKey	BYTES,
		RiskBuildingKey	BYTES,
		PolicyPeriodPublicID	STRING,
		JobNumber	STRING,
		PolicyNumber	STRING,
		CoveragePublicID	STRING,
		
		-- PrimaryLocationNumber	INT64,
		CoverageLevel	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		BOPLimit	NUMERIC,
		BOPDeductible	NUMERIC,
		CoverageCode	STRING,
		CoverageNumber	INT64,
		BOPLocationPublicID	STRING,
		BOPBuildingPublicId	STRING,
		PolicyLocationPublicID	STRING,
		EPLICode	STRING,
		PropertyRateNum	STRING,
		TheftAdjPropertyRateNum	STRING,
		RetroactiveDate	STRING,
		RateBookUsed	STRING,
		IsTransactionSliceEffective	INT64,
		FixedCoverageInBranchRank	INT64,
		CoverageFixedID INT64,
		bq_load_date	DATE	
    )
   PARTITION BY
   bq_load_date;	
       
#############################################################################################
####Creating Table ILM_Coverage ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.CoverageIM`
	(    
		SourceSystem	STRING,
		IMCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		PolicyPeriodStatus INT64,
		RiskLocationKey	BYTES,
		RiskStockKey	BYTES,
		PolicyPeriodPublicID	STRING,
		JobNumber	STRING,
		PolicyNumber	STRING,
		CoveragePublicID	STRING,
		CoverageLevel	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		IMLimit	NUMERIC,
		IMDeductible	NUMERIC,
		CoverageNumber	INT64,
		CoverageCode	STRING,
		IMStockPublicID	STRING,
		PolicyLocationPublicID	STRING,
		IMLocationPublicID	STRING,
		SpecifiedCarrier	STRING,
		SpecifiedCarrierExposure	NUMERIC,
		IsTransactionSliceEffective	INT64,
		FixedCoverageInBranchRank	INT64,
		CostPublicID	STRING	,	
		CoverageFixedID	INT64,
		bq_load_date	DATE
)
   PARTITION BY
   bq_load_date;     
   
#############################################################################################
####Creating Table UMB_Coverage ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.CoverageUMB`
	(    
		SourceSystem	STRING,
		UMBCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskLocationKey	BYTES,
		CoveragePublicID	STRING,
		CoverageLevel	STRING,		
		-- PrimaryLocationNumber	INT64,
		JobNumber	STRING,
		PolicyNumber	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		UMBLimit	NUMERIC,
		UMBDeductible	NUMERIC,
		CoverageNumber	INT64,
		CoverageCode	STRING,		
		PolicyPeriodPublicID	STRING,
		BOPLocationPublicID	STRING,
		IsTransactionSliceEffective	INT64,
	    PolicyPeriodFixedID		INT64,
		FixedCoverageInBranchRank	INT64,
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date; 
   
######creating table PJDirectFinancials ###############################
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionPJDirect`
	(
		SourceSystem	STRING	,	
		FinancialTransactionKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		ItemCoverageKey	BYTES	,	
		RiskJewelryItemKey	BYTES	,	
		BusinessType	STRING	,	
		CoveragePublicID	STRING	,	
		TransactionPublicID	STRING	,	
		JobNumber	STRING	,	
		IsTransactionSliceEffective	INT64	,	
		PolicyNumber	STRING	,	
		CoverageLevel	STRING	,	
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,	
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		ItemPublicId	STRING	,	
		ItemNumber	INT64	,	
		ItemLocatedWith	STRING	,	
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,	
		RatedState	STRING	,	
		CostPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		PolicyItemLocationPublicId	STRING	,	
		LineCode	STRING,
		bq_load_date	DATE

    )
   PARTITION BY
   bq_load_date;
   
   
#############################################################################################
####Creating Table UMB_Ceded_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionUMBCeded`
	(
		SourceSystem	STRING	,	
		FinancialTransactionKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		UMBCoverageKey	BYTES	,	
		RiskLocationKey	BYTES	,	
		BusinessType	STRING	,	
		CededCoverable	STRING	,	
		CoveragePublicID	STRING	,	
		TransactionPublicID	STRING	,	
		JobNumber	STRING	,	
		PolicyNumber	STRING	,	
		CoverageLevel	STRING	,	
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,	
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,	
		RatedState	STRING	,	
		CostPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		RatingLocationPublicID	STRING	,	
		CedingRate	NUMERIC	,	
		CededAmount	NUMERIC	,	
		CededCommissions	NUMERIC	,	
		CededTermAmount	NUMERIC	,	
		RIAgreementType	STRING	,	
		RIAgreementNumber	STRING	,	
		CededID	INT64	,	
		CededAgreementID	INT64	,	
		RICoverageGroupID	INT64	,	
		RICoverageGroupType	STRING	,	
		LineCode	STRING	,
        bq_load_date	DATE			
    )
   PARTITION BY
   bq_load_date;	
   
   
#############################################################################################
####Creating Table UMB_Direct_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionUMBDirect`
	(   
		SourceSystem	STRING	,	
		FinancialTransactionKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		UMBCoverageKey	BYTES	,	
		RiskLocationKey	BYTES	,	
		BusinessType	STRING	,	
		CoveragePublicID	STRING	,	
		TransactionPublicID	STRING	,	
		BOPLocationPublicID	STRING	,	
		IsPrimaryLocationBOPLocation	INT64	,	
		JobNumber	STRING	,	
		IsTransactionSliceEffective	INT64	,	
		PolicyNumber	STRING	,	
		CoverageLevel	STRING	,	
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,	
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,	
		RatedState	STRING	,	
		RatingLocationPublicID	STRING	,	
		CostPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		LineCode	STRING,
		bq_load_date	DATE	
    )
   PARTITION BY
   bq_load_date;	
   

   
#############################################################################################
####Creating Table PA_Ceded_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionPACeded`
	(     
		SourceSystem	STRING,
		FinancialTransactionKey	BYTES,
		PolicyTransactionKey	BYTES,
		PAJewelryCoverageKey	BYTES,
		RiskPAJewelryKey	BYTES,
		BusinessType	STRING,
		ArticlePublicId	STRING,
		CoveragePublicID	STRING,
		TransactionPublicID	STRING,
		PrimaryPolicyLocationNumber	INT64,
		PrimaryPolicyLocationPublicID	STRING,
		ArticleNumber	STRING,	
		JobNumber	STRING,
		IsTransactionSliceEffective	INT64,
		PolicyNumber	STRING,
	--	SegmentCode	STRING,
		CoverageLevel	STRING,
		EffectiveDate	DATE,
		ExpirationDate	DATE,
		TransactionAmount	NUMERIC,
		TransactionPostedDate	TIMESTAMP,
		TransactionWrittenDate	DATE,
		ItemLocatedWith	STRING,
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,	
		CedingRate	NUMERIC	,	
		CededAmount	NUMERIC	,	
		CededCommissions	NUMERIC	,	
		CededTermAmount	NUMERIC	,	
		RIAgreementType	STRING	,	
		RIAgreementNumber	STRING	,	
		CededID	INT64	,	
		CededAgreementID	INT64	,	
		--RICoverageGroupID	INT64	,	
		RICoverageGroupType	STRING	,	
		CostPublicID	STRING	,	
		RatedState	STRING	,	
		PolicyArticleLocationPublicId	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		LineCode	STRING,
		bq_load_date	DATE		
	)
   PARTITION BY
   bq_load_date; 
   
#############################################################################################
####Creating Table PA_Direct_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionPADirect`
	(  
		SourceSystem	STRING	,	
		FinancialTransactionKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		PAJewelryCoverageKey	BYTES	,	
		RiskPAJewelryKey	BYTES	,	
		BusinessType	STRING	,	
		ArticlePublicId	STRING	,	
		CoveragePublicID	STRING	,	
		TransactionPublicID	STRING	,	
		PrimaryPolicyLocationNumber	INT64	,	
		PrimaryPolicyLocationPublicID	STRING	,	
		ArticleNumber	INT64	,	
		JobNumber	STRING	,	
		IsTransactionSliceEffective	INT64	,	
		PolicyNumber	STRING	,	
		SegmentCode	STRING	,	
		CoverageLevel	STRING	,	
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,	
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		ItemLocatedWith	INT64	,	
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualBaseRate	NUMERIC	,	
		ActualAdjRate	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,	
		RatedState	STRING	,	
		CostPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		PolicyArticleLocationPublicId	STRING	,	
		LineCode	STRING, 
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date; 
   
#############################################################################################
####Creating Table PJ_Ceded_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionPJCeded`
	(    
		SourceSystem	STRING,
		FinancialTransactionKey	BYTES,
		PolicyTransactionKey	BYTES,
		ItemCoverageKey	BYTES,
	    --RiskPAJewelryKey	BYTES,
		RiskJewelryItemKey BYTES,
		BusinessType	STRING,
		CoveragePublicID	STRING,
		TransactionPublicID	STRING,
		PrimaryPolicyLocationNumber	INT64,	
		PrimaryPolicyLocationPublicID	STRING,
		JobNumber	STRING,
		IsTransactionSliceEffective	INT64,
		PolicyNumber	STRING,
		SegmentCode	STRING,
		CoverageLevel	STRING,
		EffectiveDate	DATE,
		ExpirationDate	DATE,
		TransactionAmount	NUMERIC,
		TransactionPostedDate	TIMESTAMP,
		TransactionWrittenDate	DATE,
	--	PrecisionAmount	NUMERIC,
		ItemPublicId	STRING,
		ItemNumber	INT64,
		ItemLocatedWith	STRING,
		NumDaysInRatedTerm	INT64,
		AdjustedFullTermAmount	NUMERIC,
		ActualBaseRate	NUMERIC,	
		ActualAdjRate	NUMERIC,
		ActualTermAmount	NUMERIC,	
		ActualAmount	NUMERIC,		
	--	CostSubtypeCode	INT64,
	--	CostBaseRate	NUMERIC,
	--	CostAdjRate	NUMERIC,
	--	CostTermAmount	NUMERIC,
	--	CostProratedAmount	NUMERIC,
	--	CostEffectiveDate	DATE,
	--	CostExpirationDate	DATE,
	--	Onset	INT64,
	--	CostOverridden	INT64,
	--	CostChargePatternCode	STRING,
		ChargePattern	STRING,
		ChargeGroup	STRING,
		ChargeSubGroup	STRING,
		RateType	INT64,
	--	RatedState	INT64,
	    RatedState	STRING,
	--	IsPremium	INT64,
	--	IsReportingPolicy	INT64,
		RateBookUsed	STRING,
	--	PeriodStart	DATE,
	--	PeriodEnd	DATE,
		CostPublicID	STRING,
		PolicyPeriodPublicID	STRING,
	--	PolicyLinePublicID	STRING,
	--	PolicyPublicID	STRING,
		PolicyItemLocationPublicId	STRING,
		CedingRate	NUMERIC,
		CededAmount	NUMERIC,
		CededCommissions	NUMERIC,
		CededTermAmount	NUMERIC,
		RIAgreementType	STRING,
		RIAgreementNumber	STRING,
		CededID	INT64,
		CededAgreementID	INT64,
	--	RICoverageGroupID	INT64,
		RICoverageGroupType	STRING,
		LineCode	STRING, 
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date; 
       
   
#############################################################################################
####Creating Table ILM_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionIMDirect`
	(    
		SourceSystem	STRING	,	
		FinancialTransactionKey	BYTES	,	
		PolicyTransactionKey	BYTES	,	
		IMCoverageKey	BYTES	,	
		RiskLocationKey	BYTES	,	
		RiskStockKey	BYTES	,	
		BusinessType	STRING	,	
		TransactionPublicID	STRING	,	
		IsTransactionSliceEffective	INT64	,	
		JobNumber	STRING	,	
		PolicyNumber	STRING	,	
		CoverageLevel	STRING	,	
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,	
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		NumDaysInRatedTerm	INT64	,	
		AdjustedFullTermAmount	NUMERIC	,	
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,	
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	STRING	,	
		RatedState	STRING	,	
		RatingLocationPublicID	STRING	,	
		CostPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		LineCode	STRING,
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date;  

#############################################################################################
####Creating Table ILM_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionIMCeded`
	( 
		SourceSystem	STRING,
		FinancialTransactionKey	BYTES,
		PolicyTransactionKey	BYTES,
		IMCoverageKey	BYTES,
		RiskLocationKey	BYTES,
		RiskStockKey	BYTES,
		BusinessType	STRING,
		CededCoverable	STRING	,
		CoveragePublicID	STRING	,
		TransactionPublicID	STRING	,
		IsTransactionSliceEffective	INT64	,
		JobNumber	STRING	,	
		PolicyNumber	STRING	,	
		CoverageLevel	STRING	,
		EffectiveDate	DATE	,	
		ExpirationDate	DATE	,
		TransactionAmount	NUMERIC	,	
		TransactionPostedDate	TIMESTAMP	,	
		TransactionWrittenDate	DATE	,	
		NumDaysInRatedTerm	INT64	,
		AdjustedFullTermAmount	NUMERIC	,
		ActualTermAmount	NUMERIC	,	
		ActualAmount	NUMERIC	,
		ChargePattern	STRING	,	
		ChargeGroup	STRING	,	
		ChargeSubGroup	STRING	,	
		RateType	INT64	,
		RatedState	STRING	,	
		CedingRate	NUMERIC	,	
		CededAmount	NUMERIC	,	
		CededCommissions	NUMERIC	,	
		CededTermAmount	NUMERIC	,			
		RIAgreementType	STRING	,	
		RIAgreementNumber	STRING	,	
		CededID	INT64	,	
		CededAgreementID	INT64	,			
		RICoverageGroupType	STRING	,		
		RatingLocationPublicID	STRING	,		
		CostPublicID	STRING	,		
		PolicyPeriodPublicID	STRING	,
		LineCode	STRING,		
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date;  



#############################################################################################
####Creating Table BOP_Direct_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionBOPDirect`
	(    
		SourceSystem	STRING,
		FinancialTransactionKey	BYTES,
		PolicyTransactionKey	BYTES,
		BOPCoverageKey	BYTES,
		RiskLocationKey	BYTES,
		RiskBuildingKey	BYTES,
		BusinessType	STRING,
		CoveragePublicID	STRING,
		TransactionPublicID	STRING,
		BOPLocationPublicID	STRING,	
        IsPrimaryLocationBOPLocation	INT64,
		JobNumber	STRING,
		IsTransactionSliceEffective	INT64,
		PolicyNumber	STRING,
		--SegmentCode	STRING,
		CoverageLevel	STRING,
		EffectiveDate	DATE,
		ExpirationDate	DATE,
		TransactionAmount	NUMERIC,
		TransactionPostedDate	TIMESTAMP,
		TransactionWrittenDate	DATE,
	--	CanBeEarned	INT64,
	--	PrecisionAmount	NUMERIC,
		NumDaysInRatedTerm	INT64,
		AdjustedFullTermAmount	NUMERIC,
		ActualTermAmount	NUMERIC,
		ActualAmount	NUMERIC,
	--	CostBaseRate	NUMERIC,
	--	CostAdjRate	NUMERIC,
	--	CostTermAmount	NUMERIC,
	--	CostProratedAmount	NUMERIC,
	--	Onset	INT64,
	--	CostOverridden	INT64,
		ChargePattern	STRING,
		ChargeGroup	STRING,
		ChargeSubGroup	STRING,
		RateType	INT64,
		RatedState	STRING,
		RatingLocationPublicID	STRING,		
		CostPublicID	STRING,
	--	PolicyLinePublicID	STRING,
		PolicyPeriodPublicID	STRING,
		LineCode	STRING,
		bq_load_date	DATE
	)
   PARTITION BY
   bq_load_date;   
   
#############################################################################################
####Creating Table BOP_Ceded_Financials ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.FinancialTransactionBOPCeded`
	(
		SourceSystem	STRING,
		FinancialTransactionKey	BYTES,
		PolicyTransactionKey	BYTES,
		BOPCoverageKey	BYTES,
		RiskLocationKey	BYTES,
		RiskBuildingKey	BYTES,
		BusinessType	STRING,
		CededCoverable	STRING,
		CoveragePublicID	STRING,
		TransactionPublicID	STRING,
		BOPLocationPublicID STRING,
		IsPrimaryLocationBOPLocation INT64,
		JobNumber	STRING,
		IsTransactionSliceEffective	INT64,
		PolicyNumber	STRING,
		SegmentCode	STRING,
		CoverageLevel	STRING,
		EffectiveDate	DATE,
		ExpirationDate	DATE,
		TransactionAmount	NUMERIC,
		TransactionPostedDate	TIMESTAMP,
		TransactionWrittenDate	DATE,
		NumDaysInRatedTerm	INT64,		
		AdjustedFullTermAmount	NUMERIC,
        ActualTermAmount	NUMERIC,
		ActualAmount	NUMERIC,
		ChargePattern	STRING,
		ChargeGroup	STRING,
		ChargeSubGroup	STRING,		
		RateType	INT64,
		RatedState	STRING,
		CedingRate	NUMERIC,
		CededAmount	NUMERIC,
		CededCommissions	NUMERIC,
		CededTermAmount	NUMERIC,
		RIAgreementType	STRING,
		RIAgreementNumber	STRING,
		CededID	INT64,
		CededAgreementID	INT64,
	--	RICoverageGroupID	INT64,
		RICoverageGroupType	STRING,
		RatingLocationPublicID	STRING,		
		CostPublicID	STRING,
		PolicyPeriodPublicID	STRING,
		LineCode	STRING, 
		bq_load_date	DATE 
	)
   PARTITION BY
   bq_load_date; 

   
-- #############################################################################################
-- ####Creating Table Account ###########
-- CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.Account`
-- 	(
-- 		AccountKey	BYTES,
-- 		SourceSystem	STRING,
-- 		AccountHolderContactPublicID	STRING,
-- 		BillingPrimaryPayerContactPublicID	STRING,
-- 		AccountJewelerContactPublicID	STRING,
-- 		InsuredContactPublicID	STRING,
-- 		AccountNumber	STRING,			
-- 		AccountCreatedDate	TIMESTAMP,
-- 		AccountOrigInceptionDate	TIMESTAMP,
-- 		AccountDistributionSource  STRING,
-- 		ApplicationTakenBy STRING,
-- 		AccountStatus	STRING,
-- 		AccountIsJeweler	INT64,
-- 		SpecialAccount	INT64,
-- 		ProxyReceivedDate	TIMESTAMP,
-- 		AccountResolicit	INT64,
-- 		AccountResolicitDate	TIMESTAMP,
-- 		PaperlessDeliveryIndicator	INT64,
-- 		PaperlessDeliveryStatusDate	TIMESTAMP,
-- 		PreferredCommunicationMethodCode STRING,
-- 		RetiredDateTime	TIMESTAMP,	
-- 		BillingAccountType	STRING,
-- 		AutoPayIndicator	INT64,
-- 		PaymentMethod	STRING,
-- 		CreditCardExpDate	TIMESTAMP,		
-- 		CreateTimeStamp	DATETIME,	
--         bq_load_date	DATE
--     )
--    PARTITION BY
--    bq_load_date;	
  
#############################################################################################
####Creating Table RiskLocationBusinessOwners ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskLocationBusinessOwners`
	(  
		SourceSystem	STRING,
		RiskLocationKey	BYTES,
		PolicyTransactionKey	BYTES,
		FixedLocationRank	INT64,
		LocationPublicID	STRING,
		PolicyPeriodPublicID	STRING,
		JobNumber	STRING,
		PolicyNumber	STRING,
		RiskLevel	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTransactionSliceEffective	INT64,
		LocationNumber	INT64,
		LocationFixedID	INT64,
		LocationAddress1	STRING,
		LocationAddress2	STRING,
		LocationCity	STRING,
		LocationState	STRING,
		LocationStateCode	STRING,
		LocationCountry	STRING,
		LocationPostalCode	STRING,
		LocationAddressStatus	STRING,
		LocationCounty	STRING,
		LocationCountyFIPS	STRING,
		LocationCountyPopulation	INT64,
		TerritoryCode	STRING,
		Coastal	STRING,
		CoastalZone	STRING,
		SegmentationCode	STRING,
		RetailSale	NUMERIC,
		RepairSale	NUMERIC,
		AppraisalSale	NUMERIC,
		WholesaleSale	NUMERIC,
		ManufacturingSale	NUMERIC,
		RefiningSale	NUMERIC,
		GoldBuyingSale	NUMERIC,
		PawnSale	NUMERIC,
		Casting	NUMERIC,
		Plating	NUMERIC,
		AllOtherMfg	NUMERIC,
		FullTimeEmployees	INT64,
		PartTimeEmployees	INT64,
		Owners	INT64,
		PublicProtection	STRING,
		LocationType	STRING,
		LocationTypeName	STRING,
		LocationTypeOtherDescription	STRING,
		AnnualSales	INT64,
		AnnualSalesAttributableToMfg	INT64,
		WindOrHailDeductiblePercent	STRING,
		IsBusIncomeAndExtraExpDollarLimit	INT64,
		NuclBioChemRadExcl	INT64,
		JwlryExclBusIncomeExtraExpTheftExcl	INT64,
		RemoveInsToValueProvision	INT64,
		OutdoorTheftExcl	INT64,
		PunitiveDamagesCertTerrorismExcl	INT64,
		AmdCancProvisionsCovChg	INT64,
		AmdCancProvisionsCovChgDaysAdvNotice	NUMERIC,
		ForkliftExtCond	INT64,
		ForkliftExtCondBlanketLimit	NUMERIC,
		ForkliftExtCondDeduct	NUMERIC,
		PattrnFrmsCond	INT64,
		PattrnFrmsCondLimit	STRING,
		BusinessPremise	STRING,
		InviteJewelryClients	STRING,
		SignageAtHome	STRING,
		AnimalsInPremise	STRING,
		WhatSpecies	STRING,
		SwimmingPool	STRING,
		Trampoline	STRING,
		MerchandiseAccessChildren	STRING,
		DescribeHow	STRING,
		HasBeenInBusiness	STRING,
		PreviousLocationAddress	STRING,
		PastLosses	STRING,
		ExplainLoss	STRING,
		ExposureToFlammables	STRING,
		ExplainExposure	STRING,
		ContainLead	STRING,
		ExplainContainLead	STRING,
		IsPrimaryLocation STRING,
		bq_load_date	DATE
      )
   PARTITION BY
   bq_load_date; 	
   
#############################################################################################
####Creating Table RiskLocationIM ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskLocationIM`
	(    
		SourceSystem	STRING	,	
		RiskLocationKey	BYTES	,	
		PolicyTransactionKey	BYTES	,
		PolicyPeriodStatus INT64,
		FixedLocationRank	INT64	,	
		IsTransactionSliceEffective	INT64	,	
		LocationPublicID	STRING	,	
		PolicyPeriodPublicID	STRING	,	
		RiskLevel	STRING	,	
		EffectiveDate	TIMESTAMP	,	
		ExpirationDate	TIMESTAMP	,	
		PolicyNumber	STRING	,	
		JobNumber	STRING	,			
		LocationNumber	INT64	,	
		LocationFixedID	INT64	,	
		LocationAddress1	STRING	,	
		LocationAddress2	STRING	,	
		LocationCity	STRING	,	
		LocationState	STRING	,	
		LocationStateCode	STRING	,	
		LocationCountry	STRING	,	
		LocationPostalCode	STRING	,	
		LocationAddressStatus	STRING	,	
		LocationCounty	STRING	,	
		LocationCountyFIPS	STRING	,	
		LocationCountyPopulation	INT64	,	
		TerritoryCode	STRING	,	
		Coastal	STRING	,	
		CoastalZone	STRING	,	
		SegmentationCode	STRING	,	
		RetailSale	NUMERIC	,	
		RepairSale	NUMERIC	,	
		AppraisalSale	NUMERIC	,	
		WholesaleSale	NUMERIC	,	
		ManufacturingSale	NUMERIC	,	
		RefiningSale	NUMERIC	,	
		GoldBuyingSale	NUMERIC	,	
		PawnSale	NUMERIC	,	
		RatedAs	STRING	,	
		FullTimeEmployees	INT64	,	
		PartTimeEmployees	INT64	,	
		Owners	INT64	,	
		PublicProtection	STRING	,	
		LocationTypeCode	STRING	,	
		LocationTypeName	STRING	,	
		LocationTypeOtherDescription	STRING	,	
		AnnualSales	INT64	,	
		AreSalesPerformedViaInternet	INT64	,	
		InternetSalesAmount	INT64	,	
		NormalBusinessHours	STRING	,	
		TotalValueShippedInLast12Months	INT64	,	
		ConstructionCode	STRING	,	
		ConstructionType	STRING	,	
		ConstructionYearBuilt	INT64	,	
		NumberOfFloors	INT64	,	
		FloorNumbersOccupied	STRING	,	
		TotalBuildngAreaSQFT	INT64	,	
		AreaOccupiedSQFT	INT64	,	
		Basement	INT64	,	
		SmokeDetectors	INT64	,	
		PercentSprinklered	STRING	,	
		BuildingClass	STRING	,	
		LocationClassInsuredBusinessCode	STRING	,	
		LocationClassInsuredBusinessClassification	STRING	,	
		HeatingYear	INT64	,	
		PlumbingYear	INT64	,	
		RoofingYear	INT64	,	
		WiringYear	INT64	,	
		AdjacentOccupancies	STRING	,	
		SharedPremises	INT64	,	
		PremisesSharedWith	STRING	,	
		HasShowCaseWindows	INT64	,	
		NumberOfShowWindows	INT64	,	
		WindowsEquippedWithLocks	INT64	,	
		WindowsKeptLocked	INT64	,	
		WindowsKeptUnlockedReason	STRING	,	
		WindowsMaxValue	INT64	,	
		LockedDoorBuzzer	INT64	,	
		BarsOnWindows	INT64	,	
		SteelCurtainsGates	INT64	,	
		MonitoredFireAlarm	INT64	,	
		DoubleCylDeadBoltLocks	INT64	,	
		SafetyBollardCrashProt	INT64	,	
		GlassProtection	INT64	,	
		ManTrapEntry	INT64	,	
		RecordingCameras	INT64	,	
		DaytimeArmedUniformGuard	INT64	,	
		ElecTrackingRFTags	INT64	,	
		MultipleAlarmSystems	INT64	,	
		DualMonitoring	INT64	,	
		SecuredBuilding	INT64	,	
		AddtlProtectionNotMentioned	STRING	,	
		OperatingCameraSystem	INT64	,	
		CameraCovPremExclRestrooms	INT64	,	
		CameraOperRecording	INT64	,	
		CameraBackUpRecOffSite	INT64	,	
		CameraAccessRemotely	INT64	,	
		IsCamerasOnExterior	INT64	,	
		PremiseBurglarAlarmDetMotion	INT64	,	
		CoverAreaMerchLeftOutSafeVault	INT64	,	
		CoverAlarmControlPanel	INT64	,	
		CoverSafeVaultArea	INT64	,	
		HoldupAlarmSystem	INT64	,	
		MobileDevicesUsed	INT64	,	
		BurglarAlarmSysMonitored	STRING	,	
		BurglarAlarmWhenClosed	INT64	,	
		BurglarAlarmWhenClosedReason	STRING	,	
		OpeningClosingMonitored	INT64	,	
		OpeningClosingSupervised	INT64	,	
		NbrOnCallInAlarmConditions	STRING	,	
		RespondToAlarmConditions	INT64	,	
		RespondToAlarmConditionsNoReason	STRING	,	
		OtherEmployAbleDeactivateAlarm	STRING	,	
		SafeScatteredOnPremiseOrInOneArea	STRING	,	
		SafeVaultStkroomUsedByOther	INT64	,	
		SafeVaultStkroomUsedByOtherReason	STRING	,	
		AnySafeVaultStkroomsOnExtWall	INT64	,	
		IsSafeOnExterior	INT64	,	
		LeastNbrEmployOnPremiseBusHrs	INT64	,	
		YearsInBusiness	STRING	,	
		JBTFinancial	STRING	,	
		Inventory	STRING	,	
		ClaimsFree	STRING	,	
		PhysicalProtection	STRING	,	
		ArmedUniformGuard	STRING	,	
		MetalDetManTrap	STRING	,	
		ElecTrackDevices	STRING	,	
		HoldUpAlarm	STRING	,	
		ElectronicProt	STRING	,	
		PhysProtectedDoorWindow	STRING	,	
		NoExtGrndFloorExposure	STRING	,	
		LockedDoorBuzzSys	STRING	,	
		SecuredBldg	STRING	,	
		MonitoredFireAlarmSys	STRING	,	
		ClosedBusStoragePractices	STRING	,	
		CameraSystem	STRING	,	
		DblCylDeadBoltLocks	STRING	,	
		AddlProtNotMentioned	STRING	,	
		MltplAlarmsMonitoring	STRING	,	
		TotalInStoragePercent	NUMERIC	,	
		BankVaultPercent	NUMERIC	,	
		OutOfStorageAmount	INT64	,	
		BurglarylInclSpecProp	INT64	,	
		BurglaryInclSpecPropValue	STRING	,	
		ExclBurglary	INT64	,	
		ExclBurglaryClosed	INT64	,	
		ExclBurglaryClosedHoursOpen	STRING	,	
		ExclBurglaryClosedTimeZone	STRING	,	
		ExclBurglaryClosedTravelLimit	NUMERIC	,	
		ExclBurglaryJwlWtchSpecified	INT64	,	
		ExclBurglaryJwlWtchSpecifiedValue	NUMERIC	,	
		ExclBurglaryJwlySpecifiedAmt	INT64	,	
		ExclBurglaryJwlySpecifiedAmtValue	NUMERIC	,	
		ExclOutOfSafe	INT64	,	
		ExclFireLightningSmoke	INT64	,	
		HasSpecifiedBurglaryLimit	INT64	,	
		SpecifiedBurglaryLimitValue	NUMERIC	,	
		AlarmSignalResponseReq	INT64	,	
		BankVaultReq	INT64	,	
		BankVaultReqOutOfSafeVaultPercent	NUMERIC	,	
		BankVaultReqInSafeVaultPercent	STRING	,	
		BankVaultReqBankVaultPercent	NUMERIC	,	
		BurglaryDeductible	INT64	,	
		BurglaryDeductibleValue	STRING	,	
		EstimatedInventory	INT64	,	
		IndivSafeMaxLimit	INT64	,	
		IndivSafeMaxLimitInSafeVaultStkPercent	STRING	,	
		IndivSafeMaxLimitMfg	INT64	,	
		IndivSafeVaultMaxCap	INT64	,	
		InnerSafeChest	INT64	,	
		InnerSafeChestInSafePercent	STRING	,	
		InSafePercent	INT64	,	
		InSafePercentIndivSafeVaultMaxCapacity	INT64	,	
		KeyedInvestigatorResponse	INT64	,	
		KeyedInvestigatorResponseReq	STRING	,	
		LockedCabinets	INT64	,	
		LockedCabinetsPercentKept	NUMERIC	,	
		IsMaxDollarLimit	INT64	,	
		MaxLimitOutOfSafeAmt	STRING	,	
		MaxLimitBurglary	INT64	,	
		MaxLimitBurglaryInSafeVaultStkPct	STRING	,	
		MaxLimitBurglaryBurgLimit	NUMERIC	,	
		MaxLimitBurglaryAOPLimit	NUMERIC	,	
		MaxLimitFinishedMerch	INT64	,	
		MaxLimitFinishedMerchOutOfSafeVaultAmt	STRING	,	
		MaxLimitWarranty	INT64	,	
		MaxLimitWarrantyOutOfSafeVaultAmt	NUMERIC	,	
		MaxStockValueOutWhenClosed	INT64	,	
		MaxJwlryValueOutWhenClosed	NUMERIC	,	
		MaxNonJwlyValueOutWhenClosed	NUMERIC	,	
		MaxOutofSafeWhenClosed	INT64	,	
		MaxOutWhenClosedMaxOutOfSafeVault	STRING	,	
		MaxOutWhenClosedWithWarranty	INT64	,	
		MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault	NUMERIC	,	
		MaxOutOfLockedSafeVaultLimitSched	INT64	,	
		MaxPerItemSafeVault	INT64	,	
		MaxPerItemSafeVaultCostPerItem	NUMERIC	,	
		MaxPerItemSafeVaultStkroom	INT64	,	
		MaxPerItemSafeVaultStkroomCostPerItem	NUMERIC	,	
		MaxValueInVault	INT64	,	
		MaxValueInVaultInSafePercent	NUMERIC	,	
		MinMaxProportionInSafe	INT64	,	
		MinMaxProportionInSafePercent	STRING	,	
		MinNbrEmployeeCond	INT64	,	
		MinNbrEmployeeCondNumber	NUMERIC	,	
		MinProportionValueSafeVault	INT64	,	
		MinProportionValueSafeVaultInnerSafe	INT64	,	
		MinProportionValueStkroom	INT64	,	
		RobberyDeductible	INT64	,	
		RobberyDeductibleValue	STRING	,	
		SafeBurglaryDeductible	INT64	,	
		SafeBurglaryDeductibleValue	STRING	,	
		SafeMinLimit	INT64	,	
		SafeVaultHurrWarningReq	INT64	,	
		SaefVaultHurrWarningReqTX	INT64	,	
		SafeVaultHurrWarningReqDeductible	INT64	,	
		SafeVaultHurrWarningReqDeductibleValue	STRING	,	
		ShowJewelryConditions	INT64	,	
		SharedPremiseSecurity	INT64	,	
		StkroomMaxDollarLimit	INT64	,	
		StkroomMaxLimitOutOfSafeVaultAmt	STRING	,	
		StkroomMaxLimit	INT64	,	
		StkroomMaxLimitInSafePercent	STRING	,	
		TheftProtection	INT64	,	
		TheftProtectionDesc	STRING	,	
		TheftProtectionGuardWarranty	INT64	,	
		TheftProtectionSecurityDevice	INT64	,	
		TheftProtectionSecurityDeviceDesc	STRING	,	
		TotalPercentInSafe	INT64	,	
		TotalPercentInSafeInSafeVaultStkrmPct	STRING	,	
		TotalPercentInSafeNotToExceedAmt	NUMERIC	,	
		ShowcaseOrWindowCondition	INT64	,	
		BusinessPremise	STRING	,	
		InviteJewelryClients	STRING	,	
		SignageAtHome	STRING	,	
		AnimalsOnPremise	STRING	,	
		AnimalSpecies	STRING	,	
		SwimmingPool	STRING	,	
		Trampoline	STRING	,	
		MerchandiseAccessibleToChildren	STRING	,	
		DescribeHowAccessRestricted	STRING	,	
		IsPrimaryLocation STRING,
		bq_load_date	DATE		
      )
   PARTITION BY
   bq_load_date; 	
  
#############################################################################################
####Creating Table RiskStockIM ###########
CREATE TABLE IF NOT EXISTS  `{project}.{core_dataset}.RiskStockIM`
	(   
		SourceSystem	STRING,
		RiskStockKey	BYTES,
		RiskLocationKey	BYTES,
		PolicyTransactionKey	BYTES,
		FixedStockRank	INT64,
		StockPublicID	STRING,
		StockFixedID	INT64,
		LocationPublicID	STRING,
		PolicyPeriodPublicID	STRING,
		RiskLevel	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		JobNumber	STRING,
		PolicyNumber	STRING,
		LocationNumber	INT64,
		IsTransactionSliceEffective	INT64,
		IsCompleteInvAnnualOrMore	INT64,
		LastInventoryTotal	INT64,
		LastInventoryDate	TIMESTAMP,
		InventoryType	STRING,
		HasDetailedInvRecords	INT64,
		HasPurchaseInvoices	INT64,
		HasAwayListings	INT64,
		PriorInventoryTotal	INT64,
		PriorInventoryDate	TIMESTAMP,
		MaxStockValue	INT64,
		MaxDailyScrapValue	INT64,
		IsCustPropertyRecorded	INT64,
		CustomerProperyAverage	INT64,
		ConsignmentPropertyAverage	INT64,
		InventoryPremiumBase	INT64,
		OutofSafeVltStkrmExposure	INT64,
		PawnPropertyHandled	INT64,
		PawnCoverageIncluded	INT64,
		PawnLastInventoryTotal	INT64,
		PawnLastInventoryDate	TIMESTAMP,
		PawnPriorInventoryTotal	INT64,
		PawnPriorInventoryDate	TIMESTAMP,
		PawnMaxStockValue	INT64,
		InventoryLooseDiamonds	STRING,
		InventoryWatchesLowValue	STRING,
		InventoryWatchesHighValue	STRING,
		InventoryHighValue	STRING,
		InventoryLowValue	STRING,
		InventoryScrap	STRING,
		InventoryNonJewelry	STRING,
		InventoryOther	STRING,
		HasLuxuryBrandWatches	INT64,
		HasWatchBlancpain	INT64,
		HasWatchBreitling	INT64,
		HasWatchCartier	INT64,
		HasWatchOmega	INT64,
		HasWatchPatekphilippe	INT64,
		HasWatchRolex	INT64,
		HasWatchOther	INT64,
		WatchOtherExplanation	STRING,
		IsOfficialRolexDealer	INT64,
		ProtectionClassCode	STRING,
		IsExclStkForSaleFromTheft	INT64,
		ExclStkForSaleFromTheftPremium	NUMERIC,
		IsExclFromTheftExclBurglary	INT64,
		IsExcludeNonJwlryInv	INT64,
		IsExclSpecStkForSaleInv	INT64,
		ExclSpecStkForSaleInvPropNotIncl	STRING,
		IsJwlryPawnPledgedVal	INT64,
		JwlryPawnPledgedValMethod	STRING,
		JwlryPawnPledgedValOtherDesc	STRING,
		JwlryPawnPledgedValMethodMultiplier	STRING,
		IsJwlryPawnUnpledgedVal	INT64,
		JwlryPawnUnpledgedValMethod	STRING,
		JwlryPawnUnpledgedValOtherDesc	STRING,
		JwlryPawnUnpledgedValMethodMultiplier	STRING,
		IsUnrepInv	INT64,
		UnrepInvDescValue	STRING,
		IsStockLimitClsd	INT64,
		StockLimitClsdOpenStockLimit	NUMERIC,
		StockLimitClsdClsdStockLimit	NUMERIC,
		EQTerritory	STRING,
		EQZone	STRING,
		EQZoneDesc	STRING,
		FloodZone	STRING,
		FloodZoneDesc	STRING,
		FirmIndicator	STRING,
		FloodInceptionDate	TIMESTAMP,
		OtherFloodInsurance	INT64,
		OtherFloodInsuranceCarrier	STRING,
		OtherFloodPolicyNumber	STRING,
		OtherFloodPrimaryNFIP	INT64,
		OtherFloodInformation	STRING,
		OtherFloodInsUndWaiverJMPrimary	INT64,
		bq_load_date	DATE
      )
   PARTITION BY
   bq_load_date; 	
   
#############################################################################################
####Creating Table TransactionConfiguration ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.TransactionConfiguration`
	(  
		SourceSystem	STRING	,	
		PolicyTransactionKey	BYTES	,	
		AccountNumber	STRING	,
		Segment		STRING	,
		BusinessUnit	STRING	,
		OfferingCode 	STRING	,
		PolicyNumber	STRING	,	
		LegacyPolicyNumber	STRING	,	
		--Segment	STRING	,	
		--JobNumber	STRING	,	
		--TranType	STRING	,	
		PeriodEffDate	DATE	,	
		PeriodEndDate	DATE	,
		JobNumber	STRING	,
		TranType	STRING	,	
		TermNumber	INT64	,	
		ModelNumber	INT64	,
		TransEffDate	DATE	,
		JobCloseDate	DATE	,	
		CancelEffDate	DATE	,
		WrittenDate	TIMESTAMP	,	
		AccountingDate	TIMESTAMP	,
		CalendarYearBeginDate	DATE	,	
		CalendarYearEndDate	DATE	,
		--TermNumber	INT64	,	
		--ModelNumber	INT64	,	
		--PolicyNumberGroup	STRING	,	
		PolicyYearBeginDate	DATE	,	
		--Version	INT64	,	
		PolicyYearEndDate	DATE	,
		CalendarYearMultiplier	INT64	,	
		CurrentPolicyStatus	STRING	,
		--BusinessUnit	STRING	,	
		--TransactionStatus	STRING	,	
		bq_load_date	DATE
	)	
   PARTITION BY
   bq_load_date; 	

#############################################################################################
####Creating Table ProductAndRiskConfiguration ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.ProductAndRiskConfiguration` 
	(
  SourceSystem STRING,
  PolicyTransactionKey BYTES,
  RiskJewelryItemKey BYTES,
  RiskPAJewelryKey BYTES,
  RiskLocationBusinessOwnersKey BYTES,
  RiskBuildingKey BYTES,
  RiskLocationIMKey BYTES,
  RiskStockKey BYTES,
  AccountNumber STRING,
  Segment STRING,
  BusinessUnit STRING,
  PolicyNumber STRING,
  LegacyPolicyNumber STRING,
  PeriodEffDate DATE,
  PeriodEndDate DATE,
  JobNumber STRING,
  TranType STRING,
  TermNumber INT64,
  ModelNumber INT64,
  TransEffDate DATE,
  JobCloseDate DATE,
  WrittenDate TIMESTAMP,
  CancelEffDate DATE,
  AccountingDate TIMESTAMP,
  TranCYBegDate DATE,
  TranCYEndDate DATE,
  TranCYMultiplier INT64,
  TranPYBegDate DATE,
  TranPYEndDate DATE,
  ProductCode STRING,
  ProductType STRING,
  ItemNumber INT64,
  JewelryArticleNumber INT64,
  IsInactive INT64,
  LocationNumber INT64,
  IsPrimaryLocation STRING,
  BuildingNumber INT64,
  IsPrimaryBuildingLocation STRING,
  bq_load_date DATE
  )
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Holds Product and Risk Configuration dates and facts",
      labels=[("ref_kimberlite_buildingblock","")]
);
   
#############################################################################################
####Creating Table PolicyLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.PolicyLevelAttributes` 
	(     
		SourceSystem	STRING	,	
		PolicyTransactionKey	BYTES	,	
		PolicyTenure	FLOAT64	,
		TransEffDate	TIMESTAMP,
		WrittenDate	TIMESTAMP	,	
		SubmissionDate	TIMESTAMP	,	
		JobCloseDate	TIMESTAMP	,	
		DeclineDate	TIMESTAMP	,	
		TermNumber	INT64	,	
		ModelNumber	INT64	,	
		TransactionStatus	STRING	,	
		TransactionCostRPT	NUMERIC	,	
		TotalCostRPT	NUMERIC	,	
		EstimatedPremium	NUMERIC	,	
		TotalMinimumPremiumRPT	NUMERIC	,	
		TotalMinimumPremiumRPTft	NUMERIC	,	
		TotalPremiumRPT	NUMERIC	,	
		TotalSchedPremiumRPT	NUMERIC	,	
		TotalUnschedPremiumRPT	NUMERIC	,	
		NotTakenReason	STRING	,	
		NotTakenExplanation	STRING	,	
		PolicyChangeReason	STRING	,	
		CancelSource	STRING	,	
		CancelType	STRING	,	
		CancelReason	STRING	,	
		CancelReasonDescription	STRING	,	
		CancelEffectiveDate	TIMESTAMP	,	
		ReinstReason	STRING	,	
		RewriteType	STRING	,	
		RenewalCode	STRING	,	
		PreRenewalDirection	STRING	,	
		NonRenewReason	STRING	,	
		NonRenewExplanation	STRING	,	
		IsConditionalRenewal	INT64	,	
		IsStraightThrough	INT64	,	
		AccountingDate	TIMESTAMP	,	
		PolicyTransactionProduct	STRING	,	
		bq_load_date	DATE		
	)   
   PARTITION BY
   bq_load_date;    
  
#############################################################################################
####Creating Table CLRiskBusinessOwnersAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.CLRiskBusinessOwnersAttributes`  
	(
		SourceSystem	STRING,
		RiskLocationKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskBuildingKey	BYTES,
		JobNumber	STRING,
		TransactionStatus	STRING,
		IsTransactionSliceEffLocn	INT64,
		IsTransactionSliceEffBldg	INT64,
		IsPrimaryBuildingLocation STRING, 
		IsPrimaryLocation STRING,
		RiskLevel	STRING,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		LocationNumber	INT64,
		LocationAddress1	STRING,
		LocationAddress2	STRING,
		LocationCity	STRING,
		LocationState	STRING,
		LocationStateCode	STRING,
		LocationCountry	STRING,
		LocationPostalCode	STRING,
		LocationAddressStatus	STRING,
		LocationCounty	STRING,
		LocationCountyFIPS	STRING,
		LocationCountyPopulation	INT64,
		TerritoryCode	STRING,
		Coastal	STRING,
		CoastalZone	STRING,
		SegmentationCode	STRING,
		RetailSale	NUMERIC,
		RepairSale	NUMERIC,
		AppraisalSale	NUMERIC,
		WholesaleSale	NUMERIC,
		ManufacturingSale	NUMERIC,
		RefiningSale	NUMERIC,
		GoldBuyingSale	NUMERIC,
		PawnSale	NUMERIC,
		Casting	NUMERIC,
		Plating	NUMERIC,
		AllOtherMfg	NUMERIC,
		FullTimeEmployees	INT64,
		PartTimeEmployees	INT64,
		Owners	INT64,
		PublicProtection	STRING,
		LocationType	STRING,
		LocationTypeName	STRING,
		LocationTypeOtherDescription	STRING,
		AnnualSales	INT64,
		AnnualSalesAttributableToMfg	INT64,
		WindOrHailDeductiblePercent	STRING,
		IsBusIncomeAndExtraExpDollarLimit	INT64,
		NuclBioChemRadExcl	INT64,
		JwlryExclBusIncomeExtraExpTheftExcl	INT64,
		RemoveInsToValueProvision	INT64,
		OutdoorTheftExcl	INT64,
		PunitiveDamagesCertTerrorismExcl	INT64,
		AmdCancProvisionsCovChg	INT64,
		AmdCancProvisionsCovChgDaysAdvNotice	NUMERIC,
		ForkliftExtCond	INT64,
		ForkliftExtCondBlanketLimit	NUMERIC,
		ForkliftExtCondDeduct	NUMERIC,
		PattrnFrmsCond	INT64,
		PattrnFrmsCondLimit	STRING,
		BusinessPremise	STRING,
		InviteJewelryClients	STRING,
		SignageAtHome	STRING,
		AnimalsInPremise	STRING,
		WhatSpecies	STRING,
		SwimmingPool	STRING,
		Trampoline	STRING,
		MerchandiseAccessChildren	STRING,
		DescribeHow	STRING,
		HasBeenInBusiness	STRING,
		PreviousLocationAddress	STRING,
		PastLosses	STRING,
		ExplainLoss	STRING,
		ExposureToFlammables	STRING,
		ExplainExposure	STRING,
		ContainLead	STRING,
		ExplainContainLead	STRING,
	--	BuildingPublicID	STRING,
		LocationLevelRisk	STRING,
		BuildingLevelRisk	STRING,
		BldgEffDate	TIMESTAMP,
		BldgExpDate	TIMESTAMP,
		BuildingNumber	INT64,
		BuildingDescription	STRING,
		LocationClassPredominantBldgOccupancyCode	STRING,
		LocationClassPredominantBldgOccupancyClass	STRING,
		LocationClassInsuredBusinessCode	STRING,
		LocationClassInsuredBusinessClass	STRING,
		BldgCodeEffectivenessGrade	STRING,
		BldgLimitToValuePercent	STRING,
		BPPLimitToValuePercent	STRING,
		IsTheftExclusion	INT64,
		IsBrandsAndLabels	INT64,
		IsWindOrHail	INT64,
		PublicUtilities	STRING,
		BuildingClass	STRING,
		EQTerritory	STRING,
		PremiumBasisAmount	INT64,
		ConstructionCode	STRING,
		ConstructionType	STRING,
		ConstructionYearBuilt	INT64,
		NumberOfFloors	INT64,
		TotalBuildngAreaSQFT	INT64,
		AreaOccupiedSQFT	INT64,
		Basement	INT64,
		IsBasementFinished	INT64,
		RoofMaterial	STRING,
		SmokeDetectors	INT64,
		PercentSprinklered	STRING,
		HeatingYear	INT64,
		PlumbingYear	INT64,
		RoofingYear	INT64,
		WiringYear	INT64,
		LastBldgInspectionDate	TIMESTAMP,
		LastBldgValidationDate	TIMESTAMP,
		PercentOccupied	INT64,
		AdjacentOccupancies	STRING,
		SharedPremises	INT64,
		PremisesSharedWith	STRING,
		BldgHandlePawnPropOtherJwlry	INT64,
		BldgNatlPawnAssociation	INT64,
		BldgMemberOtherPawnAssociations	STRING,
		BldgHavePawnLocalLicense	INT64,
		BldgHavePawnStateLicense	INT64,
		BldgHavePawnFederalLicense	INT64,
		BldgPawnLicenseAreYouBounded	INT64,
		BldgTotalAnnualSales	INT64,
		BldgSalesPawnPercent	NUMERIC,
		BldgSalesRetailPercent	NUMERIC,
		BldgSalesCheckCashingPercent	NUMERIC,
		BldgSalesGunsAndAmmunitionPercent	NUMERIC,
		BldgSalesAutoPawnPercent	NUMERIC,
		BldgSalesTitlePawnPercent	NUMERIC,
		BldgSalesOtherPercent	NUMERIC,
		BldgSalesOtherPercentDescription	STRING,
		BldgAvgDailyAmtOfNonJewelryPawnInventory	NUMERIC,
		BldgReportTransactionsRealTime	INT64,
		BldgReportTransactionsDaily	INT64,
		BldgReportTransactionsWeekly	INT64,
		BldgReportTransactionsOther	INT64,
		BldgReportTransactionsOtherDesc	STRING,
		BldgLoanPossessOrSellFireArms	INT64,
		BldgFedFirearmsDealerLicense	INT64,
		BldgTypesFedFirearmsLicenses	STRING,
		BldgFirearmsDisplayLockAndKey	INT64,
		BldgHowSecureLongGuns	STRING,
		BldgHowSecureOtherFirearms	STRING,
		BldgShootingRangeOnPrem	INT64,
		BldgHowSafeguardAmmoGunPowder	STRING,
		BuildingAddlCommentsAboutBusiness	STRING,
		bq_load_date	DATE
	)   
   PARTITION BY
   bq_load_date; 
   
#############################################################################################
####Creating Table CLRiskInLandMarineAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.CLRiskInLandMarineAttributes`
	(
		SourceSystem	STRING,	
		JobNumber	STRING,	
		PolicyTransactionKey	BYTES,	
		RiskLocationKey	BYTES,	
		RiskStockKey	BYTES,
		TransactionStatus STRING,
		IsTransactionSliceEffStock INT64 ,
        IsTransactionSliceEffLocn INT64,
		LocationEffectiveDate	TIMESTAMP,	
		LocationExpirationDate	TIMESTAMP,	
		StockEffectiveDate	TIMESTAMP,	
		StockExpirationDate	TIMESTAMP,	
		LocationNumber	INT64,	
		LocationFixedID	INT64,	
		LocationAddress1	STRING,	
		LocationAddress2	STRING,	
		LocationCity	STRING,	
		LocationState	STRING,	
		LocationStateCode	STRING,	
		LocationCountry	STRING,	
		LocationPostalCode	STRING,	
		LocationAddressStatus	STRING,	
		LocationCounty	STRING,	
		LocationCountyFIPS	STRING,	
		LocationCountyPopulation	INT64,	
		TerritoryCode	STRING,	
		Coastal	STRING,	
		CoastalZone	STRING,	
		SegmentationCode	STRING,	
		RetailSale	NUMERIC,	
		RepairSale	NUMERIC,	
		AppraisalSale	NUMERIC,	
		WholesaleSale	NUMERIC,	
		ManufacturingSale	NUMERIC,	
		RefiningSale	NUMERIC,	
		GoldBuyingSale	NUMERIC,	
		PawnSale	NUMERIC,	
		RatedAS	STRING,	
		FullTimeEmployees	INT64,	
		PartTimeEmployees	INT64,	
		Owners	INT64,	
		PublicProtection	STRING,	
		LocationTypeCode	STRING,	
		LocationTypeName	STRING,	
		LocationTypeOtherDescription	STRING,	
		AnnualSales	INT64,	
		AreSalesPerformedViaInternet	INT64,	
		InternetSalesAmount	INT64,	
		NormalBusinessHours	STRING,	
		TotalValueShippedInLast12Months	INT64,	
		ConstructionCode	STRING,	
		ConstructionType	STRING,	
		ConstructionYearBuilt	INT64,	
		NumberOfFloors	INT64,	
		FloorNumbersOccupied	STRING,	
		TotalBuildngAreaSQFT	INT64,	
		AreaOccupiedSQFT	INT64,	
		Basement	INT64,	
		SmokeDetectors	INT64,	
		PercentSprinklered	STRING,	
		BuildingClass	STRING,	
		LocationClassInsuredBusinessCode	STRING,	
		LocationClassInsuredBusinessClassification	STRING,	
		HeatingYear	INT64,	
		PlumbingYear	INT64,	
		RoofingYear	INT64,	
		WiringYear	INT64,	
		AdjacentOccupancies	STRING,	
		SharedPremises	INT64,	
		PremisesSharedWith	STRING,	
		HasShowCaseWindows	INT64,	
		NumberOfShowWindows	INT64,	
		WindowsEquippedWithLocks	INT64,	
		WindowsKeptLocked	INT64,	
		WindowsKeptUnlockedReason	STRING,	
		WindowsMaxValue	INT64,	
		LockedDoorBuzzer	INT64,	
		BarsonWindows	INT64,	
		SteelCurtainsGates	INT64,	
		MonitoredFireAlarm	INT64,	
		DoubleCylDeadBoltLocks	INT64,	
		SafetyBollardCrashProt	INT64,	
		GlassProtection	INT64,	
		ManTrapEntry	INT64,	
		RecordingCameras	INT64,	
		DaytimeArmedUniformGuard	INT64,	
		ElecTrackingRFTags	INT64,	
		MultipleAlarmSystems	INT64,	
		DualMonitoring	INT64,	
		SecuredBuilding	INT64,	
		AddlProtNotMentioned	STRING,	
		OperatingCameraSystem	INT64,	
		CameraCovPremExclRestrooms	INT64,	
		CameraOperRecording	INT64,	
		CameraBackUpRecOffSite	INT64,	
		CameraAccessRemotely	INT64,	
		IsCamerasOnExterior	INT64,	
		PremiseBurglarAlarmDetMotion	INT64,	
		CoverAreaMerchLeftOutSafeVault	INT64,	
		CoverAlarmControlPanel	INT64,	
		CoverSafeVaultArea	INT64,	
		HoldupAlarmSystem	INT64,	
		MobileDevicesUsed	INT64,	
		BurglarAlarmSysMonitored	STRING,	
		BurglarAlarmWhenClosed	INT64,	
		BurglarAlarmWhenClosedReason	STRING,	
		OpeningClosingMonitored	INT64,	
		OpeningClosingSupervised	INT64,	
		NbrOnCallInAlarmConditions	STRING,	
		RespondToAlarmConditions	INT64,	
		RespondToAlarmConditionsNoReason	STRING,	
		OtherEmployAbleDeactivateAlarm	STRING,	
		SafeScatteredOnPremiseOrInOneArea	STRING,	
		SafeVaultStkroomUsedByOther	INT64,	
		SafeVaultStkroomUsedByOtherReason	STRING,	
		AnySafeVaultStkroomsOnExtWall	INT64,	
		IsSafeOnExterior	INT64,	
		LeastNbrEmployOnPremiseBusHrs	INT64,	
		YearsInBusiness	STRING,	
		JBTFinancial	STRING,	
		Inventory	STRING,	
		ClaimsFree	STRING,	
		PhysicalProtection	STRING,	
		ArmedUniformGuard	STRING,	
		MetalDetManTrap	STRING,	
		ElecTrackDevices	STRING,	
		HoldUpAlarm	STRING,	
		ElectronicProt	STRING,	
		PhysProtectedDoorWindow	STRING,	
		NoExtGrndFloorExposure	STRING,	
		LockedDoorBuzzSys	STRING,	
		SecuredBldg	STRING,	
		MonitoredFireAlarmSys	STRING,	
		ClosedBusStoragePractices	STRING,	
		CameraSystem	STRING,	
		DblCylDeadBoltLocks	STRING,	
		MltplAlarmsMonitoring	STRING,	
		TotalInStoragePercent	NUMERIC,	
		BankVaultPercent	NUMERIC,	
		OutOfStorageAmount	INT64,	
		BurglarylInclSpecProp	INT64,	
		BurglaryInclSpecPropValue	STRING,	
		ExclBurglary	INT64,	
		ExclBurglaryClosed	INT64,	
		ExclBurglaryClosedHoursOpen	STRING,	
		ExclBurglaryClosedTimeZone	STRING,	
		ExclBurglaryClosedTravelLimit	NUMERIC,	
		ExclBurglaryJwlWtchSpecified	INT64,	
		ExclBurglaryJwlWtchSpecifiedValue	NUMERIC,	
		ExclBurglaryJwlySpecifiedAmt	INT64,	
		ExclBurglaryJwlySpecifiedAmtValue	NUMERIC,	
		ExclOutOfSafe	INT64,	
		ExclFireLightningSmoke	INT64,	
		HasSpecifiedBurglaryLimit	INT64,	
		SpecifiedBurglaryLimitValue	NUMERIC,	
		AlarmSignalResponseReq	INT64,	
		BankVaultReq	INT64,	
		BankVaultReqOutOfSafeVaultPercent	NUMERIC,	
		BankVaultReqInSafeVaultPercent	STRING,	
		BankVaultReqBankVaultPercent	NUMERIC,	
		BurglaryDeductible	INT64,	
		BurglaryDeductibleValue	STRING,	
		EstimatedInventory	INT64,	
		IndivSafeMaxLimit	INT64,	
		IndivSafeMaxLimitInSafeVaultStkPercent	STRING,	
		IndivSafeMaxLimitMfg	INT64,	
		IndivSafeVaultMaxCap	INT64,	
		InnerSafeChest	INT64,	
		InnerSafeChestInSafePercent	STRING,	
		InSafePercent	INT64,	
		InSafePercentIndivSafeVaultMaxCapacity	INT64,	
		KeyedInvestigatorResponse	INT64,	
		KeyedInvestigatorResponseReq	STRING,	
		LockedCabinets	INT64,	
		LockedCabinetsPercentKept	NUMERIC,	
		IsMaxDollarLimit	INT64,	
		MaxLimitOutOfSafeAmt	STRING,	
		MaxLimitBurglary	INT64,	
		MaxLimitBurglaryInSafeVaultStkPct	STRING,	
		MaxLimitBurglaryBurgLimit	NUMERIC,	
		MaxLimitBurglaryAOPLimit	NUMERIC,	
		MaxLimitFinishedMerch	INT64,	
		MaxLimitFinishedMerchOutOfSafeVaultAmt	STRING,	
		MaxLimitWarranty	INT64,	
		MaxLimitWarrantyOutOfSafeVaultAmt	NUMERIC,	
		MaxStockValueOutWhenClosed	INT64,	
		MaxJwlryValueOutWhenClosed	NUMERIC,	
		MaxNonJwlyValueOutWhenClosed	NUMERIC,	
		MaxOutofSafeWhenClosed	INT64,	
		MaxOutWhenClosedMaxOutOfSafeVault	STRING,	
		MaxOutWhenClosedWithWarranty	INT64,	
		MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault	NUMERIC,	
		MaxOutOfLockedSafeVaultLimitSched	INT64,	
		MaxPerItemSafeVault	INT64,	
		MaxPerItemSafeVaultCostPerItem	NUMERIC,	
		MaxPerItemSafeVaultStkroom	INT64,	
		MaxPerItemSafeVaultStkroomCostPerItem	NUMERIC,	
		MaxValueInVault	INT64,	
		MaxValueInVaultInSafePercent	NUMERIC,	
		MinMaxProportionInSafe	INT64,	
		MinMaxProportionInSafePercent	STRING,	
		MinNbrEmployeeCond	INT64,	
		MinNbrEmployeeCondNumber	NUMERIC,	
		MinProportionValueSafeVault	INT64,	
		MinProportionValueSafeVaultInnerSafe	INT64,	
		MinProportionValueStkroom	INT64,	
		RobberyDeductible	INT64,	
		RobberyDeductibleValue	STRING,	
		SafeBurglaryDeductible	INT64,	
		SafeBurglaryDeductibleValue	STRING,	
		SafeMinLimit	INT64,	
		SafeVaultHurrWarningReq	INT64,	
		SaefVaultHurrWarningReqTX	INT64,	
		SafeVaultHurrWarningReqDeductible	INT64,	
		SafeVaultHurrWarningReqDeductibleValue	STRING,	
		ShowJewelryConditions	INT64,	
		SharedPremiseSecurity	INT64,	
		StkroomMaxDollarLimit	INT64,	
		StkroomMaxLimitOutOfSafeVaultAmt	STRING,	
		StkroomMaxLimit	INT64,	
		StkroomMaxLimitInSafePercent	STRING,	
		TheftProtection	INT64,	
		TheftProtectionDesc	STRING,	
		TheftProtectionGuardWarranty	INT64,	
		TheftProtectionSecurityDevice	INT64,	
		TheftProtectionSecurityDeviceDesc	STRING,	
		TotalPercentInSafe	INT64,	
		TotalPercentInSafeInSafeVaultStkrmPct	STRING,	
		TotalPercentInSafeNotToExceedAmt	NUMERIC,	
		ShowcaseOrWindowCondition	INT64,	
		BusinessPremise	STRING,	
		InviteJewelryClients	STRING,	
		SignageAtHome	STRING,	
		AnimalsOnPremise	STRING,	
		AnimalSpecies	STRING,	
		SwimmingPool	STRING,	
		Trampoline	STRING,	
		MerchandiseAccessibleToChildren	STRING,	
		DescribeHowAccessRestricted	STRING,	
		IsCompleteInvAnnualOrMore	INT64,	
		LastInventoryTotal	INT64,	
		LastInventoryDate	TIMESTAMP,	
		InventoryType	STRING,	
		HasDetailedInvRecords	INT64,	
		HasPurchaseInvoices	INT64,	
		HasAwayListings	INT64,	
		PriorInventoryTotal	INT64,	
		PriorInventoryDate	TIMESTAMP,	
		MaxStockValue	INT64,	
		MaxDailyScrapValue	INT64,	
		IsCustPropertyRecorded	INT64,	
		CustomerProperyAverage	INT64,	
		ConsignmentPropertyAverage	INT64,	
		InventoryPremiumBase	INT64,	
		OutofSafeVltStkrmExposure	INT64,	
		PawnPropertyHandled	INT64,	
		PawnCoverageIncluded	INT64,	
		PawnLastInventoryTotal	INT64,	
		PawnLastInventoryDate	TIMESTAMP,	
		PawnPriorInventoryTotal	INT64,	
		PawnPriorInventoryDate	TIMESTAMP,	
		PawnMaxStockValue	INT64,	
		InventoryLooseDiamonds	STRING,	
		InventoryWatchesLowValue	STRING,	
		InventoryWatchesHighValue	STRING,	
		InventoryHighValue	STRING,	
		InventoryLowValue	STRING,	
		InventoryScrap	STRING,	
		InventoryNonJewelry	STRING,	
		InventoryOther	STRING,	
		HasLuxuryBrandWatches	INT64,	
		HasWatchBlancpain	INT64,	
		HasWatchBreitling	INT64,	
		HasWatchCartier	INT64,	
		HasWatchOmega	INT64,	
		HasWatchPatekphilippe	INT64,	
		HasWatchRolex	INT64,	
		HasWatchOther	INT64,	
		WatchOtherExplanation	STRING,	
		IsOfficialRolexDealer	INT64,	
		ProtectionClassCode	STRING,	
		IsExclStkForSaleFromTheft	INT64,	
		ExclStkForSaleFromTheftPremium	NUMERIC,	
		IsExclFromTheftExclBurglary	INT64,	
		IsExcludeNonJwlryInv	INT64,	
		IsExclSpecStkForSaleInv	INT64,	
		ExclSpecStkForSaleInvPropNotIncl	STRING,	
		IsJwlryPawnPledgedVal	INT64,	
		JwlryPawnPledgedValMethod	STRING,	
		JwlryPawnPledgedValOtherDesc	STRING,	
		JwlryPawnPledgedValMethodMultiplier	STRING,	
		IsJwlryPawnUnpledgedVal	INT64,	
		JwlryPawnUnpledgedValMethod	STRING,	
		JwlryPawnUnpledgedValOtherDesc	STRING,	
		JwlryPawnUnpledgedValMethodMultiplier	STRING,	
		IsUnrepInv	INT64,	
		UnrepInvDescValue	STRING,	
		IsStockLimitClsd	INT64,	
		StockLimitClsdOpenStockLimit	NUMERIC,	
		StockLimitClsdClsdStockLimit	NUMERIC,	
		EQTerritory	STRING,	
		EQZone	STRING,	
		EQZoneDesc	STRING,	
		FloodZone	STRING,	
		FloodZoneDesc	STRING,	
		FirmIndicator	STRING,	
		FloodInceptionDate	TIMESTAMP,	
		OtherFloodInsurance	INT64,	
		OtherFloodInsuranceCarrier	STRING,	
		OtherFloodPolicyNumber	STRING,	
		OtherFloodPrimaryNFIP	INT64,	
		OtherFloodInformation	STRING,	
		OtherFloodInsUndWaiverJMPrimary	INT64,	
		IsPrimaryLocation STRING,
		bq_load_date	DATE
	)   
   PARTITION BY
   bq_load_date; 

#############################################################################################
####Creating Table PJRiskLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.PJRiskLevelAttributes`
	(
		SourceSystem	STRING,	
		RiskJewelryItemKey	BYTES,	
		PolicyTransactionKey	BYTES,
		IsTransactionSliceEffective		INT64,  
		PolicyNumber	STRING,
		JobNumber	STRING,	
		RiskLevel	STRING,	
		ItemNumber	INT64,	
		ItemEffectiveDate	TIMESTAMP,	
		ItemExpirationDate	TIMESTAMP,	
		ItemDescription	STRING,	
		ItemClassOtherDescText	STRING,	
		ItemBrand	STRING,	
		ItemStyle	STRING,	
		ItemStyleOtherDescText	STRING,	
		ItemInitialValue	INT64,	
		ItemDescriptionDate	TIMESTAMP,	
		ItemAppraisalReceived	STRING,	
		ItemAppraisalDocType	STRING,	
		ItemAppraisalViewEntireDoc	INT64,	
		ItemIVADate	TIMESTAMP,	
		ItemIVAPercentage	NUMERIC,	
		ItemHasIVAApplied	INT64,	
		ItemUseInitialLimit	INT64,	
		ItemPremiumDiffForIVA	INT64,	
		ItemJewelerAppraiser	STRING,	
		ItemValuationType	STRING,	
		ItemBankVault	STRING,	
		ItemDamage	INT64,	
		ItemDamagaeDescText	STRING,	
		ItemStored	STRING,	
		ItemPLSafe	INT64,	
		ItemSafe	INT64,	
		ItemExpressDescText	STRING,	
		ItemExpressDescIsAppraisal	INT64,	
		IsItemInactive	INT64,	
		InactiveReason	INT64,	
		Length	STRING,	
		SideStone	STRING,	
		CenterStone	STRING,	
		WatchMounting	STRING,	
		Other	STRING,	
		Mounting	STRING,	
		ModelNo	STRING,	
		SerialNo	STRING,	
		Grams	STRING,	
		PreOwned	STRING,	
		Pearl	STRING,	
		MilliMeter	STRING,	
		bq_load_date	DATE
	)   
   PARTITION BY
   bq_load_date; 

#############################################################################################
####Creating Table PAJewelryRiskLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.PAJewelryRiskLevelAttributes`  
	(
		SourceSystem	STRING	,	
		RiskPAJewelryKey	BYTES	,
		PolicyTransactionKey	BYTES	,		
		JobNumber	STRING	,		
		RiskLevel	STRING	,		
		JewelryArticleNumber	INT64	,		
		EffectiveDate	TIMESTAMP	,
		ExpirationDate	TIMESTAMP	,	
		IsTransactionSliceEffective	INT64	,		
		ArticleType	STRING	,		
		ArticleSubType	STRING	,		
		ArticleGender	STRING	,		
		IsWearableTech	INT64	,	
		ArticleBrand	STRING	,		
		ArticleStyle	STRING	,	
		InitialValue	INT64	,	
		IsFullDescOverridden	INT64	,		
		FullDescription	STRING		,
		IsAppraisalRequested	INT64	,		
		IsAppraisalReceived	INT64	,		
		AppraisalDate	TIMESTAMP	,	
		InspectionDate	TIMESTAMP	,	
		IVAPercentage	NUMERIC	,	
		IsIVADeclined	INT64	,		
		IVADate	TIMESTAMP	,	
		IsIVAApplied	INT64	,	
		ValuationType	STRING	,	
		IsDamaged	INT64	,		
		DamageType	STRING	,	
		IsArticleInactive	INT64,		
		InactiveReason	INT64	,	
		ArticleStored	STRING	,	
		SafeDetails	INT64	,
		TimeOutOfVault	STRING	,	
		HasCarePlan	INT64	,	
		CarePlanID	STRING	,	
		CarePlanExpirationDate	TIMESTAMP,		
		DurationWithOtherInsurer	STRING,		
		ArticleHowAcquired	STRING	,	
		ArticleYearAcquired	INT64	,	
		CenterStoneFeatures	STRING	,	
		SideStoneFeatures	STRING	,		
		GramsFeatures	STRING	,
		LengthFeatures	STRING	,	
		MilliMeterFeatures	STRING	,	
		ModelNoFeatures	STRING		,
		MountingFeatures	STRING	,	
		PearlFeatures	STRING		,
		PreOwnedFeatures	STRING	,	
		SerialNoFeatures	STRING	,	
		WatchMountingFeatures	STRING	,		
		OtherFeatures	STRING	,
		bq_load_date	DATE	,
	)   
   PARTITION BY
   bq_load_date;    
   
#############################################################################################
####Creating Table PAJewelryCoverageLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.PAJewelryCoverageLevelAttributes`  
	(
		SourceSystem	STRING,
		ProductCode		STRING,
		ProductType		STRING,
		PAJewelryCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskPAJewelryKey	BYTES,
		CoverageLevel	STRING,
		JobNumber	STRING,
		TransactionStatus	STRING,
		CoverageTypeCode	STRING,
		PolicyNumber	STRING,
		CoverageNumber	INT64,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		PerOccurenceLimit	NUMERIC,
		PerOccurenceDeductible	NUMERIC,
		ItemValue	NUMERIC,
		ItemAnnualPremium	NUMERIC,
		CoverageCode	STRING,
		bq_load_date	DATE
	)   
   PARTITION BY
   bq_load_date; 

#############################################################################################
####Creating Table PJCoverageLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.PJCoverageLevelAttributes`  
	(
		SourceSystem	STRING,
		ProductCode		STRING,
		ProductType		STRING,
		ItemCoverageKey	BYTES,
		PolicyTransactionKey	BYTES,
		RiskJewelryItemKey	BYTES,
		CoverageLevel	STRING,
		JobNumber	STRING,
		TransactionStatus	STRING,
		CoverageTypeCode	STRING,
		PolicyNumber	STRING,
		CoverageNumber	INT64,
		EffectiveDate	TIMESTAMP,
		ExpirationDate	TIMESTAMP,
		IsTempCoverage	INT64,
		PerOccurenceLimit	NUMERIC,
		PerOccurenceDeductible	NUMERIC,
		ItemValue	NUMERIC,
		ItemAnnualPremium	NUMERIC,
		CoverageCode	STRING,
		bq_load_date	DATE
	)   
   PARTITION BY
   bq_load_date; 

#############################################################################################
####Creating Table CLCoverageLevelAttributes ###########
CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.CLCoverageLevelAttributes`  
(
SourceSystem	STRING	,	
ProductCode	STRING	,	
ProductType STRING	,
CoverageKey	BYTES	,	
RiskBOPLocationKey	BYTES	,	
RiskIMLocationKey	BYTES	,	
RiskBOPBuildingKey	BYTES	,	
RiskIMStockKey	BYTES	,	
PolicyTransactionKey	BYTES	,	
CoveragePublicID	STRING	,	
BOPLocationPublicID	STRING	,	
IMLocationPublicID	STRING	,	
BOPBuildingPublicId	STRING	,	
IMStockPublicID	STRING	,	
PolicyLocationPublicID	STRING	,	
PolicyPeriodPublicID	STRING	,	
JobNumber	STRING	,	
PolicyNumber	STRING	,	
-- PrimaryLocationNumber	INT64	,	
CoverageLevel	STRING	,	
EffectiveDate	TIMESTAMP	,	
ExpirationDate	TIMESTAMP	,	
IsTempCoverage	INT64	,	
PerOccurenceLimit	NUMERIC	,	
PerOccurenceDeductible	NUMERIC	,	
CoverageCode	STRING	,	
CoverageNumber	INT64	,	
BOPEPLICode	STRING	,	
BOPPropertyRateNum	STRING	,	
BOPTheftAdjPropertyRateNum	STRING	,	
BOPRetroactiveDate	STRING	,	
BOPRateBookUsed	STRING	,	
IMSpecifiedCarrier	STRING	,	
SpecifiedCarrierExposure	NUMERIC,
IsTransactionSliceEffective	INT64,
bq_load_date	DATE
)   
   PARTITION BY
   bq_load_date;    
   
   
############################ DQ tables ######################################
  
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.Account_DupesOverall`   
(
UnitTest	STRING	,	
AccountKey	BYTES	,	
AccountHolderContactPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);
   
	
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageBOP_DupesOverall`   
(
UnitTest	STRING	,	
BOPCoverageKey	BYTES	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageBOP_MissingByX`   
(
UnitTest	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
PolicyNumber	STRING	,
CoveragePublicID	STRING,	
--CoverageFixedID	INT64	,	
CoverageEffectiveDate	TIMESTAMP	,	
CoverageExpirationDate	TIMESTAMP	,	
--CoverageFinalPersistedLimit	NUMERIC	,	
--CoverageFinalPersistedDeductible	NUMERIC	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageBOP_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskBuildingKey	BYTES	,
IsTransactionSliceEffective	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageIM_DupesOverall`   
(
UnitTest	STRING	,	
IMCoverageKey	BYTES	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageIM_MissingByX`   
(
UnitTest	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
CoverageFixedID	INT64	,	
CoverageBranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
CoverageEffectiveDate	TIMESTAMP	,	
CoverageExpirationDate	TIMESTAMP	,	
CoveragePatternCode	STRING	,	
CoverageFinalPersistedLimit	NUMERIC	,	
CoverageFinalPersistedDeductible	NUMERIC	,	
CoverageFinalPersistedTempFromDt	TIMESTAMP	,	
CoverageFinalPersistedTempToDt	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageIM_MissingRisks`   
(
UnitTest	STRING	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskStockKey	BYTES	,	
IsTransactionSliceEffective	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageJewelryItem_DupesOverall`   
(
UnitTest	STRING	,	
ItemCoverageKey	BYTES	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageJewelryItem_MissingByX`   
(
UnitTest	STRING	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
CoverageFixedID	INT64	,	
CoverageFinalPersistedLimit	NUMERIC	,	
CoverageFinalPersistedDeductible	NUMERIC	,	
EditEffectiveDate	TIMESTAMP	,	
CoverageExpirationDate	TIMESTAMP	,	
CoverageEffectiveDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageJewelryItem_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
RiskJewelryItemKey	BYTES	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoveragePAJewelry_DupesOverall`   
(
UnitTest	STRING	,	
PAJewelryCoverageKey	BYTES	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoveragePAJewelry_MissingByX`   
(
UnitTest	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
CoverageFixedID	INT64	,	
PolicyNumber	STRING	,	
CoverageFinalPersistedLimit	NUMERIC	,	
CoverageFinalPersistedDeductible	NUMERIC	,	
EditEffectiveDate	TIMESTAMP	,	
CoverageExpirationDate	TIMESTAMP	,	
CoverageEffectiveDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoveragePAJewelry_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
RiskPAJewelryKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageUMB_DupesOverall`   
(
UnitTest	STRING	,	
UMBCoverageKey	BYTES	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageUMB_MissingByX`   
(
UnitTest	STRING	,	
JobNumber	STRING	,	
CoverageLevel	STRING	,	
PublicID	STRING	,	
FixedID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
EffectiveDate	TIMESTAMP	,	
ExpirationDate	TIMESTAMP	,	
PatternCode	STRING	,	
FinalPersistedLimit_JMIC	NUMERIC	,	
FinalPersistedDeductible_JMIC	NUMERIC	,	
bq_load_date	DATE
);	

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.CoverageUMB_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
RiskLocationKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPCeded_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPCeded_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPCeded_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,
JobNumber	STRING	,	
PolicyNumber	STRING	,
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPCeded_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskBuildingKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPDirect_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPDirect_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPDirect_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
--FixedID	INT64	,	
JobID	INT64	,
JobNumber	STRING	,
PolicyNumber	STRING	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionBOPDirect_MissingRisks`
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskBuildingKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMCeded_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMCeded_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMCeded_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
FixedID	INT64	,	
JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
DatePosted	TIMESTAMP	,	
DateWritten	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMCeded_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskStockKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMDirect_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMDirect_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMDirect_MissingByX`
(
UnitTest	STRING	,	
JobNumber	STRING	,	
PublicID	STRING	,	
FixedID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
ILMLineCov	INT64	,	
ILMSubLineCov	INT64	,	
ILMLocationCov	INT64	,	
ILMSubLocCov	INT64	,	
JewelryStockCov	INT64	,	
ILMSubStockCov	INT64	,	
ILMOneTimeCredit	INT64	,	
ILMMinPremPolicyLocation	INT64	,	
ILMTaxLocation_JMIC	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionIMDirect_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
RiskLocationKey	BYTES	,	
RiskStockKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPACeded_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPACeded_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPACeded_MissingByX`   
(
UnitTest	STRING	,	
CoveragePublicID	STRING	,	
JobNumber	STRING	,
PolicyNumber	STRING	,
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
TransactionPostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPACeded_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,
CoveragePublicID	STRING	,	
RiskPAJewelryKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPADirect_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPADirect_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPADirect_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,
PolicyNumber	STRING	,	
JobNumber	STRING	,
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPADirect_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
RiskPAJewelryKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJCeded_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJCeded_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJCeded_MissingByX`   
(
UnitTest	STRING	,	
CoveragePublicID	STRING	,	
JobNumber	STRING	,	
PolicyNumber	STRING	,
--PublicID	STRING	,	
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
TransactionPostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE	
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJCeded_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,	
CoveragePublicID	STRING	,
RiskJewelryItemKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJDirect_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJDirect_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJDirect_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
PolicyNumber	STRING	,
JobNumber	STRING	,
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionPJDirect_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,
CoveragePublicID	STRING	,	
TransactionPublicID	STRING	,	
RiskJewelryItemKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBCeded_DupesByCoverage`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBCeded_DupesOverall`   
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBCeded_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,
JobNumber	STRING	,
PolicyNumber	STRING	,	
--FixedID	INT64	,	
--JobID	INT64	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBCeded_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,
CoveragePublicID	STRING	,	
RiskLocationKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBDirect_DupesByCoverage`
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
CoverageLevel	STRING	,	
CoveragePublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBDirect_DupesOverall`
(
UnitTest	STRING	,	
FinancialTransactionKey	BYTES	,	
TransactionPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBDirect_MissingByX`
(
UnitTest	STRING	,	
PublicID	STRING	,	
JobNumber	STRING	,	
PolicyNumber	STRING	,	
BranchID	INT64	,	
EditEffectiveDate	TIMESTAMP	,	
PostedDate	TIMESTAMP	,	
WrittenDate	TIMESTAMP	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.FinancialTransactionUMBDirect_MissingRisks`   
(
UnitTest	STRING	,	
CoverageLevel	STRING	,	
TransactionPublicID	STRING	,
CoveragePublicID	STRING	,	
RiskLocationKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.PolicyTransactionProduct_DupesOverall`   
(
UnitTest	STRING	,	
PolicyTransactionProductKey	BYTES	,	
PolicyTransactionKey	BYTES	,	
PolicyPeriodPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.PolicyTransaction_DupesOverall`   
(
UnitTest	STRING	,	
PolicyTransactionKey	BYTES	,	
PolicyPeriodPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskBuilding_DupesOverall`   
(
UnitTest	STRING	,	
RiskBuildingKey	BYTES	,	
BuildingPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskBuilding_MissingByX`   
(
UnitTest	STRING	,	
BuildingPublicID	STRING	,	
LocationPublicID	STRING	,	
PolicyNumber	STRING	,
JobNumber	STRING	,
RiskLevel	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskBuilding_MissingRisks`   
(
UnitTest	STRING	,	
BuildingPublicID	STRING	,	
LocationPublicID	STRING	,	
RiskBuildingKey	BYTES	,	
RiskLocationKey	BYTES	,	
--PolicyNumber	STRING	,
--JobNumber	STRING	,
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskJewelryItemFeature_DupesOverall`   
(
UnitTest	STRING	,	
RiskItemFeatureKey	BYTES	,	
ItemFeatureDetailPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskJewelryItemFeature_MissingByX`   
(
UnitTest	STRING	,	
ItemFeaturePublicID	STRING	,	
JobNumber	STRING	,	
FeatureType	INT64	,	
RiskLevel	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskJewelryItemFeature_MissingRisks`   
(
UnitTest	STRING	,	
RiskItemFeatureKey	BYTES	,	
ItemFeatureDetailPublicID	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskJewelryItem_DupesOverall`   
(
UnitTest	STRING	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
ItemPublicID	STRING	,	
ItemNumber	INT64	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskJewelryItem_MissingRisks`   
(
UnitTest	STRING	,	
ItemNumber	INT64	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationBusinessOwners_DupesOverall`   
(
UnitTest	STRING	,		
LocationPublicID	STRING	,
JobNumber	STRING	,
LocationNumber	INT64	,
FixedLocationRank	INT64	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationBusinessOwners_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
PolicyNumber	STRING	,
JobNumber	STRING	,	
TYPECODE	STRING	,
RiskLevel	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationBusinessOwners_MissingRisks`   
(
UnitTest	STRING	,	
LocationPublicID	STRING	,	
RiskLocationKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationIM_DupesOverall`   
(
UnitTest	STRING	,		
LocationPublicID	STRING	,	
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationIM_MissingByX`   
(
UnitTest	STRING	,	
LocationPublicID	STRING	,	
PolicyNumber	STRING	,
JobNumber	STRING	,
RiskLevel	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskLocationIM_MissingRisks`   
(
UnitTest	STRING	,	
LocationPublicID	STRING	,	
RiskLocationKey	BYTES	,	
JobNumber	STRING	,
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelryFeature_DupesOverall`   
(
UnitTest	STRING	,	
PAJewelryPublicID	STRING	,
PAJewelryFeaturePublicID	STRING	,
JobNumber	STRING	,
PAJewelryFeatureFixedID		INT64	,
FixedFeatureRank	INT64	,
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelryFeature_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
PolicyNumber	STRING	,	
JobNumber	STRING	,	
FeatureType	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelryFeature_MissingRisks`   
(
UnitTest	STRING	,	
PAJewelryFeaturePublicID	STRING	,	
JobNumber	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelry_DupesOverall`   
(
UnitTest	STRING	,	
JobNumber	STRING	,
JewelryArticleFixedID	INT64	,
FixedArticleRank INT64	,
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelry_MissingByX`   
(
UnitTest	STRING	,
PublicID	STRING	,
PolicyNumber	STRING	,	
JobNumber	STRING	,	
--RiskLevel	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskPAJewelry_MissingRisks`   
(
UnitTest	STRING	,		
RiskPAJewelryKey	BYTES	,	
JobNumber	STRING	,
bq_load_date	DATE
);


CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskStockIM_DupesOverall`   
(
UnitTest	STRING	,		
StockPublicID	STRING	,	
LocationPublicID	STRING	,
JobNumber	STRING	,
LocationNumber	INT64	,
FixedStockRank	INT64	,
NumRecords	INT64	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskStockIM_MissingByX`   
(
UnitTest	STRING	,	
PublicID	STRING	,	
PolicyNumber	STRING	,
JobNumber	STRING	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.RiskStockIM_MissingRisks`   
(
UnitTest	STRING	,	
StockPublicID	STRING	,	
RiskStockKey	BYTES	,	
bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_CLCoverageLevelAttributes_MissingByX`   
( 	
    UnitTest	STRING	,
	JobNumber	STRING,
    RiskBOPLocationKey 	BYTES,
    RiskIMLocationKey 	BYTES,
    RiskBOPBuildingKey 	BYTES,
    RiskIMStockKey 	BYTES,
	--TransactionStatus	STRING,
	bq_load_date	DATE	
);	

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_CLRiskBusinessOwnersAttributes_Dupes`   
( 	
    UnitTest	STRING	,
	JobNumber	STRING,
	LocationNumber	INT64,
	BuildingNumber	INT64,
	TransactionStatus	STRING,
	bq_load_date	DATE	
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_CLRiskBusinessOwnersAttributes_MissingByX`   
( 	
    UnitTest	STRING	,
	JobNumber	STRING,
    RiskLocationKey 	BYTES,
    RiskBuildingKey 	BYTES,
	LocationNumber	INT64,
	BuildingNumber	INT64,
	TransactionStatus	STRING,
	bq_load_date	DATE	
);	

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_CLRiskInLandMarineAttributes_LocnStock`   
( 	
    UnitTest	STRING	,
	JobNumber	STRING,
	LocationNumber	INT64,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_CLRiskInLandMarineAttributes_TransxLocn`   
( 	
    UnitTest	STRING	,
	PolicyTransactionKey	BYTES,	
	JobNumber	STRING,
	LocationNumber	INT64,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_PAJewelryCoverageLevelAttributes_SCHTransCovCode`   
( 	
    UnitTest	STRING	,
	PolicyTransactionKey	BYTES,
	CoverageTypeCode	STRING,
	CoverageNumber	INT64,
	JobNumber	STRING,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_PAJewelryCoverageLevelAttributes_TransCovCode`   
( 	
    UnitTest	STRING	,
	PolicyTransactionKey	BYTES,
	CoverageTypeCode	STRING,
	JobNumber	STRING,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_PJCoverageLevelAttributes_SCHTransCovCode`   
( 	
    UnitTest	STRING	,
	PolicyTransactionKey	BYTES,
	CoverageTypeCode	STRING,
	CoverageNumber	INT64,
	JobNumber	STRING,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_PJCoverageLevelAttributes_TransCovCode`   
( 	
    UnitTest	STRING	,
	PolicyTransactionKey	BYTES,
	CoverageTypeCode	STRING,
	JobNumber	STRING,
	KeyCount	INT64,
	bq_load_date	DATE
);

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.BBDQ_TransactionConfiguration`
(
UnitTest	STRING	,	
PolicyNumber	STRING	,	
TermNumber	INT64	,	
JobNumber	STRING	,
bq_load_date	DATE
);

-- CREATE TABLE IF NOT EXISTS  `{project}.{dp_dataset}.PremiumWritten`   
-- ( 	
-- 	AccountingDate	DATE	,	
-- 	AccountingYear	INT64	,	
-- 	AccountingQuarter	INT64	,	
-- 	AccountingMonth	INT64	,	
-- 	BusinessUnit	STRING	,	
-- 	Product	STRING	,	
-- 	AccountSegment	STRING	,	
-- 	JobNumber	STRING	,	
-- 	PolicyNumber	STRING	,	
-- 	ItemNumber	INT64	,	
-- 	CoverageCode	STRING	,	
-- 	TransactionType	STRING	,	
-- 	TransactionCount	INT64	,	
-- 	PolicyCoverageRank	INT64	,
-- 	WrittenPremium	NUMERIC	,	
-- 	bq_load_date	DATE
-- )   
--    PARTITION BY
--    bq_load_date;

   CREATE TABLE IF NOT EXISTS `{project}.{dataset}.PremiumWritten`
   (
    SourceSystem STRING,
    FinancialTransactionKey BYTES,
    PolicyTransactionKey BYTES,
    RiskLocationKey BYTES,
    RiskBuildingKey BYTES,
    RiskStockKey BYTES,
    RiskPAJewelryKey BYTES,
    RiskJewelryItemKey BYTES,
    CoverageKey BYTES,
	PolicyPeriodPublicID STRING,
	PolicyTermPublicID STRING,
    AccountingDate DATE,
    AccountingYear INT64,
    AccountingQuarter INT64,
    AccountingMonth INT64,
    BusinessUnit STRING,
    ProductCode STRING,
	ProductType STRING,
    AccountSegment STRING,
    JobNumber STRING,
    PolicyNumber STRING,
    PolicyExpirationDate TIMESTAMP,
    PolicyTransEffDate TIMESTAMP,
    ItemNumber INT64,
    ModelNumber INT64,
    CoverageCode STRING,
    TransactionType STRING,
    TransactionCount INT64,
    PolicyCoverageRank INT64,
    WrittenPremium NUMERIC,
	TermNumber INT64,
	LocationCountry STRING,
    bq_load_date DATE
)
   PARTITION BY
   bq_load_date;


/*****************************************
	Building Blocks Table Create
	    ClaimBOPDirect table.sql
		Ramesh L - 02/03/2023
******************************************/

   CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimBOPDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  BOPCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskBuildingKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  BOPLocationPublicID STRING,
  BuildingPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  LocationNumber STRING,
  AccountNumber STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  IsErodingReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveLossRecovery NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveDCCExpenseRecovery NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimReserveAOExpenseRecovery NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  ClaimLossRecovery NUMERIC,
  ClaimRecoveryDCCExpense NUMERIC,
  ClaimRecoveryAOExpense NUMERIC,
  ClaimRecoveryType STRING,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  ReserveLineCategoryCode STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  ClaimPaymentContactFullName STRING,
  ClaimPaymentContactIsJeweler INT64,
  ClaimPaymentContactIsJMJeweler INT64,
  ClaimPaymentContactPublicID STRING,
  ItemState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicID STRING,
  ItemAddressPublicID STRING,
  RiskUnitPublicID STRING,
  RiskUnitAddress STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicID STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="BOP Direct claims attributes and metrics at Financial Transaction Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);


/*******************************************************
	Kimberlite Table Create
	    ClaimFinancialTransactionLineBOPDirect table.sql
********************************************************/

   CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLineBOPDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  BOPCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskBuildingKey BYTES,
  BusinessType STRING,
  TransactionLinePublicID STRING,
  TransactionLineDate TIMESTAMP,
  TransactionAmount NUMERIC,
  TransactionPublicID STRING,
  TransactionDate TIMESTAMP,
  TransactionSetID INT64,
  DoesNotErodeReserves BOOL,
  TransactionStatus INT64,
  ClaimContactID INT64,
  BOPLocationPublicID STRING,
  BuildingPublicID STRING,
  ClaimPublicId STRING,
  InsuredID INT64,
  LossLocationID INT64,
  ClaimNumber STRING,
  LossDate TIMESTAMP,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ClaimPolicyPublicId STRING,
  ProducerCode STRING,
  AccountNumber STRING,
  ccPolicyID INT64,
  LOBCode STRING,
  ReserveLinePublicID STRING,
  IsAverageReserveSource BOOL,
  PolicyPeriodPublicID STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  peSegment STRING,
  LineCategory STRING,
  TransactionType STRING,
  TransactionStatusCode STRING,
  AccountingDate TIMESTAMP,
  TransactionsSubmittedPrior INT64,
  OfferingCode STRING,
  CostType STRING,
  ClaimTransactionType STRING,
  CostCategory STRING,
  PaymentType STRING,
  ccCoveragePublicID STRING,
  CoveragePublicID STRING,
  LineCode STRING,
  CoverageCode STRING,
  CoverageLevel STRING,
  ClaimRecoveryType STRING,
  IncidentPublicID STRING,
  IncidentPropertyID INT64,
  ExposurePublicID STRING,
  UnderWritingCompanyType STRING,
  IsTransactionSliceEffective INT64,
  RiskLocationAddressID INT64,
  RiskUnitTypeCode STRING,
  PolicyAddressPublicId STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  UWCompanyPublicID STRING,
  ReserveLineCategoryCode STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  ProducerPublicID STRING,
  JewelerContactPublicID STRING,
  VendorID STRING,
  DefaultSegment STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="BOP Direct claims at financial line key level ",
      labels=[("ref_kimberlite","claim")]
);


/*******************************************************
	Building Blocks Table Create
	    ClaimFinancialTransactionLineBOPCeded table.sql
********************************************************/
   CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLineBOPCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  BOPCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskBuildingKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionDate TIMESTAMP,
  TransactionAmount NUMERIC,
  TransactionSetID INT64,
  ClaimPublicID STRING,
  ClaimNumber STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  LossDate TIMESTAMP,
  InsuredID INT64,
  LossLocationID INT64,
  ClaimPolicyPublicID STRING,
  AccountNumber STRING,
  ProducerCode STRING,
  ccPolicyID INT64,
  LOBCode STRING,
  ReserveLinePublicID STRING,
  IsAverageReserveSource BOOL,
  PolicyPeriodPublicID STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  TransactionType STRING,
  TransactionStatusCode STRING,
  AccountingDate TIMESTAMP,
  TransactionsSubmittedPrior INT64,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  CoveragePublicID STRING,
  ccCoveragePublicID STRING,
  LineCode STRING,
  CoverageCode STRING,
  CoverageSubType STRING,
  CoverageLevel STRING,
  IncidentPublicID STRING,
  IncidentPropertyID INT64,
  ExposurePublicID STRING,
  UnderWritingCompanyType STRING,
  IsTransactionSliceEffective INT64,
  RiskLocationAddressID INT64,
  RiskUnitTypeCode STRING,
  PolicyAddressPublicID STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  UWCompanyPublicID STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  BOPLocationPublicID STRING,
  BuildingPublicID STRING,
  ProducerPublicID STRING,
  JewelerContactPublicID STRING,
  DefaultSegment STRING,
  peSegment STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="BOP Ceded claims at financial key level ",
      labels=[("ref_kimberlite","claim")]
);


/*****************************************
	Building Blocks Table Create
	    ClaimBOPCeded table.sql
******************************************/

   CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimBOPCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  BOPCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskBuildingKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  BOPLocationPublicID STRING,
  BuildingPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  LocationNumber STRING,
  AccountNumber STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  IsErodingReserves INT64,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  PropertyState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicID STRING,
  PropertyAddressPublicID STRING,
  RiskUnitPublicID STRING,
  RiskUnitAddress STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicID STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="BOP Ceded claims attributes and metrics at Financial Transaction Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);



#############################################################################################
####Creating Table ClaimIMCeded ************
####  Ramesh L - 02/09/2023 - Trang 152 retrofit 

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimIMCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  IMCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskStockKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  IMLocationPublicID STRING,
  IMStockPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  LocationNumber STRING,
  AccountNumber STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  IsErodingReserves INT64,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  PropertyState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicID STRING,
  PropertyAddressPublicID STRING,
  RiskUnitPublicID STRING,
  RiskUnitAddress STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicID STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE 
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="IM Ceded claims attributes and metrics at Financial Transaction Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);

#############################################################################################
####Creating Table ClaimFinancialTransactionLineIMCeded ************

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLineIMCeded`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  IMCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskStockKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionDate TIMESTAMP,
  TransactionAmount NUMERIC,
  TransactionSetID INT64,
  ClaimPublicID STRING,
  ClaimNumber STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  LossDate TIMESTAMP,
  InsuredID INT64,
  LossLocationID INT64,
  ClaimPolicyPublicID STRING,
  AccountNumber STRING,
  ProducerCode STRING,
  ccPolicyID INT64,
  LOBCode STRING,
  ReserveLinePublicID STRING,
  IsAverageReserveSource BOOL,
  PolicyPeriodPublicID STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  TransactionType STRING,
  TransactionStatusCode STRING,
  AccountingDate TIMESTAMP,
  TransactionsSubmittedPrior INT64,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  CoverageLevel STRING,
  CoveragePublicID STRING,
  ccCoveragePublicID STRING,
  LineCode STRING,
  CoverageCode STRING,
  CoverageSubType STRING,
  IncidentPublicID STRING,
  IncidentPropertyID INT64,
  ExposurePublicID STRING,
  UnderWritingCompanyType STRING,
  IsTransactionSliceEffective INT64,
  RiskLocationAddressID INT64,
  RiskUnitTypeCode STRING,
  PolicyAddressPublicID STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  UWCompanyPublicID STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IMLocationPublicID STRING,
  IMStockPublicID STRING,
  ProducerPublicID STRING,
  JewelerContactPublicID STRING,
  DefaultSegment STRING,
  peSegment STRING,
  RIAgreementNumber STRING,
  RIAgreementType STRING,
  RIAgreementID INT64,
  RIAgreementPublicID STRING,
  RICodingID INT64,
  RIAgreementGroupID INT64,
  RIPCCoverageGroupType STRING,
  RIType STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="IM Ceded claims at financial key level ",
      labels=[("ref_kimberlite","claim")]
);

#############################################################################################
####Creating Table ClaimFinancialTransactionLineIMDirect ************

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimIMDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  IMCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskStockKey BYTES,
  BusinessType STRING,
  TransactionPublicID STRING,
  TransactionLinePublicID STRING,
  ClaimPublicId STRING,
  PolicyPeriodPublicID STRING,
  CoveragePublicID STRING,
  IMLocationPublicID STRING,
  IMStockPublicID STRING,
  CoverageLevel STRING,
  CoverageTypeCode STRING,
  CoverageCode STRING,
  ClaimNumber STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  LocationNumber STRING,
  AccountNumber STRING,
  TransactionType STRING,
  ClaimTransactionType STRING,
  CostType STRING,
  CostCategory STRING,
  LineCategory STRING,
  PaymentType STRING,
  IsErodingReserves BOOL,
  LossDate TIMESTAMP,
  TransactionDate TIMESTAMP,
  TransactionLineDate TIMESTAMP,
  AccountingDate TIMESTAMP,
  TransactionStatusCode STRING,
  TransactionAmount NUMERIC,
  ClaimReserveLoss NUMERIC,
  ClaimReserveLossRecovery NUMERIC,
  ClaimReserveDCCExpense NUMERIC,
  ClaimReserveDCCExpenseRecovery NUMERIC,
  ClaimReserveAOExpense NUMERIC,
  ClaimReserveAOExpenseRecovery NUMERIC,
  ClaimPaidLossExcludingRecovery NUMERIC,
  ClaimPaidDCCExpense NUMERIC,
  ClaimPaidAOExpense NUMERIC,
  ClaimLossRecovery NUMERIC,
  ClaimRecoveryDCCExpense NUMERIC,
  ClaimRecoveryAOExpense NUMERIC,
  ClaimRecoveryType STRING,
  PolicyLineCode STRING,
  LineCode STRING,
  glLineCode STRING,
  IsAverageReserveSource BOOL,
  TransactionSetID INT64,
  TransactionsSubmittedPrior INT64,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ContactKey BYTES,
  ClaimPrimaryContactID STRING,
  ReserveLineCategoryCode STRING,
  ProducerCode STRING,
  UWCompanyPublicID STRING,
  CompanyCode STRING,
  AnnualStmntLine INT64,
  RatedStateCode STRING,
  RatedPostalCode STRING,
  LossCountry STRING,
  FinSegment STRING,
  ClaimPaymentContactFullName STRING,
  ClaimPaymentContactIsJeweler INT64,
  ClaimPaymentContactIsJMJeweler INT64,
  ClaimPaymentContactPublicID STRING,
  ItemState STRING,
  LossState STRING,
  PolicyPeriodState STRING,
  LossAddressPublicID STRING,
  ItemAddressPublicID STRING,
  RiskUnitPublicID STRING,
  RiskUnitAddress STRING,
  ExposurePublicId STRING,
  IncidentPublicID STRING,
  JewelerContactPublicID STRING,
  PolicyAddressPublicID STRING,
  bq_load_date DATE
)  
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="IM Direct claims attributes and metrics at Financial Transaction Key level",
      labels=[("ref_kimberlite_buildingblock","")]
);

#############################################################################################
####Creating Table ClaimFinancialTransactionLineIMDirect ************

CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.ClaimFinancialTransactionLineIMDirect`
(
  SourceSystem STRING,
  FinancialTransactionKey BYTES,
  FinancialTransactionLineKey BYTES,
  ClaimTransactionKey BYTES,
  PolicyTransactionKey BYTES,
  IMCoverageKey BYTES,
  RiskLocationKey BYTES,
  RiskStockKey BYTES,
  BusinessType STRING,
  TransactionLinePublicID STRING,
  TransactionLineDate TIMESTAMP,
  TransactionAmount NUMERIC,
  TransactionPublicID STRING,
  TransactionDate TIMESTAMP,
  TransactionSetID INT64,
  DoesNotErodeReserves BOOL,
  TransactionStatus INT64,
  ClaimContactID INT64,
  ClaimPublicId STRING,
  InsuredID INT64,
  LossLocationID INT64,
  ClaimNumber STRING,
  LossDate TIMESTAMP,
  IsClaimForLegacyPolicy BOOL,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  ClaimPolicyPublicId STRING,
  ProducerCode STRING,
  AccountNumber STRING,
  ccPolicyID INT64,
  LOBCode STRING,
  ReserveLinePublicID STRING,
  IsAverageReserveSource BOOL,
  PolicyPeriodPublicID STRING,
  PolicyNumber STRING,
  TermNumber INT64,
  peSegment STRING,
  LineCategory STRING,
  TransactionType STRING,
  TransactionStatusCode STRING,
  AccountingDate TIMESTAMP,
  TransactionsSubmittedPrior INT64,
  OfferingCode STRING,
  CostType STRING,
  ClaimTransactionType STRING,
  CostCategory STRING,
  PaymentType STRING,
  ccCoveragePublicID STRING,
  CoveragePublicID STRING,
  LineCode STRING,
  CoverageCode STRING,
  CoverageLevel STRING,
  ClaimRecoveryType STRING,
  IncidentPublicID STRING,
  IncidentPropertyID INT64,
  ExposurePublicID STRING,
  UnderWritingCompanyType STRING,
  IsTransactionSliceEffective INT64,
  RiskLocationAddressID INT64,
  RiskUnitTypeCode STRING,
  PolicyAddressPublicId STRING,
  PolicyAddressStateID INT64,
  PolicyAddressCountryID INT64,
  PolicyAddressPostalCode STRING,
  UWCompanyPublicID STRING,
  ReserveLineCategoryCode STRING,
  TransactionOrigin STRING,
  TransactionChangeType STRING,
  IMLocationPublicID STRING,
  IMStockPublicID STRING,
  ProducerPublicID STRING,
  JewelerContactPublicID STRING,
  VendorID STRING,
  DefaultSegment STRING,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="IM Direct claims at financial line key level ",
      labels=[("ref_kimberlite","claim")]
);


#############################################################################################
####Creating Table Claim ************


CREATE TABLE IF NOT EXISTS `{project}.{core_dataset}.Claim` 
(
  SourceSystem STRING,
  ClaimKey BYTES,
  PolicyTransactionKey BYTES,
  PolicyNumber STRING,
  ClaimPublicId STRING,
  ClaimID INT64,
  ClaimPolicyPublicId STRING,
  PolicyPeriodPublicID STRING,
  ClaimNumber STRING,
  ClosedOutcomeCode INT64,
  ClosedOutcomeDesc STRING,
  CloseDate TIMESTAMP,
  LossDate TIMESTAMP,
  ReportedDate TIMESTAMP,
  CreatedDate TIMESTAMP,
  MadeDate TIMESTAMP,
  ClaimStatusName STRING,
  ClaimSource STRING,
  HowReportedChannel STRING,
  LossTypeName STRING,
  LossCauseName STRING,
  LossCauseTypecode STRING,
  ClaimPolicyTypecode STRING,
  SecondaryCauseOfLossCode STRING,
  SecondaryCauseOfLossDesc STRING,
  ClaimDescription STRING,
  LossLocationID INT64,
  ClaimEvent STRING,
  CatastropheType STRING,
  LitigationStatus STRING,
  IsClaimantRepresented INT64,
  ClaimType STRING,
  IsVerified INT64,
  SurveyPreference STRING,
  InvolesRolexWatches INT64,
  IsVerifiedManually INT64,
  CoverageInQuestion BOOL,
  SIUStatus STRING,
  BaseSIUScore INT64,
  ReferredtoSIUTeam STRING,
  Flagged STRING,
  AssignedUserID INT64,
  TravelLossLocationID INT64,
  UWCompanyCode STRING,
  IsClaimForLegacyPolicy INT64,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  IsTransactionSliceEffective INT64,
  bq_load_date DATE
)
	PARTITION BY
		bq_load_date
	OPTIONS(
      description="Claims and core attributes, plus foreign keys"
   );

#############################################################################################
####Creating Table ClaimAttributes ************
-----------------------------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ClaimAttributes`
(
  SourceSystem STRING,
  ClaimKey BYTES,
  PolicyTransactionKey BYTES,
  PolicyNumber STRING,
  ClaimPublicId STRING,
  ClaimID INT64,
  PolicyPeriodPublicID STRING,
  ClaimPolicyPublicId STRING,
  ClaimNumber STRING,
  ClosedOutcomeCode INT64,
  ClosedOutcomeDesc STRING,
  CloseDate TIMESTAMP,
  LossDate TIMESTAMP,
  ReportedDate TIMESTAMP,
  CreatedDate TIMESTAMP,
  MadeDate TIMESTAMP,
  TrialDate TIMESTAMP,
  LastOpenDate TIMESTAMP,
  LastCloseDate TIMESTAMP,
  CauseOfLoss STRING,
  CauseOfLossGroup STRING,
  ClaimStatusName STRING,
  ClaimSource STRING,
  HowReportedChannel STRING,
  LossTypeName STRING,
  LossCauseName STRING,
  LossCauseTypecode STRING,
  ClaimPolicyTypecode STRING,
  SecondaryCauseOfLossCode STRING,
  SecondaryCauseOfLossDesc STRING,
  ClaimDescription STRING,
  LossCounty STRING,
  LossCity STRING,
  LossPostalCode STRING,
  LossStateCode STRING,
  LossCountryCode STRING,
  ExaminerFullName STRING,
  ExaminerLoginID STRING,
  ExaminerPublicID STRING,
  ReportedByFullName STRING,
  ReportedByLoginID STRING,
  ReportedByPublicID STRING,
  ClaimEvent STRING,
  CatastropheType STRING,
  ClaimLawSuit INT64,
  ClaimCourtType STRING,
  LitigationStatus STRING,
  IsClaimantRepresented INT64,
  IsAverageReserveExpense INT64,
  IsAverageReserveIndemnity INT64,
  ClaimType STRING,
  IsVerified INT64,
  SurveyPreference STRING,
  ReportToJSAandJVC STRING,
  InvolesRolexWatches INT64,
  IsVerifiedManually INT64,
  CoverageInQuestion BOOL,
  SIUStatus STRING,
  SIUScore INT64,
  ReferredtoSIUTeam STRING,
  Flagged STRING,
  FirstLitigationStartDate TIMESTAMP,
  TravelLossLocationCity STRING,
  TravelLossLocationStateCode STRING,
  TravelLossLocationCountryCode STRING,
  ClaimantContactPublicID STRING,
  LawEnfcAgcyContactPublicID STRING,
  IsClaimForLegacyPolicy INT64,
  LegacyPolicyNumber STRING,
  LegacyClaimNumber STRING,
  bq_load_date DATE
)
	PARTITION BY
		bq_load_date
	OPTIONS(
      description="All claims and attributes"
   );