CREATE OR REPLACE VIEW
        `{project}.{dataset}.v_qq_summary`
AS

SELECT

  qq_pc.SourceOfBusiness

  ,qq_pc.ProducerCode

  ,qq_pc.ProducerCodeDescription

  ,qq_pc.MasterAgencyCode

  ,qq_pc.MasterAgencyName

  ,qq_pc.AgencyCode

  ,qq_pc.AgencyName

  ,qq_pc.PC_AccountNumber

  ,qq_ratabase.AccountNumber

  ,qq_pc.PC_JobNumber

  ,qq_pc.PolicyPeriodStatus

  ,qq_pc.PolicyStatusDescription

  ,qq_pc.PC_PrimaryInsuredName

  ,qq_pc.QuotingAgentFirstName

  ,qq_pc.QuotingAgentLastName

  ,qq_pc.QuotingAgentID

  ,qq_ratabase.QuoteServiceID

  ,qq_ratabase.QuoteUpdateTimestamp

  ,qq_ratabase.QuoteSequence

  ,qq_pc.QuoteType

  ,qq_pc.QuoteTypeCode

  ,qq_ratabase.PolicyState

  ,qq_ratabase.ArticleCount

  ,qq_ratabase.PolicyValue

  ,qq_pc.TotalSchedPremiumRPT_JMIC

  ,qq_ratabase.InsuranceScoreGroup

  ,qq_ratabase.InsuranceScoreProfile

  ,qq_items.ArticleID

  ,qq_items.CoverageID

  ,qq_items.AppraisalDate

  ,qq_items.AppraisalReceived

  ,qq_items.AppraisalRequested

  ,qq_items.AffinityLevel

  ,qq_items.ArticleManufactureYear

  ,qq_items.ArticleDamage

  ,qq_items.ArticleNumber

  ,qq_items.ArticleStored

  ,qq_items.ArticleType

  ,qq_items.ArticleSubtype

  ,qq_items.DaysOutOfVault

  ,qq_items.DistributionSource

  ,qq_items.GemCertification

  ,qq_items.GemType

  ,qq_items.DiscGradingRpt

  ,qq_items.IRPM

  ,qq_items.JMCarePlan

  ,qq_items.ValuationType

  ,qq_items.CovTypeCode

  ,qq_items.ArticleDeductible

  ,qq_items.ArticleLimit

  ,qq_items.ConvictionFelony

  ,qq_items.ConvictionMisdmnr

  ,qq_items.DateOfBirth

  ,qq_items.Occupation

  ,qq_items.RecordType

  ,qq_items.WearerID

  ,qq_items.AlarmType

  ,qq_items.LocationID

  ,qq_items.TerritoryCode

  ,qq_items.NbrCovsOnLoc


--  ,CAST('{date}' AS date) AS bq_load_date



FROM

  --`prod-edl.gld_quickquote.t_ratabase_quotes` AS qq_ratabase

`{project}.{dataset}.t_quickquote_v1_ratabase_quotes` AS qq_ratabase


LEFT JOIN

`{project}.{dataset}.t_quickquote_v1_ratabase_itemleveldetails` AS qq_items

  ON  qq_ratabase.QuoteServiceID = qq_items.QuoteServiceID


LEFT JOIN

  --(SELECT * from `prod-edl.gld_quickquote.t_policycenter_quotes`) AS qq_pc

(SELECT * from `{project}.{dataset}.t_quickquote_v1_policycenter_quotes` where DATE(_PARTITIONTIME) = '{date}') AS qq_pc

  ON  qq_ratabase.JobNumber = qq_pc.PC_JobNumber




WHERE

     qq_pc.QuoteProductType = 'JPAPersonalArticles'

  AND qq_pc.QuoteTypeCode IS NOT NULL





GROUP BY

  qq_pc.SourceOfBusiness

  ,qq_pc.ProducerCode

  ,qq_pc.ProducerCodeDescription

  ,qq_pc.MasterAgencyCode

  ,qq_pc.MasterAgencyName

  ,qq_pc.AgencyCode

  ,qq_pc.AgencyName

  ,qq_pc.PC_AccountNumber

  ,qq_ratabase.AccountNumber

  ,qq_pc.PC_JobNumber

  ,qq_pc.PolicyPeriodStatus

  ,qq_pc.PolicyStatusDescription

  ,qq_pc.PC_PrimaryInsuredName

  ,qq_pc.QuotingAgentFirstName

  ,qq_pc.QuotingAgentLastName

  ,qq_pc.QuotingAgentID

  ,qq_ratabase.QuoteServiceID

  ,qq_ratabase.QuoteUpdateTimestamp

  ,qq_ratabase.QuoteSequence

  ,qq_pc.QuoteType

  ,qq_pc.QuoteTypeCode

  ,qq_ratabase.PolicyState

  ,qq_ratabase.ArticleCount

  ,qq_ratabase.PolicyValue

  ,qq_pc.TotalSchedPremiumRPT_JMIC

  ,qq_ratabase.InsuranceScoreGroup

  ,qq_ratabase.InsuranceScoreProfile

  ,qq_items.ArticleID

  ,qq_items.CoverageID

  ,qq_items.AppraisalDate

  ,qq_items.AppraisalReceived

  ,qq_items.AppraisalRequested

  ,qq_items.AffinityLevel

  ,qq_items.ArticleManufactureYear

  ,qq_items.ArticleDamage

  ,qq_items.ArticleNumber

  ,qq_items.ArticleStored

  ,qq_items.ArticleType

  ,qq_items.ArticleSubtype

  ,qq_items.DaysOutOfVault

  ,qq_items.DistributionSource

  ,qq_items.GemCertification

  ,qq_items.GemType

  ,qq_items.DiscGradingRpt

  ,qq_items.IRPM

  ,qq_items.JMCarePlan

  ,qq_items.ValuationType

  ,qq_items.CovTypeCode

  ,qq_items.ArticleDeductible

  ,qq_items.ArticleLimit

  ,qq_items.ConvictionFelony

  ,qq_items.ConvictionMisdmnr

  ,qq_items.DateOfBirth

  ,qq_items.Occupation

  ,qq_items.RecordType

  ,qq_items.WearerID

  ,qq_items.AlarmType

  ,qq_items.LocationID

  ,qq_items.TerritoryCode

  ,qq_items.NbrCovsOnLoc