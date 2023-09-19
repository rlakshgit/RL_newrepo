use DW_DDS;
-- Personal Jewelry first then UNION with Personal Articles for PL Item/Article level inforce

SELECT
	date_inforce.DateKey
	,CONVERT (date,convert(char(8),date_inforce.DateKey)) InforceDate
	,date_inforce.QuarterNumber
	,d_account.AccountNumber
	,d_policy.PolicyNumber
	,d_policy.PolicyOrigEffDate
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.SourceOfBusiness
	,d_policy.AccountSegment
	,d_lob.LOBProductLineCode
	,d_lob.LOBCode
	,d_biz.BusinessTypeDesc
	,d_account.AutoPayIndicator
	,d_account.PaymentMethod
	,d_asl.AnnualStatementLine
	,d_asl.ASLDescription
	,d_agency.AgencyMasterCode
	,d_agency.AgencyMasterName
	,d_agency.AgencyCode
	,d_agency.AgencyName
	,d_geo.GeographyStateCode AS InforceStateCode
	,d_geo.GeographyStateDesc AS InforceStateDesc
	,d_geo.GeographyRegion AS InforceRegion
	,d_geo.GeographyCountryCode AS InforceCountryCode
	,d_geo.GeographyCountryDesc AS InforceCountryDesc
	,lob_geo.GeographyStateCode AS LOBStateCode
	,lob_geo.GeographyStateDesc AS LOBStateDesc
	,lob_geo.GeographyRegion AS LOBRegion
	,lob_geo.GeographyCountryCode AS LOBCountryCode
	,lob_geo.GeographyCountryDesc AS LOBCountryDesc
	,r_lob.PrimaryRatingCity
	,r_lob.PrimaryRatingCounty
	,r_lob.PrimaryRatingPostalCode   -- until here for policylevel
	,PrimaryInsuredGeo.GeographyStateCode AS PrimaryInsured
	,r_item.ItemNumber  --- count 0r count distinct --policyitemcount
	,r_item.ItemDescription --remove
	,r_item.ItemBrand -- remove
	,d_itemclass.ItemClassDescription
	,CASE WHEN d_covtype.ConformedCoverageTypeCode = 'SCH' THEN d_cov.PerOccurenceLimit ELSE 0 END AS ItemLimit
	,item_geo.GeographyStateCode AS ItemStateCode
	,item_geo.GeographyStateDesc AS ItemStateDesc
	,item_geo.GeographyRegion AS ItemRegion
	,item_geo.GeographyCountryCode AS ItemCountryCode
	,item_geo.GeographyCountryDesc AS ItemCountryDesc -- remove all in between
	,SUM(inforce.PremiumInforce) AS PremiumInforce -- item/policypremiuminforce
	,CONVERT (date,'{date}') as bq_load_date

FROM
			-- History should use the below fact table filtering by date ranges
			--bi_dds.FactMonthlyPremiumInforce AS inforce
			-- Most recent month can be appended to history using the snapshot fact table
			bi_dds.FactMonthlyPremiumInforce AS inforce
INNER JOIN	bief_dds.DimDate AS date_inforce
		ON	inforce.DateKey = date_inforce.DateKey
INNER JOIN	bi_dds.DimPolicy AS d_policy
		ON	inforce.PolicyKey = d_policy.PolicyKey
INNER JOIN	bi_dds.DimAccount AS d_account
		ON	d_policy.AccountKey = d_account.AccountKey
INNER JOIN	bi_dds.DimPolicyTerm AS d_term
		ON	inforce.PolicyTermKey = d_term.PolicyTermKey
INNER JOIN	bi_dds.DimLineOfBusiness AS d_lob
		ON	inforce.LineOfBusinessKey = d_lob.LineOfBusinessKey
		AND	d_lob.LOBCode = 'PJ'
INNER JOIN  bi_dds.DimBusinessType AS d_biz
		ON	inforce.BusinessTypeKey = d_biz.BusinessTypeKey
INNER JOIN	bi_dds.DimASL AS d_asl
		ON	inforce.ASLKey = d_asl.ASLKey
INNER JOIN	bi_dds.DimAgency AS d_agency
		ON	inforce.AgencyKey = d_agency.AgencyKey
INNER JOIN	bi_dds.DimGeography AS d_geo
		ON	inforce.GeographyKey = d_geo.GeographyKey
INNER JOIN	bi_dds.DimCoverageType AS d_covtype
		ON	inforce.CoverageTypeKey = d_covtype.CoverageTypeKey
INNER JOIN	bi_dds.DimCoverage AS d_cov
		ON	inforce.CoverageKey = d_cov.CoverageKey
INNER JOIN	bi_dds.DimRiskSegmentCore AS r_core
		ON	inforce.RiskSegmentKey = r_core.RiskSegmentKey
INNER JOIN	bi_dds.DimRiskSegmentLOB AS r_lob
		ON	r_core.RiskSegmentLOBKey = r_lob.RiskSegmentKey
INNER JOIN	bi_dds.DimGeography AS lob_geo
		ON	r_lob.PrimaryRatingGeographyKey = lob_geo.GeographyKey
INNER JOIN	bi_dds.DimRiskSegmentJewelryItem AS r_item
		ON	r_core.RiskSegmentJewelryItemKey = r_item.RiskSegmentKey
INNER JOIN	bi_dds.DimItemClass AS d_itemclass
		ON	r_item.ItemClassKey = d_itemclass.ItemClassKey
INNER JOIN	bi_dds.DimGeography AS item_geo
		ON	r_item.ItemGeographyKey = item_geo.GeographyKey
INNER JOIN bi_dds.DimGeography AS PrimaryInsuredGeo
		on PrimaryInsuredGeo.GeographyKey=d_policy.PrimaryInsuredGeographyKey
WHERE {date_filter}
GROUP BY
	date_inforce.DateKey
	,date_inforce.QuarterNumber
	,d_account.AccountNumber
	,d_policy.PolicyNumber
	,d_policy.PolicyOrigEffDate
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.SourceOfBusiness
	,d_policy.AccountSegment
	,d_lob.LOBProductLineCode
	,d_lob.LOBCode
	,d_biz.BusinessTypeDesc
	,d_account.AutoPayIndicator
	,d_account.PaymentMethod
	,d_asl.AnnualStatementLine
	,d_asl.ASLDescription
	,d_agency.AgencyMasterCode
	,d_agency.AgencyMasterName
	,d_agency.AgencyCode
	,d_agency.AgencyName
	,d_geo.GeographyStateCode
	,d_geo.GeographyStateDesc
	,d_geo.GeographyRegion
	,d_geo.GeographyCountryCode
	,d_geo.GeographyCountryDesc
	,lob_geo.GeographyStateCode
	,lob_geo.GeographyStateDesc
	,lob_geo.GeographyRegion
	,lob_geo.GeographyCountryCode
	,lob_geo.GeographyCountryDesc
	,r_lob.PrimaryRatingCity
	,r_lob.PrimaryRatingCounty
	,r_lob.PrimaryRatingPostalCode
	,PrimaryInsuredGeo.GeographyStateCode
	,r_item.ItemNumber
	,r_item.ItemDescription
	,r_item.ItemBrand
	,d_itemclass.ItemClassDescription
	,d_covtype.ConformedCoverageTypeCode
	,d_cov.PerOccurenceLimit
	,item_geo.GeographyStateCode
	,item_geo.GeographyStateDesc
	,item_geo.GeographyRegion
	,item_geo.GeographyCountryCode
	,item_geo.GeographyCountryDesc

UNION ALL

SELECT
	date_inforce.DateKey
	,CONVERT (date,convert(char(8),date_inforce.DateKey)) InforceDate
	,date_inforce.QuarterNumber
	,d_account.AccountNumber
	,d_policy.PolicyNumber
	,d_policy.PolicyOrigEffDate
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.SourceOfBusiness
	,d_policy.AccountSegment
	,d_lob.LOBProductLineCode
	,d_lob.LOBCode
	,d_biz.BusinessTypeDesc
	,d_account.AutoPayIndicator
	,d_account.PaymentMethod
	,d_asl.AnnualStatementLine
	,d_asl.ASLDescription
	,d_agency.AgencyMasterCode
	,d_agency.AgencyMasterName
	,d_agency.AgencyCode
	,d_agency.AgencyName
	,d_geo.GeographyStateCode AS InforceStateCode
	,d_geo.GeographyStateDesc AS InforceStateDesc
	,d_geo.GeographyRegion AS InforceRegion
	,d_geo.GeographyCountryCode AS InforceCountryCode
	,d_geo.GeographyCountryDesc AS InforceCountryDesc
	,lob_geo.GeographyStateCode AS LOBStateCode
	,lob_geo.GeographyStateDesc AS LOBStateDesc
	,lob_geo.GeographyRegion AS LOBRegion
	,lob_geo.GeographyCountryCode AS LOBCountryCode
	,lob_geo.GeographyCountryDesc AS LOBCountryDesc
	,r_lob.PrimaryRatingCity
	,r_lob.PrimaryRatingCounty
	,r_lob.PrimaryRatingPostalCode
	,PrimaryInsuredGeo.GeographyStateCode AS PrimaryInsured
	,r_article.JewelryArticleNumber AS ItemNumber
	,r_article.JewelryArticleDescription AS ItemDescription
	,NULL AS ItemBrand
	,d_itemclass.ItemClassDescription
	,CASE WHEN d_covtype.ConformedCoverageTypeCode = 'SCH' THEN d_cov.PerOccurenceLimit ELSE 0 END AS ItemLimit
	,article_geo.GeographyStateCode AS ItemStateCode
	,article_geo.GeographyStateDesc AS ItemStateDesc
	,article_geo.GeographyRegion AS ItemRegion
	,article_geo.GeographyCountryCode AS ItemCountryCode
	,article_geo.GeographyCountryDesc AS ItemCountryDesc
	,SUM(inforce.PremiumInforce) AS PremiumInforce
	,CONVERT (date,'{date}') as bq_load_date

FROM
			-- History should use the below fact table filtering by date ranges
			--bi_dds.FactMonthlyPremiumInforce AS inforce
			-- Most recent month can be appended to history using the snapshot fact table
			bi_dds.FactMonthlyPremiumInforce AS inforce
INNER JOIN	bief_dds.DimDate AS date_inforce
		ON	inforce.DateKey = date_inforce.DateKey
INNER JOIN	bi_dds.DimPolicy AS d_policy
		ON	inforce.PolicyKey = d_policy.PolicyKey
INNER JOIN	bi_dds.DimAccount AS d_account
		ON	d_policy.AccountKey = d_account.AccountKey
INNER JOIN	bi_dds.DimPolicyTerm AS d_term
		ON	inforce.PolicyTermKey = d_term.PolicyTermKey
INNER JOIN	bi_dds.DimLineOfBusiness AS d_lob
		ON	inforce.LineOfBusinessKey = d_lob.LineOfBusinessKey
		AND d_lob.LOBCode = 'PA'
INNER JOIN  bi_dds.DimBusinessType AS d_biz
		ON	inforce.BusinessTypeKey = d_biz.BusinessTypeKey
INNER JOIN	bi_dds.DimASL AS d_asl
		ON	inforce.ASLKey = d_asl.ASLKey
INNER JOIN	bi_dds.DimAgency AS d_agency
		ON	inforce.AgencyKey = d_agency.AgencyKey
INNER JOIN	bi_dds.DimGeography AS d_geo
		ON	inforce.GeographyKey = d_geo.GeographyKey
INNER JOIN	bi_dds.DimCoverageType AS d_covtype
		ON	inforce.CoverageTypeKey = d_covtype.CoverageTypeKey
INNER JOIN	bi_dds.DimCoverage AS d_cov
		ON	inforce.CoverageKey = d_cov.CoverageKey
INNER JOIN	bi_dds.DimRiskSegmentCore AS r_core
		ON	inforce.RiskSegmentKey = r_core.RiskSegmentKey
INNER JOIN	bi_dds.DimRiskSegmentLOB AS r_lob
		ON	r_core.RiskSegmentLOBKey = r_lob.RiskSegmentKey
INNER JOIN	bi_dds.DimGeography AS lob_geo
		ON	r_lob.PrimaryRatingGeographyKey = lob_geo.GeographyKey
INNER JOIN	bi_dds.DimRiskSegmentJewelryArticle AS r_article
		ON	r_core.RiskSegmentJewelryArticleKey = r_article.RiskSegmentKey
INNER JOIN	bi_dds.DimItemClass AS d_itemclass
		ON	r_article.JewelryArticleItemClassKey = d_itemclass.ItemClassKey
INNER JOIN	bi_dds.DimGeography AS article_geo
		ON	r_article.JewelryArticleGeographyKey = article_geo.GeographyKey
INNER JOIN bi_dds.DimGeography AS PrimaryInsuredGeo
		on PrimaryInsuredGeo.GeographyKey=d_policy.PrimaryInsuredGeographyKey
WHERE {date_filter}
GROUP BY
	date_inforce.DateKey
	,date_inforce.QuarterNumber
	,d_account.AccountNumber
	,d_policy.PolicyNumber
	,d_policy.PolicyOrigEffDate
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.SourceOfBusiness
	,d_policy.AccountSegment
	,d_lob.LOBProductLineCode
	,d_lob.LOBCode
	,d_biz.BusinessTypeDesc
	,d_account.AutoPayIndicator
	,d_account.PaymentMethod
	,d_asl.AnnualStatementLine
	,d_asl.ASLDescription
	,d_agency.AgencyMasterCode
	,d_agency.AgencyMasterName
	,d_agency.AgencyCode
	,d_agency.AgencyName
	,d_geo.GeographyStateCode
	,d_geo.GeographyStateDesc
	,d_geo.GeographyRegion
	,d_geo.GeographyCountryCode
	,d_geo.GeographyCountryDesc
	,lob_geo.GeographyStateCode
	,lob_geo.GeographyStateDesc
	,lob_geo.GeographyRegion
	,lob_geo.GeographyCountryCode
	,lob_geo.GeographyCountryDesc
	,r_lob.PrimaryRatingCity
	,r_lob.PrimaryRatingCounty
	,r_lob.PrimaryRatingPostalCode
	,PrimaryInsuredGeo.GeographyStateCode
	,r_article.JewelryArticleNumber
	,r_article.JewelryArticleDescription
	,d_itemclass.ItemClassDescription
	,d_covtype.ConformedCoverageTypeCode
	,d_cov.PerOccurenceLimit
	,article_geo.GeographyStateCode
	,article_geo.GeographyStateDesc
	,article_geo.GeographyRegion
	,article_geo.GeographyCountryCode
	,article_geo.GeographyCountryDesc