-- FULL LOAD Query
use DW_DDS;

SELECT
	date_inforce.DateKey																AS InforceDateKey
	,cast(bief_dds.fn_GetDateFromDateKey(date_inforce.DateKey) as date)					AS InforceDate
	,d_account.AccountNumber															AS AccountNumber
	,d_policy.PolicyNumber																AS PolicyNumber
	,CONCAT(d_policy.PolicyNumber,'-',	CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingLocationNumber
		ELSE r_loc.LocationNumber
		END)																			AS PolicyNumber_LocationNumber
	,d_policy.PolicyOrigEffDate	
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.AccountSegment															AS AccountSegment
	,d_lob.LOBProductLineCode															AS ProductLineCode
	,d_lob.LOBCode																		AS LOB_Code
	,case when d_policy.PolicyEffectiveDate - d_policy.PolicyOrigEffDate < 15 
		then 'New' else 'Ren' end														AS NewRenewal								
	,d_account.AutoPayIndicator															AS AutoPayIndicator
	,d_account.PaymentMethod															AS PaymentMethod
	,d_agency.AgencyMasterCode															AS Producer_Service_Agency_Master_Code
	,d_agency.AgencyMasterName															AS Producer_Service_Agency_Master_Name
	,d_agency.AgencySubCode																AS Producer_Service_Agency_SubCode
	,d_agency.AgencyCode																AS Producer_Service_Agency_Code
	,d_agency.AgencyName																AS Producer_Service_Agency_Name
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN lob_geo.GeographyStateCode
		ELSE loc_geo.GeographyStateCode
		END																				AS Location_State_Code
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN lob_geo.GeographyCountryCode
		ELSE loc_geo.GeographyCountryCode
		END																				AS Location_Country_Code		
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingCity
		ELSE r_loc.LocationCity
		END																				AS Location_City
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingCounty
		ELSE r_loc.LocationCounty
		END																				AS Location_County
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingPostalCode
		ELSE r_loc.LocationPostalCode
		END																				AS LocationPostalCode
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingLocationNumber
		ELSE r_loc.LocationNumber
		END																				AS LocationNumber
	,PrimaryInsuredGeo.GeographyStateCode												AS Primary_Insured_State
	,PrimaryInsuredGeo.GeographyCountryCode												AS Primary_Insured_Country
	,Sum(CASE WHEN d_lob.LOBCode in ('JB','JS') THEN inforce.PremiumInforce ELSE 0 END)	AS LocationJB_JS_Premium
	,SUM(CASE WHEN d_lob.LOBCode = 'BOP' THEN inforce.PremiumInforce ELSE 0 END)		AS LocationBOP_Premium
	,SUM(CASE WHEN d_lob.LOBCode = 'UMB' THEN inforce.PremiumInforce ELSE 0 END)		AS LocationUMB_Premium
	,SUM(inforce.PremiumInforce)														AS LocationPremiumInforce
	,CONVERT (date,'{date}')                                                            AS bq_load_date


FROM
-- History should use the below fact table filtering by date ranges
			bi_dds.FactMonthlyPremiumInforce AS inforce
-- Most recent month can be appended to history using the snapshot fact table
			--bi_dds.FactMonthlyPremiumInforce_LatestMonthlySnapshot AS inforce
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
		AND d_lob.LOBProductLineCode = 'CL'

INNER JOIN  bi_dds.DimBusinessType AS d_biz
		ON	inforce.BusinessTypeKey = d_biz.BusinessTypeKey
		AND d_biz.BusinessTypeDesc = 'Direct'


-- Producer of Service-----------------------------------------------
INNER JOIN	bi_dds.DimAgency AS d_agency
		 ON	d_policy.AgencyKey = d_agency.AgencyKey

INNER JOIN	bi_dds.DimGeography AS d_geo
		ON	inforce.GeographyKey = d_geo.GeographyKey

INNER JOIN	bi_dds.DimRiskSegmentCore AS r_core
		ON	inforce.RiskSegmentKey = r_core.RiskSegmentKey

INNER JOIN	bi_dds.DimRiskSegmentLOB AS r_lob
		ON	r_core.RiskSegmentLOBKey = r_lob.RiskSegmentKey

INNER JOIN	bi_dds.DimGeography AS lob_geo
		ON	r_lob.PrimaryRatingGeographyKey = lob_geo.GeographyKey

INNER JOIN	bi_dds.DimRiskSegmentLocation AS r_loc
		ON	r_core.RiskSegmentLocationKey = r_loc.RiskSegmentKey

INNER JOIN	bi_dds.DimGeography AS loc_geo
		ON	r_loc.LocationGeographyKey = loc_geo.GeographyKey

INNER JOIN	bi_dds.DimGeography AS PrimaryInsuredGeo
		ON	PrimaryInsuredGeo.GeographyKey = d_policy.PrimaryInsuredGeographyKey

WHERE {date_filter}
GROUP BY
	date_inforce.DateKey
	,d_account.AccountNumber
	,d_policy.PolicyNumber
	,CONCAT(d_policy.PolicyNumber,'-',	CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingLocationNumber
		ELSE r_loc.LocationNumber
		END)	
	,d_policy.PolicyOrigEffDate
	,d_policy.PolicyEffectiveDate
	,d_policy.PolicyExpirationDate
	,d_policy.SourceOfBusiness
	,d_policy.AccountSegment
	,d_lob.LOBProductLineCode
	,d_lob.LOBCode
	,(case when d_policy.PolicyEffectiveDate - d_policy.PolicyOrigEffDate < 15 then 'New' else 'Ren' end)
	,d_account.AutoPayIndicator
	,d_account.PaymentMethod
	,d_agency.AgencyMasterCode
	,d_agency.AgencyMasterName
	,d_agency.AgencySubCode
	,d_agency.AgencySubName
	,d_agency.AgencyCode
	,d_agency.AgencyName
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN lob_geo.GeographyStateCode
		ELSE loc_geo.GeographyStateCode
		END
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN lob_geo.GeographyCountryCode
		ELSE loc_geo.GeographyCountryCode
		END
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingCity
		ELSE r_loc.LocationCity
		END
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingCounty
		ELSE r_loc.LocationCounty
		END
	,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingPostalCode
		ELSE r_loc.LocationPostalCode
		END
		,CASE
		WHEN r_core.RiskType = 'LOB' THEN r_lob.PrimaryRatingLocationNumber
		ELSE r_loc.LocationNumber
		END
	,PrimaryInsuredGeo.GeographyStateCode
	,PrimaryInsuredGeo.GeographyCountryCode