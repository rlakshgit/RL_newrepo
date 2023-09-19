
USE DW_DDS_CURRENT;

/*
	Create variables to capture rolling 12-month history and 5-year lookback at claims
*/

DECLARE	@YearStart date															-- 12-month rolling start date for JC/PP
		,@YearEnd date															-- 12-month rolling end date for JC/PP
		,@DateKey int															-- DateKey for FactMonthlyPremiumInforce date filtering
		,@ClaimStart date														-- 5-year start date for claims
		,@ClaimEnd date;														-- 5-year end date for claims

SET @YearEnd = CAST(
	(CASE
		WHEN EOMONTH(GETDATE(), 0) = GETDATE()
		THEN GETDATE()
		ELSE EOMONTH(GETDATE(), -1)
	END)
	AS date)

SET @YearStart = DATEADD(year, -1, DATEADD(day, 1, @YearEnd))

SET @DateKey = bief_dds.fn_GetDateKeyFromDate(DATEADD(month, -1, @YearStart)) + 10000;

/*
	Begin CTEs to piece together the various datasets into one
*/

WITH cte AS (
	SELECT
		DimPolicy.PolicyNumber													AS PolicyNumber
		,DimPolicy.PolicyKey
		,DimAcct.AccountNumber													AS AccountNumber
		,DimPolicy.PolicyInsuredContactFullName									AS Name
		,DimPolicy.AccountSegment												AS AccountSegment
		,Geoloc.GeographyCountryCode											AS PrimaryCountry
		,LOB.LOBCode
		,Agency.AgencyMasterCode												AS ProducerMasterCode
		,Agency.AgencyCode														AS ProducerAgencyCode
		,CONVERT(varchar(10), PolEffDate.FullDate, 101)							AS PolEffDate
		,CONVERT(varchar(10), PolExpDate.FullDate, 101)							AS PolExpDate
		,CONVERT(varchar(10), OriginalEffDate.FullDate, 101)					AS OriginalEffDate
		,SUM(PremiumInforce)													AS Prem
		,RiskLoc.LocationNumber													AS LocNumber
		,MAX(RiskLoc.LocationFullTimeEmployees)									AS NumberofFullTimeEmpl
		,MAX(RiskLoc.LocationPartTimeEmployees)									AS NumberofPartTimeEpl

	FROM [DW_DDS_CURRENT].[bi_dds].[FactMonthlyPremiumInforce]					AS MonthlyInforce

		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimPolicyTerm]					AS PolTerm
			ON MonthlyInforce.PolicyTermKey	= PolTerm.PolicyTermKey

		LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentCore]				AS RiskSeg
			ON MonthlyInforce.RiskSegmentKey = RiskSeg.RiskSegmentKey

		LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentLocation]			AS RiskLoc
			ON RiskSeg.RiskSegmentLocationKey = RiskLoc.RiskSegmentKey

		LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimRiskSegmentBuilding]			AS RiskBld
			ON RiskSeg.RiskSegmentBuildingKey = RiskBld.RiskSegmentKey
		
		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimLineOfBusiness]				AS LOB
			ON MonthlyInforce.LineOfBusinessKey = LOB.LineOfBusinessKey

		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimCoverageType]					AS CovType
			ON MonthlyInforce.CoverageTypeKey = CovType.CoverageTypeKey

		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimCoverage]						AS DimCoverage
			ON DimCoverage.CoverageKey = MonthlyInforce.CoverageKey
	
		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimPolicy]						AS DimPolicy
			ON DimPolicy.PolicyKey = MonthlyInforce.PolicyKey
	
		INNER JOIN [DW_DDS_CURRENT].[bief_dds].[DimDate]						AS AcctgDate
			ON AcctgDate.DateKey = MonthlyInforce.DateKey
	
		INNER JOIN [DW_DDS_CURRENT].[bief_dds].[DimDate]						AS PolEffDate
			ON PolEffDate.DateKey = DimPolicy.PolicyEffectiveDate
	
		INNER JOIN [DW_DDS_CURRENT].[bief_dds].[DimDate]						AS PolExpDate
			ON PolExpDate.DateKey = DimPolicy.PolicyExpirationDate
	
		INNER JOIN [DW_DDS_CURRENT].[bief_dds].[DimDate]						AS OriginalEffDate
			ON OriginalEffDate.DateKey = DimPolicy.PolicyOrigEffDate
 
		LEFT JOIN [DW_DDS_CURRENT].[bi_dds].[DimGeography]						AS GeoLoc
			ON DimPolicy.PrimaryInsuredGeographyKey = GeoLoc.GeographyKey
	
		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimAgency]						AS Agency
			ON DimPolicy.AgencyKey = Agency.AgencyKey
	
		INNER JOIN [DW_DDS_CURRENT].[bi_dds].[DimAccount]						AS DimAcct
			ON DimPolicy.AccountKey = DimAcct.AccountKey

	WHERE
			MonthlyInforce.DateKey = @DateKey
		AND MonthlyInforce.BusinessTypeKey = 2
		AND LOB.LOBProductLineCode = 'CL'
		AND DimPolicy.PolicyNumber <> '?'

	GROUP BY
		DimPolicy.PolicyNumber
		,DimPolicy.PolicyKey
		,AccountNumber
		,DimPolicy.PolicyInsuredContactFullName
		,DimPolicy.AccountSegment
		,LocationNumber
		,Geoloc.GeographyCountryCode 
		,LOB.LOBCode
		,Agency.AgencyMasterCode
		,Agency.AgencyCode 
		,CONVERT(varchar(10), PolEffDate.FullDate, 101) 
		,CONVERT(varchar(10), PolExpDate.FullDate, 101)
		,CONVERT(varchar(10), OriginalEffDate.FullDate, 101) 
),

attributes AS (
	SELECT
		cte.ProducerMasterCode
		,cte.ProducerAgencyCode
		,cte.PolicyNumber
		,cte.AccountNumber
		,cte.Name
		,MAX(cte.NumberofFullTimeEmpl) AS NumberofEmployees
		,MAX(cte.LocNumber) AS LocationCount
		,PolEffDate
		,PolExpDate
		,OriginalEffDate
		,cte.PrimaryCountry

	FROM cte

	GROUP BY
		cte.PolicyNumber
		,cte.AccountNumber
		,cte.Name
		,cte.ProducerAgencyCode
		,cte.ProducerMasterCode
		,PolEffDate
		,PolExpDate
		,cte.PrimaryCountry
		,cte.OriginalEffDate
),

/*
	Jewelers Cut data pull
*/

jc_enroll AS (
	SELECT
		jeweler.CommercialPolicyNo												AS PolicyNumber
		,MAX(CASE
				WHEN program_status.ProgramStatusDescription = 'Active'
				THEN 'Yes'
				ELSE 'No'
				END)															AS JC_Enrollment

	FROM [PLEcom].[JewelersCut].[Jeweler] AS jeweler

		INNER JOIN PLEcom.JewelersCut.Program AS program
			ON jeweler.JewelerId = program.JewelerId

		INNER JOIN PLEcom.JewelersCut.ProgramDescription AS program_desc
			ON program.ProgramDescriptionId = program_desc.ProgramDescriptionId

		INNER JOIN PLEcom.JewelersCut.ProgramStatus AS program_status
			ON program.ProgramStatusId = program_status.ProgramStatusId

	GROUP BY
		jeweler.CommercialPolicyNo
),

payments AS (
	SELECT
		jeweler.Name
		,jeweler.CommercialPolicyNo												AS PolicyNumber
		,jeweler.JewelerId
		,payment.Amount
		,payment.PaidDate 

	FROM [PLEcom].[JewelersCut].[Jeweler] AS jeweler

		INNER JOIN [PLEcom].[JewelersCut].[Program] AS program
			ON program.JewelerId = jeweler.JewelerId

		INNER JOIN [PLEcom].[JewelersCut].[ProgramStatus] AS program_status
			ON program.ProgramStatusId = program_status.ProgramStatusId
	
		INNER JOIN [PLEcom].[JewelersCut].[Payment] AS payment
			ON jeweler.JewelerId = payment.JewelerId

	WHERE	YEAR(payment.PaidDate) is not null
		-- WHY THIS SPECIFIC CUTOFF DATE?
--		AND payment.PaidDate >= CAST('2019-02-11' AS date)
		AND program_status.ProgramStatusDescription= 'Active'

	GROUP BY
		jeweler.Name
		,jeweler.JewelerId
		,jeweler.CommercialPolicyNo
		,payment.Amount
		,payment.PaidDate 
),

jewelers_cut AS (
	SELECT
		Name
		,payments.PolicyNumber
		,JewelerId
		,SUM(CASE
				WHEN PaidDate BETWEEN @YearStart AND @YearEnd
				THEN Amount
				ELSE 0
				END)															AS JC_CurrentYearTotal
		,SUM(Amount)															AS JC_PaidAmount

	FROM payments

	GROUP BY
		Name
		,payments.PolicyNumber
		,JewelerId
),

/* 
	Platinum Points data pull
*/

pp_enroll AS (
	SELECT
		jeweler.PolicyNumber
		,MAX(CASE
				WHEN jeweler_status.StatusName = 'Active'
				THEN 'Yes'
				ELSE 'No'
				END)															AS PP_Enrollment

	FROM [JMServices].[PlatinumPoints].[jm_Jeweler_tb] AS jeweler

	INNER JOIN [JMServices].[PlatinumPoints].[jm_JewelerStatus_tb] AS jeweler_status
		ON jeweler.StatusId = jeweler_status.StatusId

	-- Data cleaning to remove testing data or invalid data
	WHERE PolicyNumber LIKE '[0-9][0-9]%'

	GROUP BY
		jeweler.PolicyNumber
),

platinum AS (
	SELECT
		jeweler.JewelerCode
		,jeweler.PolicyNumber
		,jeweler.CompanyName
		,SUM(CASE
				WHEN SentDate BETWEEN @YearStart AND @YearEnd
				THEN Points.Points
				ELSE 0
				END)															AS PP_CurrentYearTotal
		,SUM(Points.Points)														AS PP_PaidAmount

	FROM [JMServices].[PlatinumPoints].[jm_Jeweler_tb] AS jeweler

		INNER JOIN [JMServices].[PlatinumPoints].[jm_JewelerPoints_tb] AS points
			ON jeweler.JewelerId = points.JewelerID

		INNER JOIN [JMServices].[PlatinumPoints].[jm_JewelerStatus_tb] AS jeweler_status
			ON jeweler.StatusId = jeweler_status.StatusId

	-- Data cleaning to remove testing data or invalid data
	WHERE	jeweler.PolicyNumber LIKE '[0-9][0-9]%'
		AND jeweler_status.StatusName = 'Active'

	GROUP BY
		jeweler.JewelerCode
		,jeweler.PolicyNumber
		,jeweler.CompanyName
),

/*
	Membership related metrics
*/

jvc AS (
	SELECT
		PolicyNumber
		,CASE
			WHEN attributes.PrimaryCountry = 'USA'
			THEN CASE
					WHEN attributes.LocationCount < 10
					THEN 180 + (attributes.LocationCount - 1) * 50
					WHEN attributes.LocationCount < 25
					THEN 350 + (attributes.LocationCount - 1) * 50
					WHEN attributes.LocationCount < 65
					THEN 525 + (attributes.LocationCount - 1) * 50
					WHEN attributes.LocationCount < 100
					THEN 675 + (attributes.LocationCount - 1) * 50
					ELSE 830 + (attributes.LocationCount - 1) * 50
					END
			ELSE CASE
					WHEN attributes.LocationCount * 115 < 2375
					THEN attributes.LocationCount * 115
					ELSE 2375
					END
			END																		AS [JSA_JVC_Membership_Fees]
		,DATEDIFF(year, attributes.OriginalEffDate, attributes.PolEffDate) + 1		AS NumberOfYears_JM

	FROM attributes
	
	GROUP BY
		PolicyNumber
		,PrimaryCountry
		,PolEffDate
		,OriginalEffDate
		,LocationCount
)

SELECT
	attributes.ProducerMasterCode												AS Master_Code
	,attributes.ProducerAgencyCode												AS Agency_Code
	,attributes.PolicyNumber
	,attributes.AccountNumber
	,attributes.Name															AS Jeweler
	,attributes.NumberofEmployees												AS Number_of_Employees
	,attributes.LocationCount													AS Number_of_Locations
	,attributes.PolEffDate														AS Policy_Effective_Date
	,attributes.PolExpDate														AS Policy_Expiration_Date
	,attributes.OriginalEffDate													AS Original_Effective_Date
	,attributes.PrimaryCountry													AS Primary_Country
	,ROUND(SUM(COALESCE(jewelers_cut.JC_CurrentYearTotal, 0)), -1)				AS JC_YTD_Total
	,ROUND(SUM(COALESCE(jewelers_cut.JC_PaidAmount, 0)), -1)					AS JC_Paid_Amount
	,COALESCE(jc_enroll.JC_Enrollment, 'No')									AS JC_Enrollment
	,SUM(COALESCE(platinum.PP_CurrentYearTotal, 0))								AS PP_YTD_Total
	,SUM(COALESCE(platinum.PP_PaidAmount, 0))									AS PP_Paid_Amount
	,COALESCE(pp_enroll.PP_Enrollment, 'No')									AS PP_Enrollment
	,SUM(jvc.JSA_JVC_Membership_Fees)											AS JSA_JVC_Membership_Fees
	,SUM(jvc.NumberOfYears_JM)													AS Years_with_JM
	,SUM(jvc.JSA_JVC_Membership_Fees * jvc.NumberOfYears_JM)					AS JSA_JVC_Lifetime_Fees

FROM attributes

LEFT JOIN jewelers_cut
	ON attributes.PolicyNumber = jewelers_cut.PolicyNumber

LEFT JOIN jc_enroll
	ON attributes.PolicyNumber = jc_enroll.PolicyNumber

LEFT JOIN platinum
	ON attributes.PolicyNumber = platinum.PolicyNumber

LEFT JOIN pp_enroll
	ON attributes.PolicyNumber = pp_enroll.PolicyNumber

LEFT JOIN jvc
	ON attributes.PolicyNumber = jvc.PolicyNumber

GROUP BY
	attributes.ProducerMasterCode
	,attributes.ProducerAgencyCode
	,attributes.PolicyNumber
	,attributes.AccountNumber
	,attributes.Name
	,attributes.NumberofEmployees
	,attributes.LocationCount
	,attributes.PolEffDate
	,attributes.PolExpDate
	,attributes.OriginalEffDate
	,attributes.PrimaryCountry
	,COALESCE(jc_enroll.JC_Enrollment, 'No')
	,COALESCE(pp_enroll.PP_Enrollment, 'No')

ORDER BY
	Master_Code
	,Agency_Code
	,AccountNumber
	,PolicyNumber