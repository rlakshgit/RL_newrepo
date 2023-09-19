USE PLEcom;

DECLARE @StartDate DATE = '{start_date}';
DECLARE @EndDate   DATE = '{end_date}';


WITH aggregates AS (
		SELECT A.ApplicationId,
		SUM(CASE WHEN J.JewelryType = 'Ring' THEN 1 ELSE 0 END)											AS n_Rings,
		SUM(CASE WHEN JST.Description = 'Engagement Ring' THEN 1 ELSE 0 END)							AS n_subtype_EngRings,
		SUM(CASE WHEN J.JewelryType = 'Ring' THEN RetailReplacementValue ELSE 0 END)					AS val_Rings,
		SUM(CASE WHEN JST.Description = 'Engagement Ring' THEN RetailReplacementValue ELSE 0 END)		AS val_subtype_EngRings,
		COUNT(1)																						AS n_Items,
		SUM(RetailReplacementValue)																		AS TotalRetailReplacement
	FROM [PLEcom].[QuoteApp].[t_Application] A

		LEFT JOIN QuoteApp.t_ApplicationQuote AQ			ON AQ.ApplicationQuoteId = A.ApplicationQuoteId
		LEFT JOIN QuoteApp.t_QuotedJewelryItem QJI			ON QJI.ApplicationId = A.ApplicationId
		LEFT JOIN QuoteApp.t_Jewelry J						ON J.JewelryId = QJI.JewelryId
		LEFT JOIN QuoteApp.t_JewelrySubType JST				ON JST.JewelrySubTypeId = QJI.JewelrySubTypeId

	WHERE 1=1
		AND A.SubmissionDate IS NOT NULL
		AND SubmissionDate >= @StartDate
		AND SubmissionDate <= @EndDate
		--AND SubmissionDate = @YesterdayDate
	GROUP BY A.ApplicationId
)
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, distribution_source AS (
	SELECT ApplicationId,
		AccountNumber,
		ds.DistributionSourceType,
		dsst.Description As DistributionSourceSubType,
		A.ReferralCode
	FROM QuoteApp.t_Application A
	LEFT JOIN QuoteApp.t_DistributionSource ds					ON ds.DistributionSourceId = A.DistributionSourceId
	LEFT JOIN QuoteApp.t_DistributionSourceSubType dsst			ON dsst.DistributionSourceSubTypeId = A.DistributionSourceSubTypeId
)
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, time_elements AS (
	SELECT ApplicationId,
		DATEPART(hour, A.SubmissionDate) AS hour_submitted, --enhancement idea: modify the hour based on timezone of user
		DATENAME(weekday, A.SubmissionDate) AS dow_submitted

	FROM QuoteApp.t_Application A
	WHERE SubmissionDate IS NOT NULL
)
, all_claims AS (
	SELECT
		tapp.ApplicationId,
		vDC.ClaimCauseOfLossGroup,
		DATEDIFF(day,
					DW_DDS_CURRENT.bief_dds.fn_getdatefromdateKey(acct.AccountOrigInceptionDate),
					DW_DDS_CURRENT.bief_dds.fn_getdatefromdateKey(vDC.ClaimReportedDateKey))										AS DaysToClaim
	FROM DW_DDS_CURRENT.bief_dds.vwFactClaim vFC
	LEFT JOIN DW_DDS_CURRENT.bief_dds.vwDimClaim vDC				ON vDC.ClaimKey = vFc.ClaimKey
	LEFT JOIN DW_DDS_CURRENT.bi_dds.DimCoverage cov					ON cov.CoverageKey = vFC.CoverageKey
	LEFT JOIN DW_DDS_CURRENT.bi_dds.DimPolicy pol					ON pol.PolicyKey = cov.PolicyKey
	LEFT JOIN DW_DDS_CURRENT.bi_dds.DimAccount acct					ON acct.AccountKey = pol.AccountKey
	LEFT JOIN PLEcom.QuoteApp.t_Application tapp					ON tapp.AccountNumber = acct.AccountNumber

	WHERE pol.SourceSystem = 'GW'
	AND pol.AccountSegment = 'Personal Lines'
	AND cov.PerOccurenceLimit > 0
	AND tapp.AccountNumber IS NOT NULL

	GROUP BY
		tapp.ApplicationId,
		vDC.ClaimCauseOfLossGroup,
		DATEDIFF(day,
					DW_DDS_CURRENT.bief_dds.fn_getdatefromdateKey(acct.AccountOrigInceptionDate),
					DW_DDS_CURRENT.bief_dds.fn_getdatefromdateKey(vDC.ClaimReportedDateKey))
)
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, first_year_loss AS (
		SELECT
		all_claims.ApplicationId,
		all_claims.ClaimCauseOfLossGroup,
		1 AS first_year_loss
	FROM all_claims all_claims

	WHERE 1=1
		AND all_claims.DaysToClaim <= 365
		AND all_claims.ClaimCauseOfLossGroup IN ('Crime', 'Lost Item')
)
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, bound_apps AS (
	SELECT AccountNumber
	FROM DW_DDS_CURRENT.bi_dds.DimPolicy dpol
	LEFT JOIN DW_DDS_CURRENT.bi_dds.DimAccount dacct ON dacct.AccountKey = dpol.AccountKey
	WHERE dpol.JobType='Submission'
	AND dpol.PolicyStatus = 'Bound'
)

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Putting it all together -
	Joining on the application ID
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

SELECT
	a.ApplicationId,
	a.AccountNumber,
	aggregates.n_Rings,
	aggregates.n_subtype_EngRings,
	aggregates.val_Rings,
	aggregates.val_subtype_EngRings,
	aggregates.n_Items,
	aggregates.TotalRetailReplacement,
	time_elements.hour_submitted,
	ISNULL(first_year_loss.first_year_loss, 0)	AS first_year_loss,
	CASE
	WHEN aggregates.TotalRetailReplacement is null and SubmissionDate <= '1/1/2020' then 1
	else 0
	END as bad_record,
	a.SubmissionDate,
	'{date}' as bq_load_date
FROM PLEcom.QuoteApp.t_Application a

INNER JOIN bound_apps								ON a.AccountNumber = bound_apps.AccountNumber
LEFT JOIN aggregates								ON a.ApplicationId = aggregates.ApplicationId
LEFT JOIN distribution_source						ON a.ApplicationId = distribution_source.ApplicationId
LEFT JOIN time_elements								ON a.ApplicationId = time_elements.ApplicationId
LEFT JOIN first_year_loss							ON a.ApplicationId = first_year_loss.ApplicationId

WHERE a.SubmissionDate IS NOT NULL -- "If they don't submit, you must acquit", no submission = no response variable to train on, same reason for inner joining bound_apps. PLEcom does not indicate result of an application.
AND SubmissionDate >= @StartDate
AND SubmissionDate <= @EndDate
--AND SubmissionDate = @YesterdayDate
AND DistributionSourceType = 'Web' --Since later merging with browser data, create other models for other sources.

ORDER BY SubmissionDate