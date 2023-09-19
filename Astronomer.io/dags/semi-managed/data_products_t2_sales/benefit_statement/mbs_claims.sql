
Declare @Country varchar(5) = 'ALL'
Declare @ProductLine varchar(5) = 'PL'
Declare @SourceSystem varchar(5) = 'All'
---------------------------------------------------

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

SET @DateKey = bief_dds.fn_GetDateKeyFromDate(DATEADD(month, -1, @YearStart)) + 10000

SET @ClaimEnd = @YearEnd

SET @ClaimStart = CAST(CONCAT(YEAR(@YearEnd)-4,'-01-01') AS date);

/*
	Union YTD claims with historic claims
*/

WITH claims AS (
	SELECT 
		PaymentVendorContact.FullName											AS AccountJewelerName
		,PaymentVendorContact.ContactKey
		,CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine1, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine1  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine2, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine2  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine3, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine3  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressCity, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressCity + ', ','')
			END + 
		CASE
			WHEN COALESCE(JewelerState.GeographyStateCode, '?') = '?'
			THEN ''
			ELSE COALESCE(JewelerState.GeographyStateCode + ' ','')
			END + 
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressPostalCode, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressPostalCode,'')
			END																	AS AccountJewelerAddress
		,d_geo.GeographyCountryDesc
		,d_LOB.LOBProductLineDescription
		,COUNT(Distinct f_claim.ClaimKey)										AS PersonalLinesWorkedClaims5Yrs
		,SUM(f_claim.ClaimPaidLossExcludingRecovery)							AS PersonalLinesClaimPayments5Yrs
		,0																		AS PersonalLinesWorkedClaimsCurrent
		,0																		AS PersonalLinesClaimPaymentsCurrent

	FROM bi_dds.FactClaim														AS f_claim

		INNER JOIN bi_dds.DimLineOfBusiness										AS d_LOB
			ON f_claim.LineOfBusinessKey										= d_LOB.LineOfBusinessKey

		INNER JOIN bi_dds.DimGeography											AS d_geo
			ON f_claim.CovRatedStateGeographyKey								= d_geo.GeographyKey

		INNER JOIN bief_dds.DimDate												AS AcctDate
			ON f_claim.AccountingDateKey										= AcctDate.DateKey

		INNER JOIN bi_dds.DimBusinessType										AS d_biz_type
			ON f_claim.BusinessTypeKey											= d_biz_type.BusinessTypeKey

		INNER JOIN bi_dds.DimPayment											AS d_payment
			ON f_claim.PaymentKey												= d_payment.PaymentKey

		INNER JOIN bi_dds.DimContact											AS PaymentVendorContact
			ON d_payment.PaymentVendorContactKey								= PaymentVendorContact.ContactKey

		INNER JOIN bi_dds.DimGeography											AS JewelerState
			ON PaymentVendorContact.PrimaryAddressGeographyKey					= JewelerState.GeographyKey		

		INNER JOIN bief_dds.DimDate												AS LossDate
			ON f_claim.LossDateKey												= LossDate.DateKey		

	WHERE 1=1
		AND BusinessTypeDesc = 'Direct'
		AND ClaimPaidLossExcludingRecovery <> 0
		AND LossDate.FullDate BETWEEN @ClaimStart AND @ClaimEnd
		AND AcctDate.FullDate <= @ClaimEnd
		AND (f_claim.SourceSystem = @SourceSystem or ISNULL(@SourceSystem,'All') = 'All')
		AND f_claim.SourceSystem <> 'PCA' -- No PCA, but could be PAS & GW
		AND (d_LOB.LOBProductLineCode in (@ProductLine) or ISNULL(@ProductLine,'All') = 'All') 
		AND (d_geo.GeographyCountryCode in (@Country) or ISNULL(@Country,'All') = 'All')

	GROUP BY
		PaymentVendorContact.FullName
		,PaymentVendorContact.ContactKey			
		,CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine1, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine1  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine2, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine2  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine3, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine3  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressCity, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressCity + ', ','')
			END + 
		CASE
			WHEN COALESCE(JewelerState.GeographyStateCode, '?') = '?'
			THEN ''
			ELSE COALESCE(JewelerState.GeographyStateCode + ' ','')
			END + 
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressPostalCode, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressPostalCode,'')
			END
		,d_geo.GeographyCountryDesc
		,d_LOB.LOBProductLineDescription

	HAVING SUM(ClaimPaidLossExcludingRecovery) 	<> 0		

UNION

	SELECT 
		PaymentVendorContact.FullName											AS AccountJewelerName
		,PaymentVendorContact.ContactKey
		,CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine1, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine1  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine2, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine2  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine3, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine3  + ' ','')
			END  +
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressCity, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressCity + ', ','')
			END + 
		CASE
			WHEN COALESCE(JewelerState.GeographyStateCode, '?') = '?'
			THEN ''
			ELSE COALESCE(JewelerState.GeographyStateCode + ' ','')
			END + 
		CASE
			WHEN COALESCE(PaymentVendorContact.PrimaryAddressPostalCode, '?') = '?'
			THEN ''
			ELSE COALESCE(PaymentVendorContact.PrimaryAddressPostalCode,'')
			END																	AS AccountJewelerAddress
		,d_geo.GeographyCountryDesc
		,d_LOB.LOBProductLineDescription
		,0																		AS PersonalLinesWorkedClaims5Yrs
		,0																		AS PersonalLinesClaimPayments5Yrs
		,COUNT(Distinct f_claim.ClaimKey)										AS PersonalLinesWorkedClaimsCurrent
		,SUM(f_claim.ClaimPaidLossExcludingRecovery)							AS PersonalLinesClaimPaymentsCurrent

	FROM bi_dds.FactClaim														AS f_claim

		INNER JOIN bi_dds.DimLineOfBusiness										AS d_LOB
			ON f_claim.LineOfBusinessKey										= d_LOB.LineOfBusinessKey

		INNER JOIN bi_dds.DimGeography											AS d_geo
			ON f_claim.CovRatedStateGeographyKey								= d_geo.GeographyKey

		INNER JOIN bief_dds.DimDate												AS AcctDate
			ON f_claim.AccountingDateKey										= AcctDate.DateKey

		INNER JOIN bi_dds.DimBusinessType										AS d_biz_type
			ON f_claim.BusinessTypeKey											= d_biz_type.BusinessTypeKey

		INNER JOIN bi_dds.DimPayment											AS d_payment
			ON f_claim.PaymentKey												= d_payment.PaymentKey

		INNER JOIN bi_dds.DimContact											AS PaymentVendorContact
			ON d_payment.PaymentVendorContactKey								= PaymentVendorContact.ContactKey

		INNER JOIN bi_dds.DimGeography											AS JewelerState
			ON PaymentVendorContact.PrimaryAddressGeographyKey					= JewelerState.GeographyKey		

		INNER JOIN bief_dds.DimDate												AS LossDate
			ON f_claim.LossDateKey												= LossDate.DateKey

		WHERE 1=1
			AND d_biz_type.BusinessTypeDesc = 'Direct'
			AND f_claim.ClaimPaidLossExcludingRecovery <> 0
			AND (f_claim.SourceSystem = @SourceSystem OR ISNULL(@SourceSystem,'All') = 'All')
			AND f_claim.SourceSystem <> 'PCA' -- No PCA, but could be PAS & GW
			AND (d_LOB.LOBProductLineCode IN (@ProductLine) or ISNULL(@ProductLine,'All') = 'All') 
			AND (d_geo.GeographyCountryCode IN (@Country) or ISNULL(@Country,'All') = 'All')
			AND AcctDate.FullDate BETWEEN @YearStart AND @ClaimEnd

		GROUP BY
			PaymentVendorContact.FullName
			,PaymentVendorContact.ContactKey
			,CASE
				WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine1, '?') = '?'
				THEN ''
				ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine1  + ' ','')
				END  +
			CASE
				WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine2, '?') = '?'
				THEN ''
				ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine2  + ' ','')
				END  +
			CASE
				WHEN COALESCE(PaymentVendorContact.PrimaryAddressLine3, '?') = '?'
				THEN ''
				ELSE COALESCE(PaymentVendorContact.PrimaryAddressLine3  + ' ','')
				END  +
			CASE
				WHEN COALESCE(PaymentVendorContact.PrimaryAddressCity, '?') = '?'
				THEN ''
				ELSE COALESCE(PaymentVendorContact.PrimaryAddressCity + ', ','')
				END + 
			CASE
				WHEN COALESCE(JewelerState.GeographyStateCode, '?') = '?'
				THEN ''
				ELSE COALESCE(JewelerState.GeographyStateCode + ' ','')
				END + 
			CASE
				WHEN COALESCE(PaymentVendorContact.PrimaryAddressPostalCode, '?') = '?'
				THEN ''
				ELSE COALESCE(PaymentVendorContact.PrimaryAddressPostalCode,'')
				END
			,d_geo.GeographyCountryDesc
			,d_LOB.LOBProductLineDescription
	
	HAVING SUM(f_claim.ClaimPaidLossExcludingRecovery) 	<> 0	
)

SELECT
	claims.AccountJewelerName
	--,claims.GeographyCountryDesc
	--,claims.LOBProductLineDescription
	,SUM(claims.PersonalLinesWorkedClaims5Yrs) AS WorkedClaims5yrs
	,ROUND(SUM(claims.PersonalLinesClaimPayments5Yrs), -2) AS WorkedClaimPayments5yrs
	,SUM(claims.PersonalLinesWorkedClaimsCurrent) AS WorkedClaimsCurrent
	,ROUND(SUM(claims.PersonalLinesClaimPaymentsCurrent), -2) AS WorkedClaimsPaidCurrent

FROM claims


GROUP BY
	claims.AccountJewelerName
	--,claims.GeographyCountryDesc
	--,claims.LOBProductLineDescription
