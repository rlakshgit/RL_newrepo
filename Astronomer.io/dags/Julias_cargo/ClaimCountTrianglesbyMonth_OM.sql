SET NOCOUNT ON

DECLARE @ENDDATE DATE
SELECT @ENDDATE = DATEADD(dd, -1, DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0))
DECLARE @ENDDATEKEY6 INT = LEFT(DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@ENDDATE),6)
DECLARE @ENDYEAR INT = LEFT(@ENDDATEKEY6,4)

CREATE TABLE #HISTORY
(
	OutstandingClaim				INT
	,ClosedWithOutPay				INT
	,ClosedWithPay					INT
	,PaidLoss						DECIMAL(20,4)
	,LastStatus						VARCHAR(20)
	,AccidentYear					VARCHAR(20)
	,AYM							VARCHAR(20)
	,AnnualStatementLine			VARCHAR(20)
	,ASLDescription					VARCHAR(50)
	,ASLGroup						VARCHAR(20)
	,ClaimNumber					VARCHAR(20)
	,LOBProductLineCode				VARCHAR(20)
	,LOBProductLineDescription		VARCHAR(20)
	,LOBCode						VARCHAR(20)
	,AS_LOB							VARCHAR(10)
	,GeographyCountryCode			VARCHAR(20)
	,GeographyCountryDesc			VARCHAR(20)
	,GeographyStateCode				VARCHAR(20)
	,[Claim Business Type]			VARCHAR(20)
	,AsOfDate						INT
	,AY2							VARCHAR(20)
	,CompanyName					VARCHAR(50)
)

DECLARE @BEGYEAR INT = @ENDYEAR - 15 + 1
DECLARE @i INT = @BEGYEAR
DECLARE @j INT
DECLARE @MAXJ INT

WHILE @i <= @ENDYEAR

	BEGIN
	
	SET @j = 1
	SET @MAXJ = 12

	IF @i = @ENDYEAR
		BEGIN
		SET @MAXJ = @ENDDATEKEY6 - @ENDYEAR * 100
		END

	WHILE @j <= @MAXJ
	
		BEGIN

		DECLARE @BEGDATEKEY INT
		DECLARE @ENDDATEKEY INT
		DECLARE @CURRENTCY INT
		DECLARE @STRYEAR VARCHAR(4)
		
		SET @STRYEAR = CONVERT(VARCHAR(4),@i)
		
		IF @j = 1
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0101' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0131' AS INT)
			END
		ELSE IF @j = 2
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0201' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0229' AS INT)
			END
		ELSE IF @j = 3
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0301' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0331' AS INT)
			END
		ELSE IF @j = 4
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0401' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0430' AS INT)
			END
		ELSE IF @j = 5		
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0501' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0531' AS INT)
			END
		ELSE IF @j = 6
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0601' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0630' AS INT)
			END
		ELSE IF @j = 7
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0701' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0731' AS INT)
			END
		ELSE IF @j = 8
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0801' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0831' AS INT)
			END
		ELSE IF @j = 9		
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '0901' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '0930' AS INT)
			END
		ELSE IF @j = 10
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '1001' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '1031' AS INT)
			END
		ELSE IF @j = 11
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '1101' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '1130' AS INT)
			END
		ELSE IF @j = 12
			BEGIN
			SET @BEGDATEKEY = CAST(@STRYEAR + '1201' AS INT)
			SET @ENDDATEKEY = CAST(@STRYEAR + '1231' AS INT)
			END
		
		SELECT TOP 1 @CURRENTCY = YearNumber FROM DW_DDS_CURRENT.bief_dds.DimDate WHERE @ENDDATEKEY = DateKey
		
		INSERT INTO #HISTORY

		SELECT
			CASE WHEN UPPER(LastStatus) IN ('OPEN', 'OPENED', 'REOPENED') THEN 1 ELSE 0 END OutstandingClaim
			,CASE WHEN UPPER(LastStatus) IN ('CLOSED', 'CWP') AND PaidLoss = 0 THEN 1 ELSE 0 END AS ClosedWithOutPay
			,CASE WHEN UPPER(LastStatus) IN ('CLOSED', 'CWP') AND PaidLoss > 0 THEN 1 ELSE 0 END AS ClosedWithPay
			,PaidLoss
			,UPPER(LastStatus) AS LastStatus
			,AccidentYear
			,AYM
			,AnnualStatementLine
			,ASLDescription
			,ASLGroup = CASE WHEN ASLDescription = 'Inland Marine' THEN 'IM' ELSE 'CMP' END 
			,ClaimNumber
			,LOBProductLineCode
			,LOBProductLineDescription
			,LOBCode
			,AS_LOB
			,GeographyCountryCode
			,GeographyCountryDesc
			,GeographyStateCode
			,MAIN.[Claim Business Type]
			,@ENDDATEKEY AS AsOfDate
			,AY2
			,CompanyName
	
		FROM
		(
			SELECT 
				PaidLoss = SUM([Claim Paid Loss]) -- Only direct paid loss excluding recoveries, not LAE payments.
				,LOSSDATE.YearNumber AS AccidentYear
				,LOSSDATE.YearMonthNumber AS AYM
				,AnnualStatementLine
				,ASLDescription
				,ClaimNumber
				,LOBProductLineCode
				,LOBProductLineDescription
				,LOBCode
				,CASE
					WHEN DASL.AnnualStatementLine IN ('051','052') THEN 'CMP'
					WHEN DASL.AnnualStatementLine IN ('090') AND DLOB.LOBProductLineCode IN ('CL') THEN '9.0CL'
					WHEN DASL.AnnualStatementLine IN ('090') AND DLOB.LOBProductLineCode IN ('PL') THEN '9.0PL'
					WHEN DASL.AnnualStatementLine IN ('080') AND DLOB.LOBProductLineCode IN ('CL') THEN 'OM'
					ELSE 'UNKNOWN'
				END AS AS_LOB
				,GeographyCountryCode
				,GeographyCountryDesc
				,GeographyStateCode
				,LastStatus
				,F.[Claim Business Type]
				,CASE WHEN (DATEDIFF(YEAR,LOSSDATE.FullDate,@ENDDATE)) > 9 THEN 'Prior To ' + CAST((@CURRENTCY - 9) AS VARCHAR(10)) ELSE CAST(LOSSDATE.YearNumber AS VARCHAR(10)) END AS AY2
				,DCOMPANY.CompanyName
		
			FROM
				DW_DDS_CURRENT.bief_dds.vwFactclaim AS F
	
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS DCLAIM
					ON F.ClaimKey = DCLAIM.ClaimKey
					
				INNER JOIN DW_DDS_CURRENT.bief_dds.DimDate AS LOSSDATE
					ON DCLAIM.ClaimLossDateKey = LOSSDATE.DateKey
					
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS DGEO
					ON F.CovRatedStateGeographyKey = DGEO.GeographyKey
					
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimASL AS DASL
					ON F.ASLKey = DASL.ASLKey
					
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS DLOB
					ON F.LineOfBusinessKey = DLOB.LineOfBusinessKey
					
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS DBT
					ON F.BusinessTypeKey = DBT.BusinessTypeKey
					
				INNER JOIN DW_DDS_CURRENT.bi_dds.DimCompany AS DCOMPANY
					ON F.CompanyKey = DCOMPANY.CompanyKey
					
				LEFT JOIN
					(
						SELECT ClaimKey -- Check for voids or reversals during the time frame if present; include in report.
						FROM DW_DDS_CURRENT.bief_dds.vwFactClaim ClaimVoidsReversals
						INNER JOIN DW_DDS_CURRENT.bi_dds.DimPayment 
						ON ClaimVoidsReversals.PaymentKey = DimPayment.PaymentKey
						WHERE PaymentReversalDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY OR PaymentVoidDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY
					) AS [VoidsReversals]
				ON VoidsReversals.ClaimKey = DCLAIM.ClaimKey
				
				CROSS APPLY
				(
					SELECT TOP 1 -- Gets the latest status of a claim and date entered.
						LastStatus.ClaimStatus LastStatus
						,EnteredDateKey LastStatusDateKey
					FROM DW_DDS_CURRENT.bi_dds.FactClaimStatus LastStatus
					INNER JOIN DW_DDS_CURRENT.bi_keymap.FactClaimStatus kmFactClaimStatus
					ON kmFactClaimStatus.FactClaimStatusKey = LastStatus.FactClaimStatusKey
					WHERE 1=1
						AND LastStatus.ClaimKey = F.ClaimKey
						AND EnteredDateKey <= @ENDDATEKEY 
						AND EntryType = 'C'
					ORDER BY
						EnteredDate DESC
						,kmFactClaimStatus.SSID_PAS_MCATSTATUSHISTORY_ID DESC
				) AS [LastStatusEntered]
				
			WHERE 1=1
				--AND F.[Claim Business Type] = 'Direct'
				AND F.AccountingDateKey <= @ENDDATEKEY -- Sum paid losses to end date.
				AND
				(
					-- Check for voids/reversals during the date range to offset previous payments; this will cause claim to show up during a month.
					-- If no status change, the void/reversal applies to a payment made during prior months to change the outcome of the status during that month.
					(LastStatusDateKey <= @ENDDATEKEY AND LastStatus IN ('OPEN', 'OPENED', 'REOPENED')) -- Check for open claims up to the end date.
					OR (LastStatusDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY AND LastStatus IN ('CLOSED', 'CWP')) -- Get closed claims during date range.
					OR VoidsReversals.ClaimKey IS NOT NULL
				)
				AND DASL.AnnualStatementLine IN ('080')
				AND DLOB.LOBProductLineCode IN ('CL')
			
			GROUP BY 
				LOSSDATE.YearNumber
				,LOSSDATE.YearMonthNumber
				,AnnualStatementLine
				,ASLDescription
				,ClaimNumber
				,LOBProductLineCode
				,LOBProductLineDescription
				,LOBCode
				,CASE
					WHEN DASL.AnnualStatementLine IN ('051','052') THEN 'CMP'
					WHEN DASL.AnnualStatementLine IN ('090') AND DLOB.LOBProductLineCode IN ('CL') THEN '9.0CL'
					WHEN DASL.AnnualStatementLine IN ('090') AND DLOB.LOBProductLineCode IN ('PL') THEN '9.0PL'
					WHEN DASL.AnnualStatementLine IN ('080') AND DLOB.LOBProductLineCode IN ('CL') THEN 'OM'
					ELSE 'UNKNOWN'
				END
				,GeographyCountryCode
				,GeographyCountryDesc
				,GeographyStateCode
				,LastStatus
				,F.[Claim Business Type]
				,CASE WHEN (DATEDIFF(YEAR,LOSSDATE.FullDate,@ENDDATE)) > 9 THEN 'Prior To ' + CAST((@CURRENTCY - 9) AS VARCHAR(10)) ELSE CAST(LOSSDATE.YearNumber AS VARCHAR(10)) END
				,DCOMPANY.CompanyName
		
		) AS MAIN

		ORDER BY 
			LOBProductLineCode
			,AnnualStatementLine
			,AccidentYear
			,AYM
			,ClaimNumber

		SET @j += 1
		
		END
		
	SET @i += 1
	
	END
	
SELECT
	AsOfDate									AS EVAL_DATE
	,AccidentYear								AS AY
	,AY2										AS AY2
	,AYM										AS AYM
	,CompanyName								AS COMPANY
	,LOBProductLineCode							AS PROD
	,AnnualStatementLine						AS ASL
	,LOBCode									AS LOB
	,AS_LOB										AS AS_LOB
	,GeographyCountryCode						AS COUNTRY
	,GeographyStateCode							AS STATE
	,[Claim Business Type]						AS BUS_TYPE
	,'NA'										AS CLAIM_EVENT
	,'NA'										AS EXCESS_LOSS
	,ASLGroup									AS ASL_GROUP
	,SUM(OutstandingClaim)						AS COUNT_OPEN
	,SUM(ClosedWithOutPay)						AS COUNT_CLOSED_NO_PAY
	,SUM(ClosedWithPay)							AS COUNT_CLOSED_WITH_PAY
	,SUM(ClosedWithOutPay) + SUM(ClosedWithPay) AS COUNT_CLOSED_TOTAL

FROM #HISTORY

GROUP BY
	AsOfDate
	,AccidentYear
	,AY2
	,AYM
	,CompanyName
	,LOBProductLineCode
	,AnnualStatementLine
	,LOBCode
	,AS_LOB
	,GeographyCountryCode
	,GeographyStateCode
	,[Claim Business Type]
	,ASLGroup									

ORDER BY AsOfDate

DROP TABLE #HISTORY