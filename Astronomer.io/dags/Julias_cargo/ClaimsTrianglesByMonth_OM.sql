SET NOCOUNT ON

IF OBJECT_ID('tempdb..#HISTORY')			IS NOT NULL DROP TABLE #HISTORY
IF OBJECT_ID('tempdb..#LARGELOSS')			IS NOT NULL DROP TABLE #LARGELOSS

-- Pick up the last completed quarter.
DECLARE @ENDDATE DATE
SELECT @ENDDATE = DATEADD(dd, -1, DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0))
DECLARE @ENDDATEKEY6 INT = LEFT(DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@ENDDATE),6)
DECLARE @ENDYEAR INT = LEFT(@ENDDATEKEY6,4)

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Collection of CTEs before a temp table in order to properly age claims and flag the appropriate ones as large losses.
------------------------------------------------------------------------------------------------------------------------------------------------------

/********************************************************************************************************************************************************************************************************
In order to classifiy large claims, we need to determine at what age of the claim should we stamp it to be large or not large.
We age claims to be three months and check the reported losses gross of reinsurance and net of salvage and subrogation at the line of business level.
********************************************************************************************************************************************************************************************************/
;WITH CLOSED AS
	(
		SELECT DISTINCT
			DCLAIM.ClaimNumber AS CLAIM_NUMBER
			,DLOB.LOBCode AS LOB
			,SUM(F.[Claim Paid Loss Net Recovery]) AS PAID_LOSS_NET_SS
			,CASE
				WHEN SUM(F.[Claim Paid Loss Net Recovery]) > 0 AND LEFT(DCLAIM.ClaimStatus,1) = 'C' THEN 'CLOSED_WITH_PMT'
				WHEN SUM(F.[Claim Paid Loss Net Recovery]) = 0 AND LEFT(DCLAIM.ClaimStatus,1) = 'C' THEN 'CLOSED_WITHOUT_PMT'
				ELSE 'OPEN' END AS CLAIM_STATUS

		FROM DW_DDS_CURRENT.bief_dds.vwFactClaim AS F

			INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS DCLAIM
			ON F.ClaimKey = DCLAIM.ClaimKey
			
			INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS DLOB
			ON F.LineOfBusinessKey = DLOB.LineOfBusinessKey
			
		WHERE 1=1
			--AND ClaimNumber = '45-003958'
			--AND ClaimNumber = '45-002977'
			--AND ClaimStatus = 'OPEN'

		GROUP BY
			DCLAIM.ClaimNumber
			,DLOB.LOBCode
			,DCLAIM.ClaimStatus
	)

,CLAIMLVL AS
	(
		SELECT
			DCLAIM.ClaimNumber AS CLAIM_NUMBER
			,DLOB.LOBCode AS LOB
			,LEFT(F.AccountingDateKey,6) AS CYM
			,SUM(F.[Claim Paid Loss Net Recovery]) AS PAID_LOSS_NET_SS
			,SUM(F.[Claim Incurred Loss Net Recovery]) AS RPTD_LOSS_NET_SS
			,CLOSED.CLAIM_STATUS

		FROM DW_DDS_CURRENT.bief_dds.vwFactClaim AS F

			INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS DCLAIM
			ON F.ClaimKey = DCLAIM.ClaimKey
			
			INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS DLOB
			ON F.LineOfBusinessKey = DLOB.LineOfBusinessKey

			INNER JOIN CLOSED
			ON DCLAIM.ClaimNumber = CLOSED.CLAIM_NUMBER
			AND DLOB.LOBCode = CLOSED.LOB
			
		WHERE 1=1
			--AND ClaimNumber = '14-019716'
			--AND ClaimNumber = '45-002977'
			--AND ClaimStatus = 'OPEN'

		GROUP BY
			DCLAIM.ClaimNumber
			,DLOB.LOBCode
			,LEFT(F.AccountingDateKey,6)
			,CLOSED.CLAIM_STATUS
	) --SELECT * FROM CLAIMLVL
	
,MINCLAIM AS
	(
		SELECT
			CLAIM_NUMBER
			,CLAIMLVL.LOB
			,CLAIMLVL.CLAIM_STATUS
			,MIN(CYM) AS CYM
		FROM CLAIMLVL
		GROUP BY
			CLAIM_NUMBER
			,CLAIMLVL.LOB
			,CLAIMLVL.CLAIM_STATUS
	) --SELECT * FROM MINCLAIM
	
,EXPANDCLAIMDATES AS
	(
		SELECT 
			MINCLAIM.CLAIM_NUMBER
			,MINCLAIM.LOB
			,DATES.CYM
			,MINCLAIM.CLAIM_STATUS
		FROM
			(SELECT DISTINCT LEFT(DateKey,6) AS CYM FROM DW_DDS_CURRENT.bief_dds.DimDate) AS DATES
			CROSS JOIN MINCLAIM
		WHERE DATES.CYM >= MINCLAIM.CYM AND DATES.CYM <= @ENDDATEKEY6
	) --SELECT * FROM EXPANDCLAIMDATES ORDER BY CLAIM_NUMBER, CYM

/********************************************************************************************************************************************************************************************************
Collapse into claim-level, and create the flags for large losses that are dependent on line of business.
********************************************************************************************************************************************************************************************************/
,FINAL1 AS
	(
		SELECT DISTINCT
			EXPANDCLAIMDATES.CLAIM_NUMBER
			,EXPANDCLAIMDATES.LOB
			,EXPANDCLAIMDATES.CYM
			,DENSE_RANK() OVER(PARTITION BY EXPANDCLAIMDATES.CLAIM_NUMBER, EXPANDCLAIMDATES.LOB ORDER BY EXPANDCLAIMDATES.CYM ASC) AS RNK
			,ISNULL(CLAIMLVL.PAID_LOSS_NET_SS,0) AS PAID_LOSS_NET_SS
			,ISNULL(CLAIMLVL.RPTD_LOSS_NET_SS,0) AS RPTD_LOSS_NET_SS
			,EXPANDCLAIMDATES.CLAIM_STATUS

		FROM EXPANDCLAIMDATES

			LEFT JOIN CLAIMLVL
			ON EXPANDCLAIMDATES.CLAIM_NUMBER = CLAIMLVL.CLAIM_NUMBER
			AND EXPANDCLAIMDATES.CYM = CLAIMLVL.CYM
			AND EXPANDCLAIMDATES.LOB = CLAIMLVL.LOB
			AND EXPANDCLAIMDATES.CLAIM_STATUS = CLAIMLVL.CLAIM_STATUS
	) --SELECT * FROM FINAL1
	
,FINAL2 AS
	(
	SELECT
		FINAL1.CLAIM_NUMBER
		,FINAL1.LOB
		,CASE
			WHEN FINAL1.LOB IN ('BOP','XCL','CPP','UMB') AND X.CHECK_LL >= 250000 THEN 1
			WHEN FINAL1.LOB = 'JB' AND X.CHECK_LL >= 500000 THEN 1
			WHEN FINAL1.LOB = 'JS' AND X.CHECK_LL >= 150000 THEN 1
			WHEN FINAL1.LOB IN ('PJ','PA') AND X.CHECK_LL >= 50000 THEN 1
			WHEN FINAL1.LOB NOT IN ('BOP','XCL','CPP','UMB','JB','JS','PJ','PA') AND X.CHECK_LL >= 500000 THEN 1
			ELSE 0 END AS EXCESS_LOSS
		,SUM(FINAL1.PAID_LOSS_NET_SS) AS PAID_LOSS_NET_SS
		,SUM(FINAL1.RPTD_LOSS_NET_SS) AS RPTD_LOSS_NET_SS
		,FINAL1.CLAIM_STATUS

	FROM
		FINAL1 INNER JOIN
		(
			SELECT
				FINAL1.CLAIM_NUMBER
				,FINAL1.LOB
				,SUM(FINAL1.RPTD_LOSS_NET_SS) AS CHECK_LL
			
			FROM FINAL1

			WHERE 1=1
				AND RNK <= 3 -- Aging reported claims to be three months old (one quarter), to determine whether it's a large loss.

			GROUP BY
				FINAL1.CLAIM_NUMBER
				,FINAL1.LOB
		) AS X
		ON FINAL1.CLAIM_NUMBER = X.CLAIM_NUMBER
		AND FINAL1.LOB = X.LOB
	GROUP BY
		FINAL1.CLAIM_NUMBER
		,FINAL1.LOB
		,FINAL1.CLAIM_STATUS
		,CASE
			WHEN FINAL1.LOB IN ('BOP','XCL','CPP','UMB') AND X.CHECK_LL >= 250000 THEN 1
			WHEN FINAL1.LOB = 'JB' AND X.CHECK_LL >= 500000 THEN 1
			WHEN FINAL1.LOB = 'JS' AND X.CHECK_LL >= 150000 THEN 1
			WHEN FINAL1.LOB IN ('PJ','PA') AND X.CHECK_LL >= 50000 THEN 1
			WHEN FINAL1.LOB NOT IN ('BOP','XCL','CPP','UMB','JB','JS','PJ','PA') AND X.CHECK_LL >= 500000 THEN 1
			ELSE 0 END
	) --SELECT * FROM FINAL2

SELECT
	FINAL2.CLAIM_NUMBER
	,FINAL2.CLAIM_STATUS
	,FINAL2.LOB
	,CASE WHEN SUM(FINAL2.EXCESS_LOSS) > 0 THEN 1 ELSE 0 END AS EXCESS_LOSS
	,SUM(FINAL2.PAID_LOSS_NET_SS) AS PAID_LOSS_NET_SS
	,SUM(FINAL2.RPTD_LOSS_NET_SS) AS RPTD_LOSS_NET_SS
INTO #LARGELOSS

FROM FINAL2

WHERE 1=1

GROUP BY
	FINAL2.CLAIM_NUMBER
	,FINAL2.CLAIM_STATUS
	,FINAL2.LOB

--SELECT * FROM #LARGELOSS

/********************************************************************************************************************************************************************************************************
-- Loop to extract incremental values.
********************************************************************************************************************************************************************************************************/

CREATE TABLE #HISTORY
	(
		EVAL_DATE INT
		,CLAIM_NUMBER VARCHAR(20)
		,AY VARCHAR(4)
		,AY2 VARCHAR(20)
		,AYM VARCHAR(6)
		,RY VARCHAR(4)
		,RYM VARCHAR(6)
		,COMPANY VARCHAR(50)
		,PROD VARCHAR(10)
		,ASL VARCHAR(10)
		,LOB VARCHAR(10)
		,AS_LOB VARCHAR(10)
		,ACCT_SEG VARCHAR(50)
		,COUNTRY VARCHAR(50)
		,STATE VARCHAR(50)
		,BUS_TYPE VARCHAR(10)
		,CLAIM_EVENT VARCHAR(2)
		,EXCESS_LOSS VARCHAR(2)
		,CLAIM_STATUS VARCHAR(50)
		
		,CASE_RSV_LOSS MONEY
		,CASE_RSV_LOSS_REC MONEY
		,CASE_RSV_DCC MONEY
		,CASE_RSV_DCC_REC MONEY
		,CASE_RSV_AO MONEY
		,CASE_RSV_AO_REC MONEY

		,PAID_LOSS_NET_SS MONEY
		,RPTD_LOSS_NET_SS MONEY
		,PAID_DCC_NET_SS MONEY
		,RPTD_DCC_NET_SS MONEY
		,PAID_AO_NET_SS MONEY
		,RPTD_AO_NET_SS MONEY
		,PAID_ALAE_NET_SS MONEY
		,RPTD_ALAE_NET_SS MONEY
		,PAID_LOSS_AND_ALAE_NET_SS MONEY
		,RPTD_LOSS_AND_ALAE_NET_SS MONEY
		
		,PAID_LOSS_GROSS_SS MONEY
		,RPTD_LOSS_GROSS_SS MONEY
		,PAID_DCC_GROSS_SS MONEY
		,RPTD_DCC_GROSS_SS MONEY
		,PAID_AO_GROSS_SS MONEY
		,RPTD_AO_GROSS_SS MONEY
		,PAID_ALAE_GROSS_SS MONEY
		,RPTD_ALAE_GROSS_SS MONEY
		,PAID_LOSS_AND_ALAE_GROSS_SS MONEY
		,RPTD_LOSS_AND_ALAE_GROSS_SS MONEY
		
		,SALVAGE MONEY
		,SUBROGATION MONEY
		,DEDUCTIBLE MONEY
		,SS MONEY
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
			@ENDDATEKEY AS EVAL_DATE
			,INNER_CLAIMS.CLAIM_NUMBER
			,INNER_CLAIMS.AY
			,INNER_CLAIMS.AY2
			,INNER_CLAIMS.AYM
			,INNER_CLAIMS.RY
			,INNER_CLAIMS.RYM
			,INNER_CLAIMS.COMPANY
			,INNER_CLAIMS.PROD_LINE AS PROD -- Renamed to match triangles workbook.
			,INNER_CLAIMS.ASL
			,INNER_CLAIMS.LOB
			,CASE
				WHEN INNER_CLAIMS.ASL IN ('051','052') THEN 'CMP'
				WHEN INNER_CLAIMS.ASL IN ('090') AND INNER_CLAIMS.PROD_LINE IN ('CL') THEN '9.0CL'
				WHEN INNER_CLAIMS.ASL IN ('090') AND INNER_CLAIMS.PROD_LINE IN ('PL') THEN '9.0PL'
				WHEN INNER_CLAIMS.ASL IN ('080') AND INNER_CLAIMS.PROD_LINE IN ('CL') THEN 'OM'
				ELSE 'UNKNOWN'
			END AS AS_LOB
			,INNER_CLAIMS.ACCT_SEG
			,INNER_CLAIMS.COUNTRY
			,INNER_CLAIMS.STATE
			,INNER_CLAIMS.BUS_TYPE
			,INNER_CLAIMS.CLAIM_EVENT
			,INNER_CLAIMS.EXCESS_LOSS
			,INNER_CLAIMS.CLAIM_STATUS
			
			,SUM(INNER_CLAIMS.CASE_RSV_LOSS)		AS CASE_RSV_LOSS
			,SUM(INNER_CLAIMS.CASE_RSV_LOSS_REC)	AS CASE_RSV_LOSS_REC
			,SUM(INNER_CLAIMS.CASE_RSV_DCC)			AS CASE_RSV_DCC
			,SUM(INNER_CLAIMS.CASE_RSV_DCC_REC)		AS CASE_RSV_DCC_REC
			,SUM(INNER_CLAIMS.CASE_RSV_AO)			AS CASE_RSV_AO
			,SUM(INNER_CLAIMS.CASE_RSV_AO_REC)		AS CASE_RSV_AO_REC
			
			,SUM(INNER_CLAIMS.PAID_LOSS_NET_SS)		AS PAID_LOSS_NET_SS
			,SUM(INNER_CLAIMS.RPTD_LOSS_NET_SS)		AS RPTD_LOSS_NET_SS
			,SUM(INNER_CLAIMS.PAID_DCC_NET_SS)		AS PAID_DCC_NET_SS
			,SUM(INNER_CLAIMS.RPTD_DCC_NET_SS)		AS RPTD_DCC_NET_SS
			,SUM(INNER_CLAIMS.PAID_AO_NET_SS)		AS PAID_AO_NET_SS
			,SUM(INNER_CLAIMS.RPTD_AO_NET_SS)		AS RPTD_AO_NET_SS
			,SUM(INNER_CLAIMS.PAID_ALAE_NET_SS)		AS PAID_ALAE_NET_SS
			,SUM(INNER_CLAIMS.RPTD_ALAE_NET_SS)		AS RPTD_ALAE_NET_SS
			,SUM(INNER_CLAIMS.PAID_LOSS_NET_SS) + SUM(INNER_CLAIMS.PAID_ALAE_NET_SS) AS PAID_LOSS_AND_ALAE_NET_SS
			,SUM(INNER_CLAIMS.RPTD_LOSS_NET_SS) + SUM(INNER_CLAIMS.RPTD_ALAE_NET_SS) AS RPTD_LOSS_AND_ALAE_NET_SS

			,SUM(INNER_CLAIMS.PAID_LOSS_GROSS_SS)	AS PAID_LOSS_GROSS_SS
			,SUM(INNER_CLAIMS.RPTD_LOSS_GROSS_SS)	AS RPTD_LOSS_GROSS_SS
			,SUM(INNER_CLAIMS.PAID_DCC_GROSS_SS)	AS PAID_DCC_GROSS_SS
			,SUM(INNER_CLAIMS.RPTD_DCC_GROSS_SS)	AS RPTD_DCC_GROSS_SS
			,SUM(INNER_CLAIMS.PAID_AO_GROSS_SS)		AS PAID_AO_GROSS_SS
			,SUM(INNER_CLAIMS.RPTD_AO_GROSS_SS)		AS RPTD_AO_GROSS_SS
			,SUM(INNER_CLAIMS.PAID_ALAE_GROSS_SS)	AS PAID_ALAE_GROSS_SS
			,SUM(INNER_CLAIMS.RPTD_ALAE_GROSS_SS)	AS RPTD_ALAE_GROSS_SS
			,SUM(INNER_CLAIMS.PAID_LOSS_GROSS_SS) + SUM(INNER_CLAIMS.PAID_ALAE_GROSS_SS) AS PAID_LOSS_AND_ALAE_GROSS_SS
			,SUM(INNER_CLAIMS.RPTD_LOSS_GROSS_SS) + SUM(INNER_CLAIMS.RPTD_ALAE_GROSS_SS) AS RPTD_LOSS_AND_ALAE_GROSS_SS
			
			,SUM(INNER_CLAIMS.SALVAGE)				AS SALVAGE
			,SUM(INNER_CLAIMS.SUBROGATION)			AS SUBROGATION
			,SUM(INNER_CLAIMS.DEDUCTIBLE)			AS DEDUCTIBLE
			,SUM(INNER_CLAIMS.SALVAGE) +
			 SUM(INNER_CLAIMS.SUBROGATION) +
			 SUM(INNER_CLAIMS.DEDUCTIBLE)			AS SS

		FROM
			(
				
				SELECT
					DCOMPANY.CompanyName AS COMPANY
					,DLOB.LOBProductLineCode AS PROD_LINE
					,DLOB.LOBCode AS LOB
					,DASL.AnnualStatementLine AS ASL
					,DGEO.GeographyCountryCode AS COUNTRY
					,DGEO.GeographyStateCode AS STATE
					,DBT.BusinessTypeDesc AS BUS_TYPE
					,LOSSDATE.YearNumber AS AY
					,LOSSDATE.YearMonthNumber AS AYM
					,RPTDATE.YearNumber AS RY
					,RPTDATE.YearMonthNumber AS RYM
					,CASE WHEN (DATEDIFF(YEAR,LOSSDATE.FullDate,@ENDDATE)) > 9 THEN 'Prior To ' + CAST((@CURRENTCY - 9) AS VARCHAR(10)) ELSE CAST(LOSSDATE.YearNumber AS VARCHAR(10)) END AS AY2
					,DPOL.AccountSegment AS ACCT_SEG
					,DCLAIM.ClaimNumber AS CLAIM_NUMBER
					,CASE WHEN DCLAIM.ClaimEvent IS NULL OR DCLAIM.ClaimEvent IN ('?') THEN 0 ELSE 1 END AS CLAIM_EVENT
					,#LARGELOSS.EXCESS_LOSS
					,#LARGELOSS.CLAIM_STATUS
					
					-- Measures.
					,SUM(ISNULL([Claim Reserve Loss],0))						AS CASE_RSV_LOSS
					,SUM(ISNULL([Claim Reserve Loss Recovery],0))				AS CASE_RSV_LOSS_REC
					,SUM(ISNULL([Claim Reserve ALAE DCC],0))					AS CASE_RSV_DCC
					,SUM(ISNULL([Claim Reserve DCC Expense Recovery],0))		AS CASE_RSV_DCC_REC
					,SUM(ISNULL([Claim Reserve ALAE AO],0))						AS CASE_RSV_AO
					,SUM(ISNULL([Claim Reserve AO Expense Recovery],0))			AS CASE_RSV_AO_REC
					
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid Loss Net Recovery],0) ELSE 0 END)			AS PAID_LOSS_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred Loss Net Recovery],0) ELSE 0 END)		AS RPTD_LOSS_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid DCC Expense Net Recovery],0) ELSE 0 END)		AS PAID_DCC_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred DCC Expense Net Recovery],0) ELSE 0 END)	AS RPTD_DCC_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid AO Expense Net Recovery],0) ELSE 0 END)		AS PAID_AO_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred AO Expense Net Recovery],0) ELSE 0 END)	AS RPTD_AO_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid ALAE Net Recovery],0) ELSE 0 END)			AS PAID_ALAE_NET_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred ALAE Net Recovery],0) ELSE 0 END)		AS RPTD_ALAE_NET_SS
					
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid Loss],0) ELSE 0 END)							AS PAID_LOSS_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred Loss],0) ELSE 0 END)						AS RPTD_LOSS_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid ALAE DCC],0) ELSE 0 END)						AS PAID_DCC_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred ALAE DCC],0) ELSE 0 END)					AS RPTD_DCC_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid ALAE AO],0) ELSE 0 END)						AS PAID_AO_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred ALAE AO],0) ELSE 0 END)					AS RPTD_AO_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Paid ALAE],0) ELSE 0 END)							AS PAID_ALAE_GROSS_SS
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN ISNULL([Claim Incurred ALAE],0) ELSE 0 END)						AS RPTD_ALAE_GROSS_SS
					
					-- Recovery types not in Salvage or Deductible will be classified as Subrogation. All ALAE NULL recovery types will be mapped to Salvage.
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN
							(
								CASE
									WHEN [Claim Recovery Type] NOT IN ('Salvage', 'Deductible') AND [Claim Loss Recovery] <> 0.00 THEN [Claim Loss Recovery] 
									WHEN [Claim Recovery Type] NOT IN ('Salvage', 'Deductible') AND [Claim Recovery Type] IS NOT NULL AND [Claim Recovery AO Expense] <> 0.00 THEN [Claim Recovery AO Expense]
									WHEN [Claim Recovery Type] NOT IN ('Salvage', 'Deductible') AND [Claim Recovery Type] IS NOT NULL AND [Claim Recovery DCC Expense] <> 0.00 THEN [Claim Recovery DCC Expense]
									ELSE 0 END)
							ELSE 0 END) AS SUBROGATION
					
					-- Recovery types Salvage and NULL (only for ALAE) will be classified as Salvage.
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN
							(
								CASE
									WHEN [Claim Recovery Type] = 'Salvage' AND [Claim Loss Recovery] <> 0.00 THEN [Claim Loss Recovery] 
									WHEN ([Claim Recovery Type] = 'Salvage' OR [Claim Recovery Type] IS NULL) AND [Claim Recovery AO Expense] <> 0.00 THEN [Claim Recovery AO Expense]
									WHEN ([Claim Recovery Type] = 'Salvage' OR [Claim Recovery Type] IS NULL) AND [Claim Recovery DCC Expense] <> 0.00 THEN [Claim Recovery DCC Expense] 
									ELSE 0 END)
							ELSE 0 END) AS SALVAGE
					
					-- Recovery type Deductible will be classified as Deductible. This is the easiest classification.
					,SUM(CASE WHEN F.AccountingDateKey BETWEEN @BEGDATEKEY AND @ENDDATEKEY THEN
							(
								CASE
									WHEN [Claim Recovery Type] = 'Deductible' AND [Claim Loss Recovery] <> 0.00 THEN [Claim Loss Recovery]
									WHEN [Claim Recovery Type] = 'Deductible' AND [Claim Recovery AO Expense] <> 0.00 THEN [Claim Recovery AO Expense]
									WHEN [Claim Recovery Type] = 'Deductible' AND [Claim Recovery DCC Expense] <> 0.00 THEN [Claim Recovery DCC Expense]
									ELSE 0 END)
							ELSE 0 END) AS DEDUCTIBLE
					
				FROM
					DW_DDS_CURRENT.bief_dds.vwFactClaim AS F
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimClaim AS DCLAIM
					ON F.ClaimKey = DCLAIM.ClaimKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS DPOL
					ON F.PolicyKey = DPOL.PolicyKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimASL AS DASL
					ON F.ASLKey = DASL.ASLKey
					
					INNER JOIN DW_DDS_CURRENT.bief_dds.DimDate AS LOSSDATE
					ON F.LossDateKey = LOSSDATE.DateKey
					
					INNER JOIN DW_DDS_CURRENT.bief_dds.DimDate AS RPTDATE
					ON F.ReportedDateKey = RPTDATE.DateKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS DGEO
					ON F.CovRatedStateGeographyKey = DGEO.GeographyKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS DBT
					ON F.BusinessTypeKey = DBT.BusinessTypeKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS DLOB
					ON F.LineOfBusinessKey = DLOB.LineOfBusinessKey
					
					INNER JOIN DW_DDS_CURRENT.bi_dds.DimCompany AS DCOMPANY
					ON DPOL.CompanyKey = DCOMPANY.CompanyKey
					
					INNER JOIN #LARGELOSS
					ON DCLAIM.ClaimNumber = #LARGELOSS.CLAIM_NUMBER
					AND DLOB.LOBCode = #LARGELOSS.LOB


				WHERE 1=1
					AND AccountingDateKey <= @ENDDATEKEY
					AND DLOB.LOBProductLineCode IN ('CL')
					AND DASL.AnnualStatementLine IN ('080')
					--AND ClaimNumber = '45-004117'
					
				GROUP BY
					DCOMPANY.CompanyName
					,DLOB.LOBProductLineCode
					,DLOB.LOBCode
					,DASL.AnnualStatementLine
					,DGEO.GeographyCountryCode
					,DGEO.GeographyStateCode
					,DBT.BusinessTypeDesc
					,LOSSDATE.YearNumber
					,LOSSDATE.YearMonthNumber
					,RPTDATE.YearNumber
					,RPTDATE.YearMonthNumber
					,CASE WHEN (DATEDIFF(YEAR,LOSSDATE.FullDate,@ENDDATE)) > 9 THEN 'Prior To ' + CAST((@CURRENTCY - 9) AS VARCHAR(10)) ELSE CAST(LOSSDATE.YearNumber AS VARCHAR(10)) END
					,DPOL.AccountSegment
					,DCLAIM.ClaimNumber
					,CASE WHEN DCLAIM.ClaimEvent IS NULL OR DCLAIM.ClaimEvent IN ('?') THEN 0 ELSE 1 END
					,#LARGELOSS.EXCESS_LOSS
					,#LARGELOSS.CLAIM_STATUS

			) AS INNER_CLAIMS

		GROUP BY
			INNER_CLAIMS.CLAIM_NUMBER
			,INNER_CLAIMS.AY
			,INNER_CLAIMS.AY2
			,INNER_CLAIMS.AYM
			,INNER_CLAIMS.RY
			,INNER_CLAIMS.RYM
			,INNER_CLAIMS.COMPANY
			,INNER_CLAIMS.PROD_LINE
			,INNER_CLAIMS.ASL
			,INNER_CLAIMS.LOB
			,CASE
				WHEN INNER_CLAIMS.ASL IN ('051','052') THEN 'CMP'
				WHEN INNER_CLAIMS.ASL IN ('090') AND INNER_CLAIMS.PROD_LINE IN ('CL') THEN '9.0CL'
				WHEN INNER_CLAIMS.ASL IN ('090') AND INNER_CLAIMS.PROD_LINE IN ('PL') THEN '9.0PL'
				WHEN INNER_CLAIMS.ASL IN ('080') AND INNER_CLAIMS.PROD_LINE IN ('CL') THEN 'OM'
				ELSE 'UNKNOWN'
			END
			,INNER_CLAIMS.ACCT_SEG
			,INNER_CLAIMS.COUNTRY
			,INNER_CLAIMS.STATE
			,INNER_CLAIMS.BUS_TYPE
			,INNER_CLAIMS.CLAIM_EVENT
			,INNER_CLAIMS.EXCESS_LOSS
			,INNER_CLAIMS.CLAIM_STATUS
			
		SET @j += 1
		
		END
		
	SET @i += 1
	
	END
	
/********************************************************************************************************************************************************************************************************
Final statement rearranges columns to match triangles workbook layout. This is because Power Pivot expects columns to look a certain way.
This is cheating, but no harm done.
********************************************************************************************************************************************************************************************************/
SELECT *

FROM #HISTORY

ORDER BY EVAL_DATE