
SET NOCOUNT ON;

USE PolicyCenter;
"""Temp table  ---> dt_tran """

IF OBJECT_ID('tempdb..#dt_tran') IS NOT NULL
BEGIN
	DROP TABLE #dt_tran
END
;

SELECT
	pcAcc.AccountNumber
	,pcPolPer.PolicyNumber														AS PolicyNumber
	,'J_'+CAST(pcJob.JobNumber AS varchar(250))									AS JobNumber
	,pctlJob.NAME																AS TranType 
	,CAST(pcPolPer.PeriodStart AS date)											AS PeriodEffDate
	,CAST(pcPolPer.PeriodEnd AS date)											AS PeriodEndDate
	,CAST(pcPolPer.EditEffectiveDate AS date)									AS TransEffDate
	,CAST(pcJob.CloseDate AS date)												AS JobCloseDate
	,pcPolPer.TermNumber														AS TermNumber_PolicyGroup
	,pcPolPer.ModelNumber														AS PolicyVersion
	,pcPolPer.ModelNumber * 100													AS Version
	,CONCAT(pcPolPer.PolicyNumber, '_', pcPolPer.TermNumber)					AS PolicyNumber_Group
	,CASE
		WHEN CAST(pcPolPer.CancellationDate AS date) IS NOT NULL AND pctlJob.NAME = 'Policy Change'
		THEN 1 ELSE 0
		END																		AS Indicator_PolicyChangeEffectiveAfterCancel
	,CASE
		WHEN CAST(pcPolPer.CancellationDate AS date) IS NOT NULL AND pctlJob.NAME = 'Policy Change'
		THEN CAST(pcPolPer.CancellationDate AS date) 
		ELSE CAST(pcPolPer.EditEffectiveDate AS date)
		END																		AS TransEffDate_C

INTO #dt_tran

FROM PolicyCenter.dbo.pc_policy AS pcPol
	
	INNER JOIN PolicyCenter.dbo.pc_account AS pcAcc
		ON pcAcc.ID = pcPol.AccountID
	
	INNER JOIN PolicyCenter.dbo.pc_policyterm AS pcPolTer
		ON pcPolTer.PolicyID = pcPol.ID
	
	INNER JOIN PolicyCenter.dbo.pc_policyperiod AS pcPolPer
		ON pcPolPer.PolicyTermID = pcPolTer.ID
	
	INNER JOIN DW_STAGE.bi_stage.tempPolicyPeriodEffectiveDate AS tempEffDate
		ON tempEffDate.PolicyPeriodID = pcPolPer.ID
	
	INNER JOIN PolicyCenter.dbo.pc_policyline AS pcPolLine
		ON pcPolLine.BranchID = pcPolPer.ID 
		AND (pcPolLine.EffectiveDate <= tempEffDate.SubEffectiveDate OR pcPolLine.EffectiveDate IS NULL)
		AND (pcPolLine.ExpirationDate > tempEffDate.SubEffectiveDate OR pcPolLine.ExpirationDate IS NULL)
	
	INNER JOIN PolicyCenter.dbo.pctl_policyline AS pctlPolLine
		ON pctlPolLine.ID = pcPolLine.SubType
	
	INNER JOIN PolicyCenter.dbo.pc_job AS pcJob
		ON pcJob.ID = pcPolPer.JobID 
	
	INNER JOIN PolicyCenter.dbo.pctl_job AS pctlJob
		ON pctlJob.ID = pcJob.Subtype
	
	INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctlPolPerSta
		ON pctlPolPerSta.ID = pcPolPer.Status 

WHERE
		pctlPolPerSta.NAME = 'Bound'
	AND pcPolPer.ModelNumber IS NOT NULL

GROUP BY
	pcAcc.AccountNumber
	,pcPolPer.PolicyNumber
	,pcJob.JobNumber
	,pctlJob.NAME
	,pcPolPer.PeriodStart
	,pcPolPer.PeriodEnd
	,pcPolPer.EditEffectiveDate
	,pcJob.CloseDate
	,pcPolPer.TermNumber
	,pcPolPer.ModelNumber
	,pcPolPer.CancellationDate
	,pcPolPer.EditEffectiveDate
;

""" Temp table dt_tran_02 """

IF OBJECT_ID('tempdb..#dt_tran_02') IS NOT NULL
BEGIN
	DROP TABLE #dt_tran_02
END
;

WITH dt_c AS (
	SELECT
		PolicyNumber_Group
		,Version																AS CancelVersion
		,TransEffDate															AS CancelEffDate
	
	FROM #dt_tran
	
	WHERE TranType = 'Cancellation'
),

dt_pc_c AS (
	SELECT
		*
		,TransEffDate_C															AS CancelEffDate
	
	FROM #dt_tran
	
	WHERE Indicator_PolicyChangeEffectiveAfterCancel = 1
),

dt_pc_c_01 AS (
	SELECT
		JobNumber
		,CancelVersion
		,CancelEffDate
	FROM (
		SELECT
			dt_pc_c.JobNumber
			,dt_c.CancelVersion
			,dt_pc_c.CancelEffDate
			,DENSE_RANK() OVER (PARTITION BY	dt_pc_c.JobNumber
								ORDER BY		dt_c.CancelVersion DESC)			AS ranking

		FROM dt_pc_c

		INNER JOIN dt_c
			ON	dt_pc_c.PolicyNumber_Group	= dt_c.PolicyNumber_Group
			AND	dt_pc_c.CancelEffDate		= dt_c.CancelEffDate
	) subquery
	WHERE ranking = 1
),

dt_tran_01 AS (
	SELECT
		#dt_tran.AccountNumber
		,#dt_tran.PolicyNumber
		,#dt_tran.JobNumber
		,#dt_tran.TranType
		,#dt_tran.PeriodEffDate
		,#dt_tran.PeriodEndDate
		,#dt_tran.TransEffDate
		,#dt_tran.JobCloseDate
		,#dt_tran.TermNumber_PolicyGroup
		,#dt_tran.PolicyVersion
		,#dt_tran.PolicyNumber_Group
		,CASE
			WHEN dt_pc_c_01.CancelVersion IS NULL
			THEN #dt_tran.Version
			ELSE dt_pc_c_01.CancelVersion - 1
		END																		AS Version
		,CASE
			WHEN dt_pc_c_01.CancelVersion IS NULL
			THEN #dt_tran.TransEffDate
			WHEN #dt_tran.TransEffDate > dt_pc_c_01.CancelEffDate
			THEN dt_pc_c_01.CancelEffDate
			ELSE #dt_tran.TransEffDate
		END																		AS BegDate
	
	FROM #dt_tran
	
	LEFT JOIN dt_pc_c_01
		ON #dt_tran.JobNumber	= dt_pc_c_01.JobNumber
),

dt_tran_01a AS (
	SELECT
		*
		,ROW_NUMBER() OVER (PARTITION BY	dt_tran_01.PolicyNumber_Group
							ORDER BY		dt_tran_01.Version
											,dt_tran_01.BegDate
											,dt_tran_01.PolicyVersion)				AS RowKey
	FROM dt_tran_01
)

SELECT
	AccountNumber
	,PolicyNumber
	,JobNumber
	,TranType
	,PeriodEffDate
	,PeriodEndDate
	,TransEffDate
	,JobCloseDate
	,TermNumber_PolicyGroup
	,PolicyVersion
	,Version
	,PolicyNumber_Group
	,BegDate
	,one.RowKey
	,two.Version1
	,COALESCE(two.BegDate1, '1900-01-01') AS BegDate1
	,COALESCE(two.TranType1, 'n_a') AS TranType1
	,three.Version2
	,COALESCE(three.BegDate2, '1900-01-01') AS BegDate2
	,COALESCE(three.TranType2, 'n_a') AS TranType2
	,four.Version3
	,COALESCE(four.BegDate3, '1900-01-01') AS BegDate3
	,COALESCE(four.TranType3, 'n_a') AS TranType3

INTO #dt_tran_02

FROM dt_tran_01a AS one
LEFT JOIN (
	SELECT
		Version AS Version1
		,TranType AS TranType1
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate1
		,RowKey
	FROM dt_tran_01a) AS two
	ON	one.PolicyNumber_Group = two.Policy_Group
	AND one.RowKey = two.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version2
		,TranType AS TranType2
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate2
		,RowKey
	FROM dt_tran_01a) AS three
	ON	two.Policy_Group = three.Policy_Group
	AND two.RowKey = three.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version3
		,TranType AS TranType3
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate3
		,RowKey
	FROM dt_tran_01a) AS four
	ON	three.Policy_Group = four.Policy_Group
	AND three.RowKey = four.RowKey - 1
;

""" Temp table dt_cancel_crc """
IF OBJECT_ID('tempdb..#dt_cancel_crc') IS NOT NULL
BEGIN
	DROP TABLE #dt_cancel_crc
END
;

-- Not used in current semi-managed Python script
WITH dt_multi_4cancel AS (
	SELECT *
	FROM #dt_tran_02
	WHERE	TranType = 'Cancellation'
		AND TranType1 = 'Cancellation'
		AND	TranType2 = 'Cancellation'
		AND TranType3 = 'Cancellation'
),

dt_3cancel_01 AS (
	SELECT
		*
		,CASE
			WHEN BegDate <= BegDate1 AND BegDate <= BegDate2
			THEN BegDate
			WHEN BegDate1 <= BegDate AND BegDate1 <= BegDate2
			THEN BegDate1
			ELSE BegDate2
		END																			AS BNN
		,CASE
			WHEN prevDate1 <= BegDate AND prevDate1 <= BegDate1
			THEN prevDate1
			WHEN BegDate <= prevDate1 AND BegDate <= BegDate1
			THEN BegDate
			ELSE BegDate1
		END																			AS PBN
		,CASE
			WHEN prevDate1 <= prevDate2 AND prevDate1 <= BegDate
			THEN prevDate1
			WHEN prevDate2 <= prevDate1 AND prevDate2 <= BegDate
			THEN prevDate2
			ELSE BegDate
		END																			AS PPB

	FROM (
		SELECT
			#dt_tran_02.*
			,LAG(TranType, 1) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)						AS prevTran1
			,LAG(BegDate, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)						AS prevDate1
			,LAG(TranType, 2) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)						AS prevTran2
			,LAG(BegDate, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)						AS prevDate2

		FROM #dt_tran_02
		INNER JOIN (
			SELECT DISTINCT PolicyNumber_Group
			FROM #dt_tran_02
			WHERE	TranType = 'Cancellation'
				AND TranType1 = 'Cancellation'
				AND	TranType2 = 'Cancellation'
		) AS where_query
		ON #dt_tran_02.PolicyNumber_Group = where_query.PolicyNumber_Group
	) AS dt_multi_3cancel
),

dt_3cancel_f AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate_C
	FROM (
		SELECT
			*
			,CASE
				WHEN TranType = 'Cancellation' AND TranType1 = 'Cancellation' AND TranType2 = 'Cancellation'
				THEN BNN
				WHEN prevTran1 = 'Cancellation' AND TranType = 'Cancellation' AND TranType1 = 'Cancellation'
				THEN PBN
				WHEN prevTran2 = 'Cancellation' AND prevTran1 = 'Cancellation' AND TranType = 'Cancellation'
				THEN PPB
				ELSE NULL
			END																	AS BegDate_C

		FROM dt_3cancel_01
	) AS subquery
	WHERE BegDate_C IS NOT NULL
),

dt_2cancel_01 AS (
	SELECT
		*
		,CASE
			WHEN BegDate <= BegDate1
			THEN BegDate
			ELSE BegDate1
		END																		AS BN
		,CASE
			WHEN prevDate1 <= BegDate
			THEN prevDate1
			ELSE BegDate
		END																		AS PB
		,CASE
			WHEN TranType = 'Cancellation' AND TranType1 = 'Cancellation' AND TranType2 = 'Cancellation'
			THEN 1
			WHEN prevTran1 = 'Cancellation' AND TranType = 'Cancellation' AND TranType1 = 'Cancellation'
			THEN 1
			WHEN prevTran2 = 'Cancellation' AND prevTran1 = 'Cancellation' AND TranType = 'Cancellation'
			THEN 1
			ELSE 0
		END																		AS flag_3cancel

	FROM (
		SELECT
			#dt_tran_02.*
			,LAG(TranType, 1) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)					AS prevTran1
			,LAG(BegDate, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)					AS prevDate1
			,LAG(TranType, 2) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)					AS prevTran2
			,LAG(BegDate, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
									ORDER BY		Version
													,BegDate)					AS prevDate2

		FROM #dt_tran_02
		INNER JOIN (
			SELECT DISTINCT PolicyNumber_Group
			FROM #dt_tran_02
			WHERE	TranType = 'Cancellation'
				AND TranType1 = 'Cancellation'
		) AS where_query
		ON #dt_tran_02.PolicyNumber_Group = where_query.PolicyNumber_Group
	) AS dt_multi_3cancel
),

dt_2cancel_f AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate_C
	FROM (
		SELECT
			*
			,CASE
				WHEN TranType = 'Cancellation' AND TranType1 = 'Cancellation' AND flag_3cancel = 0
				THEN BN
				WHEN prevTran1 = 'Cancellation' AND TranType = 'Cancellation' AND flag_3cancel = 0
				THEN PB
				ELSE NULL
			END																	AS BegDate_C

		FROM dt_2cancel_01
	) AS subquery
	WHERE BegDate_C IS NOT NULL
),

dt_crc AS (
	SELECT
		#dt_tran_02.*
		,LAG(TranType, 1) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevTran1
		,LAG(BegDate, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevDate1
		,LAG(Version, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevVers1
		,LAG(TranType, 2) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevTran2
		,LAG(BegDate, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevDate2
		,LAG(Version, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevVers2
	FROM #dt_tran_02
	INNER JOIN (
		SELECT DISTINCT PolicyNumber_Group
		FROM #dt_tran_02
		WHERE	TranType = 'Cancellation'
			AND	TranType1 = 'Reinstatement'
			AND	TranType2 = 'Cancellation'
			AND BegDate2 < BegDate
	) AS where_query
	ON #dt_tran_02.PolicyNumber_Group = where_query.PolicyNumber_Group
),

dt_crc_01 AS (
	SELECT *
	FROM (
		SELECT
			*
			,CASE
				WHEN TranType = 'Cancellation' AND TranType1 = 'Reinstatement' AND TranType2 = 'Cancellation' AND BegDate2 < BegDate1
				THEN BegDate2
				WHEN prevTran1 = 'Cancellation' AND TranType = 'Reinstatement' AND TranType1 = 'Cancellation' AND BegDate1 < BegDate
				THEN BegDate1
				WHEN prevTran2 = 'Cancellation' AND prevTran1 = 'Reinstatement' AND TranType = 'Cancellation' AND BegDate < prevDate1
				THEN BegDate
				ELSE NULL
			END																		AS BegDate_C
		FROM dt_crc
	) AS subquery
	WHERE BegDate_C IS NOT NULL
),

dt_crc_f AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate_C
	FROM dt_crc_01
),

dt_crpcc AS (
	SELECT
		#dt_tran_02.*
		,LAG(TranType, 1) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevTran1
		,LAG(BegDate, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevDate1
		,LAG(Version, 1) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevVers1
		,LAG(TranType, 2) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevTran2
		,LAG(BegDate, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevDate2
		,LAG(Version, 2) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevVers2
		,LAG(TranType, 3) OVER (PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevTran3
		,LAG(BegDate, 3) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevDate3
		,LAG(Version, 3) OVER (	PARTITION BY	#dt_tran_02.PolicyNumber_Group
								ORDER BY		Version
												,BegDate)						AS prevVers3
	FROM #dt_tran_02
	INNER JOIN (
		SELECT DISTINCT PolicyNumber_Group
		FROM #dt_tran_02
		WHERE	TranType = 'Cancellation'
			AND	TranType1 = 'Reinstatement'
			AND TranType2 = 'Policy Change'
			AND TranType3 = 'Cancellation'
			AND BegDate3 < BegDate
			AND BegDate3 < BegDate1
	) AS where_query
	ON #dt_tran_02.PolicyNumber_Group = where_query.PolicyNumber_Group
),

dt_crpcc_01 AS (
	SELECT
		*
		,CASE
			WHEN	TranType = 'Cancellation' AND TranType1 = 'Reinstatement' AND TranType2 = 'Policy Change' AND TranType3 = 'Cancellation'
				AND BegDate3 < BegDate AND BegDate3 < BegDate1
			THEN	BegDate3
			WHEN	prevTran1 = 'Cancellation' AND TranType = 'Reinstatement' AND TranType1 = 'Policy Change' AND TranType2 = 'Cancellation'
				AND BegDate2 < prevDate1 AND BegDate2 < BegDate
			THEN	BegDate2
			WHEN	prevTran2 = 'Cancellation' AND prevTran1 = 'Reinstatement' AND TranType = 'Policy Change' AND TranType1 = 'Cancellation'
				AND BegDate1 < prevDate2 AND BegDate1 < prevDate1
			THEN	BegDate1
			WHEN	prevTran3 = 'Cancellation' AND prevTran2 = 'Reinstatement' AND prevTran1 = 'Policy Change' AND TranType = 'Cancellation'
				AND BegDate < prevDate3 AND BegDate < prevDate2
			THEN	BegDate
			ELSE NULL
		END																		AS BegDate_C
	FROM dt_crpcc
),

dt_crpcc_f AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate_C
	FROM dt_crpcc_01
	WHERE BegDate_C IS NOT NULL
)

SELECT *
INTO #dt_cancel_crc 
FROM (
	SELECT * FROM dt_3cancel_f

	UNION ALL

	SELECT * FROM dt_2cancel_f

	UNION ALL

	SELECT * FROM dt_crc_f

	UNION ALL

	SELECT * FROM dt_crpcc_f
) AS unioned_cancellations
;

""" Temp table dt_tran_03 """
IF OBJECT_ID('tempdb..#dt_tran_03a') IS NOT NULL
BEGIN
	DROP TABLE #dt_tran_03a
END
;

WITH dt_tran_02a AS (
	SELECT
		#dt_tran_02.AccountNumber
		,#dt_tran_02.PolicyNumber
		,#dt_tran_02.JobNumber
		,#dt_tran_02.TranType
		,#dt_tran_02.PeriodEffDate
		,#dt_tran_02.PeriodEndDate
		,#dt_tran_02.TransEffDate
		,#dt_tran_02.JobCloseDate
		,#dt_tran_02.TermNumber_PolicyGroup
		,#dt_tran_02.PolicyVersion
		,#dt_tran_02.PolicyNumber_Group
		,CASE
			WHEN #dt_cancel_crc.BegDate_C IS NULL
			THEN #dt_tran_02.BegDate
			ELSE #dt_cancel_crc.BegDate_C
		END																		AS BegDate
		,#dt_tran_02.Version
	FROM #dt_tran_02
	LEFT JOIN #dt_cancel_crc
		ON	#dt_tran_02.PolicyNumber_Group = #dt_cancel_crc.PolicyNumber_Group
		AND	#dt_tran_02.Version = #dt_cancel_crc.Version
),

dt_tran_03 AS (
	SELECT
		*
		,ROW_NUMBER() OVER (PARTITION BY	dt_tran_02a.PolicyNumber_Group
							ORDER BY		dt_tran_02a.Version
											,dt_tran_02a.BegDate)				AS RowKey
	FROM dt_tran_02a
)

SELECT
	AccountNumber
	,PolicyNumber
	,JobNumber
	,TranType
	,PeriodEffDate
	,PeriodEndDate
	,TransEffDate
	,JobCloseDate
	,TermNumber_PolicyGroup
	,PolicyVersion
	,Version
	,PolicyNumber_Group
	,BegDate
	,one.RowKey
	,two.Version1
	,COALESCE(two.BegDate1, '1900-01-01') AS BegDate1
	,COALESCE(two.TranType1, 'n_a') AS TranType1
	,three.Version2
	,COALESCE(three.BegDate2, '1900-01-01') AS BegDate2
	,COALESCE(three.TranType2, 'n_a') AS TranType2
	,four.Version3
	,COALESCE(four.BegDate3, '1900-01-01') AS BegDate3
	,COALESCE(four.TranType3, 'n_a') AS TranType3
	,five.Version4
	,COALESCE(five.BegDate4, '1900-01-01') AS BegDate4
	,COALESCE(five.TranType4, 'n_a') AS TranType4
	,six.Version5
	,COALESCE(six.BegDate5, '1900-01-01') AS BegDate5
	,COALESCE(six.TranType5, 'n_a') AS TranType5
	,seven.Version6
	,COALESCE(seven.BegDate6, '1900-01-01') AS BegDate6
	,COALESCE(seven.TranType6, 'n_a') AS TranType6
	,eight.Version7
	,COALESCE(eight.BegDate7, '1900-01-01') AS BegDate7
	,COALESCE(eight.TranType7, 'n_a') AS TranType7
	,nine.Version8
	,COALESCE(nine.BegDate8, '1900-01-01') AS BegDate8
	,COALESCE(nine.TranType8, 'n_a') AS TranType8
	,ten.Version9
	,COALESCE(ten.BegDate9, '1900-01-01') AS BegDate9
	,COALESCE(ten.TranType9, 'n_a') AS TranType9
	,eleven.Version10
	,COALESCE(eleven.BegDate10, '1900-01-01') AS BegDate10
	,COALESCE(eleven.TranType10, 'n_a') AS TranType10

INTO #dt_tran_03a

FROM dt_tran_03 AS one
LEFT JOIN (
	SELECT
		Version AS Version1
		,TranType AS TranType1
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate1
		,RowKey
	FROM dt_tran_03) AS two
	ON	one.PolicyNumber_Group = two.Policy_Group
	AND one.RowKey = two.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version2
		,TranType AS TranType2
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate2
		,RowKey
	FROM dt_tran_03) AS three
	ON	two.Policy_Group = three.Policy_Group
	AND two.RowKey = three.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version3
		,TranType AS TranType3
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate3
		,RowKey
	FROM dt_tran_03) AS four
	ON	three.Policy_Group = four.Policy_Group
	AND three.RowKey = four.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version4
		,TranType AS TranType4
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate4
		,RowKey
	FROM dt_tran_03) AS five
	ON	four.Policy_Group = five.Policy_Group
	AND four.RowKey = five.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version5
		,TranType AS TranType5
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate5
		,RowKey
	FROM dt_tran_03) AS six
	ON	five.Policy_Group = six.Policy_Group
	AND five.RowKey = six.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version6
		,TranType AS TranType6
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate6
		,RowKey
	FROM dt_tran_03) AS seven
	ON	six.Policy_Group = seven.Policy_Group
	AND six.RowKey = seven.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version7
		,TranType AS TranType7
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate7
		,RowKey
	FROM dt_tran_03) AS eight
	ON	seven.Policy_Group = eight.Policy_Group
	AND seven.RowKey = eight.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version8
		,TranType AS TranType8
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate8
		,RowKey
	FROM dt_tran_03) AS nine
	ON	eight.Policy_Group = nine.Policy_Group
	AND eight.RowKey = nine.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version9
		,TranType AS TranType9
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate9
		,RowKey
	FROM dt_tran_03) AS ten
	ON	nine.Policy_Group = ten.Policy_Group
	AND nine.RowKey = ten.RowKey - 1
LEFT JOIN (
	SELECT
		Version AS Version10
		,TranType AS TranType10
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate10
		,RowKey
	FROM dt_tran_03) AS eleven
	ON	ten.Policy_Group = eleven.Policy_Group
	AND ten.RowKey = eleven.RowKey - 1
;

IF OBJECT_ID('tempdb..#dt_pcc_f') IS NOT NULL
BEGIN
	DROP TABLE #dt_pcc_f
END
;

WITH dt_pcc_01 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate1 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Cancellation'
		AND BegDate1 < BegDate
),

dt_pcc_02 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate2 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Cancellation'
		AND BegDate2 < BegDate
),

dt_pcc_03 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate3 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Cancellation'
		AND BegDate3 < BegDate
),

dt_pcc_04 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate4 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Cancellation'
		AND BegDate4 < BegDate
),

dt_pcc_05 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate5 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Policy Change'
		AND TranType5 = 'Cancellation'
		AND BegDate5 < BegDate
),

dt_pcc_06 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate6 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Policy Change'
		AND TranType5 = 'Policy Change'
		AND TranType6 = 'Cancellation'
		AND BegDate6 < BegDate
),

dt_pcc_07 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate7 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Policy Change'
		AND TranType5 = 'Policy Change'
		AND TranType6 = 'Policy Change'
		AND TranType7 = 'Cancellation'
		AND BegDate7 < BegDate
),

dt_pcc_08 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate8 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Policy Change'
		AND TranType5 = 'Policy Change'
		AND TranType6 = 'Policy Change'
		AND TranType7 = 'Policy Change'
		AND TranType8 = 'Cancellation'
		AND BegDate8 < BegDate
),

dt_pcc_09 AS (
	SELECT
		PolicyNumber_Group
		,Version
		,BegDate9 AS BegDate_C
	FROM #dt_tran_03a
	WHERE	TranType = 'Policy Change'
		AND TranType1 = 'Policy Change'
		AND TranType2 = 'Policy Change'
		AND TranType3 = 'Policy Change'
		AND TranType4 = 'Policy Change'
		AND TranType5 = 'Policy Change'
		AND TranType6 = 'Policy Change'
		AND TranType7 = 'Policy Change'
		AND TranType8 = 'Policy Change'
		AND TranType9 = 'Cancellation'
		AND BegDate9 < BegDate
)

SELECT *
INTO #dt_pcc_f
FROM (
	SELECT * FROM dt_pcc_01
	UNION ALL
	SELECT * FROM dt_pcc_02
	UNION ALL
	SELECT * FROM dt_pcc_03
	UNION ALL
	SELECT * FROM dt_pcc_04
	UNION ALL
	SELECT * FROM dt_pcc_05
	UNION ALL
	SELECT * FROM dt_pcc_06
	UNION ALL
	SELECT * FROM dt_pcc_07
	UNION ALL
	SELECT * FROM dt_pcc_08
	UNION ALL
	SELECT * FROM dt_pcc_09
) AS unioned_policy_changes
;

IF OBJECT_ID('tempdb..#dt_tran_04a') IS NOT NULL
BEGIN
	DROP TABLE #dt_tran_04a
END
;

WITH dt_tran_03b AS (
	SELECT
		#dt_tran_03a.AccountNumber
		,#dt_tran_03a.PolicyNumber
		,#dt_tran_03a.JobNumber
		,#dt_tran_03a.TranType
		,#dt_tran_03a.PeriodEffDate
		,#dt_tran_03a.PeriodEndDate
		,#dt_tran_03a.TransEffDate
		,#dt_tran_03a.JobCloseDate
		,#dt_tran_03a.TermNumber_PolicyGroup
		,#dt_tran_03a.PolicyVersion
		,#dt_tran_03a.PolicyNumber_Group
		,CASE
			WHEN #dt_pcc_f.BegDate_C IS NULL
			THEN #dt_tran_03a.BegDate
			ELSE #dt_pcc_f.BegDate_C
		END																		AS BegDate
		,#dt_tran_03a.Version
	FROM #dt_tran_03a
	LEFT JOIN #dt_pcc_f
		ON	#dt_tran_03a.PolicyNumber_Group = #dt_pcc_f.PolicyNumber_Group
		AND #dt_tran_03a.Version = #dt_pcc_f.Version
),

dt_tran_04 AS (
	SELECT
		*
		,ROW_NUMBER() OVER (PARTITION BY	dt_tran_03b.PolicyNumber_Group
							ORDER BY		dt_tran_03b.BegDate
											,dt_tran_03b.Version
											,dt_tran_03b.JobNumber)				AS RowKey
	FROM dt_tran_03b
)

SELECT
	AccountNumber
	,PolicyNumber
	,JobNumber
	,TranType
	,PeriodEffDate
	,PeriodEndDate
	,TransEffDate
	,JobCloseDate
	,TermNumber_PolicyGroup
	,PolicyVersion
	,Version
	,PolicyNumber_Group
	,BegDate
	,one.RowKey
	,two.Version1
	,two.BegDate1 AS BegDate1
	,COALESCE(two.TranType1, 'n_a') AS TranType1

INTO #dt_tran_04a

FROM dt_tran_04 AS one
LEFT JOIN (
	SELECT
		Version AS Version1
		,TranType AS TranType1
		,PolicyNumber_Group AS Policy_Group
		,BegDate AS BegDate1
		,RowKey
	FROM dt_tran_04) AS two
	ON	one.PolicyNumber_Group = two.Policy_Group
	AND one.RowKey = two.RowKey - 1
;

-- FINAL OUTPUT
SELECT
	AccountNumber
	,PolicyNumber
	,JobNumber
	,TranType
	,PeriodEffDate
	,PeriodEndDate
	,TransEffDate
	,JobCloseDate
	,TermNumber_PolicyGroup
	,PolicyVersion
	,PolicyNumber_Group
	,BegDate
	,Version
	,CASE
		WHEN TranType = 'Cancellation'
		THEN BegDate
		ELSE
			CASE
				WHEN BegDate1 IS NULL
				THEN PeriodEndDate
				ELSE BegDate1
			END
	END																			AS EndDate

FROM #dt_tran_04a

/*

-- FINAL OUTPUT
SELECT
	PolicyNumber
	,JobNumber
	,TranType
	,PeriodEffDate
	,PeriodEndDate
	,TransEffDate
	,TermNumber_PolicyGroup
	,PolicyVersion
	,PolicyNumber_Group
	,BegDate
	,Version
	,CASE
		WHEN TranType = 'Cancellation'
		THEN BegDate
		ELSE
			CASE
				WHEN BegDate1 IS NULL
				THEN PeriodEndDate
				ELSE BegDate1
			END
	END																			AS EndDate

FROM #dt_tran_04a

WHERE PolicyNumber_Group = '24-051191_14'

ORDER BY
	BegDate
	,Version
	,PolicyVersion
	
*/