/**********************************************************************************************
	Kimberlite - Building Blocks
		TransactionConfiguration - Configuration for Bound Transactions
			Converted to BigQuery
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/15/2021	SLJ			Init 
	09/16/2021	DROBAK		Add SourceSystem field
	12/08/2021	DROBAK		Add field PolicyTransaction.OfferingCode
	07/19/2022	DROBAK		Correction: add CalendarYearMultiplier to table, codde
	-------------------------------------------------------------------------------------------
*/

DELETE `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.TransactionConfiguration` 
	(  
		SourceSystem
		,PolicyTransactionKey
		,AccountNumber
		,Segment
		,BusinessUnit
		,OfferingCode
		,PolicyNumber
		,LegacyPolicyNumber
		,PeriodEffDate
		,PeriodEndDate
		,JobNumber
		,TranType
		,TermNumber
		,ModelNumber
		,TransEffDate
		,JobCloseDate
		,CancelEffDate
		,WrittenDate
		,AccountingDate
		,CalendarYearBeginDate
		,CalendarYearEndDate
		,PolicyYearBeginDate
		,PolicyYearEndDate
		,CalendarYearMultiplier
		,CurrentPolicyStatus	
		,bq_load_date
	)

with dt_tran as
(
    SELECT 
        PolicyTransactionKey
        ,AccountNumber
        ,Segment
        ,CASE WHEN Segment = 'Personal Lines' THEN 'PL' ELSE 'CL' END AS BusinessUnit
		,OfferingCode
        ,PolicyNumber
        ,LegacyPolicyNumber
        ,CAST(PeriodEffDate AS DATE) AS PeriodEffDate
        ,CAST(PeriodEndDate AS DATE) AS PeriodEndDate
        ,JobNumber
        ,TranType
        ,TermNumber
        ,ModelNumber
        ,CAST(TransEffDate AS DATE) AS TransEffDate
        ,CAST(JobCloseDate AS DATE) AS JobCloseDate
        ,CAST(CancelEffectiveDate AS DATE) AS CancelEffDate
        ,CAST(CASE WHEN JobCloseDate < TransEffDate THEN  TransEffDate ELSE JobCloseDate END AS DATE) AS CloseBeg
        ,WrittenDate
        ,CAST(CASE WHEN IFNULL(TransEffDate,'1990-01-01') >= IFNULL(JobCloseDate, '1990-01-01') THEN TransEffDate ELSE JobCloseDate END AS TIMESTAMP) AS AccountingDate

    FROM `{project}.{core_dataset}.PolicyTransaction` PolicyTransaction

    WHERE   1 = 1
            AND TransactionStatus = 'Bound' 
            AND bq_load_date = DATE({partition_date})
)
,dt_tran1 as (
    SELECT 
        PolicyTransactionKey
        ,AccountNumber
        ,Segment
        ,BusinessUnit
		,OfferingCode
        ,PolicyNumber
        ,LegacyPolicyNumber
        ,PeriodEffDate
        ,PeriodEndDate
        ,JobNumber
        ,TranType
        ,TermNumber
        ,ModelNumber
        ,TransEffDate
        ,JobCloseDate
        ,CancelEffDate
        ,CloseBeg
        ,WrittenDate
        ,AccountingDate
        ,LAG(TranType,1)      OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS prevTranType
        ,LAG(TransEffDate,1)  OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS prevTransEff
        ,LAG(CloseBeg,1)      OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS prevCloseBeg
        ,LAG(CancelEffDate,1) OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS prevCancelEff
        ,LEAD(TranType,1)     OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS nextTranType
        ,LEAD(CloseBeg,1)     OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS nextCloseBeg
        ,LEAD(TransEffDate,1) OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS nextTransEff
    FROM dt_tran 
)
,dt_tran2 as (
    SELECT 
        PolicyTransactionKey
        ,AccountNumber
        ,Segment
        ,BusinessUnit
		,OfferingCode
        ,PolicyNumber
        ,LegacyPolicyNumber
        ,PeriodEffDate
        ,PeriodEndDate
        ,JobNumber
        ,TranType
        ,TermNumber
        ,ModelNumber
        ,TransEffDate
        ,JobCloseDate
        ,CancelEffDate
        ,CloseBeg
        ,WrittenDate
        ,AccountingDate
        ,prevTranType
        ,prevTransEff
        ,prevCloseBeg
        ,prevCancelEff
        ,nextTranType
        ,nextCloseBeg
        ,nextTransEff
        ,CASE   WHEN prevTranType IS NULL OR TranType = 'Reinstatement' OR TranType = 'Cancellation' THEN TransEffDate 
                ELSE CloseBeg END AS ExpBegCY
        ,CASE   WHEN TranType = 'Cancellation' THEN IF(TransEffDate > JobCloseDate,TransEffDate,JobCloseDate)
                ELSE COALESCE(nextCloseBeg,PeriodEndDate) END AS ExpEndCY
    FROM dt_tran1 
)
,dt_tran3 as (
    SELECT 
        PolicyTransactionKey
        ,AccountNumber
        ,Segment
        ,BusinessUnit
		,OfferingCode
        ,PolicyNumber
        ,LegacyPolicyNumber
        ,PeriodEffDate
        ,PeriodEndDate
        ,JobNumber
        ,TranType
        ,TermNumber
        ,ModelNumber
        ,TransEffDate
        ,JobCloseDate
        ,CancelEffDate
        ,CloseBeg
        ,WrittenDate
        ,AccountingDate
        ,prevTranType
        ,prevTransEff
        ,prevCloseBeg
        ,nextTranType
        ,nextCloseBeg
        ,nextTransEff
        ,ExpBegCY
        ,CASE   WHEN prevTranType = 'Cancellation' AND TranType = 'Cancellation' 
                    THEN IF(TransEffDate > prevTransEff,ExpBegCY,prevTransEff)
                WHEN prevTranType = 'Policy Change' AND TranType = 'Cancellation' AND prevCancelEff IS NOT NULL #24-330663
                    THEN IF(TransEffDate > prevCancelEff,ExpBegCY,prevCancelEff)
                WHEN TranType = 'Policy Change' AND JobCloseDate > PeriodEndDate THEN ExpBegCY
                WHEN TranType = 'Policy Change' AND CancelEffDate IS NOT NULL THEN ExpBegCY
                WHEN TranType = 'Policy Change' AND nextCloseBeg < ExpBegCY THEN ExpBegCY
                ELSE ExpEndCY END AS ExpEndCY
    FROM dt_tran2 
)
,dt_oos AS 
(
    SELECT 
        PolicyNumber
        ,TermNumber
        ,ModelNumber
        ,TransEffDate 
        ,CloseBeg
    FROM dt_tran3 
    WHERE   1 = 1
            AND TranType IN ('Cancellation', 'Policy Change') AND prevTranType = 'Policy Change'
            AND TransEffDate < prevTransEff AND JobCloseDate < prevTransEff
)
,dt_oos1 AS 
(
    SELECT 
        cn1.PolicyNumber
        ,cn1.TermNumber
        ,cn1.ModelNumber 
        ,cn1.CloseBeg
        ,min(cn2.CloseBeg) AS nextCloseBegMin
    FROM dt_oos AS cn1
        LEFT JOIN dt_oos AS cn2
        ON cn2.PolicyNumber = cn1.PolicyNumber
        AND cn2.TermNumber = cn1.TermNumber
        AND cn2.ModelNumber > cn1.ModelNumber
    GROUP BY 
        cn1.PolicyNumber
        ,cn1.TermNumber
        ,cn1.ModelNumber 
        ,cn1.CloseBeg
) 
,dt_oos2 AS 
(
    SELECT 
        PolicyNumber
        ,TermNumber
        ,COALESCE(oosPrevModelNum,1) AS oosMNBeg
        ,ModelNumber - 1 AS oosMNEnd
        ,oosCloseBeg
    FROM 
    (
        SELECT 
            PolicyNumber
            ,TermNumber
            ,ModelNumber 
            ,CloseBeg AS oosCloseBeg
            ,LAG(ModelNumber,1)   OVER (PARTITION BY	PolicyNumber
                                                        ,TermNumber
                                        ORDER BY		ModelNumber)						AS oosPrevModelNum
        FROM dt_oos1 
        WHERE CloseBeg < nextCloseBegMin OR nextCloseBegMin IS NULL
    )
)
,dt_tran4 AS 
(
    SELECT 
        dt_tran3.PolicyTransactionKey
        ,dt_tran3.AccountNumber
        ,dt_tran3.Segment
        ,dt_tran3.BusinessUnit
		,dt_tran3.OfferingCode
        ,dt_tran3.PolicyNumber
        ,dt_tran3.LegacyPolicyNumber
        ,dt_tran3.PeriodEffDate
        ,dt_tran3.PeriodEndDate
        ,dt_tran3.JobNumber
        ,dt_tran3.TranType
        ,dt_tran3.TermNumber
        ,dt_tran3.ModelNumber
        ,dt_tran3.TransEffDate
        ,dt_tran3.JobCloseDate
        ,dt_tran3.CancelEffDate
        ,dt_tran3.CloseBeg
        ,dt_tran3.WrittenDate
        ,dt_tran3.AccountingDate
        ,dt_tran3.prevTranType
        ,dt_tran3.prevTransEff
        ,dt_tran3.prevCloseBeg
        ,dt_tran3.nextTranType
        ,dt_tran3.nextCloseBeg
        ,dt_tran3.nextTransEff
        ,dt_tran3.ExpBegCY
        ,CASE   WHEN dt_tran3.ExpEndCY > dt_oos2.oosCloseBeg AND dt_tran3.ExpEndCY != dt_tran3.ExpBegCY THEN 
                    IF(dt_tran3.ExpBegCY < dt_oos2.oosCloseBeg,dt_oos2.oosCloseBeg,dt_tran3.ExpBegCY)
                ELSE dt_tran3.ExpEndCY END AS ExpEndCY

    FROM dt_tran3 

    LEFT JOIN dt_oos2
    ON dt_oos2.PolicyNumber = dt_tran3.PolicyNumber
    AND dt_oos2.TermNumber = dt_tran3.TermNumber
    AND dt_tran3.ModelNumber BETWEEN dt_oos2.oosMNBeg  AND dt_oos2.oosMNEnd
    # AND dt_oos1.MNEnd <> dt_oos1.MNBeg

)
,dt_cancel AS (
    SELECT 
        PolicyNumber
        ,TermNumber
        ,ModelNumber 
        ,TransEffDate
        ,LAG(TransEffDate,1)  OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS cnPrevTransEff
                                    
        ,LAG(ModelNumber,1)   OVER (PARTITION BY	PolicyNumber
                                                    ,TermNumber
                                    ORDER BY		ModelNumber)						AS cnPrevModelNum
    FROM dt_tran4 
    WHERE TranType = 'Cancellation'
)
,dt_cn_oos AS 
(
    SELECT 
        PolicyNumber
        ,TermNumber
        ,ModelNumber 
        ,TransEffDate

    FROM dt_cancel
    WHERE TransEffDate < cnPrevTransEff
)
,dt_cn_oos1 AS 
(
    SELECT 
        cn1.PolicyNumber
        ,cn1.TermNumber
        ,cn1.ModelNumber 
        ,cn1.TransEffDate
        ,min(cn2.TransEffDate) AS nextCancelMin
    FROM dt_cn_oos AS cn1
        LEFT JOIN dt_cn_oos AS cn2
        ON cn2.PolicyNumber = cn1.PolicyNumber
        AND cn2.TermNumber = cn1.TermNumber
        AND cn2.ModelNumber > cn1.ModelNumber
    GROUP BY 
        cn1.PolicyNumber
        ,cn1.TermNumber
        ,cn1.ModelNumber 
        ,cn1.TransEffDate
) 
,dt_cn_oos2 AS 
(
    SELECT 
        PolicyNumber
        ,TermNumber
        ,COALESCE(cnOOSPrevModelNum,1) AS cnOOSMNBeg
        ,ModelNumber - 1 AS cnOOSMNEnd
        ,oosCancelEff
    FROM 
    (
        SELECT 
            PolicyNumber
            ,TermNumber
            ,ModelNumber 
            ,TransEffDate AS oosCancelEff
            ,LAG(ModelNumber,1)   OVER (PARTITION BY	PolicyNumber
                                                        ,TermNumber
                                        ORDER BY		ModelNumber)						AS cnOOSPrevModelNum
        FROM dt_cn_oos1 
        WHERE TransEffDate < nextCancelMin OR nextCancelMin IS NULL
    )
)
,dt_cancel1 AS 
(
    SELECT 
        dt_cancel.PolicyNumber
        ,dt_cancel.TermNumber
        ,COALESCE(dt_cancel.cnPrevModelNum,1) AS cnMNBeg
        ,dt_cancel.ModelNumber - 1 AS cnMNEnd
        ,CASE   WHEN dt_cancel.TransEffDate < dt_cn_oos2.oosCancelEff THEN dt_cancel.TransEffDate
                ELSE COALESCE(dt_cn_oos2.oosCancelEff,dt_cancel.TransEffDate) END AS CancelMinEff
    FROM dt_cancel 
        LEFT JOIN dt_cn_oos2 
        ON dt_cn_oos2.PolicyNumber = dt_cancel.PolicyNumber
        AND dt_cn_oos2.TermNumber = dt_cancel.TermNumber
        AND dt_cancel.ModelNumber BETWEEN dt_cn_oos2.cnOOSMNBeg AND dt_cn_oos2.cnOOSMNEnd 
)
,dt_tran5 AS 
(
    SELECT 
        dt_tran4.PolicyTransactionKey
        ,dt_tran4.AccountNumber
        ,dt_tran4.Segment
        ,dt_tran4.BusinessUnit
		,dt_tran4.OfferingCode
        ,dt_tran4.PolicyNumber
        ,dt_tran4.LegacyPolicyNumber
        ,dt_tran4.PeriodEffDate
        ,dt_tran4.PeriodEndDate
        ,dt_tran4.JobNumber
        ,dt_tran4.TranType
        ,dt_tran4.TermNumber
        ,dt_tran4.ModelNumber
        ,dt_tran4.TransEffDate
        ,dt_tran4.JobCloseDate
        ,dt_tran4.CancelEffDate
        ,dt_tran4.CloseBeg
        ,dt_tran4.WrittenDate
        ,dt_tran4.AccountingDate
        ,dt_tran4.prevTranType
        ,dt_tran4.prevTransEff
        ,dt_tran4.prevCloseBeg
        ,dt_tran4.nextTranType
        ,dt_tran4.nextCloseBeg
        ,dt_tran4.nextTransEff
        ,dt_tran4.ExpBegCY
        ,dt_tran4.ExpEndCY
        ,dt_tran4.ExpBegCY AS ExpBegPY
        ,CASE   WHEN dt_tran4.TranType = 'Cancellation' THEN dt_tran4.TransEffDate
                WHEN dt_tran4.ExpBegCY >= dt_cancel1.CancelMinEff THEN dt_tran4.ExpBegCY 
                WHEN dt_tran4.ExpEndCY > dt_cancel1.CancelMinEff THEN dt_cancel1.CancelMinEff 
                WHEN dt_tran4.nextTranType = 'Cancellation' THEN dt_tran4.nextTransEff
                ELSE dt_tran4.ExpEndCY END AS ExpEndPY
    FROM dt_tran4

    LEFT JOIN dt_cancel1 
    ON dt_cancel1.PolicyNumber = dt_tran4.PolicyNumber
    AND dt_cancel1.TermNumber = dt_tran4.TermNumber
    AND dt_tran4.ModelNumber BETWEEN dt_cancel1.cnMNBeg AND dt_cancel1.cnMNEnd 
)
,dt_tran6 AS (
    SELECT 
        PolicyTransactionKey
        ,AccountNumber
        ,Segment
        ,BusinessUnit
		,OfferingCode
        ,PolicyNumber
        ,LegacyPolicyNumber
        ,PeriodEffDate
        ,PeriodEndDate
        ,JobNumber
        ,TranType
        ,TermNumber
        ,ModelNumber
        ,TransEffDate
        ,JobCloseDate
        ,CancelEffDate
        ,WrittenDate
        ,AccountingDate
        ,ExpBegCY AS CalendarYearBeginDate
        ,CASE   WHEN ExpEndCY > PeriodEndDate THEN IF(ExpBegCY > PeriodEndDate,ExpBegCY,PeriodEndDate)
                ELSE ExpEndCY END AS CalendarYearEndDate
        ,CASE WHEN TranType = 'Cancellation' THEN -1 ELSE 1 END AS CalendarYearMultiplier
        ,ExpBegPY AS PolicyYearBeginDate
        ,CASE   WHEN ExpEndPY > PeriodEndDate THEN IF(ExpBegPY > PeriodEndDate,ExpBegPY,PeriodEndDate)
                ELSE ExpEndPY END AS PolicyYearEndDate
        
    FROM dt_tran5 
)
,policyStatus AS (
SELECT 
    PolicyNumber
    ,SUM(CASE WHEN CURRENT_DATE() BETWEEN CalendarYearBeginDate AND CalendarYearEndDate THEN CalendarYearMultiplier ELSE 0 END) AS StatusCnt
FROM dt_tran6
GROUP BY   
    PolicyNumber
)

SELECT 
    'GW' AS SourceSystem
	,dt_tran6.PolicyTransactionKey
    ,dt_tran6.AccountNumber
    ,dt_tran6.Segment
    ,dt_tran6.BusinessUnit
	,dt_tran6.OfferingCode
    ,dt_tran6.PolicyNumber
    ,dt_tran6.LegacyPolicyNumber
    ,dt_tran6.PeriodEffDate
    ,dt_tran6.PeriodEndDate
    ,dt_tran6.JobNumber
    ,dt_tran6.TranType
    ,dt_tran6.TermNumber
    ,dt_tran6.ModelNumber
    ,dt_tran6.TransEffDate
    ,dt_tran6.JobCloseDate
    ,dt_tran6.CancelEffDate
    ,dt_tran6.WrittenDate
    ,dt_tran6.AccountingDate
    ,dt_tran6.CalendarYearBeginDate
    ,dt_tran6.CalendarYearEndDate
    ,dt_tran6.PolicyYearBeginDate
    ,dt_tran6.PolicyYearEndDate
	,dt_tran6.CalendarYearMultiplier
    ,CASE WHEN policyStatus.StatusCnt >= 1 THEN 'Active' ELSE 'Inactive' END AS CurrentPolicyStatus
	,DATE('{date}') as bq_load_date	
FROM dt_tran6
    INNER JOIN policyStatus 
    ON policyStatus.PolicyNumber = dt_tran6.PolicyNumber
