/**********************************************************************************************
	Kimberlite - DQ Check for Building Block TransactionConfiguration 
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	07/15/2021	SLJ		Init 
	-------------------------------------------------------------------------------------------
*/

SELECT 
    'Missing' AS UnitTest
    ,PolicyTransaction.PolicyNumber
    ,PolicyTransaction.TermNumber
    ,PolicyTransaction.JobNumber
	,PolicyTransaction.bq_load_date
FROM `{project}.{dest_dataset}.PolicyTransaction` PolicyTransaction

    LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.TransactionConfiguration` WHERE bq_load_date = DATE({partition_date})) TransactionConfiguration
    ON TransactionConfiguration.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey

WHERE   1 = 1
        AND PolicyTransaction.bq_load_date = DATE({partition_date})
        AND TransactionConfiguration.bq_load_date = DATE({partition_date})
        AND PolicyTransaction.TransactionStatus = 'Bound'
        AND TransactionConfiguration.PolicyTransactionKey IS NULL

UNION ALL 

SELECT 
    'Duplicates' AS UnitTest
    ,PolicyNumber
    ,TermNumber
    ,JobNumber
	,bq_load_date
FROM `{project}.{dest_dataset}.TransactionConfiguration`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date}) 
GROUP BY
    PolicyNumber
    ,TermNumber
    ,JobNumber
	,bq_load_date
HAVING COUNT(*) > 1

UNION ALL 

SELECT 
    'End Before Begin' AS UnitTest
    ,PolicyNumber
    ,TermNumber
    ,JobNumber
	,bq_load_date
FROM `{project}.{dest_dataset}.TransactionConfiguration`
WHERE   1 = 1
        AND bq_load_date = DATE({partition_date}) 
        AND (CalendarYearEndDate < CalendarYearBeginDate OR PolicyYearEndDate < PolicyYearBeginDate)

UNION ALL 

SELECT 
    'Incorrect Term Exposure Days' AS UnitTest
    ,PolicyNumber
    ,TermNumber
    ,NULL AS JobNumber
	,bq_load_date
FROM 
(
    SELECT 
        PolicyNumber
        ,TermNumber
	,bq_load_date
        ,min(PeriodEffDate) AS PeriodMin
        ,max(PeriodEndDate) AS PeriodMax
        ,sum(DATE_DIFF(CalendarYearEndDate, CalendarYearBeginDate,DAY) * CalendarYearMultiplier) AS ExposureCY
        ,sum(DATE_DIFF(PolicyYearEndDate, PolicyYearBeginDate,DAY)) AS ExposurePY
    FROM `{project}.{dest_dataset}.TransactionConfiguration` 
    WHERE   1 = 1
            AND bq_load_date = DATE({partition_date}) 
    GROUP BY  
        PolicyNumber
        ,TermNumber
	,bq_load_date
) 
WHERE   ExposureCY > DATE_DIFF(PeriodMax, PeriodMin, DAY)
        OR ExposurePY > DATE_DIFF(PeriodMax, PeriodMin, DAY)
        OR ExposureCY != ExposurePY 
;