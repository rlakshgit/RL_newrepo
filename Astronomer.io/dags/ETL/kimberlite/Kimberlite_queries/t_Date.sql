
/*****************************************************************************************************************
		Kimberlite
			DateDimension.sql
			Created One time
*******************************************************************************************************************/
/*-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/26/2023	SLJ		Init
------------------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_Date` AS
WITH dates AS 
(
  SELECT FullDate
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('1899-01-01'),DATE('2100-12-31'),INTERVAL 1 DAY)) AS FullDate
)
,dates1 AS
(
  SELECT 
  FORMAT_DATE("%Y%m%d",FullDate)                      AS DateKey,
  DATETIME(FullDate)                                  AS FullDate,
  TIMESTAMP_SUB(DATETIME(FullDate),INTERVAL 1 SECOND) AS ExpDate,
  FORMAT_DATE("%b %d, %Y",FullDate)                   AS DateName,

  CASE 
    WHEN FORMAT_DATE("%U",DATE_TRUNC(FullDate,YEAR)) = '00' THEN EXTRACT(WEEK(Sunday) FROM FullDate) + 1 
    ELSE EXTRACT(WEEK(Sunday) FROM FullDate)
  END AS WeekNumber,

  DATE_TRUNC(FullDate, WEEK)                                                    AS FirstDateOfWeek,
  LAST_DAY(FullDate,WEEK)                                                       AS LastDateOfWeek,
  -- DATE_SUB(DATE_ADD(DATE_TRUNC(FullDate,WEEK),INTERVAL 1 WEEK),INTERVAL 1 DAY)  AS LastDateOfWeek,
  EXTRACT(DAYOFWEEK FROM FullDate)                                              AS DayOfWeek,
  CAST(FullDate AS STRING FORMAT 'Day')                                         AS DayOfWeekName,
  
  FORMAT_DATE("%B",FullDate)     AS MonthName,
  FORMAT_DATE("%B, %Y",FullDate) AS MonthNameWithYear,
  FORMAT_DATE("%b",FullDate)     AS MonthShortName,
  FORMAT_DATE("%b %Y",FullDate)  AS MonthShortNameWithYear,
  FORMAT_DATE("%Y%m",FullDate)   AS YearMonthNumber,
  
  DATE_TRUNC(FullDate,MONTH)                                                     AS FirstDateOfMonth,
  LAST_DAY(FullDate,MONTH)                                                       AS LastDateOfMonth,
  -- DATE_SUB(DATE_ADD(DATE_TRUNC(FullDate,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY) AS LastDateOfMonth,
  EXTRACT(Day FROM FullDate)                                                     AS DayOfMonth,
  CONCAT(FORMAT_DATE("%B",FullDate),' ',EXTRACT(Day FROM FullDate),
          CASE 
            WHEN LEFT(FORMAT_DATE("%d",FullDate),1) = '1' THEN 'th'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '1' THEN 'st'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '2' THEN 'nd'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '3' THEN 'rd'
            ELSE 'th' 
          END
        )                                                                        AS DayOfMonthName,
  
  DATE_TRUNC(FullDate,QUARTER)                                                                                AS FirstDateOfQuarter,
  LAST_DAY(FullDate,QUARTER)                                                                                  AS LastDateOfQuarter,
  -- DATE_SUB(DATE_ADD(DATE_TRUNC(FullDate,QUARTER),INTERVAL 1 QUARTER),INTERVAL 1 DAY)                          AS LastDateOfQuarter,
  DATE_DIFF(FullDate,DATE_TRUNC(FullDate,QUARTER),DAY) + 1                                                    AS DayOfQuarter,
  CONCAT('Day ',DATE_DIFF(FullDate,DATE_TRUNC(FullDate,QUARTER),DAY) + 1,' of Q',FORMAT_DATE("%Q",FullDate))  AS DayOfQuarterName,
  
  IF(EXTRACT(QUARTER FROM FullDate) <= 2,1,2) AS HalfNumber,

  CASE 
    WHEN EXTRACT(QUARTER FROM FullDate) IN (1,2) THEN DATE(EXTRACT(YEAR FROM FullDate),01,01)
    WHEN EXTRACT(QUARTER FROM FullDate) IN (3,4) THEN DATE(EXTRACT(YEAR FROM FullDate),07,01)
  END                                                                                             AS FirstDateOfHalf,
  CASE 
    WHEN EXTRACT(QUARTER FROM FullDate) IN (1,2) THEN DATE(EXTRACT(YEAR FROM FullDate),06,30)
    WHEN EXTRACT(QUARTER FROM FullDate) IN (3,4) THEN DATE(EXTRACT(YEAR FROM FullDate),12,31)
  END                                                                                             AS LastDateOfHalf,
  
  DATE_TRUNC(FullDate,YEAR)                                                     AS FirstDateOfYear,
  LAST_DAY(FullDate,YEAR)                                                       AS LastDateOfYear,
  -- DATE_SUB(DATE_ADD(DATE_TRUNC(FullDate,YEAR),INTERVAL 1 YEAR),INTERVAL 1 DAY)  AS LastDateOfYear,
  CONCAT(FORMAT_DATE("%B",FullDate),' ',EXTRACT(Day FROM FullDate),
          CASE 
            WHEN LEFT(FORMAT_DATE("%d",FullDate),1) = '1' THEN 'th'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '1' THEN 'st'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '2' THEN 'nd'
            WHEN RIGHT(FORMAT_DATE("%d",FullDate),1) = '3' THEN 'rd'
            ELSE 'th' 
          END,
          ', ',
          FORMAT_DATE("%Y",FullDate))                                           AS DayOfYearName,

  CONCAT(FORMAT_DATE("%Y",FullDate),RIGHT(CONCAT('00',FORMAT_DATE("%Q",FullDate)),2),FORMAT_DATE("%m",FullDate)) AS YearQuarterMonthNumber,
  CAST(FORMAT_DATE("%Y",FullDate) AS INT64) AS CalendarYear,
  CAST(FORMAT_DATE("%Q",FullDate) AS INT64) AS CalendarQuarter,
  EXTRACT(Month FROM FullDate) AS CalendarMonth,
  CAST(FORMAT_DATE("%j",FullDate) AS INT64) AS CalendarDay,
  FORMAT_DATE("%Y-%m",FullDate) AS CalendarYearMonth,
  CONCAT(FORMAT_DATE("%Y",FullDate),'Q',FORMAT_DATE("%Q",FullDate)) AS CalendarYearQtr

FROM dates
)
SELECT 
  DateKey,
  FullDate,
  ExpDate,
  DateName,
  DateKey AS ISODate,

  CONCAT('Week ',WeekNumber)                                      AS WeekName,
  CONCAT('Week ',WeekNumber,', ',CalendarYear)                    AS WeekNameWithYear,
  CONCAT('WK',RIGHT(CONCAT('00',WeekNumber),2))                   AS WeekShortName,
  CONCAT('WK',RIGHT(CONCAT('00',WeekNumber),2),' ',CalendarYear)  AS WeekShortNameWithYear,
  WeekNumber,
  CONCAT(CalendarYear,RIGHT(CONCAT('00',WeekNumber),2))           AS YearWeekNumber,

  FirstDateOfWeek,
  LastDateOfWeek,
  DayOfWeek,
  DayOfWeekName,
  IF(DayOfWeek IN (1,7),0,1) AS IsWeekday,
  IF(DayOfWeek IN (1,7),1,0) AS IsWeekend,

  MonthName,
  MonthNameWithYear,
  MonthShortName,
  MonthShortNameWithYear,
  YearMonthNumber,

  FirstDateOfMonth,
  LastDateOfMonth,
  DayOfMonth,
  DayOfMonthName,
  IF(FullDate = FirstDateOfMonth,1,0) AS IsFirstDayOfMonth,
  IF(FullDate = LastDateOfMonth,1,0)  AS IsLastDayOfMonth,

  CONCAT('Quarter ',CalendarQuarter)                         AS QuarterName,
  CONCAT('Quarter ',CalendarQuarter,', ',CalendarYear)       AS QuarterNameWithYear,
  CONCAT('Q',CalendarQuarter)                                AS QuarterShortName,
  CONCAT('Q',CalendarQuarter,' ',CalendarYear)               AS QuarterShortNameWithYear,
  CONCAT(CalendarYear,RIGHT(CONCAT('00',CalendarQuarter),2)) AS YearQuarterNumber,

  FirstDateOfQuarter,
  LastDateOfQuarter,
  DayOfQuarter,
  DayOfQuarterName,

  CONCAT('Half ',HalfNumber)                             AS HalfName,
  CONCAT('Half ',HalfNumber,', ',CalendarYear)           AS HalfNameWithYear,
  CONCAT('H',HalfNumber)                                 AS HalfShortName,
  CONCAT('H',HalfNumber,' ',CalendarYear)                AS HalfShortNameWithYear,
  HalfNumber,
  CONCAT(CalendarYear,RIGHT(CONCAT("00",HalfNumber),2))  AS YearHalfNumber,

  FirstDateOfHalf,
  LastDateOfHalf,
  DATE_DIFF(FullDate,FirstDateOfHalf,DAY) + 1                                   AS DayOfHalf,
  CONCAT('Day ',DATE_DIFF(FullDate,FirstDateOfHalf,DAY) + 1,' of H',HalfNumber) AS DayOfHalfName,

  RIGHT(CONCAT('0000',CalendarYear),4)  AS YearName,
  RIGHT(CONCAT('0000',CalendarYear),2)  AS YearShortName,

  FirstDateOfYear,
  LastDateOfYear,
  DayOfYearName,

  YearQuarterMonthNumber,

  CalendarYear,
  CalendarQuarter,
  CalendarMonth,
  CalendarDay,
  CalendarYearMonth,
  CalendarYearQtr,

  CASE WHEN (MOD(CalendarYear,4) = 0 AND MOD(CalendarYear,100) != 0) OR (MOD(CalendarYear,400) = 0) THEN 1 ELSE 0 END AS LeapYear
  
FROM dates1