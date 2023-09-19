
/*****************************************************************************************************************
		Building Block - View
			DateDimensionView.sql
			Create view
*******************************************************************************************************************/
/*-----------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	04/26/2023	SLJ		Init
------------------------------------------------------------------------------------------------------------------*/

SELECT 
  DateKey,
  FullDate,
  ExpDate,
  DateName,
  ISODate,

  WeekName,
  WeekNameWithYear,
  WeekShortName,
  WeekShortNameWithYear,
  WeekNumber,
  YearWeekNumber,

  FirstDateOfWeek,
  LastDateOfWeek,
  DayOfWeek,
  DayOfWeekName,
  IsWeekday,
  IsWeekend,
  -- IsHoliday, -- Add later

  MonthName,
  MonthNameWithYear,
  MonthShortName,
  MonthShortNameWithYear,
  -- MonthNumber, -- Use CalendarMonth,
  YearMonthNumber,

  FirstDateOfMonth,
  LastDateOfMonth,
  DayOfMonth,
  DayOfMonthName,
  IsFirstDayOfMonth,
  IsLastDayOfMonth,

  QuarterName,
  QuarterNameWithYear,
  QuarterShortName,
  QuarterShortNameWithYear,
  -- QuarterNumber, -- Use CalendarQuarter
  YearQuarterNumber,

  FirstDateOfQuarter,
  LastDateOfQuarter,
  DayOfQuarter,
  DayOfQuarterName,

  HalfName,
  HalfNameWithYear,
  HalfShortName,
  HalfShortNameWithYear,
  HalfNumber,
  YearHalfNumber,

  FirstDateOfHalf,
  LastDateOfHalf,
  DayOfHalf,
  DayOfHalfName,

  YearName,
  YearShortName,
  -- YearNumber, -- Use CalendarYear

  FirstDateOfYear,
  LastDateOfYear,
  -- DayOfYear,-- Use CalendarDay
  DayOfYearName,

  YearQuarterMonthNumber,

  CalendarYear,
  CalendarQuarter,
  CalendarMonth,
  CalendarDay, -- CalDayOfYear renamed to CalendarDay
  CalendarYearMonth,
  CalendarYearQtr,

  DATE_DIFF(FullDate,CURRENT_DATE(),DAY) AS DayOffset,
  ((CalendarYear - EXTRACT(YEAR FROM CURRENT_DATE())) * 12) + (CalendarMonth -  EXTRACT(MONTH FROM CURRENT_DATE())) AS CalMonthOffset,
  ((CalendarYear - EXTRACT(YEAR FROM CURRENT_DATE())) * 4) + (CalendarQuarter -  EXTRACT(QUARTER FROM CURRENT_DATE())) AS CalQuarterOffset,
  CalendarYear - EXTRACT(YEAR FROM CURRENT_DATE()) AS CalYearOffset,
  IF(EXTRACT(DAY FROM FullDate) > EXTRACT(DAY FROM CURRENT_DATE()),0,1) AS CalMTDFlag,	
  IF(DATE(CONCAT(CalendarYear
                  ,FORMAT_DATE("-%m",CURRENT_DATE())
                  ,IF(FORMAT_DATE("%m-%d",CURRENT_DATE()) = "02-29" AND LeapYear != 1,"-28",FORMAT_DATE("-%d",CURRENT_DATE())))) < DATE(FullDate),0,1) AS CalYTDFlag

FROM `{project}.{core_dataset}.t_Date`
ORDER BY DateKey ASC