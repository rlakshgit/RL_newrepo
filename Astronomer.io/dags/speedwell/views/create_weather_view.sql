CREATE VIEW IF NOT EXISTS
 `{project}.{dataset}.v_weather`
 as
 select
 WMO WorldMeteorologicalOrgID,
 WBAN WBANID,
 ICAO AirportCodeID,
 SRC_ID SourceID,
 STATION_NAME WeatherStation,
 STATE State,
 LAT Latitude,
 LONG Longitude,
 ALT Altitude,
 DATE Date,
 MONTH Month,
 YEAR Year,
 TMAX_DATATYPE TMaxDataType,
 CAST(REPLACE(DAILY_MAX_TEMP,' ','-999999') AS NUMERIC) DailyMaxTemp,
 TMIN_DATATYPE TMinDataType,
 CAST(REPLACE(DAILY_MIN_TEMP,' ','-999999') AS NUMERIC) DailyMinTemp,
 RAIN_DATATYPE RainDataType,
 CAST(REPLACE(DAILY_RAIN,' ','-999999') AS NUMERIC) DailyRainfall,
 SNOW_DATATYPE SnowDataType,
 CAST(REPLACE(DAILY_SNOWFALL,' ','-999999') AS NUMERIC) DailySnowfall,
 WIND_AVE_DATATYPE WindAveDataType,
 CAST(REPLACE(DAILY_WIND_AVE,' ','-999999') AS NUMERIC) DailyWindAverage,
 DAILY_WIND_MAX_GUS_DATATYPE DailyWindMaxGusDataType,
 CAST(REPLACE(DAILY_WIND_MAX_GUST,' ','-999999')AS NUMERIC) DailyWindMaxGust
 from
`{project}.{dataset}.t_weather`