WITH base AS (
            SELECT DISTINCT GWPC_LocationKey
            , PlaceKey AS CLIENTKEY
            , places_name AS NAME
            , places_address1 AS ADDRESS1
            , places_address2 AS ADDRESS2
            , places_city AS CITY
            , places_state AS STATE
            , places_postal_code AS POSTALCODE
            , places_country AS COUNTRY
            , places_phone AS PHONE
            , SUNOPEN
            , SUNCLOSE
            , MONOPEN
            , MONCLOSE
            , TUEOPEN
            , TUECLOSE
            , WEDOPEN
            , WEDCLOSE
            , THROPEN
            , THRCLOSE
            , FRIOPEN
            , FRICLOSE
            , SATOPEN
            , SATCLOSE
            , LATITUDE
            , LONGITUDE
            , URL
            , GOOGLE_RATING
            , update_time
            , b.date_created
            FROM `{source_project}.{core_sales_cl_dataset}.{source_table_tag}_current_view_master` a
            LEFT JOIN `{source_project}.{core_sales_cl_dataset}.{source_table_tag}_google_places` b
              USING(PlaceKey)
            WHERE a.PlaceKey IS NOT NULL
            AND PlaceKey != 'NULL'
            AND places_name IS NOT NULL
               )

,
chains AS (SELECT DISTINCT(NAME) FROM base
                 INNER JOIN (SELECT NAME, count(*) numrows FROM base GROUP BY NAME)
                  USING(NAME)
            WHERE numrows > 50
            AND GWPC_LocationKey IS NULL
              
            )
,audit_base as
(
SELECT DISTINCT * EXCEPT(GWPC_LocationKey) FROM base WHERE NAME NOT IN(SELECT NAME FROM chains) {optional_exclude_list}
)
select CLIENTKEY ,count(*) count from audit_base group by CLIENTKEY  having count>1 order by count desc