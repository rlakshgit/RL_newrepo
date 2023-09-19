WITH
                                place_json_fix AS (
                                   SELECT * FROM
                                       (SELECT
                                            *
                                            EXCEPT(places_json)
                                            ,
                                            REPLACE(
                                              REPLACE(
                                                REPLACE(
                                                  REPLACE(
                                                    REPLACE(
                                                      REPLACE(
                                                        REPLACE(
                                                          REPLACE(
                                                            REPLACE(
                                                              REPLACE(
                                                                REPLACE(
                                                                  REPLACE(
                                                                    REPLACE(
                                                                      places_json,
                                                                      '="', '=\\"' ),
                                                                  '">', '\\"<'),
                                                                '\\\\', '\\' ),
                                                              "':", '":'),
                                                            "{{'", '{{"'), -- added escape character for f-string compatibility
                                                          "'}}", '"}}'),   -- added escape character for f-string compatibility
                                                        ": '", ': "'),
                                                      ", '", ', "'),
                                                    "',", '",'),
                                                  "['", '["'),
                                                "']", '"]'),
                                              " False", " false"),
                                            " True", " true")
                                                places_json

                                        FROM `{source_project}.{source_dataset_jdp}.{placeKey_table}`
                                        WHERE LEFT(places_json, 2) = "{{'"
                                        )
                                        UNION ALL
                                        (SELECT *
                                            EXCEPT(places_json), places_json FROM `{source_project}.{source_dataset_jdp}.{placeKey_table}`
                                        WHERE LEFT(places_json, 2) != "{{'")
                                        )
                                        ,

                                place_rankings AS (
                                    SELECT
                                        *
                                        ,RANK() OVER (PARTITION BY  PlaceKey
                                                      ORDER BY      update_time DESC, date_created DESC, current_pid DESC) AS PlaceKey_ranking
                                
                                    FROM place_json_fix,

                                        UNNEST(ARRAY(SELECT REPLACE(JSON_EXTRACT(places_json, '$.result'), '"', ''))) AS result,

                                        UNNEST(ARRAY(SELECT REPLACE(JSON_EXTRACT(places_json, '$.results[0]'), '"', ''))) AS result2,

                                        UNNEST(ARRAY(SELECT REPLACE(JSON_EXTRACT(places_json, '$.result.types'), '"', ''))) AS types

                                        --,UNNEST(ARRAY(SELECT REPLACE(JSON_EXTRACT(places_json, '$.results[0].types'), '"', ''))) AS types2   -- TURN THIS ON WHEN WE SWITCH TO THE PLACE_ID DYNAMIC MAP

                                    WHERE (REGEXP_CONTAINS(types, 'establishment') OR REGEXP_CONTAINS(types, 'premise') OR REGEXP_CONTAINS(types, 'subpremise') OR REGEXP_CONTAINS(types, 'street_address') )
                                    -- We can check for types2 similar to results2 but we would need to parse the formatted address
--                                     AND places_name = 'PLACE_NAME_NOT_FOUND'
--                                     AND date_created = '2021-10-26'

                                ),
                                
                                -- Access `address_components` array for unnesting and subsequent rejoining
                                address_components AS (
                                    SELECT
                                        PlaceKey
                                        ,`semi-managed-reporting.custom_functions.fn_ParseAddressComponents`(JSON_EXTRACT(places_json, '$.result.address_components')) AS COMPONENTS
                                    FROM  place_rankings
                                    WHERE   PlaceKey_ranking = 1
                                ),
                                
                                -- Create long dataset of address components for each location. One record per component and multiple components per PlaceKey
                                pivot_address AS (
                                    SELECT
                                        PlaceKey
                                        ,addr_comp.long_name
                                        ,addr_comp.short_name
                                        ,addr_types
                                
                                    FROM  address_components,
                                          UNNEST(COMPONENTS) AS addr_comp,
                                          UNNEST(addr_comp.types) AS addr_types
                                ),
                                
                                -- Basically reverse pivot to spread long dataset into wide by each address component type into its own column
                                parsed_addresses AS (
                                    SELECT
                                        PlaceKey
                                        ,MAX(CASE
                                            WHEN addr_types = 'administrative_area_level_1'
                                            THEN short_name
                                            END) AS administrative_area_level_1
                                        ,MAX(CASE
                                            WHEN addr_types = 'administrative_area_level_2'
                                            THEN short_name
                                            END) AS administrative_area_level_2
                                        ,MAX(CASE
                                            WHEN addr_types = 'administrative_area_level_3'
                                            THEN short_name
                                            END) AS administrative_area_level_3
                                        ,MAX(CASE
                                            WHEN addr_types = 'country'
                                            THEN short_name
                                            END) AS country
                                        ,MAX(CASE
                                            WHEN addr_types = 'establishment'
                                            THEN short_name
                                            END) AS establishment
                                        ,MAX(CASE
                                            WHEN addr_types = 'floor'
                                            THEN short_name
                                            END) AS floor
                                        ,MAX(CASE
                                            WHEN addr_types = 'locality'
                                            THEN short_name
                                            END) AS locality
                                        ,MAX(CASE
                                            WHEN addr_types = 'neighborhood'
                                            THEN short_name
                                            END) AS neighborhood
                                        ,MAX(CASE
                                            WHEN addr_types = 'point_of_interest'
                                            THEN short_name
                                            END) AS point_of_interest
                                        ,MAX(CASE
                                            WHEN addr_types = 'political'
                                            THEN short_name
                                            END) AS political
                                        ,MAX(CASE
                                            WHEN addr_types = 'post_box'
                                            THEN short_name
                                            END) AS post_box
                                        ,MAX(CASE
                                            WHEN addr_types = 'postal_code'
                                            THEN short_name
                                            END) AS postal_code
                                        ,MAX(CASE
                                            WHEN addr_types = 'postal_code_suffix'
                                            THEN short_name
                                            END) AS postal_code_suffix
                                        ,MAX(CASE
                                            WHEN addr_types = 'postal_town'
                                            THEN short_name
                                            END) AS postal_town
                                        ,MAX(CASE
                                            WHEN addr_types = 'premise'
                                            THEN short_name
                                            END) AS premise
                                        ,MAX(CASE
                                            WHEN addr_types = 'route'
                                            THEN short_name
                                            END) AS route
                                        ,MAX(CASE
                                            WHEN addr_types = 'street_number'
                                            THEN short_name
                                            END) AS street_number
                                        ,MAX(CASE
                                            WHEN addr_types = 'sublocality'
                                            THEN short_name
                                            END) AS sublocality
                                        ,MAX(CASE
                                            WHEN addr_types = 'sublocality_level_1'
                                            THEN short_name
                                            END) AS sublocality_level_1
                                        ,MAX(CASE
                                            WHEN addr_types = 'subpremise'
                                            THEN short_name
                                            END) AS subpremise
                                
                                    FROM pivot_address
                                    
                                    GROUP BY PlaceKey
                                ),
                                
                                -- Similar process as `address_components`, we have to create a long dataset then reverse pivot to restore each open/close per day per location
                                hours AS (
                                    SELECT
                                        PlaceKey
                                        ,`semi-managed-reporting.custom_functions.fn_ParseClosingHours`(JSON_EXTRACT(places_json, '$.result.opening_hours.periods')) AS CLOSED
                                        ,`semi-managed-reporting.custom_functions.fn_ParseOpeningHours`(JSON_EXTRACT(places_json, '$.result.opening_hours.periods')) AS OPEN
                                
                                    FROM place_rankings
                                ),
                                
                                unnested_hours AS (
                                    SELECT
                                        PlaceKey
                                        ,opening
                                        ,closing
                                    
                                    FROM hours,
                                          UNNEST(hours.CLOSED) AS closing,
                                          UNNEST(hours.OPEN) AS opening
                                
                                    WHERE closing.close.day = opening.open.day
                                ),
                                
                                working_hours AS (
                                    SELECT
                                        PlaceKey
                                        ,MAX(CASE
                                            WHEN opening.open.day = 0
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS SUNOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 0
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS SUNCLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 1
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS MONOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 1
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS MONCLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 2
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS TUEOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 2
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS TUECLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 3
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS WEDOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 3
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS WEDCLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 4
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS THROPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 4
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS THRCLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 5
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS FRIOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 5
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS FRICLOSE
                                        ,MAX(CASE
                                            WHEN opening.open.day = 6
                                            THEN opening.open.time
                                            WHEN opening.open.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS SATOPEN
                                        ,MAX(CASE
                                            WHEN closing.close.day = 6
                                            THEN closing.close.time
                                            WHEN closing.close.day IS NULL
                                            THEN 'CLOSED'
                                            END) AS SATCLOSE
                                    
                                    FROM unnested_hours
                                    
                                    GROUP BY PlaceKey
                                ),
                                all_records as (
                                  SELECT
                                    driver.PlaceKey
                                    ,driver.current_pid as current_placeID
                                    ,driver.places_name
                                    ,NULLIF(REPLACE(CONCAT(IFNULL(addr.premise, ''), ' ', IFNULL(addr.street_number, ''), ' ', IFNULL(addr.route, '')), '  ', ' '), ' ') AS places_address1
                                    ,addr.subpremise AS places_address2
                                    ,addr.locality AS places_city
                                    ,addr.administrative_area_level_1 AS places_state
                                    ,IF(addr.postal_code_suffix IS NULL, addr.postal_code, CONCAT(addr.postal_code, '-', addr.postal_code_suffix)) AS places_postal_code
                                    ,addr.country AS places_country
                                    ,REGEXP_REPLACE(JSON_EXTRACT(places_json, '$.result.formatted_phone_number'), '[^0-9]+', '') AS places_phone
                                    ,hours.SUNOPEN
                                    ,hours.SUNCLOSE
                                    ,hours.MONOPEN
                                    ,hours.MONCLOSE
                                    ,hours.TUEOPEN
                                    ,hours.TUECLOSE
                                    ,hours.WEDOPEN
                                    ,hours.WEDCLOSE
                                    ,hours.THROPEN
                                    ,hours.THRCLOSE
                                    ,hours.FRIOPEN
                                    ,hours.FRICLOSE
                                    ,hours.SATOPEN
                                    ,hours.SATCLOSE
                                    ,JSON_EXTRACT(places_json, '$.result.geometry.location.lat') AS LATITUDE
                                    ,JSON_EXTRACT(places_json, '$.result.geometry.location.lng') AS LONGITUDE
                                    ,JSON_EXTRACT_SCALAR(places_json, '$.result.website') AS URL
                                    ,JSON_EXTRACT_SCALAR(places_json, '$.result.rating') AS GOOGLE_RATING
                                    ,driver.update_time as update_time
                                    ,driver.date_created
                                   
                                    
                                FROM place_rankings AS driver
                                    LEFT JOIN parsed_addresses AS addr
                                        ON driver.PlaceKey = addr.PlaceKey
                                    LEFT JOIN working_hours AS hours
                                        ON driver.PlaceKey = hours.PlaceKey
                                    WHERE   PlaceKey_ranking = 1
                                                            
                                
                                GROUP BY
                                    driver.PlaceKey
                                    ,driver.current_pid
                                    ,driver.places_name
                                    ,addr.premise
                                    ,addr.street_number
                                    ,addr.route
                                    ,addr.subpremise
                                    ,addr.locality
                                    ,addr.administrative_area_level_1
                                    ,addr.postal_code
                                    ,addr.postal_code_suffix
                                    ,addr.country
                                    ,hours.SUNOPEN
                                    ,hours.SUNCLOSE
                                    ,hours.MONOPEN
                                    ,hours.MONCLOSE
                                    ,hours.TUEOPEN
                                    ,hours.TUECLOSE
                                    ,hours.WEDOPEN
                                    ,hours.WEDCLOSE
                                    ,hours.THROPEN
                                    ,hours.THRCLOSE
                                    ,hours.FRIOPEN
                                    ,hours.FRICLOSE
                                    ,hours.SATOPEN
                                    ,hours.SATCLOSE
                                    ,driver.places_json
                                    ,driver.update_time
                                    ,driver.date_created
                                  ),
                                null_count as (
                                  SELECT *, (IF(places_name IS NULL, 1, 0) + IF(places_address1 IS NULL, 1, 0) + IF(places_address2 IS NULL, 1, 0) + IF(places_city IS NULL, 1, 0)
                                      + IF(places_state IS NULL, 1, 0) + IF(places_country IS NULL, 1, 0) + IF(places_phone IS NULL, 1, 0) + IF(LATITUDE IS NULL, 1, 0) 
                                      + IF(LONGITUDE IS NULL, 1, 0) + IF(URL IS NULL, 1, 0) + IF(GOOGLE_RATING IS NULL, 1, 0)) AS sum_of_nulls 
                                  FROM all_records
                                )
                                  
                                , final as (
                                      SELECT DISTINCT tbl.* EXCEPT (sum_of_nulls)
                                      FROM null_count tbl
                                        INNER JOIN
                                        (
                                          SELECT PlaceKey, MIN(sum_of_nulls) as MinPoint
                                          FROM null_count
                                          GROUP BY PlaceKey
                                        ) tbl1
                                        ON tbl1.PlaceKey = tbl.PlaceKey
                                      WHERE tbl1.MinPoint = tbl.sum_of_nulls
                                      )

SELECT IF(places_name=places_address1, 'PLACE_NAME_NOT_FOUND', places_name) places_name
        , * EXCEPT(places_name)
FROM final
