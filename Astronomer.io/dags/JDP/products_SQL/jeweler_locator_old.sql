WITH base AS (
            SELECT DISTINCT GWPC_LocationKey
            , b.PlaceKey AS CLIENTKEY
            , b.places_name AS NAME
            , b.places_address1 AS ADDRESS1
            , b.places_address2 AS ADDRESS2
            , b.places_city AS CITY
            , b.places_state AS STATE
            , b.places_postal_code AS POSTALCODE
            , b.places_country AS COUNTRY
            , b.places_phone AS PHONE
            , b.SUNOPEN
            , b.SUNCLOSE
            , b.MONOPEN
            , b.MONCLOSE
            , b.TUEOPEN
            , b.TUECLOSE
            , b.WEDOPEN
            , b.WEDCLOSE
            , b.THROPEN
            , b.THRCLOSE
            , b.FRIOPEN
            , b.FRICLOSE
            , b.SATOPEN
            , b.SATCLOSE
            , b.LATITUDE
            , b.LONGITUDE
            , b.URL
            , b.GOOGLE_RATING
            , update_time
            , b.date_created
            ,Experian.PRIMARY_NAICS_CODE
            ,Experian.SECOND_NAICS_CODE
            ,Experian.PRIMARY_SIC_CODE
            ,Experian.SECOND_SIC_CODE
            FROM `{source_project}.{core_sales_cl_dataset}.{source_table_tag}_current_view_master` a
            LEFT JOIN `{source_project}.{core_sales_cl_dataset}.{source_table_tag}_google_places` b
              USING(PlaceKey)
            LEFT JOIN `semi-managed-reporting.core_sales_cl.promoted_experian_businesses` d
              USING( BusinessKey)
            LEFT JOIN `prod-edl.ref_experian.experian_current_brick` Experian
                ON CAST (d.experian_business_id AS STRING) = Experian.Experian_Business_Id
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

,ins as (
    select CLIENTKEY,max(CASE WHEN Insured_Status = 'Current_Insured' THEN 1 ELSE 0 END) InsuredFlaG
    FROM base
    LEFT JOIN `semi-managed-reporting.core_sales_cl.promoted_recent_active_gwpc_locations` gwpc
        ON base.GWPC_LocationKey = gwpc.GWPC_LocationKeY
    GROUP BY CLIENTKEY
    )

,final as (
      SELECT DISTINCT
          base.* EXCEPT( GWPC_LocationKey,PRIMARY_NAICS_CODE,SECOND_NAICS_CODE,PRIMARY_SIC_CODE,SECOND_SIC_CODE)
      FROM base
      LEFT JOIN ins
        ON base.CLIENTKEY  = ins.CLIENTKEY
      WHERE   NAME NOT IN(SELECT NAME FROM chains)
              AND (InsuredFlag = 1
                      OR PRIMARY_NAICS_CODE in ('448310','339910','423940','811490','212399','522291','423710')
                      OR SECOND_NAICS_CODE in ('448310','339910','423940','811490','212399','522291','423710')
                      OR substr(PRIMARY_SIC_CODE,1,4)  in ('5094','5944','7631','3479','3911','3915','6141','5094')
                      OR substr(SECOND_SIC_CODE,1,4) in ('5094','5944','7631','3479','3911','3915','6141','5094')
                      OR lower(name) like '%jewel%'
                      OR lower(name) like '%diamond%'
                      OR lower(name) like '%gem%'
                      OR lower(name) like '%gold%'
                      OR lower(name) like '%pearl%')
              )

SELECT * FROM final
WHERE NAME IS NOT NULL
AND ADDRESS1 IS NOT NULL
AND NOT (COUNTRY = 'CA' and date_created = '2022-01-15') {optional_exclude_list}

