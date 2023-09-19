WITH base_records AS
                  (SELECT DISTINCT IFNULL( GWPC_LocationKey, 'NULL') GWPC_LocationKey
                                   , IFNULL( PlaceKey , 'NULL') PlaceKey 
                                   , BusinessKey 
                                   , SF_LocationKey
                                   , LNAX_LocationKey
                                   , WXAX_LocationKey
                                   , JMAX_LocationKey
                                   , Zing_LocationKey
                                   , date_created
--                  FROM `semi-managed-reporting.core_JDP.business_master`
                  FROM `{source_project}.{source_dataset_jdp}.{business_master}`
                  WHERE (GWPC_LocationKey IS NOT NULL OR PlaceKey IS NOT NULL))
      ,
       gw_ranked AS (SELECT * EXCEPT(gw_ranking ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  GWPC_LocationKey
                                                                                      ORDER BY date_created DESC, PlaceKey DESC, BusinessKey DESC) AS gw_ranking
                                                   FROM base_records)
                    WHERE gw_ranking = 1 AND GWPC_LocationKey != 'NULL')
      ,
       pk_ranked AS (SELECT * EXCEPT(pk_ranking, GWPC_LocationKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  PlaceKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS pk_ranking
                                                   FROM base_records)
                    WHERE pk_ranking = 1 AND PlaceKey != 'NULL')

      ,
       sf_ranked AS (SELECT * EXCEPT(sf_ranking, GWPC_LocationKey, PlaceKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  SF_LocationKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS sf_ranking
                                                   FROM base_records)
                    WHERE sf_ranking = 1 AND SF_LocationKey != 'NULL')

       ,
       lnax_ranked AS (SELECT * EXCEPT(ranking, GWPC_LocationKey, PlaceKey, SF_LocationKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  LNAX_LocationKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS ranking
                                                   FROM base_records)
                    WHERE ranking = 1 AND LNAX_LocationKey != 'NULL')

      ,
       wxax_ranked AS (SELECT * EXCEPT(ranking, GWPC_LocationKey, PlaceKey, SF_LocationKey, LNAX_LocationKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  WXAX_LocationKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS ranking
                                                   FROM base_records)
                    WHERE ranking = 1 AND WXAX_LocationKey != 'NULL')

      ,
       jmax_ranked AS (SELECT * EXCEPT(ranking, GWPC_LocationKey, PlaceKey, SF_LocationKey, LNAX_LocationKey, WXAX_LocationKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  JMAX_LocationKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS ranking
                                                   FROM base_records)
                    WHERE ranking = 1 AND JMAX_LocationKey != 'NULL')

      ,
       zing_ranked AS (SELECT * EXCEPT(ranking, GWPC_LocationKey, PlaceKey, SF_LocationKey, LNAX_LocationKey, WXAX_LocationKey, JMAX_LocationKey ) FROM (
                                                   SELECT * ,RANK() OVER (PARTITION BY  Zing_LocationKey
                                                                                      ORDER BY date_created DESC, BusinessKey DESC) AS ranking
                                                   FROM base_records)
                    WHERE ranking = 1 AND Zing_LocationKey != 'NULL')

       ,
       best_mapping1 AS (SELECT DISTINCT GWPC_LocationKey
                    , PlaceKey
                    , IFNULL(a.SF_LocationKey, b.SF_LocationKey) SF_LocationKey
                    , IFNULL(a.LNAX_LocationKey, b.LNAX_LocationKey) LNAX_LocationKey
                    , IFNULL(a.WXAX_LocationKey, b.WXAX_LocationKey) WXAX_LocationKey
                    , IFNULL(a.JMAX_LocationKey, b.JMAX_LocationKey) JMAX_LocationKey
                    , IFNULL(a.Zing_LocationKey, b.Zing_LocationKey) Zing_LocationKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , a.date_created gw_date_created
                    , b.date_created pk_date_created
                      FROM gw_ranked a FULL OUTER JOIN pk_ranked b USING(PlaceKey))

       ,
       best_mapping2 AS (SELECT DISTINCT GWPC_LocationKey
                    , SF_LocationKey
                    , PlaceKey
                    , IFNULL(a.LNAX_LocationKey, b.LNAX_LocationKey) LNAX_LocationKey
                    , IFNULL(a.WXAX_LocationKey, b.WXAX_LocationKey) WXAX_LocationKey
                    , IFNULL(a.JMAX_LocationKey, b.JMAX_LocationKey) JMAX_LocationKey
                    , IFNULL(a.Zing_LocationKey, b.Zing_LocationKey) Zing_LocationKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , gw_date_created
                    , b.date_created sf_date_created
                    , pk_date_created
                      FROM best_mapping1 a FULL OUTER JOIN sf_ranked b USING(SF_LocationKey))

       ,
       best_mapping3 AS (SELECT DISTINCT GWPC_LocationKey
                    , a.SF_LocationKey
                    , LNAX_LocationKey
                    , PlaceKey
                    , IFNULL(a.WXAX_LocationKey, b.WXAX_LocationKey) WXAX_LocationKey
                    , IFNULL(a.JMAX_LocationKey, b.JMAX_LocationKey) JMAX_LocationKey
                    , IFNULL(a.Zing_LocationKey, b.Zing_LocationKey) Zing_LocationKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , gw_date_created
                    , sf_date_created
                    , b.date_created lnax_date_created
                    , pk_date_created
                      FROM best_mapping2 a FULL OUTER JOIN lnax_ranked b USING(LNAX_LocationKey))

        ,
       best_mapping4 AS (SELECT DISTINCT GWPC_LocationKey
                    , SF_LocationKey
                    , LNAX_LocationKey
                    , WXAX_LocationKey
                    , PlaceKey
                    , IFNULL(a.JMAX_LocationKey, b.JMAX_LocationKey) JMAX_LocationKey
                    , IFNULL(a.Zing_LocationKey, b.Zing_LocationKey) Zing_LocationKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , gw_date_created
                    , sf_date_created
                    , lnax_date_created
                    , b.date_created wxax_date_created
                    , pk_date_created
                      FROM best_mapping3 a FULL OUTER JOIN wxax_ranked b USING(WXAX_LocationKey))

        ,
       best_mapping5 AS (SELECT DISTINCT GWPC_LocationKey
                    , a.SF_LocationKey
                    , LNAX_LocationKey
                    , WXAX_LocationKey
                    , JMAX_LocationKey
                    , PlaceKey
                    , IFNULL(a.Zing_LocationKey, b.Zing_LocationKey) Zing_LocationKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , gw_date_created
                    , sf_date_created
                    , lnax_date_created
                    , b.date_created jmax_date_created
                    , pk_date_created
                      FROM best_mapping4 a FULL OUTER JOIN jmax_ranked b USING(JMAX_LocationKey))

        ,
       best_mapping6 AS (SELECT DISTINCT GWPC_LocationKey
                    , a.SF_LocationKey
                    , LNAX_LocationKey
                    , WXAX_LocationKey
                    , JMAX_LocationKey
                    , Zing_LocationKey
                    , PlaceKey
                    , IFNULL(a.BusinessKey, b.BusinessKey) BusinessKey
                    , gw_date_created
                    , sf_date_created
                    , lnax_date_created
                    , jmax_date_created
                    , b.date_created zing_date_created
                    , pk_date_created
                      FROM best_mapping5 a FULL OUTER JOIN zing_ranked b USING(Zing_LocationKey))


SELECT * FROM best_mapping6