WITH

        loc_rankings AS (SELECT * EXCEPT (input_name, input_address, PlaceID_Lookup, ExperianBIN_Lookup, loc_ranking)
                                  FROM (
                                          SELECT
                                              *
                                              ,RANK() OVER (PARTITION BY  JMAX_LocationKey
                                                            ORDER BY      b.date_created   DESC) AS loc_ranking
--                                            FROM `semi-managed-reporting.core_JDP.internal_source_JMAX` b
                                        FROM `{source_project}.{source_dataset_jdp}.internal_source_JMAX` b
                                           )
                                   WHERE loc_ranking = 1
                             )

SELECT * FROM loc_rankings