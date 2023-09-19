WITH wxax_base AS (SELECT DISTINCT

                     customer_number
                   , REPLACE(customer_name, 'None', '') customer_name
                   , REPLACE(customer_address_1, 'None', '') customer_address_1
                   , REPLACE(customer_address_2, 'None', '') customer_address_2
                   , REPLACE(customer_city, 'None', '') customer_city
                   , REPLACE(customer_state, 'None', '') customer_state
                   , REPLACE(customer_zip_code, 'None', '') customer_zip_code
                   , REPLACE(customer_added_date, 'None', '') customer_added_date
                   FROM `{source_project}.{source_dataset}.{source_table}`) ,

       wxax_intermediate AS (SELECT DISTINCT
                    customer_number
                   , customer_name
                   , customer_address_1
                   , customer_address_2
                   , customer_city
                   , customer_state
                   , customer_zip_code
                   , DATE(customer_added_date) customer_added_date
                   , TO_BASE64(SHA256(ARRAY_TO_STRING( [
                       customer_name
                   , customer_address_1
                   , customer_city
                   , customer_state
                   , SPLIT(customer_zip_code, '-')[OFFSET(0)]], ' '))) as WXAX_LocationKey
                   , customer_name as input_name
                   , ARRAY_TO_STRING( [
                       customer_address_1
                   , customer_address_2
                   , customer_city
                   , customer_state
                   , SPLIT(customer_zip_code, '-')[OFFSET(0)]], ' ') as input_address
                   , ARRAY_TO_STRING( [
                      customer_name
                   , customer_address_1
                   , customer_city
                   , customer_state
                   , SPLIT(customer_zip_code, '-')[OFFSET(0)]], ' ') as PlaceID_Lookup
                   , ARRAY_TO_STRING( [
                      customer_name
                   , customer_address_1
                   , customer_city
                   , customer_state
                   , SPLIT(customer_zip_code, '-')[OFFSET(0)]], '||') as ExperianBIN_Lookup
                   FROM wxax_base)
                   ,

       current_jdp_wxax AS (SELECT DISTINCT(WXAX_LocationKey)
                   FROM `{project}.{dataset_jdp}.{table}`
                   WHERE date_created < DATE('{{{{ ds }}}}'))


 SELECT *, DATE('{{{{ ds }}}}') as date_created
 FROM wxax_intermediate
 WHERE WXAX_LocationKey not in (SELECT * FROM current_jdp_wxax)

		                 