SELECT DISTINCT customer_name, customer_number, customer_address_1, customer_address_2, customer_city, customer_state, customer_zip_code, customer_added_date
FROM `{project}.{dataset}.t_jmis_active_clients`
WHERE department = 'Commercial'