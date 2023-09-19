CREATE TEMP FUNCTION
  JSON_EXTRACT_ARRAY_new(input STRING)
  RETURNS ARRAY<STRING>
  LANGUAGE js AS """
return JSON.parse(input).map(x => JSON.stringify(x));
""";

with addresses as (
select location_id,a.partition_date,store_state, zip_code from
(select id as location_id, DATE(_PARTITIONTIME) as partition_date, json_extract_scalar(addresses_flat ,'$.state' ) as store_state, json_extract_scalar(addresses_flat ,'$.zip_code' ) as zip_code from `{project}.{source_dataset}.allLocations`  , unnest(json_extract_array(addresses)) as addresses_flat) a
inner join (select b.id, max(DATE(_PARTITIONTIME)) as partition_date from `{project}.{source_dataset}.allLocations` b  group by b.id) c on c.id = a.location_id where a.partition_date  = c.partition_date
 )

SELECT distinct
case
  when sales.transaction_type = 'INITIAL_SALE' then sales.transaction_date
  else initial_sale_data.transaction_date
END as plan_original_effective_date

,sales.expiration_date as expiration_date
,sales.transaction_date as transaction_date
,sales.created_date as create_date
,JSON_EXTRACT_scalar(sales.organization, '$.corp_name') as corp_name
,JSON_EXTRACT_scalar(sales.organization, '$.business_code') as corp_id
,JSON_EXTRACT_scalar(sales.store, '$.name') as store_name
,JSON_EXTRACT_scalar(sales.store, '$.location_code') as store_code_id

--initial_sale_store_data
,case
   when sales.transaction_type = 'INITIAL_SALE' then JSON_EXTRACT_scalar(sales.organization, '$.corp_name')
   else JSON_EXTRACT_scalar(initial_sale_data.organization, '$.corp_name')
   end as initial_sale_corp_name
,case
   when sales.transaction_type = 'INITIAL_SALE' then JSON_EXTRACT_scalar(sales.organization, '$.business_code')
   else JSON_EXTRACT_scalar(initial_sale_data.organization, '$.business_code')
   end as initial_sale_corp_id
,case
   when sales.transaction_type = 'INITIAL_SALE' then JSON_EXTRACT_scalar(sales.store, '$.name')
   else JSON_EXTRACT_scalar(initial_sale_data.store, '$.name') end as initial_sale_store_name
,case
   when sales.transaction_type = 'INITIAL_SALE' then JSON_EXTRACT_scalar(sales.store, '$.location_code')
   else JSON_EXTRACT_scalar(initial_sale_data.store, '$.location_code')
   end as initial_sale_store_code_id

,claims.claim_originated_date
,claims.failure_date as product_failure_date
,claims.updated_ts as date_last_modified
,claim_id
,claim_status
,job_envelope_number
,customer_name
,repair_or_replace as claimed_for
,sales_tax_reimbursed
,send_to_repair_facility
,repair_facility
,claims_processor
,repair_date
,repair_cost
,replacement_cost
,original_requested_amount
,requested_replacement_cost
,customer_city
,claims.customer_state
,customer_zip
,sales.brand as brand
,sales.indicator  as indicator
,sales.plan_sales_tax as plan_sales_tax
,sales.product_code as product_code
,sales.product_price as product_price
,sales.product_code_description as product_code_description
,sales.plan_retail_price as plan_retail_price
,sales.plan_sku as plan_sku
,sales.transaction_id as transaction_id
,JSON_EXTRACT_scalar(repairs, '$.repair_code') as repair_code
,JSON_EXTRACT_scalar(repairs, '$.description') as repair_description
,sales.item_description as item_description
, Case
    when sales.liability_used >= sales.product_price then 'Y'
    else 'N'
    END as lol_met
 , org.ship_reimburse_flag	as ship_reimb
,addresses.store_state as store_state
,addresses.zip_code as store_zip
--,DATE(sales._PARTITIONTIME) as ref_loaded_date
,DATE('{date}') as partition_date

FROM `{project}.{source_dataset}.allClaims`  claims
left join `{project}.{source_dataset}.allSales` sales on sales.id = JSON_EXTRACT_scalar(claims.sale, '$.id')
left join `{project}.{source_dataset}.allSales` initial_sale_data on  initial_sale_data.transaction_id = sales.sales_transaction_id and initial_sale_data.transaction_date = sales.sales_transaction_date
left join `{project}.{source_dataset}.allOrganizations` org  on JSON_EXTRACT_scalar(sales.organization, '$.id') = org.id
Left join addresses  on  addresses.location_id = JSON_EXTRACT_scalar(sales.store, '$.id')
,unnest(json_extract_array(claims.repairs)) as repairs

