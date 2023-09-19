with addresses as (
select location_id,a.partition_date,store_state, zip_code from
(select id as location_id, DATE(_PARTITIONTIME) as partition_date, json_extract_scalar(addresses_flat ,'$.state' ) as store_state, json_extract_scalar(addresses_flat ,'$.zip_code' ) as zip_code from `{project}.{source_dataset}.allLocations`  , unnest(json_extract_array(addresses)) as addresses_flat) a
inner join (select b.id, max(DATE(_PARTITIONTIME)) as partition_date from `{project}.{source_dataset}.allLocations` b  group by b.id) c on c.id = a.location_id where a.partition_date  = c.partition_date
 )


SELECT distinct
case
  when sales.transaction_type = 'INITIAL_SALE' then sales.transaction_date
  else sales.sales_transaction_date
  END as  plan_original_effective_date
,sales.transaction_date as plan_effective_date
,sales.expiration_date as plan_expiration_date
,sales.transaction_date
,sales.created_date
,JSON_EXTRACT_scalar(sales.organization, '$.corp_name') as corp_name
,JSON_EXTRACT_scalar(sales.organization, '$.business_code') as corp_id
,JSON_EXTRACT_scalar(sales.store, '$.name') as store_name
,JSON_EXTRACT_scalar(sales.store, '$.location_code') as store_code_id

--initial_sale_data
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


,addresses.store_state as store_state
,addresses.zip_code as store_zip
,case
  when sales.transaction_type = 'INITIAL_SALE' then sales.transaction_id
  else sales.sales_transaction_id
  END as  original_transaction_id

,sales.payee_code
,sales.updated_ts as date_last_modified
,sales.transaction_id
,sales.transaction_type
,sales.term as plan_duration
,sales.plan_sku
,sales.plan_retail_price
,sales.plan_sales_tax
,sales.product_code
,sales.product_code_description
,sales.product_price
,sales.sales_tax
,sales.dealer_cost
,sales.plan_retail_price_actual
,sales.item_description
,sales.customer_state
,sales.cancellation_percentage
,sales.underwriter_fee
,sales.reserve_amount
,sales.premium_tax
,sales.risk_fee
,sales.affiliations
,sales.who_cut_the_check
,DATE('{date}') as partition_date

FROM `{project}.{source_dataset}.allSales` sales
Left join `{project}.{source_dataset}.allSales` initial_sale_data on initial_sale_data.transaction_id = sales.sales_transaction_id and initial_sale_data.transaction_date = sales.sales_transaction_date
Left join addresses  on addresses.location_id = JSON_EXTRACT_scalar(sales.store, '$.id')
