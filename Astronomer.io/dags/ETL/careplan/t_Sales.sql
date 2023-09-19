/*
    Dataset: gld_careplan
    table: t_Sales -> has all the columns from allSales refined table
    Frequency: daily full load append to a new partition
    dependency: allSales in ref_careplan dataset
    version: 1.0
    date: 10/20/2020
*/


select distinct SHA256(id) as	SalesKey
,	id as SalesId
,JSON_EXTRACT_scalar(sales.claims, '$.id') as claims_id
,JSON_EXTRACT_scalar(sales.organization, '$.id') as organization_id
,	JSON_EXTRACT_scalar(sales.store, '$.id') as store_id
,	created_by
,	upload_file_name
,	transaction_type
,	transaction_id
,	transaction_date
,	jeweler_item_sku
,	product_code
,	product_code_description
,	brand
,	indicator
,	product_price
,	sales_tax
,	product_description
,	created_date
,	plan_sku
,	item_description
,	dealer_cost
,	replace(replace(contract_price, 'None', 'nan' ), 'nan', '0.0') as contract_price
,	cash
,	plan_retail_price
,	plan_sales_tax
,	replace(replace(item_price_low, 'None', 'nan' ), 'nan', '0.0') as item_price_low
,	replace(replace(item_price_high, 'None', 'nan' ), 'nan', '0.0') as item_price_high
,	term
,	sales_transaction_id
,	sales_transaction_date
,	first_name
,	last_name
,	address
,	email
,	city
,	daytime_phone
,	customer_state
,	pos_type
,	payee_code
,	replace(replace(number_of_days_to_cancel, 'None', 'nan' ), 'nan', '0.0') as number_of_days_to_cancel
,	replace(replace(cancellation_percentage, 'None', 'nan' ), 'nan', '0.0') as cancellation_percentage
,	underwriter_fee
,	replace(replace(reserve_amount, 'None', 'nan' ), 'nan', '0.0') as reserve_amount
,	replace(replace(premium_tax, 'None', 'nan' ), 'nan', '0.0') as premium_tax
,	replace(replace(risk_fee, 'None', 'nan' ), 'nan', '0.0') as risk_fee
,	expiration_date
,	affiliations
,	who_cut_the_check
,	plan_retail_price_actual
,	refund_amount
,	replace(replace(liability_used, 'None', 'nan' ), 'nan', '0.0') as liability_used
,	jm_notes
,	updated_ts
,	ver
,	type
, CASE
    WHEN transaction_type != 'CANCELLATION' THEN 'FALSE'
    when expiration_date !='None' then
    case
      WHEN CAST(expiration_date AS TIMESTAMP) > CURRENT_TIMESTAMP AND CURRENT_TIMESTAMP > CAST(transaction_date AS TIMESTAMP) THEN 'TRUE'
      end
    ELSE
  'FALSE' END AS inforcePolicy
  ,case
     when expiration_date ='None' then NUll
     when TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY) = 0 then  0.0
     when expiration_date !='None' then (CAST(plan_retail_price AS NUMERIC)/TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY)) * TIMESTAMP_DIFF(CURRENT_TIMESTAMP,CAST(transaction_date AS TIMESTAMP),DAY)  END AS currentPlanEarnPrem
  ,case
      when expiration_date ='None' then NUll
      when TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY) = 0 then  0.0
     when expiration_date !='None' then(CAST(plan_retail_price AS NUMERIC)/TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY)) END AS dailyEarnedPrem
  ,case
      when expiration_date ='None' then NUll
      when TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY) = 0 then  0.0
     when expiration_date !='None' then(CAST(dealer_cost   AS NUMERIC)/TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY)) * TIMESTAMP_DIFF(CURRENT_TIMESTAMP,CAST(transaction_date AS TIMESTAMP),DAY) END AS currentPlanEarnDealerCost
 ,case
      when expiration_date ='None' then NUll
     when TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY) = 0 then  0.0
     when expiration_date !='None' then(CAST(dealer_cost AS NUMERIC)/TIMESTAMP_DIFF(CAST(expiration_date AS TIMESTAMP),CAST(transaction_date AS TIMESTAMP),DAY)) END  AS dailyEarnedDealerCost

,DATE('{date}') as LoadDate

from `{project}.{source_dataset}.allSales` sales where sales.transaction_date !='None' or sales.transaction_date is NULL
