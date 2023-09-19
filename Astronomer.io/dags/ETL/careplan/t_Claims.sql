/*
    Dataset: gld_careplan
    table: t_Claims -> has all the columns from allClaims refined table
    Frequency: daily full load append to a new partition
    dependency: allClaims in ref_careplan dataset
    version: 1.0
    date: 10/20/2020
*/


select distinct
 SHA256(id) as	 ClaimsKey
,	id as ClaimsId
,	JSON_EXTRACT_scalar(sale, '$.id') as sales_id
,	JSON_EXTRACT_scalar(store, '$.id') as store_id
,	claim_id
,	job_envelope_number
,	claim_originated_date
,	customer_name
,	customer_city
,	customer_state
,	customer_zip
,	failure_date
,	updated_ts
,	claim_status
,	repair_or_replace
,	repairs
,	original_requested_amount
,	is_replacement
,	repair_cost
,	replacement_cost
,	requested_replacement_cost
,	approved_repairs_exist
,	sales_tax_reimbursed
,	shipping_costs
,	send_to_repair_facility
,	repair_facility
,	claims_processor
,	state_log_list
,	repair_date
,	buyout
,	collaboration
,	ver
--,	replacement_date
--,	paid_date
--,	closed_date
--,	shipping_to_store_cost
--,	shipping_to_store_tracking_number
--,	shipping_to_customer_cost
--,	shipping_to_customer_tracking_number
,	type
,DATE('{date}') as LoadDate
from `{project}.{source_dataset}.allClaims`
