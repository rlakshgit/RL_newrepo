--partition_field:bq_load_date
SELECT vid
	  ,canonical_vid
      ,properties_jewelry_item_1_value AS JewelryItem1Value
	  ,properties_lead_date_value AS LeadDate
      ,properties_jcid_value AS JCIDValue
	  ,properties_lead_gen_rep_name_value AS Agent
      ,properties_quote_value  AS QuoteAmount
      ,properties_applied_after_receiving_quote_value  AS AppliedAfterReceivingQuote
      ,referral_source  AS ReferralSource
      ,properties_email_value  AS Email
	  ,CAST(date(_partitiontime) as DATE) as bq_load_date
FROM `{project}.{dataset}.t_hubspot_contacts_details_updates` 


