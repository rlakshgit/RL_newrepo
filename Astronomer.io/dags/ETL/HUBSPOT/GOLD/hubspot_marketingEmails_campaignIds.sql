--partition_field:bq_load_date
SELECT distinct
 analyticsPageId
, analyticsPageType
, campaign
, campaignName
, id
, portalId
, primaryEmailCampaignId
, emailCampaignGroupId
, LTRIM(RTRIM(REPLACE(REPLACE(ARRAY_TO_STRING([{input_data_list}], ','), 'nan' , ''), ',,', ','), ','), ',') as allEmailCampaignIds
,name
,subject
,date(TIMESTAMP_MILLIS(CAST(updated as int64))) as updated
,CAST(date(_partitiontime) as date) as bq_load_date
FROM `{project}.{dataset}.t_hubspot_marketingEmails`
where id || date(_partitiontime) in
(
   select id || date_part from (
		  select DATE(_PARTITIONTIME) date_part, ROW_NUMBER () OVER (PARTITION BY id ORDER BY DATE(_PARTITIONTIME) desc) AS Row_Num, a.*
		  FROM `{project}.{dataset}.t_hubspot_marketingEmails` a
								)
   where row_num = 1
)
