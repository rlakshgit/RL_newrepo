--partition_field:events_created_date
with events_select as (
  SELECT recipient as events_recipient
,id as events_id
,sentBy_id as events_sentBy_id
,type as events_type
,TIMESTAMP_MILLIS(CAST(CAST(sentBy_created AS DECIMAL) as int64)) as events_sent_time
,TIMESTAMP_MILLIS(CAST(CAST(created AS DECIMAL) as int64))  AS events_created_time
,date(TIMESTAMP_MILLIS(CAST(CAST(created AS DECIMAL) as int64))) as events_created_date
,cast(cast(cast(emailCampaignGroupId as decimal) as int64) as string) as events_emailCampaignGroupId
,cast(cast(cast(emailCampaignId as decimal) as int64) as string) as events_emailCampaignId
FROM `{project}.{dataset}.t_hubspot_emailEvents` )

select distinct events_select.*,
emails.htmlTitle
,emails.name
,emails.campaignName
from events_select
LEFT JOIN (
          select * from (
              select DATE(_PARTITIONTIME) date_part, ROW_NUMBER () OVER (PARTITION BY id ORDER BY DATE(_PARTITIONTIME) desc) AS Row_Num, a.*
              , case when emailcampaignGroupId <> 'nan' then cast(cast(cast(emailcampaignGroupId as decimal) as int64) as string) else emailcampaignGroupId end  emailcampaignGroupId1
              FROM `{project}.{dataset}.t_hubspot_marketingEmails` a  ) where row_num = 1
        ) emails 
ON emails.emailcampaignGroupId1 = events_select.events_emailCampaignGroupId
WHERE 1=1
AND emails.name IN (
'Retention - PL Winback 1 (Phone - GEICO)',
'Retention - PL Winback 1 (Phone)',
'Retention - PL Winback 1 (Web - GEICO)',
'Retention - PL Winback 1 (Web)'
)
AND events_type IN ("SENT", "OPEN", "CLICKED")
