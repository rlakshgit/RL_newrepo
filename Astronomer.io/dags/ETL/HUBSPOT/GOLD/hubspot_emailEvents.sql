--partition_field:events_created_date
SELECT distinct
        id as events_id
        ,TIMESTAMP_MILLIS(CAST(cast(created as decimal) as int64)) as events_created
       ,date(TIMESTAMP_MILLIS(CAST(cast(created as decimal) as int64))) as events_created_date
       ,type as events_type
       ,recipient as events_recipient
       ,browser_name as events_browser_name
       ,browser_type as events_browser_type
       ,dropReason as events_dropReason
       ,emailCampaignId as events_emailCampaignId
       ,'from' as events_from
       ,TIMESTAMP_MILLIS(CAST(cast(sentBy_created as decimal) as int64)) as events_sentBy_created
       ,sentBy_id  as events_sentBy_id
       ,subject as events_subject
       ,url as events_url
       ,suppressedReason as events_suppressedReason
       ,portalSubscriptionStatus as events_portalSubscriptionStatus
       ,subscriptions_0_status as events_subscriptions_0_status
       ,subscriptions_0_id as events_subscriptions_0_id
FROM `{project}.{dataset}.t_hubspot_emailEvents`
