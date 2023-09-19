WITH types AS (
  SELECT types.subscriptionDefinitions_id, subscriptionDefinitions_name
  FROM
   `prod-edl.ref_hubspot.t_hubspot_emailSubscriptionTypes` types
 left join (
  SELECT max(DATE(_PARTITIONTIME)) as pdate, subscriptionDefinitions_id
  FROM
   `prod-edl.ref_hubspot.t_hubspot_emailSubscriptionTypes`
   group by subscriptionDefinitions_id ) b

   on types.subscriptionDefinitions_id = b.subscriptionDefinitions_id
   and date(types._PARTITIONTIME) = b.pdate
  --WHERE DATE(_PARTITIONTIME) = '2020-07-11'
)

,events as
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_0 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_0 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId, emails.objects_allEmailCampaignIds_0

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_1 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_1 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_1)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_2 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_2 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_2)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_3 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_3 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_3)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
, emails.objects_allEmailCampaignIds_4 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_4 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_4)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_5 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_5 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_5)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_6 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_6 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_6)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_7 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_7 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_7)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_8 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_8 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_8)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_9 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_9 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_9)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_10 as objects_allEmailCampaignId
FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_10 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_10)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_11 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_11 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_11)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_12 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_12 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_12)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_13 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_13 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_13)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
, emails.objects_allEmailCampaignIds_14 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_14 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_14)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_15 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_15 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_15)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_16 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_16 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_16)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_17 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_17 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_17)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_18 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_18 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_18)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_19 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_19 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_19)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_20 as objects_allEmailCampaignId
FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_20 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_20)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_21 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_21 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_21)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_22 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_22 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_22)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_23 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_23 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_23)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
, emails.objects_allEmailCampaignIds_24 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_24 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_24)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_25 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_25 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_25)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_26 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_26 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_26)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_27 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_27 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_27)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_28 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_28 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_28)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_29 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_29 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_29)






union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_30 as objects_allEmailCampaignId
FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_30 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_30)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_31 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_31 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_31)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_32 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_32 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_32)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_33 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_33 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_33)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
, emails.objects_allEmailCampaignIds_34 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_34 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_34)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_35 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_35 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_35)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_36 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_36 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_36)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_37 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_37 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_37)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_38 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_38 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_38)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_39 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_39 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_39)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_40 as objects_allEmailCampaignId
FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_40 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_40)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_41 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_41 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_41)


union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_42 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_42 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_42)



union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_43 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_43 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_43)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
, emails.objects_allEmailCampaignIds_44 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_44 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_44)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_45 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_45 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_45)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_46 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_46 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_46)

union distinct
(select max(objects_updated) as email_date,
events_id,events_created,events_emailCampaignId
,emails.objects_allEmailCampaignIds_47 as objects_allEmailCampaignId

FROM
`prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents
inner JOIN
`prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
 ON emails.objects_allEmailCampaignIds_47 = CONCAT(emailevents.events_emailCampaignId, '.0')
and filter_date
--and events_emailCampaignId = "90598059"
and TIMESTAMP_MILLIS(CAST(emails.objects_updated as int64)) <= emailevents.events_created

group by events_id,events_created,events_emailCampaignId,emails.objects_allEmailCampaignIds_47))

select distinct
 
 emailevents.events_id
,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(emailevents.events_created as DATETIME)), ' UTC')as events_created
,events_type    
,events_recipient 
,events_browser_name
, events_browser_type
, events_dropReason
,emailevents.events_emailCampaignId            
,events_from
,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(events_sentBy_created as DATETIME)), ' UTC')as events_sentBy_created
,events_sentBy_id            
, events_subject
, events_url
, events_suppressedReason
,objects_campaignName
,objects_id
,objects_name    
,objects_subject
, events_portalSubscriptionStatus
, events_subscriptions_0_status
,types.subscriptionDefinitions_name AS subscription_name
,events_subscriptions_0_id

 

FROM `prod-edl.gld_hubspot.t_hubspot_emailevents` emailevents

left JOIN events on events.events_id = emailevents.events_id and events.events_created = emailevents.events_created
left join `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
on  emails.objects_updated = events.email_date and (events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_0	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_1	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_2	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_3	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_4	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_5	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_6	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_7	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_8	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_9	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_10	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_11	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_12	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_13	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_14	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_15	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_16	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_17	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_18	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_19	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_20	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_21	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_22	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_23	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_24	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_25	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_26	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_27	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_28	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_29	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_30	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_31	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_32	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_33	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_34	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_35	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_36	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_37	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_38	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_39	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_40	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_41	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_42	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_43	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_44	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_45	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_46	or
events.objects_allEmailCampaignId =	emails.objects_allEmailCampaignIds_47	
 )
LEFT JOIN types ON types.subscriptionDefinitions_id = emailevents.events_subscriptions_0_id
where filter_date








