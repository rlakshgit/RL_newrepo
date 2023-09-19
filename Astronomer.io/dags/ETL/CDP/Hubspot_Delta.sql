WITH types AS (
  SELECT types.id as est_id, types.name est_name
    FROM `prod-edl.ref_hubspot.t_hubspot_emailSubscriptionTypes` types
    left join ( SELECT max(DATE(_PARTITIONTIME)) as pdate, id
                  FROM `prod-edl.ref_hubspot.t_hubspot_emailSubscriptionTypes`
                 group by id ) b
         on types.id = b.id and date(types._PARTITIONTIME) = b.pdate
)
,events as
(
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_0 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_0 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_0 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_0 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_0 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_0 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_0
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_1 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_1 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_1 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_1 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_1 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_1 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_1
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
	 ,case when emails.allEmailCampaignIds_2 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_2 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_2	end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_2 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_2 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_2 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_2
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_3 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_3 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_3 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_3 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_3 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_3 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_3
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_4 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_4 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_4 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_4 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_4 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_4 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_4
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_5 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_5 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_5 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_5 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_5 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_5 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_5
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_6 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_6 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_6 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_6 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_6 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_6 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_6
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_7 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_7 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_7 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_7 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_7 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_7 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_7
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_8 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_8 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_8 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_8 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_8 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_8 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_8
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_9 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_9 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_9 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_9 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_9 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_9 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_9
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_10 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_10 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_10 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_10 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_10 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_10 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_10
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_11 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_11 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_11 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_11 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_11 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_11 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_11
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
	 ,case when emails.allEmailCampaignIds_12 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_12 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_12	end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_12 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_12 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_12 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_12
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_13 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_13 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_13 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_13 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_13 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_13 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_13
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_14 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_14 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_14 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_14 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_14 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_14 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_14
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_15 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_15 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_15 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_15 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_15 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_15 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_15
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_16 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_16 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_16 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_16 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_16 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_16 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_16
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_17 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_17 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_17 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_17 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_17 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_17 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_17
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_18 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_18 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_18 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_18 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_18 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_18 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_18
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_19 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_19 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_19 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_19 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_19 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_19 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_19
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_20 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_20 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_20 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_20 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_20 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_20 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_20
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_21 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_21 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_21 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_21 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_21 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_21 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_21
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
	 ,case when emails.allEmailCampaignIds_22 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_22 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_22	end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_22 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_22 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_22 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_22
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_23 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_23 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_23 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_23 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_23 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_23 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_23
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_24 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_24 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_24 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_24 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_24 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_24 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_24
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_25 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_25 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_25 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_25 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_25 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_25 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_25
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_26 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_26 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_26 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_26 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_26 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_26 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_26
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_27 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_27 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_27 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_27 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_27 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_27 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_27
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_28 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_28 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_28 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_28 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_28 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_28 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_28
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_29 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_29 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_29 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_29 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_29 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_29 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_29
union distinct
    select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_30 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_30 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_30 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_30 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_30 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_30 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_30
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_31 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_31 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_31 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_31 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_31 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_31 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_31
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
	 ,case when emails.allEmailCampaignIds_32 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_32 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_32	end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_32 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_32 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_32 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_32
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_33 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_33 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_33 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_33 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_33 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_33 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_33
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_34 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_34 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_34 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_34 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_34 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_34 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_34
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_35 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_35 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_35 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_35 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_35 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_35 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_35
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_36 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_36 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_36 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_36 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_36 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_36 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_36
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_37 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_37 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_37 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_37 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_37 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_37 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_37
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_38 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_38 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_38 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_38 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_38 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_38 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_38
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_39 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_39 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_39 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_39 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_39 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_39 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_39
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_40 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_40 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_40 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_40 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_40 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_40 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_40
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_41 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_41 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_41 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_41 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_41 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_41 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_41
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
	 ,case when emails.allEmailCampaignIds_42 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_42 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_42	end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_42 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_42 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_42 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_42
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_43 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_43 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_43 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_43 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_43 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_43 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_43
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_44 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_44 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_44 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_44 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_44 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_44 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_44
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_45 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_45 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_45 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_45 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_45 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_45 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_45
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_46 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_46 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_46 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_46 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_46 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_46 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_46
union distinct
   select max(updated) as email_date, events_id, events_created, events_emailCampaignId 
     ,case when emails.allEmailCampaignIds_47 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_47 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_47 end as objects_allEmailCampaignId		  
	FROM {hubspot_dataset}.t_hubspot_emailevents events
	inner JOIN `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
		ON (case when emails.allEmailCampaignIds_47 <> 'nan' 
	          then cast(cast(cast(emails.allEmailCampaignIds_47 as decimal) as int64) as string)
			  else emails.allEmailCampaignIds_47 end) = events.events_emailCampaignId
	       and events_created_date = DATE('{ydate}')
		   and TIMESTAMP_MILLIS(CAST(emails.updated as int64)) <= events.events_created
	group by events_id,events_created,events_emailCampaignId, emails.allEmailCampaignIds_47
)
select distinct emailevents.events_id
,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(emailevents.events_created as DATETIME)), ' UTC')as events_created
,events_type    ,events_recipient ,events_browser_name, events_browser_type, events_dropReason
,emailevents.events_emailCampaignId            ,events_from
,concat(FORMAT_DATETIME("%F %H:%M:%E3S",cast(events_sentBy_created as DATETIME)), ' UTC')as events_sentBy_created
,events_sentBy_id            , events_subject, events_url, events_suppressedReason
,campaignName,id as objects_id, name as objects_name    , subject as objects_subject
,events_portalSubscriptionStatus, events_subscriptions_0_status ,types.est_name AS subscription_name
,events_subscriptions_0_id
FROM {hubspot_dataset}.t_hubspot_emailevents emailevents
left JOIN events on events.events_id = emailevents.events_id and events.events_created = emailevents.events_created
left join `prod-edl.ref_hubspot.t_hubspot_marketingEmails` emails
on  emails.updated = events.email_date and (events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_0	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_1	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_2	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_3	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_4	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_5	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_6	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_7	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_8	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_9	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_10	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_11	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_12	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_13	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_14	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_15	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_16	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_17	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_18	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_19	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_20	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_21	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_22	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_23	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_24	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_25	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_26	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_27	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_28	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_29	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_30	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_31	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_32	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_33	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_34	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_35	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_36	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_37	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_38	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_39	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_40	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_41	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_42	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_43	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_44	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_45	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_46	or
events.objects_allEmailCampaignId =	emails.allEmailCampaignIds_47	
 )
LEFT JOIN types ON types.est_id = emailevents.events_subscriptions_0_id
where events_created_date = DATE('{ydate}')
