SELECT
distinct
distinct_id
,application_number
,REPLACE(application_id, '.0', '') AS application_id
,browser
,screen_width
,'{date}' as bq_load_date
FROM
`prod-edl.ref_mixpanel.t_mixpanel_events_all`
WHERE 1=1
AND DATE(_PARTITIONTIME) >= '{start_date}'
AND DATE(_PARTITIONTIME) <= '{end_date}'
AND event = 'Application Finished'