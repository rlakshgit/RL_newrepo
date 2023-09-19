with cte as
(
SELECT distinct
	l.organization_id,
	o.corp_name,
	l.name store_name,
	a.street_address,
	a.city,
	a.state,
	a.country,
	a.zip_code,
	a.phone_number,
	DATE("{{{{ ds }}}}") as bq_load_date
FROM `{project}.{dataset}.allAddresses` a
join `{project}.{dataset}.allLocations` l on a.location_id  = l.id
join `{project}.{dataset}.allOrganizations` o on l.organization_id  = o.id
)
select * from cte 
