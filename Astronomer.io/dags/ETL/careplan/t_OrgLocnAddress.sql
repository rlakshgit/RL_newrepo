/*
    Dataset: gld_careplan
    table: t_OrgLocnAddress -> joins information from allLocations, allOrganization and allAddresses so that one location_id has one record
    Frequency: daily full load append to a new partition
    dependency: allLocations, allOrganization and allAddresses in ref_careplan dataset
    version: 1.0
    date: 10/20/2020
*/
--Get all locations with partition date as column

with locations as ( select *, date(_PartitionTime) as partition_date FROM `{project}.{source_dataset}.allLocations` )

--get valid locations where location_code,location_type,zing_id are not null

, valid_locations as (
select distinct t.* FROM locations t
inner join (
  select id,
    max(
      (case when location_code  != 'None'  then 1 else 0 end) +
      (case when location_type != 'None' then 1 else 0 end) +
      (case when zing_id != 'None' then 1 else 0 end)
    ) maxnotnulls
  FROM  locations
  group by id
) g
on
  g.id = t.id
  and
  (case when t.location_code != 'None'  then 1 else 0 end) +
  (case when t.location_code != 'None'   then 1 else 0 end) +
  (case when t.zing_id != 'None'  then 1 else 0 end) = g.maxnotnulls)

--get latest location by partition time

 ,latest_loc as (select
 loc.id
,loc.name
,loc.organization_id
,loc.parent_location_id
,loc.class_type
,loc.location_code
,loc.location_type
,loc.zing_id
,loc.description
--,loc.in_house_ship_reimburse_flag
,loc.ver
,loc.type from valid_locations  loc inner join (select id, max(partition_date) as maxdate from valid_locations  group by id) latest on loc.id = latest.id and loc.partition_date = latest.maxdate)

--get valid addresses where phone number is of length 10

,addresses as (SELECT
address.location_id
,address.street_address
,address.city
,address.state
,address.zip_code
,address.phone_number
,address.ver

, case when address.country = 'USA' then 'US'
else address.country end
as country
, (DATE(_PARTITIONTIME)) as partition_date
 from `{project}.{source_dataset}.allAddresses` address where length(address.phone_number) = 10)

--get latest address based on partition date

,latest_address as (
select addresses.location_id
,addresses.street_address
,addresses.city
,addresses.state
,addresses.zip_code
,addresses.phone_number
,addresses.country
,addresses.ver as allAddresses_ver
 from addresses inner join (select location_id, max(partition_date) as maxdate from addresses group by location_id) latest on addresses.location_id = latest.location_id and addresses.partition_date = latest.maxdate)


--get latest organizations based on partition date

 ,organizations as (
select
org.id
,org.corp_name
,org.business_code
,org.payee_code
,org.org_type
,org.repair_service
,org.origin_id
,org.ship_reimburse_flag
,org.care_plan
,org.personal_insurance
,org.ver
from `{project}.{source_dataset}.allOrganizations` org
inner join (select id, max(date(_PartitionTime)) as partition_date from `{project}.{source_dataset}.allOrganizations` group by id ) latest_org
on latest_org.id = org.id and latest_org.partition_date = date(org._PartitionTime))

SELECT distinct
SHA256(concat(loc.id,org.id)) as OrgLocnAddressKey
,loc.id  as LocationId
,loc.name
,loc.organization_id
,loc.parent_location_id
,loc.class_type
,loc.location_code
,loc.location_type
,loc.zing_id
--,loc.in_house_ship_reimburse_flag
,loc.description
,loc.ver as allLocations_ver
,loc.type


,latest_address.street_address
,latest_address.city
,latest_address.state
,latest_address.zip_code
,latest_address.phone_number
,latest_address.allAddresses_ver

,org.corp_name
,org.business_code
,org.payee_code
,org.org_type
,org.repair_service
,org.origin_id
,org.ship_reimburse_flag
,org.care_plan
,org.personal_insurance
,org.ver as allOrganization_ver

,DATE('{date}') as LoadDate


FROM latest_loc loc
Left Join organizations org on org.id = loc.organization_id
Left Join latest_address  on latest_address.location_id = loc.id