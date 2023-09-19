with agent_detail as(select 
      agentId
      ,concat(agentId, '.0') as new_agentId
    ,firstName 
    ,lastName 
    ,teamId
    ,teamName  from `prod-edl.ref_nice.t_agent_detail`)


SELECT
     distinct
    contactReporting.contactId
    ,contactReporting.masterContactId
    ,contactReporting.mediaTypeName AS MediaType
    ,contactReporting.skillId
    ,contactReporting.skillName AS Skill
    ,agent_detail.agentId
    ,agent_detail.firstName AS AgentFirst
    ,agent_detail.lastName AS AgentLast
    ,agent_detail.teamId
    ,agent_detail.teamName as Team
    ,call_quality.DNIS
    ,call_quality.ANI
    ,call_quality.lineType AS PhoneType
    ,CAST(contactReporting.contactStart AS timestamp) AS StartTime
    ,contactReporting.totalDurationSeconds as DurationSeconds
    ,regexp_replace(
    cast(time(ts) as string),
    r'^\d\d',
    cast(extract(hour from time(ts)) + 24 * unix_date(date(ts)) as string)
  ) as Duration
    ,contactReporting.isLogged AS Logged
    ,contactReporting.primaryDispositionId
    ,contactReporting.secondaryDispositionId
    ,contactReporting.refuseTime
    ,contactReporting.refuseReason
    
FROM
    `prod-edl.ref_nice.t_contact_reporting` AS contactReporting,unnest([timestamp_seconds(cast(contactReporting.totalDurationSeconds as INT64))]) ts

LEFT JOIN
     agent_detail
    on contactReporting.agentId = agent_detail.new_agentId 

LEFT JOIN
    `prod-edl.ref_nice.t_contact_call_quality` AS call_quality
    USING (contactId)