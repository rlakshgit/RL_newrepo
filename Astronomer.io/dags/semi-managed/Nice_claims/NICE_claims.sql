#StandardSQL
CREATE OR REPLACE VIEW `{destination_project}.{destination_dataset}.wip_NICE_claims_dashboard`
AS
(
SELECT
    contactReporting.contactId
    ,contactReporting.masterContactId
    ,contactReporting.mediaTypeName AS MediaType
    ,contactReporting.skillId
    ,contactReporting.skillName AS Skill
    ,call_quality.DNIS
    ,call_quality.ANI
    ,call_quality.lineType AS PhoneType
    ,CAST(contactReporting.contactStart AS timestamp) AS contactStart
    ,contactReporting.totalDurationSeconds
    ,contactReporting.isLogged AS Logged
    ,contactReporting.primaryDispositionId
    ,contactReporting.secondaryDispositionId
    ,contactReporting.refuseTime
    ,contactReporting.refuseReason

FROM
    `{source_project}.{source_dataset}.t_nice_contactReporting` AS contactReporting

LEFT JOIN
    `{source_project}.{source_dataset}.t_nice_contact_call_quality` AS call_quality
    USING (contactId)
)