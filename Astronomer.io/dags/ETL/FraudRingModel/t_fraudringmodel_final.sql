with cte_plecom as
(
SELECT
distinct  * except(bq_load_date,bad_record)
FROM `{project}.{dataset}.t_fraudringmodel_plecom_data`
where bad_record = '0'
),
cte_mixpanel as
(
SELECT
distinct  * except(bq_load_date)
FROM `{project}.{dataset}.t_fraudringmodel_mixpanel_data`
)
select
p.ApplicationId,
P.AccountNumber,
Replace(ifnull(m.application_number,'None'),'None','') mixpanel_AccountNumber,
m.application_id mixpanel_ApplicationId,
m.distinct_id DistinctId,
m.browser Browser,
m.screen_width ScreenWidth,
cast(p.n_Rings as int64) n_Rings,
cast(p.n_subtype_EngRings as int64) n_subtype_EngRings,
cast(p.val_Rings as int64) val_Rings,
cast(p.val_subtype_EngRings as int64) val_subtype_EngRings,
cast(p.n_items as int64) n_Items,
cast(p.TotalRetailReplacement as int64) TotalRetailReplacement,
cast(p.hour_submitted as int64) HourSubmitted ,
cast(p.first_year_loss as int64)FirstYearLoss,
cast(p.SubmissionDate as Date) SubmissionDate,
CAST('{date}' as DATE) as bq_load_date,

from cte_plecom p left join cte_mixpanel m on p.ApplicationId = m.application_id
