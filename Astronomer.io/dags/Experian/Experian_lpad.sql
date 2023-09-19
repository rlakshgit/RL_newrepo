SELECT * except(
ZIP_CODE
,ZIP_PLUS_4
,PRIMARY_SIC_CODE
,SECOND_SIC_CODE
,THIRD_SIC_CODE
,FOURTH_SIC_CODE
,FIFTH_SIC_CODE
,SIXTH_SIC_CODE
,PRIMARY_NAICS_CODE
,SECOND_NAICS_CODE
,THIRD_NAICS_CODE
,FOURTH_NAICS_CODE),
lpad(ZIP_CODE,5,'0') AS ZIP_CODE	,	
lpad(ZIP_PLUS_4,4,'0') AS ZIP_PLUS_4	,	
case when PRIMARY_SIC_CODE = 'nan' then 'nan' else lpad(PRIMARY_SIC_CODE,8,'0') end as PRIMARY_SIC_CODE,	
case when SECOND_SIC_CODE = 'nan' then 'nan' else lpad(SECOND_SIC_CODE,8,'0') end as SECOND_SIC_CODE,	
case when THIRD_SIC_CODE = 'nan' then 'nan' else lpad(THIRD_SIC_CODE,8,'0') end as THIRD_SIC_CODE	,	
case when FOURTH_SIC_CODE = 'nan' then 'nan' else lpad(FOURTH_SIC_CODE,8,'0') end as FOURTH_SIC_CODE,	
case when FIFTH_SIC_CODE = 'nan' then 'nan' else lpad(FIFTH_SIC_CODE,8,'0') end as FIFTH_SIC_CODE	,	
case when SIXTH_SIC_CODE = 'nan' then 'nan' else lpad(SIXTH_SIC_CODE,8,'0') end as SIXTH_SIC_CODE	,	
case when PRIMARY_NAICS_CODE = 'nan' then 'nan' else lpad(PRIMARY_NAICS_CODE,6,'0')	end as PRIMARY_NAICS_CODE,	
case when SECOND_NAICS_CODE = 'nan' then 'nan' else lpad(SECOND_NAICS_CODE,6,'0')	end as SECOND_NAICS_CODE,	
case when THIRD_NAICS_CODE = 'nan' then 'nan' else lpad(THIRD_NAICS_CODE,6,'0')	end as THIRD_NAICS_CODE	,	
case when FOURTH_NAICS_CODE = 'nan' then 'nan' else lpad(FOURTH_NAICS_CODE,6,'0')	end as FOURTH_NAICS_CODE
FROM `{project}.{dataset}.experian_current_brick`

