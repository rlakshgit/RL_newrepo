import logging
import datetime as dt
import calendar
import time

import airflow
from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dateutil.relativedelta import relativedelta
import pandas as pd

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


# try:
#The try and except is having issues with variables and it is reseting the value back to 6 months.
INTERVAL_DATE = Variable.get('mixpanel_funnel_interval')
# except:
#     Variable.set('mixpanel_funnel_interval',1)
#     INTERVAL_DATE = Variable.get('mixpanel_funnel_interval')

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    folder_prefix = 'DEV_'
    source_project_prefix = 'qa-edl.B_QA_'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    folder_prefix = 'QA_'
    source_project_prefix = 'qa-edl.B_QA_'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    mssql_connection_target = 'instDW_PROD'
    folder_prefix = ''
    source_project_prefix = 'prod-edl.'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'




source = 'MIXPANEL'
source_abbr = 'mp_coded'
base_dataset = 'gld_mixpanel_conversion_funnel'

with DAG(
        'Mixpanel_Conversion_Funnel',
        schedule_interval= '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id='{prefix}{base_dataset}'.format(
                base_dataset=base_dataset, prefix=folder_prefix),
            bigquery_conn_id=bq_gcp_connector)


        # dag_date = "{date}".format(context['execution_date'])
        # dag_datetime = pd.to_datetime(dag_date)
        # six_month_target = dag_datetime + relativedelta(months=-2)
        # six_month_string = six_month_target.strftime("%Y-%m-%d")

        mixpanel_session_grouping_sql = '''
        WITH cte_base_data_group as (
          SELECT
          *,
          SUM(CASE
                WHEN event="Quote Started" THEN 1
                WHEN num_items != num_items_prev THEN 1
                WHEN total_value_of_items != total_value_of_items_prev THEN 1
                WHEN zip_postal_code != zip_postal_code_prev THEN 1
                ELSE 0
                END
                ) OVER (ORDER BY distinct_id,zip_postal_code,state_province,num_items,total_value_of_items,time_central ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)+1 AS EVENT_GROUP
          FROM(
        
          SELECT
            distinct_id,event, 
              acquisition_channel,
              acquisition_source, 
              SPLIT(zip_postal_code, '-')[OFFSET(0)] zip_postal_code,
              COALESCE(state_province,"None") as state_province,
              #COALESCE(city,"None") as city,
              num_items,
              CAST(total_value_of_items as FLOAT64) as total_value_of_items,
              time_central,item_types,item_values,item_deductibles,
              
              LAG(SPLIT(zip_postal_code, '-')[OFFSET(0)],1) OVER (
                                    ORDER BY distinct_id,zip_postal_code,state_province,num_items,total_value_of_items,time_central
                                  ) as zip_postal_code_prev,
              LAG(CAST(total_value_of_items as FLOAT64),1) OVER (
                                    ORDER BY distinct_id,zip_postal_code,state_province,num_items,total_value_of_items,time_central
                                  ) as total_value_of_items_prev,
              LAG(num_items,1) OVER (
                                    ORDER BY distinct_id,zip_postal_code,state_province,num_items,total_value_of_items,time_central
                                  ) as num_items_prev
            FROM
            `{source_project_prefix}ref_mixpanel.t_mixpanel_events_all`
          WHERE
            DATE(time_central) > DATE_SUB(DATE("{{{{ ds }}}}"), INTERVAL {inteval_date} MONTH)
          #  distinct_id = "1726d62047c6ef-0cad1cb763c00a-1b3f6257-13c680-1726d62047d64" 
          ORDER BY
            distinct_id,zip_postal_code,state_province,num_items,total_value_of_items,time_central ASC))
        
        SELECT *,
        
        FROM (
        SELECT 
        DISTINCT(a.EVENT_GROUP),a.distinct_id,a.acquisition_channel,
          a.state_province,a.num_items,a.acquisition_source,a.zip_postal_code,a.total_value_of_items,a.item_types,a.item_values,a.item_deductibles,
          quote_started.quote_started_time,
          quote_finished.quote_finished_time,
          app_start.app_start_time,
          app_finished.app_finished_time, 
          COUNT(*) as count_of_events
        FROM cte_base_data_group a
        
        LEFT JOIN (SELECT distinct_id,acquisition_channel,COALESCE(state_province,"None") as state_province,num_items,EVENT_GROUP,MAX(CAST(time_central as TIMESTAMP)) as quote_started_time from cte_base_data_group WHERE event = "Quote Started" GROUP BY distinct_id,acquisition_channel,state_province,num_items,EVENT_GROUP) quote_started
        ON a.distinct_id=quote_started.distinct_id and a.acquisition_channel=quote_started.acquisition_channel and a.state_province = quote_started.state_province and a.num_items=quote_started.num_items and a.EVENT_GROUP = quote_started.EVENT_GROUP
        
        LEFT JOIN (SELECT distinct_id,acquisition_channel,COALESCE(state_province,"None") as state_province,num_items,EVENT_GROUP,MAX(CAST(time_central as TIMESTAMP)) as quote_finished_time from cte_base_data_group WHERE event ="Quote Finished" GROUP BY distinct_id,acquisition_channel,state_province,num_items,EVENT_GROUP) quote_finished
        ON a.distinct_id=quote_finished.distinct_id and a.acquisition_channel=quote_finished.acquisition_channel and a.state_province = quote_finished.state_province and a.num_items=quote_finished.num_items and a.EVENT_GROUP = quote_finished.EVENT_GROUP
        
        LEFT JOIN (SELECT distinct_id,acquisition_channel,COALESCE(state_province,"None") as state_province,num_items,EVENT_GROUP,MAX(CAST(time_central as TIMESTAMP)) as app_start_time from cte_base_data_group WHERE event ="Application Started" GROUP BY distinct_id,acquisition_channel,state_province,num_items,EVENT_GROUP) app_start
        ON a.distinct_id=quote_finished.distinct_id and a.acquisition_channel=quote_finished.acquisition_channel and a.state_province = quote_finished.state_province and a.num_items=quote_finished.num_items and a.EVENT_GROUP = app_start.EVENT_GROUP
        
        LEFT JOIN (SELECT distinct_id,acquisition_channel,COALESCE(state_province,"None") as state_province,num_items,EVENT_GROUP,MAX(CAST(time_central as TIMESTAMP)) as app_finished_time from cte_base_data_group WHERE event ="Application Finished" GROUP BY distinct_id,acquisition_channel,state_province,num_items,EVENT_GROUP) app_finished
        ON a.distinct_id=quote_finished.distinct_id and a.acquisition_channel=quote_finished.acquisition_channel and a.state_province = quote_finished.state_province and a.num_items=quote_finished.num_items and a.EVENT_GROUP = app_finished.EVENT_GROUP
        
        GROUP BY a.distinct_id,a.zip_postal_code,a.total_value_of_items,a.acquisition_channel,a.acquisition_source,a.state_province,a.num_items,a.EVENT_GROUP,quote_started.quote_started_time,quote_finished.quote_finished_time,app_start.app_start_time,app_finished.app_finished_time,a.item_types,a.item_values,a.item_deductibles
        )

                            '''.format(source_project_prefix=source_project_prefix,inteval_date=INTERVAL_DATE)
        print(mixpanel_session_grouping_sql)
        mixpanel_session_grouping = BigQueryOperator(task_id='mixpanel_session_grouping',
                                                     sql=mixpanel_session_grouping_sql,
                                                     destination_dataset_table ='{project}.{prefix}{base_dataset}.mixpanel_session_group'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                            base_dataset=base_dataset,
                                                           source_abbr=source_abbr.lower()),
                                                     write_disposition='WRITE_TRUNCATE',
                                                     create_disposition='CREATE_IF_NEEDED',
                                                     gcp_conn_id=bq_gcp_connector,
                                                     allow_large_results=True,
                                                     use_legacy_sql=False)





        mixpanel_session_funnel_sql = '''
                                            SELECT  * EXCEPT(item_types,item_values,item_deductibles,acquisition_source),
                                            REPLACE(REPLACE(acquisition_source,"'",""),'"',"") as acquisition_source,
                                            REPLACE(REPLACE(REPLACE(item_types,"'",""),'"',"")," ","") as item_types,
                                            REPLACE(REPLACE(REPLACE(item_values,"'",""),'"',"")," ","") as item_values,
                                            REPLACE(REPLACE(REPLACE(item_deductibles,"'",""),'"',"")," ","") as item_deductibles,
                                            
                                            COALESCE(TIMESTAMP_DIFF(quote_finished_time,quote_started_time,SECOND),0) as quote_finish_quote_start_delta,
                                            COALESCE(TIMESTAMP_DIFF(app_start_time ,quote_finished_time,SECOND),0) as app_start_quote_finish_delta,
                                            COALESCE(TIMESTAMP_DIFF(app_finished_time,app_start_time,SECOND),0) as app_finish_app_start_delta,
                                            COALESCE(TIMESTAMP_DIFF(app_finished_time,quote_started_time,SECOND),0) as app_finish_quote_start_delta,
                                            COALESCE(TIMESTAMP_DIFF(app_finished_time,quote_finished_time,SECOND),0) as app_finish_quote_finish_delta,
                                            EXTRACT(MONTH from app_finished_time) as month_of_conversion,
                                            EXTRACT(YEAR from app_finished_time) as year_of_conversion,
                                            CASE 
                                              WHEN app_finished_time is Null then 0
                                              ELSE 1
                                             END conversion_complete,
                                            CASE 
                                              WHEN app_finished_time is Null then 1
                                              ELSE 0
                                             END conversion_incomplete
                                        FROM `{project}.{prefix}{base_dataset}.mixpanel_session_group` 
                                        ORDER BY EVENT_GROUP ASC
                                    '''.format(project=bq_target_project, prefix=folder_prefix, base_dataset=base_dataset)
        mixpanel_session_funnel = BigQueryOperator(task_id='mixpanel_session_funnel',
                                                       sql=mixpanel_session_funnel_sql,
                                                       destination_dataset_table='{project}.{prefix}{base_dataset}.mixpanel_session_funnel'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                           base_dataset=base_dataset,
                                                           source_abbr=source_abbr.lower()),
                                                       write_disposition='WRITE_TRUNCATE',
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       gcp_conn_id=bq_gcp_connector,
                                                       allow_large_results=True,
                                                       use_legacy_sql=False)

        mixpanel_session_funnel_people_sql = '''
                                                 SELECT a.*,
                                                        b.first_name,
                                                        b.last_name,
                                                        b.gender,
                                                        b.email,
                                                        b.last_seen,
                                                        b.zip_postal_code as people_zip_postal_code,
                                                        CASE 
                                                          WHEN a.acquisition_source = "JamesAllen" THEN "James Allen"
                                                          WHEN a.acquisition_source = "jamesallen" THEN "James Allen"
                                                          WHEN a.acquisition_source = "BlueNile" THEN "Blue Nile"
                                                          WHEN a.acquisition_source = "bluenile" THEN "Blue Nile"
                                                          WHEN a.acquisition_source = "allurez" THEN "Allurez"
                                                          WHEN a.acquisition_source = "bestbrilliance" THEN "Best Brillance"
                                                          WHEN a.acquisition_source = "apresjewelry" THEN "Apres Jewelry"
                                                          WHEN a.acquisition_source = "asdgems" THEN "ASD Gems"
                                                          WHEN a.acquisition_source = "adiamor" THEN "Adiamor"
                                                          WHEN a.acquisition_source = "anyedesigns" THEN "Any EDesigns"
                                                          WHEN a.acquisition_source = "briangavindiamonds" THEN "Brian Gavin Diamonds"
                                                          WHEN a.acquisition_source = "crownandcaliber" THEN "Crown and Caliber"
                                                          WHEN a.acquisition_source = "CrownandCaliber" THEN "Crown and Caliber"
                                                          WHEN a.acquisition_source = "danielsjewelers" THEN "Daniels Jewelers"
                                                          WHEN a.acquisition_source = "davidyurman" THEN "David Yurman"
                                                          WHEN a.acquisition_source = "dercofinejewelers" THEN "Derco Fine Jewelers"
                                                          WHEN a.acquisition_source = "forgejewelryworks" THEN "Forge Jewelry Works"
                                                          WHEN a.acquisition_source = "gemjewel" THEN "Gem Jewel"
                                                          WHEN a.acquisition_source = "gemprint" THEN "Gem Print"
                                                          WHEN a.acquisition_source = "heartsonfire" THEN "Hearts On Fire"
                                                          WHEN a.acquisition_source = "hemmingplazajewelers" THEN "Hemming Plaza Jewelers"
                                                          WHEN a.acquisition_source = "heritageappraisers" THEN "Heritage Appraisers"
                                                          WHEN a.acquisition_source = "hydepark" THEN "Hyde Park"
                                                          WHEN a.acquisition_source = "josephschubachjewelers" THEN "Joseph Schubach Jewelers"
                                                          WHEN a.acquisition_source = "kendanadesign" THEN "Kendana Design"
                                                          WHEN a.acquisition_source = "link" THEN "LINK"
                                                          WHEN a.acquisition_source = "loverly" THEN "Loverly"
                                                          WHEN a.acquisition_source = "mydiamond" THEN "My Diamond"
                                                          WHEN a.acquisition_source = "oliveave" THEN "Olive Ave"
                                                          WHEN a.acquisition_source = "peterindorfdesigns" THEN "Peter Indorf Designs"
                                                          WHEN a.acquisition_source = "pointnopointstudio" THEN "Point No Point Studio"
                                                          WHEN a.acquisition_source = "prestigetime" THEN "Prestige Time"
                                                          WHEN a.acquisition_source = "pricescope" THEN "Price Scope"
                                                          WHEN a.acquisition_source = "princessbridediamonds" THEN "Princess Bride Diamonds"
                                                          WHEN a.acquisition_source = "raleighdiamond" THEN "Raleigh Diamond"
                                                          WHEN a.acquisition_source = "ringwraps" THEN "Ring Wraps"
                                                          WHEN a.acquisition_source = "rockher" THEN "Rock Her"
                                                          WHEN a.acquisition_source = "summitdiamondcorp" THEN "Summit Diamond Corp"
                                                          WHEN a.acquisition_source = "whiteflash" THEN "White Flash"
                                                          WHEN a.acquisition_source = "taylorandhart" THEN "Taylor And Hart"
                                                          WHEN a.acquisition_source = "thatspecificcompanyinc" THEN "That Specific Company Inc"
                                                          WHEN a.acquisition_source = "tiffanyco" THEN "Tiffany & Co"
                                                          WHEN a.acquisition_source = "trumpethorn" THEN "Trumpet & Horn"
                                                          WHEN a.acquisition_source = "verifymydiamond" THEN "Verify My Diamond"
                                                          WHEN a.acquisition_source = "veleska" THEN "Veleska"
                                                          WHEN a.acquisition_source = "vraioro" THEN "Vraioro"
                                                          WHEN a.acquisition_source = "zoara" THEN "Zoara"
                                                          WHEN a.acquisition_source = "zola" THEN "Zola"
                                                          WHEN a.acquisition_source = "BrianGavinDiamonds" THEN "Brian Gavin Diamonds"
                                                          WHEN a.acquisition_source is Null THEN "None"
                                                          ELSE a.acquisition_source
                                                        END as acquisition_source_clean,
                                                        CASE 
                                                          WHEN b.gender = "Male" THEN "Male"
                                                          WHEN b.gender = "Female" THEN "Female"
                                                          ELSE "UNKNOWN"
                                                        END as gender_clean,
                                                        
                                                        CASE 
                                                          WHEN UPPER(a.item_types) like "%WATCH%" THEN 1
                                                          ELSE 0 
                                                         END as watch_flag,
                                                        
                                                        CASE 
                                                          WHEN UPPER(a.item_types) like "%RING%" THEN 1
                                                          ELSE 0 
                                                         END as ring_flag
                                                        
                                                        #b.acquia_lift_ids,
                                                        #b.google_analytics_client_ids,
                                                        #b.jmi_session_ids
                                                
                                                FROM `{project}.{prefix}{base_dataset}.mixpanel_session_funnel` a
                                                FULL OUTER JOIN (SELECT * EXCEPT(zip_postal_code),SPLIT(zip_postal_code, '-')[OFFSET(0)] zip_postal_code FROM `{source_project_prefix}ref_mixpanel_people.t_mixpanel_people`) b
                                                ON a.distinct_id = b.distinct_id and a.zip_postal_code = b.zip_postal_code
                                            '''.format(project=bq_target_project, prefix=folder_prefix, base_dataset=base_dataset,source_project_prefix=source_project_prefix)
        mixpanel_session_funnel_people = BigQueryOperator(task_id='mixpanel_session_funnel_people',
                                                   sql=mixpanel_session_funnel_people_sql,
                                                   destination_dataset_table='{project}.{prefix}{base_dataset}.mixpanel_session_funnel_with_people'.format(
                                                       project=bq_target_project,
                                                       prefix=folder_prefix,
                                                       base_dataset=base_dataset,
                                                       source_abbr=source_abbr.lower()),
                                                   write_disposition='WRITE_TRUNCATE',
                                                   create_disposition='CREATE_IF_NEEDED',
                                                   gcp_conn_id=bq_gcp_connector,
                                                   allow_large_results=True,
                                                   use_legacy_sql=False)


        create_dataset >> mixpanel_session_grouping >> mixpanel_session_funnel >> mixpanel_session_funnel_people


