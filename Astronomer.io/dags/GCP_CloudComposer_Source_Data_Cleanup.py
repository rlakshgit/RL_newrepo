import logging
import datetime as dt
import calendar
import time
from airflow import configuration
import os
import airflow
from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_BigQueryUDFOperator import BigQueryUDFOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    folder_prefix = 'DEV_'
    source_project_prefix = 'dev-edl.DEV_jm_'
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




source_list = ['MIXPANEL:mp_coded']
source_split = source_list[0].split(':')
source = source_split[0]
source_abbr = source_split[1]
with DAG(
        'Source_Data_Cleanup',
        schedule_interval= '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


        source_base_dataset = 'ref_{source}'.format(source=source.lower())
        base_dataset = 'gld_{source}'.format(source=source.lower())

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id='{prefix}{base_dataset}'.format(
                base_dataset=base_dataset, prefix=folder_prefix),
            bigquery_conn_id=bq_gcp_connector)

        # create_udf_dataset = BigQueryCreateEmptyDatasetOperator(
        #     task_id='check_for_dataset_custom_functions',
        #     dataset_id='custom_functions',
        #     bigquery_conn_id=bq_gcp_connector,
        #     dag=dag)
        #
        # # UDF Query:
        # udf_sql = 'CREATE OR REPLACE FUNCTION `{bq_target_project}.custom_functions.fn_quoteSpaceClean`(input STRING) as (REPLACE(REPLACE(input,\"\'\",\"\"),\'\"\',\"\"));'.format(bq_target_project=bq_target_project)
        # fn_quoteSpaceClean_udf_create = BigQueryUDFOperator(task_id='fn_quoteSpaceClean_udf_create',
        #                                  udf_sql=udf_sql,
        #                                  google_cloud_bq_conn_id=bq_gcp_connector,
        #                                  dag=dag)

        #UDF Query:
        # sql = 'CREATE OR REPLACE FUNCTION `dev-edl.custom_functions.fn_quoteSpaceClean`(input STRING) as (REPLACE(REPLACE(input,"'",""),'"',""));',

        with open(os.path.join(configuration.get('core', 'dags_folder'),r'ETL/MIXPANEL/gld_mixpanel_t_mixpanel_events_all.sql')) as f:
            mixpanel_data_cleanup_sql_list =f.readlines()
        mixpanel_data_cleanup_sql = "\n".join(mixpanel_data_cleanup_sql_list)
        mixpanel_data_cleanup_sql = mixpanel_data_cleanup_sql.format(source_project_prefix=source_project_prefix,
                                                                     prefix=folder_prefix,
                                                                     base_dataset=source_base_dataset,
                                                                     project=bq_target_project)

        #2020-06-29
        #DATE("{{{{ ds }}}}")
        mixpanel_data_cleanup = BigQueryOperator(task_id='mixpanel_data_cleanup',
                                                       sql=mixpanel_data_cleanup_sql,
                                                     #udf_config=[""""""],
                                                       destination_dataset_table='{project}.{prefix}{base_dataset}.t_mixpanel_events_all${date}'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                           base_dataset=base_dataset,
                                                           source_abbr=source_abbr.lower(),
                                                            date="{{ ds_nodash }}"),
                                                       write_disposition='WRITE_TRUNCATE',
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       gcp_conn_id=bq_gcp_connector,
                                                       allow_large_results=True,
                                                       use_legacy_sql=False,
                                                       time_partitioning={"type": "DAY"},
                                                 schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                        'ALLOW_FIELD_RELAXATION'])


        # [create_dataset,create_udf_dataset]>>fn_quoteSpaceClean_udf_create >> mixpanel_data_cleanup
        create_dataset >> mixpanel_data_cleanup


