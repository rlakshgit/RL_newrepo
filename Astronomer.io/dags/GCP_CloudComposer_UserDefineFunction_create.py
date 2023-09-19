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


###
default_dag_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 23),
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
    base_bucket = 'jm-edl-landing'
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

UDF_LIST_STRING = Variable.get('UDF_LIST')
UDF_LIST = UDF_LIST_STRING.split(',')

UDF_list_dir = os.listdir(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/UDF/'))
kimberlite_dataset = '{source_project_prefix}ref_kimberlite'.format(source_project_prefix=source_project_prefix)


with DAG(
        'UserDefinedFunction_dag',
        schedule_interval= "@once", #'0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


        create_udf_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_custom_functions',
            dataset_id='custom_functions',
            bigquery_conn_id=bq_gcp_connector)

        for udf in UDF_list_dir:
            udf_temp = udf.replace('.sql','')
            udf_split = udf_temp.split('/')

            if UDF_LIST[0].upper() == 'ALL':
                pass
            elif udf_split[len(udf_split)-1] not in UDF_list_dir:
                continue

            with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/UDF/',udf)) as f:
                udf_read_sql =f.readlines()
            udf_sql = "\n".join(udf_read_sql)
            udf_sql = udf_sql.format(bq_target_project=bq_target_project,project = bq_target_project, dataset ='custom_functions',
                                     dest_dataset=kimberlite_dataset)
            udf_create = BigQueryUDFOperator(task_id='{udf}_create'.format(udf=udf_split[len(udf_split)-1]),
                                                     #udf_sql='CREATE OR REPLACE FUNCTION `dev-edl.custom_functions.bl_fn_quoteSpaceClean`(input STRING) as (REPLACE(REPLACE(input,\"\'\",\"\"),\'\"\',\"\"));',
                                                     udf_sql=udf_sql,
                                                    google_cloud_bq_conn_id=bq_gcp_connector)


            create_udf_dataset >> udf_create 