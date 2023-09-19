import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import json
import os
from airflow import configuration
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.ltv_load_data_operator import SQLServertoBQOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_mssql_to_gcs_triangles import TrianglesSQLServerToGCSOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

AIRFLOW_ENV = Variable.get('ENV')

trigger_object = 'triggers/inbound/dw_complete.json'

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'DEV_'
    source_project_gcp_connector = 'prod_edl'
    source_target_gcs_bucket='jm-edl-landing-wip'
    bq_gcp_connector = 'dev_edl'
    base_gcs_folder = 'l1'
    bq_target_project = 'dev-edl'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'B_QA_'
    source_project_gcp_connector = 'prod_edl'
    source_target_gcs_bucket = 'jm_prod_edl_lnd'
    bq_gcp_connector = 'qa_edl'
    base_gcs_folder = 'l1'
    bq_target_project = 'qa-edl'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    folder_prefix = ''
    source_project_gcp_connector = 'prod_edl'
    source_target_gcs_bucket = 'jm_prod_edl_lnd'
    bq_gcp_connector = 'prod_edl'
    base_gcs_folder = 'l1'
    bq_target_project = 'prod-edl'
    mssql_connection_target = 'instDW_PROD'


source = 'Triangles'
source_abbr = 'triangles'
base_file_location = folder_prefix + 'l1/{source}/'.format(source=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source}/'.format(source=source_abbr)
metadata_location = '{base_gcs_folder}/{source}/'.format(source=source_abbr, base_gcs_folder=base_gcs_folder)
triangles_dataset = '{source}'.format(source=source_abbr)
triangles_dataset = 'ref_{source}'.format(source=source_abbr)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Triangles_landing_dag',
        schedule_interval='0 14 1 * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        start_task = EmptyOperator(task_id = 'start_task'  )
        end_task = EmptyOperator(task_id='end_task')

        get_file_list = os.listdir(os.path.join(configuration.get('core','dags_folder'),r'ETL/Triangles'))
        for file in get_file_list:
            table = file.replace('.sql','')
            with open((os.path.join(configuration.get('core', 'dags_folder'),
                        r'ETL/Triangles/{file}'.format(file=file)))) as f:
                sql = f.readlines()
            sql = "\n".join(sql)

            land_data = TrianglesSQLServerToGCSOperator(
                    task_id='land_data_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=table),
                    sql=sql,
                    bucket=base_bucket,
                    filename='{location_base}data/{table}/{date}/l1_data_{table}'.format(
                                        location_base=base_file_location,
                                        date = "{{ ds_nodash }}",
                                        table=table) + '_{}.json', 
                    schema_filename='{location_base}schema/{table}/{date}/l1_schema_{table}.json'.format(
                                        location_base=base_file_location,
                                        source = source_abbr,
                                        date = "{{ ds_nodash }}",
                                        table=table),
                    metadata_filename='{location_base}metadata/{table}/{date}/l1_metadata_{table}.json'.format(
                                        location_base=base_file_location,
                                        schema=source_abbr,
                                        date = "{{ ds_nodash }}",
                                        table=table),
                    google_cloud_storage_conn_id=base_gcp_connector,
                    mssql_conn_id=mssql_connection_target,
                    pool='singel_task')

            landing_audit = APIAuditOperator(
                task_id='landing_audit_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=table),
                bucket=base_bucket,
                project=project,
                dataset=triangles_dataset,
                base_gcs_folder=None,
                target_gcs_bucket=base_bucket,
                google_cloud_storage_conn_id=base_gcp_connector,
                source_abbr=source_abbr,
                source=source,
                metadata_filename='{base_folder}metadata/{table}/{date}/l1_metadata_{table}.json'.format(
                    base_folder=base_file_location,
                    table=table,
                    date="{{ ds_nodash }}"),
                audit_filename='{location_base}{table}/{date}/l1_audit_{table}.json'.format(
                    location_base=base_audit_location,
                    table=table,
                    date='{{ ds_nodash }}'),
                check_landing_only=True,
                table=table)

            start_task >> land_data >> landing_audit >> end_task
    
