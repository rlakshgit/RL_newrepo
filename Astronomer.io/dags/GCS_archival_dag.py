"""
--This Dag archives GCS files
--We have to pass project,bucket,file prefix,
  through airflow variable called gcs_archive_file_prefix in Airflow
"""
import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
#from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from plugins.operators.jm_gcs_file_archive import GCSToGCSOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)
AIRFLOW_ENV = Variable.get('ENV')
# Airflow Environment Variables


if AIRFLOW_ENV.lower() == 'dev':
    project = 'jm-dl-landing'
    base_bucket = 'jm-edl-landing-wip'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'DEV_'
    backup_bucket = 'jm-edl-landing-wip'

elif AIRFLOW_ENV.lower() == 'qa':
    project = 'jm-dl-landing'
    base_bucket = 'jm-edl-landing-wip'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'B_QA_'
    backup_bucket = 'jm-edl-landing-wip'

elif AIRFLOW_ENV.lower() == 'prod':
    project = 'jm-dl-landing'
    base_bucket = 'jm-edl-landing-prod'
    base_gcp_connector = 'jm_landing_prod'
    folder_prefix = ''
    backup_bucket = 'jm-edl-landing-prod'

files_prefix = Variable.get('gcs_archive_file_prefix')

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

backup_files_prefix = 'archive_' + files_prefix


#params = '{"source_dataset_id":"{source_dataset}","source_project_id":"{source_project}","overwrite_destination_table":"true"}'.format(source_dataset = source_dataset,source_project=source_project )
with DAG(
        'Archive_GCS_file_DAG',
        schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    copy_files_without_wildcard = GCSToGCSOperator(
        task_id="archive_files",
        source_project = project,
        source_gcs_conn_id = base_gcp_connector,
        source_bucket=base_bucket,
        source_file_prefix =files_prefix,
        destination_bucket=base_bucket
        )