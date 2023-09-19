"""Dag to remove folders/files from GCS
"""
import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow import configuration
from airflow.operators.bash_operator import BashOperator
from plugins.operators.jm_bq_archive import BigQueryArchiveOperator
from datetime import datetime, timedelta


from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)
AIRFLOW_ENV = Variable.get('ENV')
# Airflow Environment Variables
if AIRFLOW_ENV.lower() == 'dev':
    base_gcp_connector = 'jm_landing_dev'

elif AIRFLOW_ENV.lower() == 'qa':
    base_gcp_connector = 'jm_landing_dev'

elif AIRFLOW_ENV.lower() == 'prod':
    base_gcp_connector = 'jm_landing_prod'


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
# Getting values from archival_variable and parse it into other variables
path =Variable.get('gcs_delete_object_path')

#params = '{"source_dataset_id":"{source_dataset}","source_project_id":"{source_project}","overwrite_destination_table":"true"}'.format(source_dataset = source_dataset,source_project=source_project )
with DAG(
        'Delete_folders_or_files_from_gcs',
        schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


        delete_files_from_gcs = BashOperator(task_id='delete_objects_from_gcs',
                                             bash_command = 'gsutil -m rm -r {path}'.format(path = path)
                                             )

        delete_files_from_gcs




