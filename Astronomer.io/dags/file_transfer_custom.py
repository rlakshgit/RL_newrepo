from datetime import datetime, timedelta
from airflow.models import Variable
import logging
import calendar
import json
import time
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    prefix = ''

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'bnagandla@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

file_transfer = Variable.get('file_transfer_variable')
json_data = json.loads(file_transfer)
source_bucket = json_data["source_bucket"]
destination_bucket = json_data["destination_bucket"]
source_path = str(prefix)+str(json_data["source_path"])
destination_path = str(prefix)+str(json_data["destination_path"])
move_object = json_data["delete_from_source"]
last_modified_time = json_data["last_modified_time"]
maximum_modified_time = json_data["maximum_modified_time"]

if last_modified_time == "":
    last_modified_time = None
else:
    last_modified_time = datetime.strptime(last_modified_time, '%m/%d/%Y %H:%M:%S')

if maximum_modified_time == "":
    maximum_modified_time = None
else:
    maximum_modified_time = datetime.strptime(maximum_modified_time, '%m/%d/%Y %H:%M:%S')

with DAG(
        'file_transfer_GCS_to_GCS',
        schedule_interval='@once',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    file_transfer_custom = GCSToGCSOperator(
        task_id="file_transfer_GCS_to_GCS_custom",
        source_bucket=source_bucket,
        source_object=source_path,
        destination_bucket = destination_bucket,
        destination_object=destination_path,
        last_modified_time=last_modified_time,
        maximum_modified_time=maximum_modified_time,
        move_object=move_object
        )
