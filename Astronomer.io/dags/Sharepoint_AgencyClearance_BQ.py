import logging
import calendar
import time
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from plugins.operators.agency_clearance import agency_clearance
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync


ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')

source = 'AgencyClearance'
source_abbr = 'agencyclearance'

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_'
    destination_dataset = 'DEV_data_products_t2_sales'
elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_'
    destination_dataset = 'B_QA_data_products_t2_sales'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'semi_managed_gcp_connection'
    bq_gcp_connector = 'semi_managed_gcp_connection'
    bq_target_project = 'semi-managed-reporting'
    base_gcs_folder = ''
    destination_dataset = 'data_products_t2_sales'


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        'AgencyClearance_dag',
        schedule_interval='0 12-18 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    task_sensor = ExternalTaskSensorAsync(
        task_id='check_agency_dag',
        external_dag_id='agency_information_dag',
        external_task_id='set_source_bit',
     #   execution_delta=timedelta(hours=3),
        check_existence=True,
        timeout=7200)

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')

    #create agency clearance output tables
    clearance = PythonOperator(
        task_id='clearance_processing',
        python_callable=agency_clearance,
        op_kwargs={'destination_project': bq_target_project, 'destination_dataset': destination_dataset, 'gcp_connection': bq_gcp_connector}
    )

    task_sensor >> clearance >> bit_set