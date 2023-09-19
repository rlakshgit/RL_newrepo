import logging
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_SharepointtoGCSOperator import SharepointtoGCSOperator


# Variables declaration
source_abbr = 'JBSelect'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    sharepoint_connection = 'sharepoint_connection'


elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    sharepoint_connection = 'sharepoint_connection'


elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod-edl'
    folder_prefix = ''
    sharepoint_connection = 'sharepoint_connection'

source = 'JBSelect'
destination_dataset = folder_prefix + 'JBSelect'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)



with DAG(
        'JB_Select_GCS_Landing_dag',
        schedule_interval='0 20 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    

    # Copy data from Team Hermes site into GCP storage    
    copy_data_from_sharepoint_to_gcs = SharepointtoGCSOperator(
            task_id='copy_jb_select_data_from_sharepoint_to_GCS',
            sharepoint_connection=sharepoint_connection,
            source_file_url='/sites/TeamHermes/Shared Documents/JBSelectCL/',
            google_storage_connection_id=base_gcp_connector,
            destination_bucket=base_bucket,
            gcs_destination_path='{base_file_location}{date}/'.format(base_file_location=base_file_location,
                                                                               date="{{ ds_nodash }}"))
            
            
            
    
    start_task >> copy_data_from_sharepoint_to_gcs >> end_task

