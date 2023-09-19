import logging
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_HSBMasterenrollmentfileOperator import HSBMasterenrollmentfileOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_HSBrequirements import hsb_requirements

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,12,15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
AIRFLOW_ENV = Variable.get('ENV')
source = 'HSB'

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection = 'instDW_STAGE'

elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection = 'instDW_STAGE'

####Change this to semi managed connection
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'semi-managed-reporting'
    bq_gcp_connector='semi_managed_gcp_connection'
    folder_prefix = ''
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection = 'instDW_PROD'



destination_dataset = folder_prefix + 'HSB_automation'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source)


with DAG(
        'HSB_automation_dag',
        schedule_interval='0 15 * * 3',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    history_check = 'True'

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/HSB/HSB_required_sql.sql'), 'r') as f:
        hsb_sql = f.read()



    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/HSB/waterlosses.sql'), 'r') as f:
        waterlosses_sql = f.read()


    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset',
        dataset_id=destination_dataset,
        bigquery_conn_id=bq_gcp_connector
    )

    ###Why do we have to query semi managed data - How was it created initially?
    Automate_HSB_Master_enrollment_file = HSBMasterenrollmentfileOperator(
        task_id = 'Automate_HSB_Master_enrollment_file',
        sharepoint_connection = sharepoint_connection ,
        semi_managed_connection= semimanged_gcp_connector,
        google_storage_connection_id=base_gcp_connector,
        bq_gcp_connection_id = bq_gcp_connector,
        gcs_destination_path='{base_file_location}{date}/'.format(base_file_location=base_file_location,
                                                                  date="{{ ds_nodash }}"),
        base_gcs_folder=folder_prefix + 'l1/',
        source_file_url='/sites/TeamHermes/Shared Documents/HSB/Account_to_date_master_files/',
        source=source,
        history_check=history_check,
        destination_bucket=base_bucket,
        project = bq_target_project,
        dataset = destination_dataset,
        table = 'hsb_enrollments'
    )

    load_HSB_requirements_table = hsb_requirements(
            task_id='load_HSB_requirements_table',
            hsb_req_sql = hsb_sql  ,
            #inforce_policy_sql = inforcepolicies_sql ,
            water_loss_sql = waterlosses_sql,
            mssql_conn_id=mssql_connection,
            bigquery_conn_id=bq_gcp_connector,
            destination_dataset=destination_dataset,
            destination_table='hsb_requirements',
            project=bq_target_project)



    create_dataset>>load_HSB_requirements_table>>Automate_HSB_Master_enrollment_file

