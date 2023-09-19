import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_EASNormalizeData import EASNormalizeData
from plugins.operators.jm_EASAuditOperator import EASAuditOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'rlaksh@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'RL_DEV_'
    #source_project_gcp_connector = 'jm-dl-landing'
    source_target_gcs_bucket='jm-edl-landing-wip'
    source_project_gcp_connector = 'dev_edl'
    #source_target_gcs_bucket='jm_prod_edl_lnd'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    environment = 'dev'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    folder_prefix = 'RL_QA_'
    source_project_gcp_connector = 'prod_edl'
    source_target_gcs_bucket = 'jm_prod_edl_lnd'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    environment = 'qa'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    folder_prefix = ''
    source_project_gcp_connector = 'prod_edl'
    source_target_gcs_bucket = 'jm_prod_edl_lnd'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    environment = 'prod'

source = 'EAS'
source_abbr = 'EAS'
ct_enable = True
base_file_location = folder_prefix + 'l1/{source}/'.format(source=source_abbr)
normalize_file_location = folder_prefix + 'l1_norm/{source}/'.format(source=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source}/'.format(source=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source}/'.format(source=source_abbr)
source_dataset_id = '{prefix}ref_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
destination_dataset_id = '{prefix}gld_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)

##
with DAG(
        'eas_landing_zone_dag_v2',
        schedule_interval= '0 6 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    print('{{ ds_nodash }}')

    start_task = EmptyOperator(task_id = 'start_task'  )
    end_task = EmptyOperator(task_id='end_task')

    #This step is to flattening records (Vendor Data / Transactions) from L1 and copy it to L1_NORM filtering out some accounts while copying
    normalize_transaction_data = EASNormalizeData(task_id='normalize_transaction_data_{source_abbr}'.format(source_abbr=source_abbr),
                                      project=project,
                                      source=source_abbr,
                                      source_abbr=source_abbr,
                                      target_gcs_bucket=base_bucket,
                                      source_target_gcs_bucket=source_target_gcs_bucket,
                                      google_cloud_storage_conn_id=base_gcp_connector,
                                      source_google_cloud_storage_conn_id=source_project_gcp_connector,
                                      prefix_for_filename_search='{folder_prefix}l1/{source}/Vendor_Data/{date}'.format(folder_prefix=folder_prefix,source=source_abbr,date='{{ ds_nodash }}'),
                                      schema_filename= '{base_schema_location}Vendor_Data/{date}/l1_schema_eas_transactions.json'.format(base_schema_location=base_schema_location,date='{{ ds_nodash }}'),
                                      metadata_filename = base_audit_location + 'Vendor_Data/{date}'.format(date='{{ ds_nodash }}'),
                                      folder_prefix=folder_prefix,
                                      subject_area='Transactions',
                                      environment= environment)

    #This step is to flattening records (Vendor Information / Lookup) from L1 and copy it to L1_NORM 
    normalize_transactions = EASNormalizeData(
        task_id='normalize_transactions_{source_abbr}'.format(source_abbr=source_abbr),
        project=project,
        source=source_abbr,
        source_abbr=source_abbr,
        target_gcs_bucket=base_bucket,
        source_target_gcs_bucket=source_target_gcs_bucket,
        google_cloud_storage_conn_id=base_gcp_connector,
        source_google_cloud_storage_conn_id=source_project_gcp_connector,
        prefix_for_filename_search='{folder_prefix}l1/{source}/Vendor_Information/{date}'.format(folder_prefix=folder_prefix,source=source_abbr, date='{{ ds_nodash }}'),
        schema_filename='{base_schema_location}Vendor_Information/{date}/l1_schema_eas_vendor_info.json'.format(base_schema_location=base_schema_location,date='{{ ds_nodash }}'),
        folder_prefix=folder_prefix,
        metadata_filename=base_audit_location + 'Vendor_Information/{date}'.format(date='{{ ds_nodash }}'),
        subject_area='transactions',
        environment=AIRFLOW_ENV.lower())

    #This step is to perform audit checks to Vendor Data file
    audit_normalize_transaction_data = EASAuditOperator(
                                                        task_id='audit_normalize_transaction_data_{source_abbr}'.format(source_abbr=source_abbr),
                                                        metadata_filename = '{base_audit_location}Vendor_Data/{date}'.format(base_audit_location=base_audit_location,date='{{ ds_nodash }}'),
                                                        landing_file_location = '{normalize_file_location}Vendor_Data/{date}'.format(normalize_file_location=normalize_file_location,date='{{ ds_nodash }}'),
                                                        check_landing_only=True,
                                                        google_cloud_storage_conn_id=base_gcp_connector,
                                                        bucket=base_bucket)

    #This step is to perform audit checks to Vendor Information file 
    audit_normalize_transactions = EASAuditOperator(
        task_id='audit_normalize_transactions_{source_abbr}'.format(source_abbr=source_abbr),
        metadata_filename='{base_audit_location}Vendor_Information/{date}'.format(base_audit_location=base_audit_location,
                                                                           date='{{ ds_nodash }}'),
        landing_file_location='{normalize_file_location}Vendor_Information/{date}'.format(
            normalize_file_location=normalize_file_location, date='{{ ds_nodash }}'),

        check_landing_only=True,
        google_cloud_storage_conn_id=base_gcp_connector,
        bucket=base_bucket)

    #DAG pipeline defined here. Both Vendor Data and Vendor Information process run in parallel
    start_task>>normalize_transaction_data >> audit_normalize_transaction_data >>end_task
    start_task>>normalize_transactions >> audit_normalize_transactions >>end_task
