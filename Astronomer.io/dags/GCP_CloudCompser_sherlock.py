import logging
import datetime as dt
import calendar
import time
import sys
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow import configuration
# Import the code that will be run for this dag
from plugins.operators.jm_sherlock_to_gcp import SherlockToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_XMLAuditOperator import XMLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable


ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'mgiddings@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

####

ENV = Variable.get('ENV')
if ENV.lower() == 'dev':
    # target_env = 'dev01'
    sherlock_env = 'prod' #'dev01'
    folder_prefix = 'DEV_'
    source_bucket = 'jm_{env}_sherlock_lnd'.format(env=sherlock_env)
    target_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    target_project = 'jm-dl-landing'
    source_project = 'prod-sherlock' #'dev-sherlock'
    source_gcp_connector = 'prod_sherlock' #'dev_sherlock'
    target_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_sherlock'
    bq_target_project = 'dev-sherlock'
    archive_project= 'jm-dl-landing'

elif ENV.lower() == 'qa':
    # target_env = 'dev01'
    sherlock_env = 'dev01'
    folder_prefix = 'B_QA_'
    source_bucket = 'jm_{env}_sherlock_lnd'.format(env=sherlock_env)
    target_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    target_project = 'jm-dl-landing'
    source_project = 'dev-sherlock'
    source_gcp_connector = 'dev_sherlock'
    target_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    archive_project = 'jm-dl-landing'

elif ENV.lower() == 'prod':
    # target_env = 'dev01'
    sherlock_env = 'prod'
    folder_prefix = ''
    source_bucket = 'jm_{env}_sherlock_lnd'.format(env=sherlock_env)
    target_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    target_project = 'jm-dl-landing'
    target_gcp_connector = 'jm_landing_prod'
    source_project = 'prod-sherlock'  
    source_gcp_connector = 'prod_sherlock'
    bq_target_project = 'prod-sherlock'
    bq_gcp_connector = 'prod_sherlock'
    archive_project = 'prod-edl'


target_table = 't_sherlock_data'
target_dataset = folder_prefix + 'ref_sherlock'

source = 'sherlock'


base_file_location = '{folder_prefix}l1_norm/{source}/'.format(folder_prefix=folder_prefix,
                                                               source=source)
base_schema_location = '{folder_prefix}l1_schema/{source}/'.format(folder_prefix=folder_prefix,
                                                                   source=source)
base_archive_location = '{folder_prefix}l1_archive/{source}/'.format(folder_prefix=folder_prefix,
                                                                     source=source)
base_audit_location = '{folder_prefix}l1_audit/{source}/'.format(folder_prefix=folder_prefix,
                                                                 source=source)


with DAG(
        '{source}_dag_v2'.format(source=source),
        schedule_interval= '0 7 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{table}'.format(table=target_table),
        dataset_id=target_dataset,
        project_id=bq_target_project,
        bigquery_conn_id=bq_gcp_connector
    )

    land_data = SherlockToGoogleCloudStorageOperator(
        task_id='land_{source}'.format(source=source.lower()), #group=group),
        source_project=source_project,
        target_project=target_project,
        # source_file_list=SOURCE_FILE_LIST,
        source_file_list = [],
        target_file_base=base_file_location + '{}/l1_norm_{}', # + file_name_prep,
        target_schema_base=base_schema_location + '{}/l1_schema_sherlock.json',
        target_metadata_base=base_schema_location + '{}/l1_metadata_file_list.json',
        target_archive_base=base_archive_location + '{}/{}',  # + file_name_prep,
        source_bucket=source_bucket,
        target_bucket=target_bucket,
        target_dataset=target_dataset,
        target_table=target_table,
        source_gcs_conn_id=source_gcp_connector,
        target_gcs_conn_id=target_gcp_connector,
        airflow_files_var_set='NA',
        airflow_present_files_var_set='NA',
        reprocess_history=True,
        archive_project=archive_project
    )

    # Put all files in directory for dag execution date, regardless of timestamp in file name.
    refine_data = GoogleCloudStorageToBigQueryOperator(
        task_id='refine_data_{source}'.format(source=source),
        bucket=target_bucket,
        source_objects=['{location_base}{date}/l1_norm_*'.format(location_base=base_file_location,
                                                                  date="{{ ds_nodash }}")],
        destination_project_dataset_table='{project}.{dataset_name}.{table}${date}'.format(project=bq_target_project,
                                                                                           dataset_name=target_dataset,
                                                                                           table=target_table,
                                                                                           date="{{ ds_nodash }}"),
        schema_fields=[],
        schema_object='{schema_location}{date}/l1_schema_sherlock.json'.format(schema_location=base_schema_location,
                                                                          date="{{ ds_nodash }}"),
        source_format='NEWLINE_DELIMITED_JSON',
        compression='NONE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=0,
        bigquery_conn_id=bq_gcp_connector,
        google_cloud_storage_conn_id=target_gcp_connector,
        schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
        src_fmt_configs={},
        autodetect=True,
        trigger_rule="all_done")

    audit_data = XMLAuditOperator(task_id='audit_health_{source}'.format(source=source),
                                  filename=base_audit_location + '{date}/l1_audit_results.json'.format(
                                      date="{{ ds_nodash }}"),
                                  bucket=target_bucket,
                                  l1_prefix='{base}{date}/'.format(base=base_file_location,
                                                                   date="{{ ds_nodash }}"),
                                  google_cloud_storage_conn_id=target_gcp_connector,
                                  google_cloud_bigquery_conn_id=bq_gcp_connector,
                                  google_cloud_bq_config={'project': bq_target_project,
                                                          'dataset': target_dataset,
                                                          'table': target_table},
                                  bq_count_field='filename')

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='sherlock',
                                 mode='SET')

    create_dataset >> land_data >> refine_data >> audit_data >> bit_set







