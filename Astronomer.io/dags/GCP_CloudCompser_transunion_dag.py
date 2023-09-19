import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow import configuration
# Import the code that will be run for this dag
from plugins.operators.jm_transunion_to_gcp import TransunionToGoogleCloudStorageOperator
from plugins.operators.jm_transunion_gld import TransunionGoldFileHitIndicatorOperator
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
    'start_date': datetime(2023, 4, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'mgiddings@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


#source_project = 'prod-transunion'

ENV = Variable.get('ENV')
Full_load = Variable.get('Transunion_Full_load')
try:
    transunion_archive_failed_files = Variable.get('transunion_archive_failed_files')
except:
    Variable.set('transunion_archive_failed_files','[]')
    transunion_archive_failed_files = Variable.get('transunion_archive_failed_files')

###python function to notify failed archive files#####

def archive_failure_notify_task(**kwargs):
    file_string = kwargs['file_list']
    if file_string != '[]':
        file_list = file_string.replace('[', '').replace(']', '').split(',')
        print(file_list)
        if len(file_list )> 0:
            raise ValueError("Some of the files are failed to archive..")
        else:
            pass
    return 'Success'


if ENV.lower() == 'dev':
    # target_env = 'dev01'
    transunion_env = 'dev01'#'prod'#'dev01'
    folder_prefix = 'DEV_'
    source_bucket = 'jm_{env}_transunion_lnd'.format(env=transunion_env)
    target_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    target_project = 'jm-dl-landing'
    source_project ='dev-transunion' #'prod-transunion'
    source_gcp_connector = 'dev_transunion'
    target_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'jm_landing_dev'
    bq_target_project = 'jm-dl-landing'


elif ENV.lower() == 'qa':
    # target_env = 'dev01'
    transunion_env = 'prod' # 'dev01'
    folder_prefix = 'B_QA_'
    source_bucket = 'jm_{env}_transunion_lnd'.format(env=transunion_env)
    target_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    target_project = 'jm-dl-landing'
    source_project = 'dev-transunion'
    source_gcp_connector = 'dev_transunion' #'prod_transunion'  # 'dev_transunion'
    target_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_transunion' #'jm_landing_dev'
    bq_target_project ='dev-transunion' #'jm-dl-landing'

elif ENV.lower() == 'prod':
    transunion_env = 'prod'
    folder_prefix = ''
    source_bucket = 'jm_{env}_transunion_lnd'.format(env=transunion_env)
    target_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    target_project = 'jm-dl-landing'
    target_gcp_connector = 'jm_landing_prod'
    source_project = 'prod-transunion'
    source_gcp_connector = 'prod_transunion'
    bq_target_project = 'prod-transunion' #'prod-edl' 
    bq_gcp_connector = 'prod_transunion' #'prod_edl' 


target_table = 't_transunion_data'
target_dataset = folder_prefix + 'ref_transunion'
gld_dataset = folder_prefix + 'gld_transunion'
gld_table = 'transunion'

source = 'transunion'


base_file_location = '{folder_prefix}l1_norm/{source}/'.format(folder_prefix=folder_prefix,
                                                               source=source)
base_schema_location = '{folder_prefix}l1_schema/{source}/'.format(folder_prefix=folder_prefix,
                                                                  source=source)
base_archive_location = '{folder_prefix}l1_archive/{source}/'.format(folder_prefix=folder_prefix,
                                                                    source=source)
base_audit_location = '{folder_prefix}l1_audit/{source}/'.format(folder_prefix=folder_prefix,
                                                                 source=source)



###
with DAG(
        'jm_Transunion_dag',
        schedule_interval='0 16 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{table}'.format(table=target_table),
        dataset_id=target_dataset,
        project_id=bq_target_project,
        bigquery_conn_id=bq_gcp_connector
    )

    land_data = TransunionToGoogleCloudStorageOperator(
        task_id='land_{source}'.format(source=source.lower()),  # group=group),
        source_project=source_project,
        target_project=target_project,
        target_file_base=base_file_location + '{}/l1_norm_{}',  # + file_name_prep,
        target_schema_base=base_schema_location + '{}/l1_schema_transunion.json',
        target_metadata_base=base_schema_location + '{}/l1_metadata_file_list.json',
        target_failed_archive_file_list = base_schema_location + '{}/l1_failed_archive_file_list.json',
        target_archive_base=base_archive_location + '{}/{}',  # + file_name_prep,
        source_bucket=source_bucket,
        target_bucket=target_bucket,
        target_dataset=target_dataset,
        target_table=target_table,
        source_gcs_conn_id=source_gcp_connector,
        target_gcs_conn_id=target_gcp_connector,
        airflow_files_var_set='NA',
        airflow_present_files_var_set='NA',
        Full_load = Full_load,
        reprocess_history=True,
        archive_project = bq_target_project,
        target_prefix = base_archive_location,
        #running_env = ENV
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
        schema_object='{schema_location}{date}/l1_schema_transunion.json'.format(schema_location=base_schema_location,
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
                                 source='transunion',
                                 mode='SET')


    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gold_dataset_{table}'.format(table=gld_table),
        dataset_id=gld_dataset,
        project_id=bq_target_project,
        bigquery_conn_id=bq_gcp_connector
    )

    gold_data = TransunionGoldFileHitIndicatorOperator(
        task_id='gold_data_{source}'.format(source=source),
        project=bq_target_project,
        source_dataset=target_dataset,
        source_table='{dataset_name}.{table_name}'.format(dataset_name=target_dataset,
                                                          table_name=target_table),
        destination_dataset=gld_dataset,
        destination_table=gld_table,
        google_cloud_storage_conn_id=bq_gcp_connector)

    archive_failure_notify_task = PythonOperator(
        task_id='archive_failure_notify_task',
        python_callable=archive_failure_notify_task,
        op_kwargs= {"file_list":transunion_archive_failed_files})



    create_dataset >> land_data >> refine_data >> audit_data >> bit_set >> create_gold_dataset >> gold_data >> archive_failure_notify_task

   






