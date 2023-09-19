import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import configuration
from airflow.contrib.operators import gcs_to_bq
from plugins.operators.jm_APIAuditOperator import APIAuditOperator

# Import custom operators
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_mixpanel_people_to_gcs import MixpanelPeopleToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.operators.empty import EmptyOperator

#variables section
AIRFLOW_ENV = Variable.get('ENV')



if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    prefix_folder = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    prefix_folder = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    base_gcs_folder = 'l1'
    prefix_folder = ''
    mssql_connection_target = 'instDW_PROD'

#Folder Details for landing and Datase Details for refinement
source = 'mixpanel_people'
source_abbr = 'mixpanel_people'
base_file_location = '{base}_norm/{source_abbr}/'.format(base=base_gcs_folder,source_abbr=source_abbr)
base_schema_location = '{base}_schema/{source_abbr}/'.format(base=base_gcs_folder,source_abbr=source_abbr)
audit_filename = '{base_gcs_folder}_audit/{source}/'.format(source=source_abbr.lower(),base_gcs_folder=base_gcs_folder)
metadata_filename = '{base_gcs_folder}/{source}/'.format(source=source_abbr.lower(),base_gcs_folder=base_gcs_folder)
refine_dataset = '{base_gcs_folder}ref_{source}'.format(source=source,base_gcs_folder=prefix_folder)
refine_table = 't_mixpanel_people'

#DAG default args
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2020, 7, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

#Creating DAG Object
dag = DAG(
        'Mixpanel_People_dag',
        schedule_interval= '0 6 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args)

#Task - create empty dataset if the dataset is not present
create_dataset_ref = BigQueryCreateEmptyDatasetOperator(
                                                        task_id='check_for_refine_dataset_{source}'.format(source=source),
                                                        dataset_id='{dataset}'.format(dataset=refine_dataset),
                                                        bigquery_conn_id=bq_gcp_connector,
                                                        dag=dag
                                                        )

bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET',
                                 dag=dag)


try:
    MIXPANEL_PEOPLE_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
except:
    Variable.set(source + '_gcs_to_bq', 'True')

try:
    CHECK_HISTORY = Variable.get(source + '_check')
except:
    Variable.set(source + '_check', 'True')
    CHECK_HISTORY = 'True'

#Task - Land Data from API to Google CLoud Storage#
land_data =MixpanelPeopleToGoogleCloudStorageOperator(
                                                        task_id='landing_data_{source}'.format(source=source),
                                                        api_connection_id='MixpanelPeopleAPI',
                                                        google_cloud_storage_connection_id=base_gcp_connector,
                                                        project = project,
                                                        target_gcs_bucket=base_bucket,
                                                        target_env=AIRFLOW_ENV.upper(),
                                                        base_gcs_folder=base_gcs_folder,
                                                        source=source,
                                                        source_abbr=source_abbr,
                                                        history_check=CHECK_HISTORY,
                                                        dag=dag
                                                        )
audit_data_lnd = APIAuditOperator(task_id='audit_l1_data_{source}'.format(source=source),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=refine_dataset,
                                         base_gcs_folder=base_gcs_folder,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                         metadata_filename='{metadata_filename}{date_nodash}/l1_metadata_mp_people.json'.format(
                                                                                              metadata_filename=metadata_filename,
                                                                                              date_nodash="{ds_nodash}"),
                                         audit_filename='{audit_filename}{date_nodash}/l1_audit_mp_people.json'.format(
                                                                                              audit_filename=audit_filename,
                                                                                              date_nodash="{ds_nodash}"),
                                         check_landing_only=True,
                                         table=refine_table,
                                         dag= dag)

MIXPANEL_PEOPLE_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
refine_data = GoogleCloudStorageToBigQueryOperator(
                                                    task_id='refine_data_{source}'.format(source=source),
                                                    bucket=base_bucket,
                                                    source_objects=[
                                                        '{base_file_location}{date}/l1_data_mp_*'.format(
                                                            base_file_location=base_file_location,
                                                            date="{{ ds_nodash }}")],
                                                    destination_project_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                                                                                                project=bq_target_project,
                                                                                                                dataset=refine_dataset,
                                                                                                                table=refine_table,
                                                                                                                date="{{ ds_nodash }}"),
                                                    schema_fields=None,
                                                    schema_object='{schema_location}{date}/l1_schema_mp_people.json'.format(
                                                                                                                schema_location=base_schema_location,
                                                                                                                date="{{ ds_nodash }}"),
                                                    source_format='NEWLINE_DELIMITED_JSON',
                                                    compression='NONE',
                                                    create_disposition='CREATE_IF_NEEDED',
                                                    skip_leading_rows=0,
                                                    write_disposition='WRITE_TRUNCATE',
                                                    max_bad_records=0,
                                                    bigquery_conn_id=bq_gcp_connector,
                                                    google_cloud_storage_conn_id=base_gcp_connector,
                                                    schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                                                    src_fmt_configs=None,
                                                    autodetect=True,
                                                    gcs_to_bq=MIXPANEL_PEOPLE_BQ_PROCESS,
                                                    dag=dag)
if MIXPANEL_PEOPLE_BQ_PROCESS.upper() == 'TRUE':
    audit_data_refine = APIAuditOperator(task_id='audit_ref_data_{source}'.format(source=source),
                                         bucket=base_bucket,
                                         project=bq_target_project,
                                         dataset=refine_dataset,
                                         base_gcs_folder=base_gcs_folder,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                         metadata_filename='{metadata_filename}{date_nodash}/l1_metadata_mp_people.json'.format(
                                             metadata_filename=metadata_filename,
                                             date_nodash="{ds_nodash}"),
                                         audit_filename='{audit_filename}{date_nodash}/l1_audit_mp_people.json'.format(
                                             audit_filename=audit_filename,
                                             date_nodash="{ds_nodash}"),
                                         check_landing_only=False,
                                         table=refine_table,
                                         dag=dag)
else:
    audit_data = EmptyOperator(task_id='health_{source}'.format(source=source), dag=dag)


create_dataset_ref >> land_data >>audit_data_lnd>> refine_data>>audit_data_refine>>bit_set




