import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_mixpanel_to_gcs import MixpanelToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_mixpanel_gld import MixpanelDigitalDashboardUsageCountSummary
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json


print(sys.path)

file_path = os.path.join(configuration.get('core', 'dags_folder'),
                                 r'ini/mixpanel_digital_dashboard_schema.json')
#file_path = file_path.replace('dags/', '')
with open(file_path, 'rb') as inputJSON:
    schemaList = json.load(inputJSON)


ts = calendar.timegm(time.gmtime())
logging.info(ts)


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



source = 'mixpanel'
source_abbr = 'mp_coded'
base_file_location = '{base}_norm/{source_abbr}/'.format(base=base_gcs_folder,source_abbr=source_abbr)
base_schema_location = '{base}_schema/{source_abbr}/'.format(base=base_gcs_folder,source_abbr=source_abbr)
gold_file_location = '{base}_gld/{source}_gld/UsageCountSummary/'.format(base=base_gcs_folder,source=source)
gold_schema_location = '{base}_schema/{source}_gld/UsageCountSummary/mixpanel_digital_dashboard_schema.json'.format(base=base_gcs_folder,source=source)
#refine_dataset = 'GCP_ClComp_jm_ref_{source}'.format(source=source)
refine_dataset = '{prefix}ref_{source}'.format(prefix=prefix_folder,source=source)
audit_filename = '{base_gcs_folder}_audit/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)
metadata_filename = '{base_gcs_folder}/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)

refine_table = 't_mixpanel_events_all'
gold_dataset = '{base_gcs_folder}gld_digital_dashboard'.format(base_gcs_folder=prefix_folder)
gold_table = 't_Mixpanel_UsageCountSummary'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
        'Mixpanel_dag',
        schedule_interval='0 6 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset_ref = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_refine_dataset_{source}'.format(source=source),
        dataset_id='{dataset}'.format(
            dataset=refine_dataset),
        bigquery_conn_id=bq_gcp_connector)

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')


    try:
        MIXPANEL_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
    except:
        Variable.set(source + '_gcs_to_bq', 'True')

    try:
        CHECK_HISTORY = Variable.get(source + '_check')
    except:
        Variable.set(source + '_check', 'True')
        CHECK_HISTORY = 'True'

    land_data = MixpanelToGoogleCloudStorageOperator(
        task_id='landing_data_{source}'.format(source=source),
        api_connection_id='MixpanelAPI',
        google_cloud_storage_conn_id=base_gcp_connector,
        project = project,
        target_gcs_bucket=base_bucket,
        target_env=AIRFLOW_ENV.upper(),
        base_gcs_folder=base_gcs_folder,
        source=source,
        source_abbr=source_abbr,
        history_check=CHECK_HISTORY)

    audit_data_lnd = APIAuditOperator(task_id='audit_l1_data_{source}'.format(source=source),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=refine_dataset,
                                         base_gcs_folder=base_gcs_folder,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                      metadata_filename='{metadata_filename}{date_nodash}/l1_metadata_mp_cd_all.json'.format(
                                          metadata_filename=metadata_filename,
                                          date_nodash="{ds_nodash}"),
                                      audit_filename='{audit_filename}{date_nodash}/l1_audit_mp_cd_all.json'.format(
                                          audit_filename=audit_filename,
                                          date_nodash="{ds_nodash}"),
                                         check_landing_only=True,
                                         table=refine_table)

    MIXPANEL_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')

    refine_data = GoogleCloudStorageToBigQueryOperator(
        task_id='refine_data_{source}'.format(source=source),
        bucket=base_bucket,
        source_objects=[
            '{base_file_location}{date}/l1_norm_mp_*'.format(
                base_file_location=base_file_location,
                date="{{ ds_nodash }}")],
        destination_project_dataset_table='{project}.{dataset}.{table}${date}'.format(
            project=bq_target_project,
            dataset=refine_dataset,
            table=refine_table,
            date="{{ ds_nodash }}"),
        schema_fields=None,
        schema_object='{schema_location}{date}/l1_schema_mp_cd_all.json'.format(
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
        gcs_to_bq=MIXPANEL_BQ_PROCESS)


    if MIXPANEL_BQ_PROCESS.upper() == 'TRUE':
        audit_data_refine = APIAuditOperator( task_id='audit_ref_data_{source}'.format(source=source),
                                       bucket=base_bucket,
                                       project=bq_target_project,
                                       dataset=refine_dataset,
                                       base_gcs_folder=base_gcs_folder,
                                       target_gcs_bucket=base_bucket,
                                       google_cloud_storage_conn_id = base_gcp_connector,
                                       source_abbr=source_abbr,
                                       source=source,
                                       metadata_filename='{metadata_filename}{date_nodash}/l1_metadata_mp_cd_all.json'.format(
                                                           metadata_filename=metadata_filename,
                                                            date_nodash = "{ds_nodash}"),
                                       audit_filename='{audit_filename}{date_nodash}/l1_audit_mp_cd_all.json'.format(
                                                           audit_filename=audit_filename,
                                                            date_nodash = "{ds_nodash}"),
                                       check_landing_only=False,
                                       table=refine_table)
    else:
        audit_data_refine = EmptyOperator(task_id='audit_ref_data_{source}'.format(source=source))

    create_dataset_gld = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gld_dataset_{source}'.format(source=source),
        dataset_id='{dataset}'.format(
            dataset=gold_dataset),
        bigquery_conn_id=bq_gcp_connector)

    gen_gold_data = MixpanelDigitalDashboardUsageCountSummary(task_id='generate_{source}_digital_dashboard'.format(source=source),
                                   google_cloud_storage_conn_id=base_gcp_connector,
                                   project=bq_target_project,
                                   source_dataset=refine_dataset,
                                   source_table=refine_table,
                                   destination_dataset=gold_dataset,
                                   destination_table=gold_table,
                                   target_gcs_bucket = base_bucket,
                                   file_name = gold_file_location,
                                    schema_filename=gold_schema_location,
                                    schema_data=schemaList)

    load_gold_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_{source}_digital_dashboard'.format(source=source),
        bucket=base_bucket,
        source_objects=[
            '{gold_file_location}{date}/gld_*'.format(
                gold_file_location=gold_file_location,
                date="{{ ds_nodash }}")],
        destination_project_dataset_table='{project}.{dataset}.{table}${date}'.format(
            project=bq_target_project,
            dataset=gold_dataset,
            table=gold_table,
            date="{{ ds_nodash }}"),
        schema_fields=schemaList,
        source_format='NEWLINE_DELIMITED_JSON',
        compression='NONE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=0,
        bigquery_conn_id=bq_gcp_connector,
        google_cloud_storage_conn_id=base_gcp_connector,
        schema_update_options=[],
        src_fmt_configs=None,
        autodetect=True,
        gcs_to_bq=MIXPANEL_BQ_PROCESS)



    create_dataset_ref >> land_data >> audit_data_lnd >> refine_data >> audit_data_refine >> bit_set >> create_dataset_gld >>  gen_gold_data >> load_gold_data
