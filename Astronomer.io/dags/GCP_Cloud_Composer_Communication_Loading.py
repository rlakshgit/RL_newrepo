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
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_shipping_staging import ShippingStagingOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.contrib.operators.bigquery_operator import  BigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_BQSchemaGenerationOperator import BQSchemaGenerationOperator
from airflow.models import Variable
import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')



if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    folder_prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    folder_prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    base_gcs_folder = 'l1'
    folder_prefix = ''

source='communication'
source_abbr = 'communication'
l2_storage_location = folder_prefix + 'l2/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)
staging_file_location = folder_prefix+'staging/{source}/'.format(source=source_abbr)
metadata_storage_location = '{base_gcs_folder}/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
refine_dataset = '{prefix}ref_zing_{source}'.format(prefix=folder_prefix,source=source)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,2,17),
    #'end_date': datetime(2022,6,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'smaguluri@jminsure.com',
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
}


confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'communication', r'ini', r"communication_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())

    communication_tables = confJSON.keys()

with DAG(
        'zing_communication_loading_dag',
        schedule_interval= '30 10 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source_abbr),
        dataset_id= refine_dataset,
        bigquery_conn_id=bq_gcp_connector)

    ##generate  schema

    for table in communication_tables:
        if '_' in table:
            patent_table = table.split('_')[0]
            task_sensor_core = ExternalTaskSensorAsync(
            task_id='check_{source}_{table}_landing_task'.format(source=source_abbr, table = table),
            external_dag_id='zing_communication_landing_dag',
            external_task_id='landing_audit_{patent_table}'.format(patent_table = patent_table),
            execution_delta=timedelta(minutes=30),
            timeout=7200)

        else: 
            task_sensor_core = ExternalTaskSensorAsync(
                task_id='check_{source}_{table}_landing_task'.format(source=source_abbr, table = table),
                external_dag_id='zing_communication_landing_dag',
                external_task_id='landing_audit_{table}'.format(table = table),
                execution_delta=timedelta(minutes=30),
                timeout=7200)

        generate_schema = BQSchemaGenerationOperator(
            task_id = 'generate_schema_{source}_{table}'.format(source = source , table = table ),
            project = project,
            config_file = confJSON,
            source = source_abbr,
            table = table,
            target_gcs_bucket = base_bucket,
            schema_location = base_schema_location,
            google_cloud_storage_conn_id=base_gcp_connector

        )

        stage_files = ShippingStagingOperator(task_id = 'stage_files_{source}_{table}'.format(source = source , table = table ),
                                                 source = source_abbr,
                                                 table = table,
                                                 config_file = confJSON,
                                                 google_cloud_storage_conn_id=base_gcp_connector,
                                                 l2_storage_location=l2_storage_location,
                                                 staging_file_location=staging_file_location,
                                                 target_gcs_bucket=base_bucket
                                                 )

        load_data_to_BQ = GoogleCloudStorageToBigQueryOperator(
                        task_id='load_data_{source}_{api_name}'.format(source=source_abbr,
                                                                       api_name=table),
                        bucket=base_bucket,
                        source_objects=[
                        '{staging_file_location}{table}/{date}/staging_{source}_{table}*.csv'.format(staging_file_location=staging_file_location,
                                                                                                       source = source_abbr,
                                                                                        table=table,
                                                                                        date="{{ ds_nodash }}")],
                        destination_project_dataset_table='{project}.{dataset}.t_{table}${date}'.format(
                        project=bq_target_project,
                            dataset = refine_dataset,                        
                        table=table,
                        #prefix=prefix,
                        date="{{ ds_nodash }}"),
                        #schema_fields=None,
                        schema_object='{base_schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                                                                                              base_schema_location=base_schema_location,
                                                                                              table=table,
                                                                                              source = source_abbr,
                                                                                              date="{{ ds_nodash }}"),
                        source_format='CSV',
                        compression='NONE',
                        ignore_unknown_values=True,
                        allow_quoted_newlines =True,
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=1,
                        write_disposition='WRITE_APPEND',
                        max_bad_records=0,
                        bigquery_conn_id=bq_gcp_connector,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION']
                        )

        bq_load_audit_check = APIAuditOperator(
            task_id='bq_load_audit_check_{source}_{api_name}'.format(source=source_abbr, api_name=table),
            bucket=base_bucket,
            project=bq_target_project,
            dataset=refine_dataset,
            base_gcs_folder= base_gcs_folder,
            target_gcs_bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            source=source,
            source_abbr=source_abbr.lower(),
            metadata_filename='{location_base}{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                location_base=metadata_storage_location,
                table=table,
                source = source_abbr,
                date="{ds_nodash}"),
            audit_filename='{location_base}{table}/{date}/l1_audit_{source}_{table}.json'.format(
                location_base=base_audit_location,
                source=source_abbr.lower(),
                table=table,
                date="{ds_nodash}"),
                #prefix=prefix),
            table='t_{table}'.format(table=table),
            check_landing_only=False
        )

        create_dataset >> task_sensor_core>> generate_schema >> stage_files >> load_data_to_BQ >> bq_load_audit_check