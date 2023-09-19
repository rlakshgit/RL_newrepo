import logging 
from datetime import timedelta, datetime
import time
import os
import json
import calendar
from airflow import DAG
from airflow import configuration
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_pbi_audit_log import PBIAuditLogOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_BQSchemaGenerationOperator import BQSchemaGenerationOperator


ts = calendar.timegm(time.gmtime())
logging.info(ts)

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-internal-data-upload'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    folder_prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-internal-data-upload'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    folder_prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-internal-data-upload'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    base_gcs_folder = 'l1'
    folder_prefix = ''
    

source = 'PowerBI-Auditlogs' 
source_abbr = 'pbi_audit_logs'
source_backup = 'PowerBI-Auditlogs-Backup'
table = 'audit_log'

base_file_location = '{source}/'.format(source=source)
base_normalized_location = '{folder_prefix}l1_norm/{source}/'.format(folder_prefix = folder_prefix, source=source_abbr)
base_schema_location = '{folder_prefix}l1_schema/{source}/'.format(folder_prefix = folder_prefix, source=source_abbr)
metadata_storage_location = '{base_gcs_folder}_metadata/{source}/'.format(source=source_abbr, base_gcs_folder=base_gcs_folder)
base_audit_location = '{folder_prefix}l1_audit/{source}/'.format(folder_prefix = folder_prefix, source=source_abbr)
base_backup_location = '{source}/'.format(source=source_backup)

refine_dataset = '{prefix}ref_{source}'.format(prefix=folder_prefix, source=source_abbr)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,12,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retires': 2,
    'retry_delay': timedelta(minutes = 5)
}

confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'PBI_AuditLogs', r'ini', r"dags_PBI_auditLogs_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())

with DAG(
    'PBI_audit_log_dag',
    schedule_interval = '0 14 1 * *' ,
    catchup = True, 
    max_active_runs = 1,
    default_args = default_args,
) as dag: 

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = "check_for_dataset_{source}".format(source=source_abbr),
        dataset_id = refine_dataset,
        bigquery_conn_id = bq_gcp_connector
    )


    generate_schema = BQSchemaGenerationOperator(
        task_id = 'generate_schema_{source}'.format(source=source_abbr),
        project = project,
        config_file = confJSON,
        source = source_abbr,
        table = table,
        target_gcs_bucket = base_bucket,
        schema_location = base_schema_location,
        google_cloud_storage_conn_id = base_gcp_connector
    )

    stage_data = PBIAuditLogOperator(task_id = 'stage_files_{source}'.format(source = source_abbr),
                                    target_gcs_bucket=base_bucket,
                                    source = source,
                                    source_abbr = source_abbr,
                                    table = table,
                                    config_file = confJSON,
                                    metadata_storage_location = metadata_storage_location,
                                    base_file_location = base_file_location,
                                    base_normalized_location = base_normalized_location,
                                    base_backup_location = base_backup_location,
                                    google_cloud_storage_conn_id = base_gcp_connector)


    load_data_to_BQ = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_{source}'.format(source=source_abbr),
        bucket = base_bucket,
        source_objects=['{base_normalized_location}{date}*.csv'.format(
                base_normalized_location=base_normalized_location, api_name=table, date='{{ ds_nodash }}')],
        destination_project_dataset_table = '{project}.{dataset}.t_{table}${date}'.format(
                project = bq_target_project,
                dataset = refine_dataset,
                table = table,
                date = '{{ ds_nodash }}' ),
        schema_object = base_schema_location +'{api_name}/{date}/l1_schema_{source}_{api_name}.json'.format(
                api_name=table,
                source = source_abbr,
                date='{{ ds_nodash }}'),
        source_format = 'CSV',
        compression = 'NONE',
        ignore_unknown_values = True,
        allow_quoted_newlines = True,
        create_disposition = 'CREATE_IF_NEEDED',
        skip_leading_rows = 1,
        write_disposition = 'WRITE_APPEND',
        max_bad_records=0,
        bigquery_conn_id=bq_gcp_connector,
        google_cloud_storage_conn_id=base_gcp_connector,
        schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION']
        )

    bq_load_audit_check = APIAuditOperator(
            task_id='bq_load_audit_check_{source}'.format(source=source_abbr),
            bucket=base_bucket,
            project=bq_target_project,
            dataset=refine_dataset,
            base_gcs_folder= base_gcs_folder,
            target_gcs_bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            source=source,
            source_abbr=source_abbr,
            metadata_filename='{location_base}{date}/l1_metadata_{source}.json'.format(
                location_base=metadata_storage_location,
                source = source_abbr,
                date='{ds_nodash}'),
            audit_filename='{location_base}{date}/l1_audit_{source}.json'.format(
                location_base=base_audit_location,
                source=source_abbr,
                date='{ds_nodash}'),
            table='t_{table}'.format(table=table),
            check_landing_only=True)

    
    create_dataset >> generate_schema >> stage_data >> load_data_to_BQ >> bq_load_audit_check



    
