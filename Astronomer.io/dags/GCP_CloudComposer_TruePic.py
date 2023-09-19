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
from plugins.operators.jm_TruePic_to_gcs import TruePicToGCS
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json


ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')

source = 'TruePic'
source_abbr = 'truepic'

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_'
    api_connection_id = 'TruePic_ID'
    access_connection_id = 'TruePic_access_ID'
    event_connection_id = 'TruePic_event_ID'
elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_'
    api_connection_id = 'TruePic_ID'
    access_connection_id = 'TruePic_access_ID'
    event_connection_id = 'TruePic_event_ID'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector ='prod_edl'
    bq_target_project = 'prod-edl'
    base_gcs_folder = ''
    api_connection_id = 'TruePic_ID'
    access_connection_id = 'TruePic_access_ID'
    event_connection_id = 'TruePic_event_ID'

events_table = 'events'
additional_tables = ['events_timeline', 'events_photos']
additional_columns = ['timeline', 'photos']
base_file_location = '{base}l1/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)
base_normalized_location = '{base}l1_norm/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)
base_schema_location = '{base}l1_schema/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)

refine_dataset = '{prefix}ref_{source}'.format(prefix=base_gcs_folder,
                                               source=source_abbr.lower())
audit_filename = '{base_gcs_folder}l1_audit/{source}'.format(source=source_abbr.lower(),
                                                             base_gcs_folder=base_gcs_folder)
metadata_filename = '{base_gcs_folder}l1/{source}'.format(source=source_abbr.lower(),
                                                          base_gcs_folder=base_gcs_folder)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'mgiddings@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        'TruePic_dag',
        # schedule_interval='0 6 * * *',  # "@daily",#dt.timedelta(days=1), #
        schedule_interval='@hourly',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    MAX_EVENTS_TO_PROCESS = Variable.get('TruePic_max_events', default_var='5000')

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')

    get_data = TruePicToGCS(task_id='get_{source}_data'.format(source=source_abbr),
                            base_gcs_folder=base_file_location,
                            google_cloud_storage_conn_id=base_gcp_connector,
                            target_gcs_bucket=base_bucket,
                            table=events_table,
                            additional_tables=additional_tables,
                            additional_column_names=additional_columns,
                            normalized_location=base_normalized_location,
                            schema_location=base_schema_location,
                            history_check='False',
                            metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                base=metadata_filename,
                                table=events_table,
                                date="{{ ds_nodash }}",
                                source=source_abbr),
                            additional_metadata_filenames=[
                                '{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                    base=metadata_filename,
                                    table=additional_tables[0],
                                    date="{{ ds_nodash }}",
                                    source=source_abbr),
                                '{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                    base=metadata_filename,
                                    table=additional_tables[1],
                                    date="{{ ds_nodash }}",
                                    source=source_abbr),
                                ],
                            api_connection_id=api_connection_id,
                            access_connection_id=access_connection_id,
                            event_connection_id=event_connection_id,
                            api_configuration={},
                            target_env='DEV',
                            max_events=MAX_EVENTS_TO_PROCESS,
                            begin_pull="{{ ds }}",
                            end_pull="{{ tomorrow_ds }}")

    # L1 audit - check source record count against landed record count in L1
    landing_audit = APIAuditOperator(task_id='landing_audit_{source}'.format(source=source_abbr),
                                     bucket=base_bucket,
                                     project=project,
                                     dataset=refine_dataset,
                                     table='t_{table}'.format(table=events_table),
                                     base_gcs_folder=None,
                                     target_gcs_bucket=base_bucket,
                                     google_cloud_storage_conn_id=base_gcp_connector,
                                     source_abbr=source_abbr,
                                     source=source,
                                     metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                         base=metadata_filename,
                                         table=events_table,
                                         date="{{ ds_nodash }}",
                                         source=source_abbr),
                                     audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                         base=audit_filename,
                                         table=events_table,
                                         date="{{ ds_nodash }}",
                                         source=source_abbr),
                                     check_landing_only=True)

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id='check_for_dataset_{table}'.format(table=events_table),
                                                        dataset_id='{prefix_value}ref_{source_abbr}'.format(source_abbr=source_abbr,
                                                                                                            prefix_value=base_gcs_folder),
                                                        bigquery_conn_id=bq_gcp_connector)

    refine_data = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source_abbr}_{table}'.format(source_abbr=source_abbr,
                                                                   table=events_table),
                bucket=base_bucket,
                source_objects=['{location_base}{table}/{date}/l1_norm_{source}_{table}_*'.format(
                    location_base=base_normalized_location,
                    source=source_abbr,
                    table=events_table,
                    date="{{ ds_nodash }}")],
                destination_project_dataset_table='{project}.{prefix_value}ref_{source_abbr}.{table}${date}'.format(project=bq_target_project,
                                                                                                                    source_abbr=source_abbr,
                                                                                                                    table='t_{table}'.format(table=events_table),
                                                                                                                    date="{{ ds_nodash }}",
                                                                                                                    prefix_value=base_gcs_folder),
                schema_fields=[],
                schema_object='{schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                    schema_location=base_schema_location,
                    table=events_table,
                    date="{{ ds_nodash }}",
                    source=source_abbr),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs={},
                autodetect=False)

    refine_audit = APIAuditOperator(task_id='refine_audit_{table}'.format(table=events_table),
                                    bucket=base_bucket,
                                    project=bq_target_project,
                                    dataset=refine_dataset,
                                    base_gcs_folder=None,
                                    target_gcs_bucket=base_bucket,
                                    google_cloud_storage_conn_id=base_gcp_connector,
                                    source_abbr=source_abbr,
                                    source=source,
                                    metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                        base=metadata_filename,
                                        table=events_table,
                                        date="{{ ds_nodash }}",
                                        source=source_abbr),
                                    audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                        base=audit_filename,
                                        table=events_table,
                                        date="{{ ds_nodash }}",
                                        source=source_abbr),
                                    check_landing_only=False,
                                    table='t_{table}'.format(table=events_table))

    for additional_table in additional_tables:
        # L1 audit - check source record count against landed record count in L1
        additional_landing_audit = APIAuditOperator(task_id='landing_audit_{source}_{table}'.format(source=source_abbr,
                                                                                                    table=additional_table),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=refine_dataset,
                                         table='t_{table}'.format(table=additional_table),
                                         base_gcs_folder=None,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                         metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                             base=metadata_filename,
                                             table=additional_table,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                             base=audit_filename,
                                             table=additional_table,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         check_landing_only=True)

        additional_refine_data = GoogleCloudStorageToBigQueryOperator(
                    task_id='refine_data_{source_abbr}_{table}'.format(source_abbr=source_abbr,
                                                                       table=additional_table),
                    bucket=base_bucket,
                    source_objects=['{location_base}{table}/{date}/l1_norm_{source}_{table}_*'.format(
                        location_base=base_normalized_location,
                        source=source_abbr,
                        table=additional_table,
                        date="{{ ds_nodash }}")],
                    destination_project_dataset_table='{project}.{prefix_value}ref_{source_abbr}.{table}${date}'.format(project=bq_target_project,
                                                                                                                        source_abbr=source_abbr,
                                                                                                                        table='t_{table}'.format(table=additional_table),
                                                                                                                        date="{{ ds_nodash }}",
                                                                                                                        prefix_value=base_gcs_folder),
                    schema_fields=[],
                    schema_object='{schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                        schema_location=base_schema_location,
                        table=additional_table,
                        date="{{ ds_nodash }}",
                        source=source_abbr),
                    source_format='NEWLINE_DELIMITED_JSON',
                    compression='NONE',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=0,
                    write_disposition='WRITE_TRUNCATE',
                    max_bad_records=0,
                    bigquery_conn_id=bq_gcp_connector,
                    google_cloud_storage_conn_id=base_gcp_connector,
                    schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                    src_fmt_configs={},
                    autodetect=False)

        additional_refine_audit = APIAuditOperator(task_id='refine_audit_{table}'.format(table=additional_table),
                                                 bucket=base_bucket,
                                                 project=bq_target_project,
                                                 dataset=refine_dataset,
                                                 base_gcs_folder=None,
                                                 target_gcs_bucket=base_bucket,
                                                 google_cloud_storage_conn_id=base_gcp_connector,
                                                 source_abbr=source_abbr,
                                                 source=source,
                                                 metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                                    base=metadata_filename,
                                                    table=additional_table,
                                                    date="{{ ds_nodash }}",
                                                    source=source_abbr),
                                                 audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                                    base=audit_filename,
                                                    table=additional_table,
                                                    date="{{ ds_nodash }}",
                                                    source=source_abbr),
                                                 check_landing_only=False,
                                                 table='t_{table}'.format(table=additional_table))

        get_data >> landing_audit >> create_dataset >> refine_data >> refine_audit >> additional_landing_audit >> additional_refine_data >> additional_refine_audit >> bit_set
