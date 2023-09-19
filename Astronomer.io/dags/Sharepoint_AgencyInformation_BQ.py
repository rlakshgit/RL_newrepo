import logging
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_sharepointtogcs import sharepointtogcsoperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_TeamsAuditOperator import TeamsAuditOperator
ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1,14),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'

elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'

elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''


refine_dataset = folder_prefix + 'ref_agency_information'
with DAG(
        'agency_information_dag',
        schedule_interval='0 12-18 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    history_check = 'False'
    try:
        SOURCES_LIST = Variable.get('agency_information_list').split(',')
    except:
        Variable.set('agency_information_list','jmis./Shared Documents/JMIS Agency Client Information/JMIS Active Clients,lunar./Shared Documents/Lunar Agency Client Information/Active Client List,'
                                               'wexler./Shared Documents/Wexler Agency Client Information/Active Client and New Submission Lists')
        #Variable.set('agency_information_list', 'jmis.JMIS Active Clients,lunar,wexler')
        SOURCES_LIST = Variable.get('agency_information_list').split(',')

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset',
        dataset_id=refine_dataset,
        project_id=bq_target_project,
        bigquery_conn_id=bq_gcp_connector
    )

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='agency_information',
                                 mode='SET')

    for source in SOURCES_LIST:
        source =source.split('.')
        source_abbr = source[0]
        file_loc = source[1]


        base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source_abbr)
        norm_file_location = folder_prefix + 'l1_norm/{source_abbr}/'.format(source_abbr=source_abbr)
        base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
        base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)


        Connect_to_sharepoint = sharepointtogcsoperator(
        task_id='{source}_information_to_gcs'.format(source =source_abbr),
        google_storage_connection_id = base_gcp_connector,
            source=source_abbr.lower(),
            history_check = history_check,
            base_gcs_folder = folder_prefix + 'l1/',
        sharepoint_connection ='{source}_sharepoint_connection'.format(source =source_abbr),
        source_file_url = file_loc,
        gcs_destination_path ='{base_file_location}{date}/'.format(base_file_location=base_file_location,date="{{ ds_nodash }}"),
        destination_bucket = base_bucket
        )

        refine_table = 't_{source}_active_clients'.format(source = source_abbr.lower())

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{source}_active_clients'.format(source=source_abbr.lower()),
            bucket=base_bucket,
            source_objects=[
                '{base_file_location}{date}/l1_norm_{source}_active_clients.json'.format(
                    base_file_location=norm_file_location,
                    source=source_abbr.lower(),
                    date="{{ ds_nodash }}")],
            destination_project_dataset_table='{project}.{dataset}.{table}'.format(
                project=bq_target_project,
                dataset=refine_dataset,
                table=refine_table),
            schema_fields=None,
            schema_object='{schema_location}{date}/l1_schema_{source}_active_clients.json'.format(
                schema_location=base_schema_location,
                source =source_abbr.lower(),
                date="{{ ds_nodash }}"),
            source_format='NEWLINE_DELIMITED_JSON',
            compression='NONE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=0,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=0,
            bigquery_conn_id=bq_gcp_connector,
            google_cloud_storage_conn_id=base_gcp_connector,
            #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
            src_fmt_configs=None,
            autodetect=True)

        audit_data_refine = TeamsAuditOperator(task_id='audit_ref_data_{source}'.format(source=source_abbr.lower()),
                                             bucket=base_bucket,
                                             project=bq_target_project,
                                             dataset=refine_dataset,
                                             base_gcs_folder=folder_prefix+'l1',
                                             target_gcs_bucket=base_bucket,
                                             google_cloud_storage_conn_id=base_gcp_connector,
                                             source_abbr=source_abbr,
                                             schema_object='{schema_location}{date}/l1_schema_{source}_report_submission.json'.format(
                                                 schema_location=base_schema_location,
                                                 source=source,
                                                 date="{{ ds_nodash }}"),
                                             source=source,
                                             metadata_filename='{base_file_location}{date}/l1_metadata_{source}_active_clients.json'.format(
                                                 base_file_location=base_file_location, source=source_abbr.lower(), date="{{ ds_nodash }}"),
                                             audit_filename='{base_file_location}{date}/l1_metadata_{source}_active_clients.json'.format(
                                                 base_file_location=base_file_location, source=source_abbr.lower(), date="{{ ds_nodash }}"),
                                             check_landing_only=False,
                                             table=refine_table)

        if source_abbr.lower() == 'wexler':
            refine_table = 't_{source}_report_submission'.format(source=source_abbr.lower())

            refine_data_submission_report = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source}_repot_submission'.format(source=source_abbr.lower()),
                bucket=base_bucket,
                source_objects=[
                    '{base_file_location}{date}/l1_norm_{source}_report_submission.json'.format(
                        base_file_location=norm_file_location,
                        source=source_abbr.lower(),
                        date="{{ ds_nodash }}")],
                destination_project_dataset_table='{project}.{dataset}.{table}'.format(
                    project=bq_target_project,
                    dataset=refine_dataset,
                    table=refine_table),
                schema_fields=None,
                schema_object='{schema_location}{date}/l1_schema_{source}_report_submission.json'.format(
                    schema_location=base_schema_location,
                    source=source_abbr.lower(),
                    date="{{ ds_nodash }}"),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs=None,
                autodetect=True)

            audit_data_refine_submission_report = TeamsAuditOperator(task_id='audit_ref_report_submission_data_{source}'.format(source=source_abbr.lower()),
                                                 bucket=base_bucket,
                                                 project=bq_target_project,
                                                 dataset=refine_dataset,
                                                 base_gcs_folder=folder_prefix + 'l1',
                                                 target_gcs_bucket=base_bucket,
                                                 google_cloud_storage_conn_id=base_gcp_connector,
                                                 schema_object='{schema_location}{date}/l1_schema_{source}_report_submission.json'.format(
                                                                       schema_location=base_schema_location,
                                                                       source=source_abbr.lower(),
                                                                       date="{{ ds_nodash }}"),
                                                 source_abbr=source_abbr,
                                                 source=source,
                                                 metadata_filename='{base_file_location}{date}/l1_metadata_{source}_report_submission.json'.format(
                                                     base_file_location=base_file_location, source=source_abbr.lower(),
                                                     date="{{ ds_nodash }}"),
                                                 audit_filename='{base_file_location}{date}/l1_metadata_{source}_report_submission.json'.format(
                                                     base_file_location=base_file_location, source=source_abbr.lower(),
                                                     date="{{ ds_nodash }}"),
                                                 check_landing_only=False,
                                                 table=refine_table)
        else:
            refine_data_submission_report = EmptyOperator(task_id='refine_data_{source}_report_submission'.format(source=source_abbr.lower()))
            audit_data_refine_submission_report = EmptyOperator(task_id='audit_ref_report_submission_data_{source}'.format(source=source_abbr.lower()))

        create_dataset >> Connect_to_sharepoint >> refine_data_submission_report >> audit_data_refine_submission_report >> bit_set

        create_dataset >> Connect_to_sharepoint >> refine_data >>  audit_data_refine >> bit_set