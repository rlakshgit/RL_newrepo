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
from plugins.operators.jm_ratabase_to_gcp_v1 import RatabaseToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_XMLAuditOperator import XMLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable


ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'mgiddings@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')
if AIRFLOW_ENV.lower() == 'dev':
    target_bucket = 'jm-edl-landing-wip'
    target_project = 'jm-dl-landing'
    target_gcp_connector = 'jm_landing_dev'
    source_gcp_connector = 'prod_edl'
    bq_gcp_connector = 'dev_edl'
    source_bucket = 'jm_prod_edl_lnd'
    source_project = 'prod-edl'
    bq_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    base_prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    target_bucket = 'jm-edl-landing-wip'
    target_project = 'jm-dl-landing'
    target_gcp_connector = 'jm_landing_dev'
    source_gcp_connector = 'prod_edl'
    bq_gcp_connector = 'qa_edl'
    source_bucket = 'jm_prod_edl_lnd'
    source_project = 'prod-edl'
    bq_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    base_prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    target_bucket = 'jm-edl-landing-prod'
    target_project = 'jm-dl-landing'
    target_gcp_connector = 'jm_landing_prod'
    source_gcp_connector = 'prod_edl'
    bq_gcp_connector = 'prod_edl'
    source_project = 'prod-edl'
    bq_project = 'prod-edl'
    source_bucket = 'jm_prod_edl_lnd'
    base_gcs_folder = 'l1'
    base_prefix = ''

source = 'ratabase_v1'

target_dataset = '{prefix}ref_{source}'.format(prefix=base_prefix,
                                                  source=source)

gold_target_dataset = '{prefix}gld_{source}'.format(prefix=base_prefix,
                                                  source=source)

base_file_location = '{folder_prefix}_norm/{source}/{date}/'.format(folder_prefix=base_gcs_folder,
                                                                    source=source,
                                                                    date="{{ ds_nodash }}")
base_schema_location = '{folder_prefix}_schema/{source}/{date}/'.format(folder_prefix=base_gcs_folder,
                                                                        source=source,
                                                                        date="{{ ds_nodash }}")
base_archive_location = '{folder_prefix}_archive/{source}/'.format(folder_prefix=base_gcs_folder,
                                                                   source=source)
base_audit_location = '{folder_prefix}_audit/{source}/{date}/'.format(folder_prefix=base_gcs_folder,
                                                                      source=source,
                                                                      date="{{ ds_nodash }}")
base_etl_folder = r'ETL/{source}/GOLD'.format(source=source.upper())

with DAG(
        '{source}_dag'.format(source=source),
        schedule_interval='30 6 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    for ratabase_type in ['CL', 'PA']:
        refined_table_list = []
        target_request_table = 'request_{type}'.format(type=ratabase_type.lower())
        target_response_table = 'response_{type}'.format(type=ratabase_type.lower())

        bit_set = CompletionOperator(task_id='set_source_bit_{type}'.format(type=ratabase_type.lower()),
                                     source='{source}_{type}'.format(source=source, type=ratabase_type.lower()),
                                     mode='SET')
        if ratabase_type == 'CL':
            base_staging_location = 'staging/RATABASE/cl_rating/'
            target_soaprequest_table = target_request_table.replace('request', 'soaprequest')
            target_soapresponse_table = target_response_table.replace('response', 'soapresponse')
            refined_table_list.append(target_soaprequest_table)
            refined_table_list.append(target_soapresponse_table)
            target_all_table = 'all'
            refined_table_list.append(target_all_table)
        else:
            base_staging_location = 'staging/RATABASE/rating/'
            target_soaprequest_table = 'NA'
            target_soapresponse_table = 'NA'

        refined_table_list.append(target_request_table)
        refined_table_list.append(target_response_table)

        source_file_list = '{source}_file_list_{type}'.format(source=source.lower(),
                                                              type=ratabase_type)

        file_list = RatabaseToGoogleCloudStorageOperator(
            task_id='get_staginglist_{source}_{ratabase_type}'.format(source=source.lower(),
                                                                      ratabase_type=ratabase_type),
            project=bq_project,
            source_file_list=[],
            target_file_base='NA',
            target_schema_base='NA',
            target_metadata_base='NA',
            target_archive_base='NA',
            source_bucket=source_bucket,
            target_bucket=target_bucket,
            source_staging_location=base_staging_location,
            target_dataset='',
            target_table='',
            gcs_conn_id=target_gcp_connector,
            airflow_files_var_set=source_file_list,
            reprocess_history=True
        )

        try:
            SOURCE_FILE_LIST = Variable.get(source_file_list)
        except:
            Variable.set(source_file_list, '[]')
            SOURCE_FILE_LIST = Variable.get(source_file_list)

        try:
            INITIALIZE_TABLE = Variable.get(source + '_initialize')
        except:
            Variable.set(source + '_initialize', 'True')
            INITIALIZE_TABLE = 'True'

        # We've gotten the list of files to land the data for either CL or PA.
        # Each one of those variants produces output to multiple refined tables in BigQuery.
        for ref_table in refined_table_list:
            land_data = RatabaseToGoogleCloudStorageOperator(
                task_id='land_{source}_{ratabase_type}_{table}'.format(source=source.lower(),
                                                                       ratabase_type=ratabase_type,
                                                                       table=ref_table),
                project=bq_project,
                source_file_list=SOURCE_FILE_LIST,
                target_file_base='{file_base}{ref_table}/l1_norm_data'.format(file_base=base_file_location,
                                                                              ref_table=ref_table) + '_{}',
                target_schema_base='{file_base}{ref_table}/l1_schema.json'.format(file_base=base_schema_location,
                                                                                  ref_table=ref_table),
                target_metadata_base='{file_base}{ref_table}/l1_metadata.txt'.format(file_base=base_file_location,
                                                                                     ref_table=ref_table),
                target_archive_base=base_archive_location,
                source_bucket=source_bucket,
                target_bucket=target_bucket,
                source_staging_location=base_staging_location,
                target_dataset=target_dataset,
                target_table=ref_table,
                gcs_conn_id=target_gcp_connector,
                airflow_files_var_set='NA',
                reprocess_history=True
            )

            create_dataset = BigQueryCreateEmptyDatasetOperator(
                task_id='check_for_dataset_{ratabase_type}_{table}'.format(ratabase_type=ratabase_type,
                                                                           table=ref_table),
                dataset_id=target_dataset,
                bigquery_conn_id=bq_gcp_connector
            )

            create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
                task_id='check_for_gold_dataset_{ratabase_type}_{table}'.format(ratabase_type=ratabase_type,
                                                                           table=ref_table),
                dataset_id=gold_target_dataset,
                bigquery_conn_id=bq_gcp_connector
            )

            # Put all files in directory for dag execution date, regardless of file timestamp.
            refine_data = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source}_{ratabase_type}_{table}'.format(source=source,
                                                                              ratabase_type=ratabase_type,
                                                                              table=ref_table),
                bucket=target_bucket,
                source_objects=['{location_base}{ref_table}/l1_norm_data_*'.format(location_base=base_file_location,
                                                                                   ref_table=ref_table)],
                destination_project_dataset_table='{project}.{dataset_name}.t_{table}${date}'.format(project=bq_project,
                                                                                                     dataset_name=target_dataset,
                                                                                                     table=ref_table,
                                                                                                     date="{{ ds_nodash }}"),
                schema_fields=[],
                schema_object='{schema_location}{ref_table}/l1_schema.json'.format(schema_location=base_schema_location,
                                                                                   ref_table=ref_table),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=target_gcp_connector,
                google_cloud_storage_conn_id=target_gcp_connector,
                schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs={},
                autodetect=True,
             #   trigger_rule="all_done"
             )

            audit_data = XMLAuditOperator(task_id='audit_health_{source}_{ratabase_type}_{table}'.format(source=source,
                                                                                                         ratabase_type=ratabase_type,
                                                                                                         table=ref_table),
                                          filename='{base_file}{ref_table}/l1_audit.json'.format(base_file=base_audit_location,
                                                                                                 ref_table=ref_table),
                                          bucket=target_bucket,
                                          l1_prefix='{location_base}{ref_table}/l1_norm_data_'.format(location_base=base_file_location,
                                                                                                      ref_table=ref_table),
                                          google_cloud_storage_conn_id=target_gcp_connector,
                                          google_cloud_bigquery_conn_id=target_gcp_connector,
                                          google_cloud_bq_config={'project': bq_project,
                                                                  'dataset': target_dataset,
                                                                  'table': 't_'+ref_table},
                                          bq_count_field='Full_File_Name')

            if ratabase_type != 'CL' or ref_table == 'soapresponse_cl':
                build_gold_data = EmptyOperator(task_id='gold_data_{source}_{ratabase_type}_{table}'.format(source=source,
                                                                                  ratabase_type=ratabase_type,
                                                                                  table=ref_table))

            else:

                with open(os.path.join(configuration.get('core', 'dags_folder'),
                                       base_etl_folder, 't_' + ref_table + '.sql')) as f:
                    read_file_data = f.readlines()

                    gld_sql = "\n".join(read_file_data)

                if INITIALIZE_TABLE.upper() == 'FALSE':
                    gld_sql = gld_sql.replace('{filter}', ''' where DATE(_PARTITIONTIME) = "{date}"''')
                else:
                    gld_sql = gld_sql.replace('{filter}','')



                gld_sql = gld_sql.replace('{date}', '{{ ds }}').replace('{dataset}',target_dataset).replace('{project}', bq_project)

                build_gold_data = BigQueryOperator(task_id='gold_data_{source}_{ratabase_type}_{table}'.format(source=source,
                                                                                  ratabase_type=ratabase_type,
                                                                                  table=ref_table),
                                                   sql=gld_sql,
                                                   destination_dataset_table='{project}.{target_dataset}.t_{table}${date}'.format(
                                                       project=bq_project,
                                                       table= ref_table,
                                                       target_dataset=gold_target_dataset,
                                                       source_abbr=source.lower(),
                                                       date="{{ ds_nodash }}"),
                                                   write_disposition='WRITE_TRUNCATE',
                                                   create_disposition='CREATE_IF_NEEDED',
                                                   gcp_conn_id=bq_gcp_connector,
                                                   allow_large_results=True,
                                                   use_legacy_sql=False,
                                                   time_partitioning={"type": "DAY"},
                                                   schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                          'ALLOW_FIELD_RELAXATION'])

            [file_list >> land_data >> create_dataset >> refine_data >> audit_data >> create_gold_dataset >> build_gold_data >> bit_set]