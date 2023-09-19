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
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from plugins.operators.jm_CompletionOperator import CompletionOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    prefix = ''
    mssql_connection_target = 'instDW_PROD'

####
source = 'Hubspot'
source_abbr = 'hubspot'

base_dataset_set = prefix+'ref_'+'{source_abbr}'.format(source_abbr=source_abbr.lower())

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime(2020, 12, 5),
    'start_date': datetime(2023, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Hubspot_Gold',
        schedule_interval='0 8 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:        
    base_etl_folder = r'ETL/{source}/GOLD'.format(source=source.upper())
    get_file_list = os.listdir(os.path.join(configuration.get('core', 'dags_folder'), base_etl_folder))
    targeted_filelist = []
    for f in get_file_list:
        if f.endswith('.sql'):
            targeted_filelist.append(f)
        else:
            continue

    target_dataset = 'gld_{source}'.format(source=source.lower())
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_{prefix}{dataset}'.format(dataset=target_dataset, prefix=prefix),
        dataset_id='{prefix}{dataset}'.format(
            dataset=target_dataset.lower(), prefix=prefix),
        bigquery_conn_id=bq_gcp_connector)
    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='hubspot_gold',
                                 mode='SET')
    task_sensor1 = ExternalTaskSensorAsync(
        task_id='check_hubspot_marketingEmails_load',
        external_dag_id='Hubspot_Loading_DAG',
        external_task_id='bq_load_audit_check_hubspot_marketingEmails',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    task_sensor2 = ExternalTaskSensorAsync(
        task_id='check_hubspot_emailEvents_load',
        external_dag_id='Hubspot_Loading_DAG',
        external_task_id='bq_load_audit_check_hubspot_emailEvents',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    try:
        INITIALIZE_TABLE = Variable.get(source + '_initialize')
    except:
        Variable.set(source + '_initialize', 'True')
        INITIALIZE_TABLE = 'True'

    for target_f in targeted_filelist:
        source_string = '{source}_'.format(source=source.lower())
        target_f_id = target_f.replace('.sql', '').replace(source_string, '')
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               base_etl_folder, target_f)) as f:
            read_file_data = f.readlines()

        # Get the first line to read the partition field to use for initialization
        partition_field = read_file_data[0].strip().replace('--partition_field:', '').replace(' ', '')

        cleaned_file_data = "\n".join(read_file_data)

        # See if there is an input (matching file name) that needs to provide data to main query
        try:
            INPUT_QUERY = Variable.get(source + '_' + target_f_id + '_input')
        except:
            Variable.set(source + '_' + target_f_id + '_input', ' ')
            INPUT_QUERY = ''

            # If not initializing the table, use partition on source to pull data
        if INITIALIZE_TABLE == 'False' or len(partition_field) == 0:
            cleaned_file_data = cleaned_file_data + ''' where DATE(_PARTITIONTIME) = "{date}"'''

            if 'emailevents' in target_f.lower():
                cleaned_file_data = cleaned_file_data.replace('{prefix}', prefix).replace('{date}', '{{ ds }}').replace(
                    '{dataset}', base_dataset_set).replace('{project}', bq_target_project)

            else:
                cleaned_file_data = cleaned_file_data.replace('{prefix}', prefix).replace('{date}', '{{ ds }}').replace(
                '{dataset}', base_dataset_set).replace('{project}', bq_target_project)

            build_gold_data = BigQueryOperator(task_id='hubspot_gld_{file}'.format(file=target_f_id),
                                               sql=cleaned_file_data,
                                               destination_dataset_table='{project}.{prefix}{target_dataset}.t_{source_abbr}_{file_id}${date}'.format(
                                                   project=bq_target_project,
                                                   file_id=target_f_id.lower(),
                                                   prefix=prefix,
                                                   target_dataset=target_dataset,
                                                   source_abbr=source_abbr.lower(),
                                                   date="{{ ds_nodash }}"),
                                               write_disposition='WRITE_TRUNCATE',
                                               create_disposition='CREATE_IF_NEEDED',
                                               gcp_conn_id=bq_gcp_connector,
                                               allow_large_results=True,
                                               use_legacy_sql=False,
                                               time_partitioning={"type": "DAY", 'field': partition_field},
                                               schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                      'ALLOW_FIELD_RELAXATION'])

        else:
            if 'emailevents' in target_f.lower():
                cleaned_file_data = cleaned_file_data.replace('{prefix}', prefix).replace('{date}', '{{ ds }}').replace(
                    '{dataset}', base_dataset_set).replace('{project}', bq_target_project)

            cleaned_file_data = cleaned_file_data.replace('{prefix}', prefix).replace('{date}', '{{ ds }}').replace(
                '{dataset}', base_dataset_set).replace('{project}', bq_target_project).replace('{input_data_list}',
                                                                                        INPUT_QUERY)

            build_gold_data = BigQueryOperator(task_id='hubspot_gld_{file}'.format(file=target_f_id),
                                               sql=cleaned_file_data,
                                               destination_dataset_table='{project}.{prefix}{target_dataset}.t_{source_abbr}_{file_id}'.format(
                                                   project=bq_target_project,
                                                   file_id=target_f_id.lower(),
                                                   prefix=prefix,
                                                   target_dataset=target_dataset,
                                                   source_abbr=source_abbr.lower()),
                                               write_disposition='WRITE_TRUNCATE',
                                               create_disposition='CREATE_IF_NEEDED',
                                               gcp_conn_id=bq_gcp_connector,
                                               allow_large_results=True,
                                               use_legacy_sql=False,
                                               time_partitioning={'type': 'DAY', 'field': partition_field})

            task_sensor1 >> task_sensor2 >> create_dataset >> build_gold_data >> bit_set