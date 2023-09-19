import logging
import datetime as dt
import calendar
import time
import sys
import os
import json
from airflow import DAG
from airflow import configuration
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_BQSchemaGenerationOperator import BQSchemaGenerationOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.models import Variable


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
    folder_prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    # base_gcs_folder = 'B_QA_l1'
    base_gcs_folder = 'l1'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    base_gcs_folder = 'l1'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'


source = 'Triangles'
source_abbr = 'triangles'
base_file_location = folder_prefix + 'l1/{source}/'.format(source=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source}/'.format(source=source_abbr)
metadata_location = '{base_gcs_folder}/{source}/'.format(source=source_abbr, base_gcs_folder=base_gcs_folder)
triangles_dataset = 'ref_{source}'.format(source=source_abbr)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Triangles_loading_dag',
        schedule_interval='0 15 1 * *', 
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id= triangles_dataset,
            bigquery_conn_id=bq_gcp_connector)

        task_sensor_landing = ExternalTaskSensorAsync(
            task_id='check_{source}_update_dag'.format(source=source_abbr),
            external_dag_id='Triangles_landing_dag',
            external_task_id='end_task',
            execution_delta=timedelta(hours=1),
            timeout=7200)

        task_start = EmptyOperator(task_id='task_start')
        task_end = EmptyOperator(task_id='task_end')

        confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'triangles', r'ini', r"triangles_config.ini")
        with open(confFilePath, 'r') as configFile:
            confJSON = json.loads(configFile.read())

            triangles_tables = confJSON.keys()
                
            for table in triangles_tables:
                load_data_to_BQ = GoogleCloudStorageToBigQueryOperator(
                    task_id='load_data_{table}'.format(table=table),
                    bucket=base_bucket,
                    source_objects=['{location_base}data/{table}/{date}/l1_data_{table}_*'.format(
                                    location_base=base_file_location,
                                    date = "{{ ds_nodash }}",
                                    table=table)],
                    destination_project_dataset_table='{project}.{dataset}.{table}'.format(
                                    project=bq_target_project,
                                    dataset = triangles_dataset,
                                    table=table),
                    schema_fields = confJSON[table],
                    source_format='NEWLINE_DELIMITED_JSON',
                    compression='NONE',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=0,
                    write_disposition='WRITE_TRUNCATE',
                    max_bad_records=0,
                    bigquery_conn_id=bq_gcp_connector,
                    google_cloud_storage_conn_id=base_gcp_connector)
                 
                loading_audit = APIAuditOperator(
                        task_id='loading_audit_{table}'.format(table=table),
                        bucket=base_bucket,
                        project=project,
                        dataset=triangles_dataset,
                        base_gcs_folder=None,
                        target_gcs_bucket=base_bucket,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        source_abbr=source_abbr,
                        source=source,
                        metadata_filename='{base_folder}metadata/{table}/{date}/l1_metadata_{table}.json'.format(
                            base_folder=base_file_location,
                            table=table,
                            date="{{ ds_nodash }}"),
                        audit_filename='{location_base}{table}/{date}/l1_audit_{table}.json'.format(
                            location_base=base_audit_location,
                            table=table,
                            date='{{ ds_nodash }}'),
                        check_landing_only=False,
                        table=table)

                create_dataset >> task_sensor_landing >> task_start >> load_data_to_BQ >> loading_audit >> task_end

         