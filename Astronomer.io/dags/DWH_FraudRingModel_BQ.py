###Importing Libraries
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLServertoGCS import SQLServertoGCSOperator
from airflow.contrib.operators.mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import os
import json



###creating default dag arguments
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#Getting environment to execute the code from airflow variable
ENV = Variable.get('ENV')

try :
    fraudringmodel_variable = Variable.get("FraudRingModel_Variable")
except:
    Variable.set("FraudRingModel_Variable",'{"plecom_full_load":"True","mixpanel_full_load":"True"}')
    fraudringmodel_variable = Variable.get("FraudRingModel_Variable")

fraudringmodel_variable = json.loads(fraudringmodel_variable)


plecom_full_load = fraudringmodel_variable['plecom_full_load']
mixpanel_full_load = fraudringmodel_variable['mixpanel_full_load']

###configuring variables based on environment
if ENV.lower() == 'dev':
    target_bq_project = 'dev-edl'
    dataset_prefix = 'DEV_'
    bq_gcp_connector = 'dev_edl'
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_dev01_edl_lnd'
    bq_target_project = 'dev-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'


elif ENV.lower() == 'qa':
    target_bq_project = 'qa-edl'
    dataset_prefix = 'B_QA_'
    bq_gcp_connector = 'qa_edl'
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_qa01_edl_lnd'
    bq_target_project = 'qa-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'


elif ENV.lower() == 'prod':
    target_bq_project = 'prod-edl'
    dataset_prefix = ''
    bq_gcp_connector = 'prod_edl'
    mssql_connection = 'instDW_PROD'
    trigger_bucket = 'jm_prod_edl_lnd'
    bq_target_project = 'prod-edl'
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'


destination_dataset =  dataset_prefix+'gld_fraud_ring_model'
trigger_object = 'triggers/inbound/dw_complete.json'

with DAG(
        'FraudRingModel_dag',
        schedule_interval='0 12 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


    # checking DW sensor
    ##GCS Trigger sensor
    file_sensor_task = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id=bq_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=9 * 60
                                                 , deferrable = True)


    #creating / check if the dataset exists
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id='check_for_dataset_{table}'.format(table = destination_dataset),
                                                            dataset_id=destination_dataset,
                                                            bigquery_conn_id=bq_gcp_connector)

    table_name_dictionary =  {'t_fraudringmodel_plecom.sql':'t_fraudringmodel_plecom_data',
                              't_fraudringmodel_mixpanel.sql':'t_fraudringmodel_mixpanel_data',
                              't_fraudringmodel_final.sql':'t_fraudringmodel_final'}

    #get plecom sql

    with open(os.path.join(configuration.get('core', 'dags_folder'),r'ETL/FraudRingModel/t_fraudringmodel_plecom.sql')) as f:
        read_file_data = f.readlines()
        sql = "\n".join(read_file_data)

    if plecom_full_load.lower() == 'true':
        fraudring_plecom_mssql = sql.format(
            start_date="1900-01-01",
            end_date="{{ next_ds }}",
            date="{{ ds }}"
        )

    else:
        fraudring_plecom_mssql = sql.format(
                                 start_date="{{ ds }}",
                                 end_date = "{{ next_ds }}",
                                 date="{{ ds }}"
                                 )


    #get plecom schema
    file_path = os.path.join(configuration.get('core', 'dags_folder'), r'ETL/FraudRingModel/schemas/fraudring_plecom_schema.json')

    with open(file_path, 'rb') as inputJSON:
        schemaList = json.load(inputJSON)


    extract_fraudring_plecom_mssql_to_gcs =SQLServertoGCSOperator(
        task_id='extract_fraudringmodel_plecom_to_gcs',
        sql=fraudring_plecom_mssql,
        target_gcs_bucket=base_bucket,
        filename=dataset_prefix + 'l1_gld/fraud_ring_model/data/{{ ds_nodash }}/l1_gld_fraud_ring_model_export.json',
        mssql_conn_id=mssql_connection,
        google_cloud_storage_conn_id=base_gcp_connector
    )

    upload_fraudring_plecom_mssql_to_bq = GCSToBigQueryOperator(
        task_id='upload_fraudringmodel_plecom_to_bq',
        bucket=base_bucket,
        source_objects=[dataset_prefix +'l1_gld/fraud_ring_model/data/{{ ds_nodash }}/l1_gld_fraud_ring_model_export.json'],
        destination_project_dataset_table='{project}.{base_dataset}.{table}${date}'.format(
                                                                                project=bq_target_project,
                                                                                base_dataset=destination_dataset,
                                                                                table = table_name_dictionary['t_fraudringmodel_plecom.sql'],
                                                                                date = "{{ ds_nodash }}"
                                                                                        ),
        schema_fields=schemaList,
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=bq_gcp_connector,
        schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
        autodetect = False
        )

    # get Mixpanel sql

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/FraudRingModel/t_fraudringmodel_mixpanel.sql')) as f:
        read_file_data = f.readlines()
        sql = "\n".join(read_file_data)
    if mixpanel_full_load.lower() == 'true':
        fraudring_mixpanel_sql = sql.format(
            start_date="1900-01-01",
            end_date="{{ next_ds }}",
            date="{{ ds }}"
        )
    else:
        fraudring_mixpanel_sql = sql.format(
                                 start_date="{{ ds }}",
                                 end_date = "{{ next_ds }}",
                                 date="{{ ds }}"
                                 )

    build_fraudring_mixpanel_data = BigQueryOperator(task_id='build_fraudringmodel_mixpanel_table',
                                          sql=fraudring_mixpanel_sql,
                                          destination_dataset_table='{project}.{base_dataset}.{table}${date}'.format(
                                              project=bq_target_project,
                                              base_dataset=destination_dataset,
                                              table=table_name_dictionary['t_fraudringmodel_mixpanel.sql'],
                                              date="{{ ds_nodash }}"),
                                          write_disposition='WRITE_TRUNCATE',
                                          create_disposition='CREATE_IF_NEEDED',
                                          gcp_conn_id=bq_gcp_connector,
                                          allow_large_results=True,
                                          use_legacy_sql=False,
                                          time_partitioning={"type": "DAY"},
                                          schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                 'ALLOW_FIELD_RELAXATION'])

    # Final query

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/FraudRingModel/t_fraudringmodel_final.sql')) as f:
        read_file_data = f.readlines()
        sql = "\n".join(read_file_data)

    final_sql = sql.format(project = bq_target_project , dataset = destination_dataset , date = "{{ds}}")
    build_fraudring_model_table = BigQueryOperator(task_id='build_fraudringmodel_final_table',
                                                     sql=final_sql,
                                                     destination_dataset_table='{project}.{base_dataset}.{table}${date}'.format(
                                                         project=bq_target_project,
                                                         base_dataset=destination_dataset,
                                                         table=table_name_dictionary['t_fraudringmodel_final.sql'],
                                                         date="{{ ds_nodash }}"),
                                                     write_disposition='WRITE_TRUNCATE',
                                                     create_disposition='CREATE_IF_NEEDED',
                                                     gcp_conn_id=bq_gcp_connector,
                                                     allow_large_results=True,
                                                     use_legacy_sql=False,
                                                     time_partitioning={"type": "DAY"},
                                                     schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                            'ALLOW_FIELD_RELAXATION'])

    file_sensor_task >> create_dataset_task >> extract_fraudring_plecom_mssql_to_gcs >> upload_fraudring_plecom_mssql_to_bq>>build_fraudring_model_table
    file_sensor_task >> create_dataset_task >> build_fraudring_mixpanel_data>>build_fraudring_model_table
