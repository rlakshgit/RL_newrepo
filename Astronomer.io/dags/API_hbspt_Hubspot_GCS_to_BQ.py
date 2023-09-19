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
from plugins.operators.jm_hubspot_staging import HubspotStagingOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
# from airflow.contrib.operators.bigquery_operator import  BigQueryOperator
# from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from plugins.operators.jm_BQSchemaGenerationOperator import BQSchemaGenerationOperator

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
    prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    prefix = ''
    mssql_connection_target = 'instDW_PROD'

####
source = 'Hubspot'
source_abbr = 'hubspot'
l1_storage_location = '{prefix}l1/{source}/'.format(prefix=prefix,source=source_abbr)
l2_storage_location = '{prefix}l2/{source}/'.format(prefix=prefix,source=source_abbr)
offset_storage_location = '{prefix}offset/{source}/'.format(prefix=prefix,source=source_abbr)
metadata_storage_location = '{prefix}metadata/{source}/'.format(prefix=prefix,source=source_abbr)
schema_storage_location = '{prefix}schema/{source}/'.format(prefix=prefix,source=source_abbr)
audit_storage_location = '{prefix}audit/{source}/'.format(prefix=prefix,source=source_abbr) 
staging_file_location = '{prefix}staging/{source}/'.format(prefix=prefix,source=source_abbr)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

completed=[]

## Move thses 2 lists to external file or env vars
hubspotAPIs = ['emailEvents', 'contacts_details_updates','marketingEmails','campaigns_details_updates','emailSubscriptionTypes']
hubspotAPIsWithDetail = ['contacts_details_updates','campaigns_details_updates']


confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'hubspot', r'ini', r"hubspot_ingestion_airflow_config_update.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())

with DAG(
        'Hubspot_Loading_DAG',
        schedule_interval= '0 6 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source_abbr),
        dataset_id='{prefix}ref_{source_abbr}'.format(
            source_abbr=source_abbr.lower(),prefix=prefix),
        bigquery_conn_id=bq_gcp_connector)


    for hub_api in hubspotAPIs:

        try:
            HUBAPI_BQ_PROCESS = Variable.get(hub_api + '_gcs_to_bq')
        except:
            Variable.set(hub_api + '_gcs_to_bq', 'True')

        HUBAPI_BQ_PROCESS = Variable.get(hub_api + '_gcs_to_bq')

        task_sensor_core = ExternalTaskSensorAsync(
                    task_id='check_{source}_update_{api_name}_dag'.format(source=source_abbr,api_name=hub_api),
                    external_dag_id='Hubspot_Landing_DAG',
                    #   external_task_id='set_source_bit',
                    external_task_id='End_Task_{hub_api}'.format(hub_api=hub_api),
                    execution_delta=timedelta(hours=0),
                    timeout=7200)

        generate_schema = BQSchemaGenerationOperator(
            task_id = 'generate_schema_{source}_{table}'.format(source = source , table = hub_api ),
            project = project,
            config_file = confJSON,
            source = source_abbr,
            table = hub_api,
            target_gcs_bucket = base_bucket,
            schema_location = schema_storage_location,
            google_cloud_storage_conn_id=base_gcp_connector

        )


        stage_files = HubspotStagingOperator(
            task_id='StageFiles_{source}_{api_name}'.format(source=source_abbr,api_name=hub_api),
            api_connection_id='HubspotAPI',
            api_name=hub_api,
            api_configuration=confJSON,
            google_cloud_storage_conn_id=base_gcp_connector,
            base_gcs_folder=prefix + 'l1',
            l2_storage_location = l2_storage_location,
            staging_file_location=staging_file_location,
            api_key_encryped=True,
            target_gcs_bucket=base_bucket,
            target_env=AIRFLOW_ENV.upper(),
            metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                location_base=l1_storage_location,
                api_name=hub_api,
                date="{ds_nodash}"),
            file_prefix=prefix,
            #pool='hubspot-pool'
            )

        load_data = GoogleCloudStorageToBigQueryOperator(
                        task_id='load_data_{source}_{api_name}'.format(source=source_abbr,
                                                                       api_name=hub_api),
                        bucket=base_bucket,
                        source_objects=[
                        '{staging_file_location}{api_name}/{date}/staging_hubspot_{api_name}*'.format(staging_file_location=staging_file_location,
                                                                                        api_name=hub_api,
                                                                                        date="{{ ds_nodash }}")],
                        destination_project_dataset_table='{project}.{prefix}ref_{source_abbr}.t_{source_abbr}_{table}${date}'.format(
                        project=bq_target_project,
                        source_abbr=source_abbr.lower(),
                        table=hub_api,
                        prefix=prefix,
                        date="{{ ds_nodash }}"),
                        #schema_fields=None,
                        schema_object='{schema_storage_location}{api_name}/{date}/l1_schema_hubspot_{api_name}.json'.format(schema_storage_location=schema_storage_location,
                                                                                              api_name=hub_api,
                                                                                              date="{{ ds_nodash }}"),
                        source_format='CSV',
                        compression='NONE',
                        ignore_unknown_values=True,
                        allow_quoted_newlines =True,
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=1,
                        write_disposition='WRITE_TRUNCATE',
                        max_bad_records=0,
                        bigquery_conn_id=bq_gcp_connector,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION'],
                        #src_fmt_configs=None,
                        # autodetect=True,
                        gcs_to_bq = HUBAPI_BQ_PROCESS)



        bq_load_audit_check = APIAuditOperator(
            task_id='bq_load_audit_check_{source}_{api_name}'.format(source=source_abbr, api_name=hub_api),
            project=bq_target_project,
            dataset='{prefix}ref_{source_abbr}'.format(
                        project=bq_target_project,
                        source_abbr=source_abbr.lower(),
                        prefix=prefix),
            table='t_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=hub_api),
            source=source,
            source_abbr=source_abbr.lower(),
            base_gcs_folder=None,
            target_gcs_bucket=base_bucket,
            bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            check_landing_only=False,
            metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                location_base=metadata_storage_location,
                api_name=hub_api,
                date="{ds_nodash}"),
            audit_filename='{location_base}{api_name}/{date}/l1_audit_{source}_{api_name}.json'.format(
                location_base=audit_storage_location,
                source=source_abbr.lower(),
                api_name=hub_api,
                date="{ds_nodash}",
                prefix=prefix)
            )





        create_dataset >> task_sensor_core >> generate_schema >> stage_files >> load_data >> bq_load_audit_check