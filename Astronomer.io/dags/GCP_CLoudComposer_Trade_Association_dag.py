"""
Author : Gayathri Narayanasamy
Purpose: This dag is created to copy Trade association files from Sharepoint into GCP and then do some data processing and write back the result to Sharepoint.
Requester: Julius Alexander
Schedule: This dag is scheduled to run 7th day of each quarter   
"""
import logging
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_TradeAssociationOperator import TradeAssociationOperator
from plugins.operators.jm_SharepointtoGCSOperator import SharepointtoGCSOperator


#Variables declaration
source_abbr = 'Trade_Association'
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,7,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
AIRFLOW_ENV = Variable.get('ENV')


if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_dev01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection_target = 'instJMShipping_STAGE'

elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection_target = 'instJMShipping_PROD'

####Change this to semi managed connection
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'semi-managed-reporting'
    bq_gcp_connector='semi_managed_gcp_connection'
    folder_prefix = ''
    sharepoint_connection = 'sharepoint_connection'
    semimanged_gcp_connector = 'semi_managed_gcp_connection'
    mssql_connection_target = 'instJMShipping_PROD'


source = 'TradeAssociation'
destination_dataset = folder_prefix + 'Trade_Association'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source)


with DAG(
        'Trade_Association_dag',
        schedule_interval='0 16 7 1,4,7,10 *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:



    #creating dataset Trade_Association
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_Trade_Association_dataset',
        dataset_id=destination_dataset,
        bigquery_conn_id=bq_gcp_connector
    )

    #Copy data from Team Hermes site into GCP storage
    copy_data_from_sharepoint_to_gcs = SharepointtoGCSOperator(
        task_id = 'copy_TA_data_from_sharepoint_to_GCS',
        sharepoint_connection = sharepoint_connection,
        source_file_url = '/sites/TeamHermes/Shared Documents/TradeAssociation/Input/',
        google_storage_connection_id = base_gcp_connector ,
        destination_bucket = base_bucket,
        gcs_destination_path = '{base_file_location}{date}/'.format(base_file_location=base_file_location,
                                                                  date="{{ ds_nodash }}")
    )
    #Data Processing and write teh out put back to Team Hermes/Shared Documents/TradeAssociation/Output/ location
    Trade_Association_report_automation = TradeAssociationOperator(
        task_id = 'TA_report_data_processing',
        google_storage_connection_id = base_gcp_connector,
        bq_gcp_connection_id = bq_gcp_connector,
        sharepoint_connection = sharepoint_connection,
        destination_file_url = '/sites/TeamHermes/Shared Documents/TradeAssociation/Output/TradeAssociationReport.xlsx',
        project = bq_target_project,
        dataset = destination_dataset,
        table = 't_stacked_final',
        gcs_destination_path='{base_file_location}{date}/'.format(base_file_location=base_file_location,
                                                                  date="{{ ds_nodash }}"),
        mssql_connection=mssql_connection_target,
        destination_bucket = base_bucket
    )



    create_dataset>>copy_data_from_sharepoint_to_gcs>>Trade_Association_report_automation

