import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_CurrentVersionOperator import CurrentVersionOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_salesforce_to_gcs import SalesforceToGCSOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

###

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


AIRFLOW_ENV = Variable.get('ENV')

trigger_object = 'triggers/inbound/dw_complete.json'

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    trigger_bucket = 'jm_dev01_edl_lnd'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'


sql_source_db = ['BillingCenter:bc','ClaimCenter:cc','PolicyCenter:pc','ContactManager:cm'] #,'salesforce:sf']


with DAG(
        'Get_Tables_Lists',
        schedule_interval= '0 12 * * *',
        catchup=True,
		max_active_runs=1,
        default_args=default_dag_args) as dag:



    file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id=bq_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=9 * 60, deferrable = True)


    # from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
    # check_source_dependency = ExternalTaskSensor(
    #     task_id='check_{center}_table_list'.format(center=source_sql.lower()),
    #     external_dag_id='Guidewire_Get_Tables_Lists',
    #     external_task_id='get_tablelist_{center}'.format(center=source_sql.lower()),
    #     execution_delta=timedelta(hours=1),
    #     timeout=7200)###


    for sql_source in sql_source_db:
        source_split = sql_source.split(':')
        sql_source_db_abbr = source_split[1]
        source_sql = source_split[0]
        # if source_sql != 'salesforce':
        table_list = MsSqlToGoogleCloudStorageOperator(
                                                        task_id='get_tablelist_{center}'.format(center=source_sql.lower()),
                                                        sql='''SELECT DISTINCT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM [{source_sql}].information_schema.tables'''.format(source_sql=source_sql),
                                                        bucket=base_bucket,
                                                        filename='NA',
                                                        schema_filename='NA',
                                                        mssql_conn_id=mssql_connection_target,
                                                        google_cloud_storage_conn_id=base_gcp_connector,
                                                        airflow_var_set='{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
                                                        )
        # else:
        #     table_list = SalesforceToGCSOperator(
        #         task_id='get_tablelist_{source}'.format(source=source_sql.lower()),
        #         conn_id='salesforce_TEST',
        #         dest_bucket=base_bucket,
        #         google_cloud_storage_conn_id=base_gcp_connector,
        #         history_check=False,
        #         airflow_var_set='{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower()),
        #         dag=dag)




        file_sensor >> table_list






