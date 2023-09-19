import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_gcs_delete_operator import GoogleCloudStorageDeleteOperator


from airflow.models import Variable

##
AIRFLOW_ENV = Variable.get('ENV')
ts = calendar.timegm(time.gmtime())
logging.info(ts)

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    target_bucket = 'jm_dev01_edl_lnd'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    target_bucket = 'jm_qa01_edl_lnd'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    target_bucket = 'jm_prod_edl_lnd'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}



#source_dag

with DAG(
        'Trigger_Reset',
        schedule_interval= '0 5 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=False,
		max_active_runs=1,
        default_args=default_dag_args) as dag:

    trigger_reset = GoogleCloudStorageDeleteOperator(task_id='dw_trigger_reset'
                                                    ,bucket_name=target_bucket
                                                    ,objects=['triggers/inbound/dw_complete.json',
                                                              'triggers/outbound/ltv_complete.json',
                                                              'triggers/inbound/marketing_contacts_complete.json'
							     ,'triggers/inbound/semi_managed_dw_complete.json']
                                                    ,google_cloud_storage_conn_id=bq_gcp_connector)
