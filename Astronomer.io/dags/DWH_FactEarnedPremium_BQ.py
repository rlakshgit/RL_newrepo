####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_mssql_to_gcs_semimanaged import JM_MsSqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import configuration
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_earned_premium_operator import SQLServertoBQAppendingOperator
import os
from airflow.operators.empty import EmptyOperator

#####Initializing Variables ########
env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix='DEV_'
    #project = 'jm-dl-landing'
    prod_gcp_connector = 'dev_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project ='dev-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_dev01_edl_lnd'

elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix='B_QA_'
    #project = 'jm-dl-landing'
    prod_gcp_connector = 'qa_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_qa01_edl_lnd'


elif env.lower() == 'prod':
    base_bucket ='semi-managed-reporting'
    dataset_prefix=''
   # project = 'semi-managed-reporting'
    prod_gcp_connector = 'prod_edl'
    base_gcp_connector ='semi_managed_gcp_connection'
    bq_gcp_connector = 'semi_managed_gcp_connection'
    bq_target_project = 'semi-managed-reporting'
    folder_prefix = ''
    mssql_connection = 'instDW_PROD'
    trigger_bucket = 'jm_prod_edl_lnd'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'tverner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dataset_fy = 'financial_year'


dag = DAG('Fact_earned_Premium_dag_v1',
            max_active_runs=1,
            schedule_interval='0 11 1 * *',
            default_args=default_args,
            catchup=True
)
trigger_object = 'triggers/inbound/dw_complete.json'
##GCS Trigger sensor
file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id= prod_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=9 * 60
                                                 ,dag = dag, deferrable = True)

create_fy_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_fy_dataset',
        dataset_id= dataset_fy,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)

task_end = EmptyOperator(task_id = 'task_end', dag  = dag)

# earned_premium
earned_premium_table_name = 'fact_earned_premium'
earned_premium_file = 'Actual_Earned_Premium.sql'
with open(os.path.join(configuration.get('core', 'dags_folder'),
                            r'semi-managed/financial_year/{file}'.format(file=earned_premium_file))) as f:
    read_file_data = f.readlines()
earned_premium_sql = "\n".join(read_file_data)

startDate = '{{ ds_nodash }}'
endDate = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'

load_earned_premium_table = SQLServertoBQAppendingOperator(
    task_id='run_{file}'.format(file = earned_premium_file.replace('.sql', '')),
    sql=earned_premium_sql.format(startDate=startDate,endDate=endDate,date='{{ ds_nodash }}'),
    mssql_conn_id=mssql_connection,
    bigquery_conn_id=bq_gcp_connector,
    destination_dataset=dataset_fy,
    destination_table=earned_premium_table_name,
    project=bq_target_project,
    #pool='singel_task',
    dag=dag)
        
                              
file_sensor >> create_fy_dataset >> load_earned_premium_table >> task_end 
