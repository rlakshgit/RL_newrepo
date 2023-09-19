####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_mssql_to_gcs_semimanaged import JM_MsSqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLServertoBQ import SQLServertoBQOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
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
    'start_date': datetime(2023,1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'tverner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dataset_fy = 'financial_year'


################################################################################
# INSTANTIATE DAG TO RUN AT UTC 11AM (6AM CDT)
################################################################################

dag = DAG('semimanaged_PL_tracking_daily_dag_new',
            max_active_runs=1,
            schedule_interval='0 12 * * *',
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

#dictionary with mappings for table name based on file name
# partition_date = '{{ macros.ds_format("2023-1-1","%Y-%m-%d","%Y-%m-%d")}}'
# load_date = '{{ ds }}'
# if load_date <= partition_date:
#     fy_tables = {'RECAST_V5.0':'pl_recast_after_2014',
#                   'Actual_Written_Premium': 'fact_written_premium',
#                   'Actual_Transaction_Count': 'fact_transaction_count',
#                   'RECAST_BEFORE_2014_V5.0': 'pl_recast_before_2014',
#                   'Actual_Incurred_Loss':'fact_incurred_loss',}
# else:
fy_tables = {'RECAST_V6.0':'pl_recast_after_2014',
                  'Actual_Written_Premium': 'fact_written_premium',
                  'Actual_Transaction_Count': 'fact_transaction_count',
                  'Actual_Incurred_Loss':'fact_incurred_loss',}

# PL production report final query
pl_file='pl_tracking.sql'
pl_table_name='pl_tracking_daily'
with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/financial_year/{file}'.format(file=pl_file))) as f:
    read_file_data = f.readlines()
    sql = "\n".join(read_file_data)
pl_sql = sql.format(ddate='{{ ds_nodash }}', project=bq_target_project, dataset=dataset_fy)

final_table_pl = runbqscriptoperator(task_id='run_{file}'.format(file=pl_file.replace('.sql', '')),
                                                       sql=pl_sql,
                                                       bigquery_conn_id=bq_gcp_connector,
                                                       dag=dag)

# recast table
recast_file='pl_recast_all.sql'
recast_table_name='pl_recast_all'
with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/financial_year/{file}'.format(file=recast_file))) as f:
    read_file_data = f.readlines()
    sql = "\n".join(read_file_data)
recast_sql = sql.format(ddate="{{ ds_nodash }}", project=bq_target_project, dataset=dataset_fy)

recast_table = runbqscriptoperator(task_id='run_{file}'.format(file=recast_file.replace('.sql', '')),
                                                       sql=recast_sql,
                                                       bigquery_conn_id=bq_gcp_connector,
                                                       dag=dag)

task_end = EmptyOperator(task_id = 'task_end', dag  = dag)

get_file_list = os.listdir(os.path.join(configuration.get('core', 'dags_folder'),r'semi-managed/financial_year'))

for file in get_file_list:
    if file.replace('.sql','') in fy_tables.keys():

        with open(os.path.join(configuration.get('core', 'dags_folder'),
                            r'semi-managed/financial_year/{file}'.format(file=file))) as f:
            read_file_data = f.readlines()
            sql = "\n".join(read_file_data)
            sql = sql.format(date="{{ ds }}", project=bq_target_project, dataset=dataset_fy)

            load_table = SQLServertoBQOperator(
                task_id='run_{file}'.format(file = file.replace('.sql','')),
                sql=sql,
                mssql_conn_id=mssql_connection,
                bigquery_conn_id=bq_gcp_connector,
                destination_dataset=dataset_fy,
                destination_table=fy_tables[file.replace('.sql','')],
                project=bq_target_project,
                dag=dag)

            file_sensor >> create_fy_dataset >> load_table >> task_end >> recast_table>> final_table_pl
