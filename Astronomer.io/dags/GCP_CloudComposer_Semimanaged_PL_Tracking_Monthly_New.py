###Libraries/Operators import
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
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
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
    'email': 'nreddy@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dataset_fy = 'financial_year'

################################################################################
# INSTANTIATE DAG TO RUN AT UTC 12.30AM (7.30AM CDT)
################################################################################

dag = DAG('semimanaged_promoted_monthly_dag_New_v1',
            max_active_runs=1,
            schedule_interval='30 12 1 * *',
            default_args=default_args,
            catchup=True
)

dag.doc_md = """ 
### NOTE:
This dag is not idempotent as the daily tables are non partitioned tables.
Hence monthly snapshots are required to run successfully on first of every month. """

task_sensor_PL_tracking = ExternalTaskSensorAsync(
        task_id='check_PL_dashboard_completion',
        external_dag_id='semimanaged_PL_tracking_daily_dag_new',
        external_task_id='run_pl_tracking',
        execution_delta=timedelta(hours=0.5),
        timeout=7200,
        dag=dag)

task_sensor_CL_tracking = ExternalTaskSensorAsync(
        task_id='check_CL_dashboard_completion',
        external_dag_id='semimanaged_CL_tracking_daily_dag_new',
        external_task_id='run_cl_tracking',
        execution_delta=timedelta(hours=-4),
        timeout=7200,
        dag=dag)

fy_tables = {'RECAST_V6.0:pl_recast_after_2014:pl_recast_after_2014_monthly',
                  'cl_v2023_riskgroup_im:cl_riskgroup_im:cl_riskgroup_im_monthly', 
                  'cl_v2023_Tracking.sql:cl_tracking_daily:cl_tracking_monthly',
                  'pl_tracking.sql:pl_tracking_daily:pl_tracking_monthly',
                  }

task_start = EmptyOperator(task_id='task_start', dag=dag)

for table_dict in fy_tables:
    table_list = table_dict.split(':')
    file = table_list[0]
    daily_table=table_list[1]
    monthly_table = table_list[2]

	
    monthly_sql = '''select * from `{project}.{dataset}.{table}`'''.format(project=bq_target_project,dataset=dataset_fy, table=daily_table)
   
    if 'tracking' not in monthly_table:
        destination_dataset_table = '{project}.{dataset}.{table}${date}'.format(
        project=bq_target_project,
        table=monthly_table,
        dataset=dataset_fy,
        date="{{ ds_nodash }}")
        monthly_table = BigQueryOperator(task_id='Create_{table}'.format(table=monthly_table),
                                         sql=monthly_sql,
                                         destination_dataset_table=destination_dataset_table,
                                         write_disposition='WRITE_TRUNCATE',
                                         create_disposition='CREATE_IF_NEEDED',
                                         gcp_conn_id=bq_gcp_connector,
                                         allow_large_results=True,
                                         use_legacy_sql=False,
                                         time_partitioning={"type": "DAY"},
                                         schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                'ALLOW_FIELD_RELAXATION'],
                                         dag=dag)
    else:
        destination_dataset_table = '{project}.{dataset}.{table}'.format(
            project=bq_target_project,
            table=monthly_table,
            dataset=dataset_fy)


        monthly_table = BigQueryOperator(task_id='Create_{table}'.format(table=monthly_table),
                     sql=monthly_sql,
                     destination_dataset_table=destination_dataset_table,
                     write_disposition='WRITE_TRUNCATE',
                     create_disposition='CREATE_IF_NEEDED',
                     gcp_conn_id=bq_gcp_connector,
                     allow_large_results=True,
                     use_legacy_sql=False,
                     dag=dag)

    task_sensor_PL_tracking >>task_start >>monthly_table
    task_sensor_CL_tracking >>task_start >>monthly_table
	