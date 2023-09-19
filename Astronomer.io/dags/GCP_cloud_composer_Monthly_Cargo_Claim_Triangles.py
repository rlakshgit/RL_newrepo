####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_mssql_to_gcs_semimanaged import JM_MsSqlToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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

#####Initializing Variables ########
env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix = 'DEV_'
    project = 'dev-edl'
    prod_gcp_connector = 'dev_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_dev01_edl_lnd'


elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix = 'B_QA_'
    project = 'qa-edl'
    prod_gcp_connector = 'qa_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_qa01_edl_lnd'


elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    dataset_prefix = ''
    project = 'prod-edl'
    prod_gcp_connector = 'prod_edl'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'semi_managed_gcp_connection'
    bq_target_project = 'semi-managed-reporting'
    folder_prefix = ''
    mssql_connection = 'instDW_PROD'
    trigger_bucket = 'jm_prod_edl_lnd'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'sdonthiri@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

##LASTMONTH = '{{ ds_nodash }}'
##THISMONTH = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1)).strftime("%Y%m%d") }}'

# Datset variables
dataset_core_insurance_pjpa = dataset_prefix + 'core_insurance_pjpa'
dataset_core_insurance = dataset_prefix + 'core_insurance'
dataset_data_products_t2_sales = dataset_prefix + 'data_products_t2_sales'
dataset_wip_saleforce_admin_001 = dataset_prefix + 'wip_saleforce_admin_001'
dataset_data_products_t1_quotes_offers = dataset_prefix + 'data_products_t1_quotes_offers'
dataset_core_insurance_cl = dataset_prefix + 'core_insurance_cl'
dataset_fy_2022 = 'financial_year_2022'
dataset_wip_jalexander = dataset_prefix + 'wip_jalexander'

################################################################################
# INSTANTIATE DAG TO RUN AT UTC 12PM(6AM CST)
################################################################################

with DAG('Monthly_Cargo_Claim_Triangles',
         max_active_runs=1,
         schedule_interval='0 12 1 * *',
         catchup=True,
         default_args=default_dag_args) as dag:

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'Julias_cargo/ClaimsTrianglesByMonth_OM.sql'),'r') as f:
        CL_Claim_Triangles_sql= f.read()

    #check dataset wip_jalexander
    check_for_wip_jalexander = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_wip_jalexander',
            dataset_id= dataset_wip_jalexander,
            bigquery_conn_id=bq_gcp_connector)

    #creating CL_Claim_Triangles_to_gcs object with data and schema folder in semi_managed_reporting
    CL_Claim_Triangles_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
                        task_id='CL_Claim_Triangles_to_gcs',
                        sql=CL_Claim_Triangles_sql,
                        bucket=base_bucket,
                        filename=folder_prefix+'CL_Claim_Triangles/data/{{ ds_nodash }}/Claim_Triangles_export.json',
                        schema_filename=folder_prefix+'CL_Claim_Triangles/schemas/{{ ds_nodash }}/Claim_Triangles_export.json',
                        mssql_conn_id= mssql_connection,
                        google_cloud_storage_conn_id=base_gcp_connector
    )

    #creating table CL_Claim_Triangles_gcs_to_bq in dataset_wip_jalexander dataset
    CL_Claim_Triangles_gcs_to_bq = GCSToBigQueryOperator(
                        task_id='CL_Claim_Triangles_gcs_to_bq',
                        bucket=base_bucket,
                        source_objects=[folder_prefix+'CL_Claim_Triangles/data/{{ ds_nodash }}/Claim_Triangles_export.json'],
                        destination_project_dataset_table='{project}.{dataset}.CL_Claim_Triangles'.format(project = bq_target_project,dataset = dataset_wip_jalexander ),
                        schema_fields=None,
                        schema_object=folder_prefix+'CL_Claim_Triangles/schemas/{{ ds_nodash }}/Claim_Triangles_export.json',
                        source_format='NEWLINE_DELIMITED_JSON',
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=0,
                        write_disposition='WRITE_TRUNCATE',
                        gcp_conn_id=base_gcp_connector,
                        schema_update_options=()
    )



    with open(os.path.join(configuration.get('core', 'dags_folder'), r'Julias_cargo/ClaimCountTrianglesbyMonth_OM.sql'),'r') as f:
        CL_Claim_Count_Triangles_sql= f.read()


    #creating CL_Claim_Count_Triangles_to_gcs object with data and schema folder in semi_managed_reporting
    CL_Claim_Count_Triangles_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
                        task_id='CL_Claim_Count_Triangles_to_gcs',
                        sql=CL_Claim_Count_Triangles_sql,
                        bucket=base_bucket,
                        filename=folder_prefix+'CL_Claim_Count_Triangles/data/{{ ds_nodash }}/export.json',
                        schema_filename=folder_prefix+'CL_Claim_Count_Triangles/schemas/{{ ds_nodash }}/export.json',
                        mssql_conn_id= mssql_connection,
                        google_cloud_storage_conn_id=base_gcp_connector
    )

    #creating table CL_Claim_Count_Triangles_gcs_to_bq in dataset_wip_jalexander dataset
    CL_Claim_Count_Triangles_gcs_to_bq = GCSToBigQueryOperator(
                        task_id='CL_Claim_Count_Triangles_gcs_to_bq',
                        bucket=base_bucket,
                        source_objects=[folder_prefix+'CL_Claim_Count_Triangles/data/{{ ds_nodash }}/export.json'],
                        destination_project_dataset_table='{project}.{dataset}.CL_Claim_Count_Triangles'.format(project = bq_target_project,dataset = dataset_wip_jalexander ),
                        schema_fields=None,
                        schema_object=folder_prefix+'CL_Claim_Count_Triangles/schemas/{{ ds_nodash }}/export.json',
                        source_format='NEWLINE_DELIMITED_JSON',
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=0,
                        write_disposition='WRITE_TRUNCATE',
                        gcp_conn_id=base_gcp_connector,
                        schema_update_options=()
    )


    ################################################################################
    # AIRFLOW ROUTING
    ################################################################################

    check_for_wip_jalexander >> CL_Claim_Triangles_to_gcs >> CL_Claim_Triangles_gcs_to_bq >> CL_Claim_Count_Triangles_to_gcs >> CL_Claim_Count_Triangles_gcs_to_bq