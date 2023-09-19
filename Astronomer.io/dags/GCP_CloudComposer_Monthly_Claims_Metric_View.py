####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_mssql_to_gcs_semimanaged import JM_MsSqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLServertoBQ import SQLServertoBQOperator
import os

#####Initializing Variables ########
env = Variable.get('ENV')
# env = 'qa'

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
    base_bucket = 'jm-edl-landing-wip'
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
    'start_date': datetime(2021, 10, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': '',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

LASTMONTH = '{{ ds_nodash }}'
THISMONTH = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1)).strftime("%Y%m%d") }}'

# Datset variables
dataset_core_insurance_pjpa = dataset_prefix + 'core_insurance_pjpa'
dataset_core_insurance = dataset_prefix + 'core_insurance'
dataset_data_products_t2_sales = dataset_prefix + 'data_products_t2_sales'
dataset_wip_saleforce_admin_001 = dataset_prefix + 'wip_saleforce_admin_001'
dataset_data_products_t1_quotes_offers = dataset_prefix + 'data_products_t1_quotes_offers'
dataset_core_insurance_cl = dataset_prefix + 'core_insurance_cl'
dataset_fy_2022 = 'financial_year_2022'
dataset_ref_pe_pc = dataset_prefix + 'ref_pe_pc'
dataset_ref_pc_current = dataset_prefix + 'ref_pc_current'
dataset_data_products_t2_claims = dataset_prefix + 'data_products_t2_claims'

################################################################################
# INSTANTIATE DAG TO RUN AT UTC 14PM(9AM CDT)
################################################################################

with DAG('Monthly_claims_metric_view',
         max_active_runs=1,
         schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #
         catchup=False,
         default_args=default_dag_args) as dag:
    # creating dataset create_data_products_t2_claims_dataset
    create_data_products_t2_claims_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_data_products_t2_claims_dataset',
        dataset_id=dataset_data_products_t2_claims,
        bigquery_conn_id=bq_gcp_connector)

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/CL_Inforce_PE/ClaimsMetrics_CL_LocationCounts.sql'), 'r') as f:
        bq_sql = f.read()

        bq_sql = bq_sql.format(project=project, bq_target_project=bq_target_project, dataset=dataset_core_insurance_cl,
                           dataset_ref_pe_pc=dataset_ref_pe_pc, dataset_ref_pc_current=dataset_ref_pc_current,
                           dataset_fy_2022=dataset_fy_2022)

        nonpromoted_ClaimsMetrics_CL_LocationCounts = BigQueryCreateEmptyTableOperator(
            task_id="nonpromoted_ClaimsMetrics_CL_LocationCounts",
            dataset_id=dataset_data_products_t2_claims,
            table_id='nonpromoted_ClaimsMetrics_CL_LocationCounts',
            bigquery_conn_id=bq_gcp_connector,
            view={
            "query": '{bq_sql}'.format(bq_sql=bq_sql),
            "useLegacySql": False,
            },
        )
    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'semi-managed/PL_Inforce_PE/ClaimsMetrics_PL_ItemCounts.sql'), 'r') as f:
        bq_sql = f.read()

        bq_sql = bq_sql.format(project=project, bq_target_project=bq_target_project, dataset=dataset_core_insurance_pjpa,
                               dataset_ref_pe_pc=dataset_ref_pe_pc, dataset_ref_pc_current=dataset_ref_pc_current,
                               dataset_fy_2022=dataset_fy_2022)

        nonpromoted_ClaimsMetrics_PL_ItemCounts = BigQueryCreateEmptyTableOperator(
            task_id="nonpromoted_ClaimsMetrics_PL_ItemCounts",
            dataset_id=dataset_data_products_t2_claims,
            table_id='nonpromoted_ClaimsMetrics_PL_ItemCounts',
            bigquery_conn_id=bq_gcp_connector,
            view={
                "query": bq_sql,
                "useLegacySql": False,
            },
        )


    ################################################################################
    # AIRFLOW ROUTING
    ################################################################################

    create_data_products_t2_claims_dataset >> [nonpromoted_ClaimsMetrics_CL_LocationCounts,nonpromoted_ClaimsMetrics_PL_ItemCounts]
