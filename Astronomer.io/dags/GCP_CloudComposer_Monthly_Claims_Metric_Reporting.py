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
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
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
    'start_date': datetime(2021, 10, 6),
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

################################################################################
# INSTANTIATE DAG TO RUN AT UTC 14PM(9AM CDT)
################################################################################

with DAG('Monthly_Claims_Metric_Reporting',
         max_active_runs=1,
         schedule_interval='0 14 * * *',
         catchup=False,
         default_args=default_dag_args) as dag:

    PolicyCenter_sensor = ExternalTaskSensorAsync(
        task_id='PolicyCenter_sensor',
        external_dag_id='PolicyCenter_Prioritized_Tables',
        external_task_id='final_audit_PolicyCenter',
        ##external_dag_id='Policables',
        ##external_task_id='finalter',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    PolicyCenter_sensor1 = ExternalTaskSensorAsync(
        task_id='ProductExtension_PolicyCenter_sensor',
        external_dag_id='ProductExtension_PC_Schema_dag',
        external_task_id='set_source_bit',
        ##external_dag_id='Policables',
        ##external_task_id='finalter',
        execution_delta=timedelta(hours=0),
        timeout=7200)

################################################################################
# CL Inforce automation
################################################################################


    # creating dataset create_core_insurance_cl_dataset
    create_core_insurance_cl_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_core_insurance_cl_dataset',
        dataset_id=dataset_core_insurance_cl,
        bigquery_conn_id=bq_gcp_connector)


    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/CL_Inforce_PE/CL_Location_MonthlyInforce.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=project, dataset=dataset_core_insurance_cl, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)


    #creating table nonpromoted_CL_Location_MonthlyInforce in core_insurance_cl
    nonpromoted_CL_Location_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_CL_Location_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_CL_Location_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_cl ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/CL_Inforce_PE/CL_Policy_MonthlyInforce.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=bq_target_project, dataset=dataset_core_insurance_cl, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)


    #creating table nonpromoted_CL_Policy_MonthlyInforce in core_insurance_cl
    nonpromoted_CL_Policy_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_CL_Policy_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_CL_Policy_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_cl ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )
            
    ################################################################################
    # PL Inforce automation
    ################################################################################		

    #creating dataset create_core_insurance_pjpa_dataset
    create_core_insurance_pjpa_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_core_insurance_pjpa_dataset',
            dataset_id= dataset_core_insurance_pjpa,
            bigquery_conn_id=bq_gcp_connector)
            
    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/PL_Inforce_PE/PA_Article_MonthlyInforce.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=project, dataset = dataset_core_insurance_pjpa, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)

        
    #creating table nonpromoted_PA_Article_MonthlyInforce in core_insurance_pjpa
    nonpromoted_PA_Article_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_PA_Article_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_PA_Article_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_pjpa ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )	

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/PL_Inforce_PE/PJ_Item_MonthlyInforce.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=project, dataset = dataset_core_insurance_pjpa, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)

        
    #creating table nonpromoted_PJ_Item
    # _MonthlyInforce in core_insurance_pjpa
    nonpromoted_PJ_Item_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_PJ_Item_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_PJ_Item_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_pjpa ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )	

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/PL_Inforce_PE/PL_ItemLevel_MonthlyInforce.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=bq_target_project, dataset = dataset_core_insurance_pjpa, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)

        
    #creating table nonpromoted_PL_ItemLevel_MonthlyInforce in core_insurance_pjpa
    nonpromoted_PL_ItemLevel_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_PL_ItemLevel_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_PL_ItemLevel_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_pjpa ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )	

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/PL_Inforce_PE/PL_Policy_MonthlyInforcePremium.sql'),'r') as f:
        bq_sql = f.read()

    bq_sql = bq_sql.format(project=bq_target_project, dataset = dataset_core_insurance_pjpa, dataset_ref_pe_pc = dataset_ref_pe_pc, dataset_ref_pc_current = dataset_ref_pc_current, dataset_fy_2022 = dataset_fy_2022)

        
    #creating table nonpromoted_PL_Policy_MonthlyInforcePremium in core_insurance_pjpa
    nonpromoted_PL_Policy_MonthlyInforcePremium = BigQueryOperator(
        task_id='nonpromoted_PL_Policy_MonthlyInforcePremium',
        sql=bq_sql,
        destination_dataset_table='{project}.{dataset}.nonpromoted_PL_Policy_MonthlyInforcePremium'.format(project = bq_target_project , dataset = dataset_core_insurance_pjpa ),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'MONTH', 'field': 'AsOfDate'},
        gcp_conn_id = bq_gcp_connector,
        use_legacy_sql=False
    )

    ################################################################################
    # AIRFLOW ROUTING
    ################################################################################

    [PolicyCenter_sensor, PolicyCenter_sensor1] >> create_core_insurance_cl_dataset >> nonpromoted_CL_Location_MonthlyInforcePremium >> nonpromoted_CL_Policy_MonthlyInforcePremium

    [PolicyCenter_sensor, PolicyCenter_sensor1] >> create_core_insurance_pjpa_dataset >> [nonpromoted_PA_Article_MonthlyInforcePremium, nonpromoted_PJ_Item_MonthlyInforcePremium] >> nonpromoted_PL_ItemLevel_MonthlyInforcePremium >> nonpromoted_PL_Policy_MonthlyInforcePremium











