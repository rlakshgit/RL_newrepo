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
    'start_date': datetime(2022,3, 17),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'tverner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

LASTMONTH = '{{ ds_nodash }}'
THISMONTH = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1)).strftime("%Y%m%d") }}'
load_date= '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m-%d") }}'
risk_table= 'model-cl-im-risk-score-'+load_date


# Datset variables
dataset_core_insurance_pjpa = dataset_prefix+'core_insurance_pjpa'
dataset_core_insurance = dataset_prefix+'core_insurance'
dataset_data_products_t2_sales = dataset_prefix+'data_products_t2_sales'
dataset_wip_saleforce_admin_001 = dataset_prefix+'wip_saleforce_admin_001'
dataset_data_products_t1_quotes_offers = dataset_prefix+'data_products_t1_quotes_offers'
dataset_core_insurance_cl= dataset_prefix+'core_insurance_cl'



################################################################################
# INSTANTIATE DAG TO RUN AT UTC 11AM (6AM CDT)
################################################################################

dag = DAG('semimanaged_promoted_morning_dag_v1',
            max_active_runs=1,
            schedule_interval='0 10 * * *',
            default_args=default_args,
            catchup=True

)
dag.doc_md = """
## Promoted Morning
### Purpose
This DAG will update critical reporting and analytics resources on a daily basis. It is currently scheduled for 6AM CDT.
### Tasks
#### Recast
1. Query recast from Data Warehouse and load JSON file to Google Cloud Storage  
2. Upload JSON extract to BigQuery: `core_insurance_pjpa.recast_fy_2020`  
3. Union new table and `core_insurance_pjpa.recast_fy_2020_before_2014` into final table: `core_insurance_pjpa.promoted_recast_fy_2020_FULL`  
#### Policy Transactions  
1. Query policy transactions from Data Warehouse and load CSV file to Google Cloud Storage  
2. Upload CSV extract to BigQuery: `core_insurance.promoted_policy_transactions`  
#### Member Benefits Statement  
1. Query CL attributes from Data Warehouse and load straight to BigQuery: `data_products_t2_sales.nonpromoted_mbs_cl_attributes`  
2. Query Claims from Data Warehouse and load straight to BigQuery: `data_products_t2_sales.nonpromoted_mbs_claims`  
3. After both tables are loaded, join on JewelerKey (produced from a Regex cleanup of jeweler names) and load to `data_products_t2_sales.nonpromoted_benefit_statement`  
4. Finally, copy the final table to `wip_saleforce_admin_001.nonpromoted_jeweler_benefit_statement` for Chris C and Matt Z to consume and develop front-end interface for the data  
#### CL Locations  
1. Query locations from Data Warehouse and load JSON file to Google Cloud Storage  
2. Upload JSON extract to BigQuery: `core_insurance_cl.nonpromoted_cl_locations`  
#### CL Offers (Quotes) Dashboard  
1. Query CL Offers from Data Warehouse and load JSON file to Google Cloud Storage  
2. Upload JSON extract to BigQuery: `data_products_t1_quotes_offers.cl_offers`  
"""

trigger_object = 'triggers/inbound/dw_complete.json'
##GCS Trigger sensor
file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id= prod_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=9 * 60
                                                 ,dag = dag, deferrable = True)



################################################################################
# POLICY TRANSACTIONS
################################################################################


with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/core_insurance/policy_transactions.sql'),'r') as f:
    sql_file = f.read()
# creating dataset core_insurance
create_core_insurance_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_core_insurance_dataset',
        dataset_id= dataset_core_insurance,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)
#creating object core_insurance with data folder in semi_managed_reporting
policy_transactions_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
    task_id='extract_policy_transactions',
    sql=sql_file.format(LASTMONTH, THISMONTH),
    bucket=base_bucket,
    export_format='CSV',
    filename=folder_prefix+'core_insurance/data/{{ ds_nodash }}/export.csv',
    mssql_conn_id= mssql_connection,
    google_cloud_storage_conn_id= base_gcp_connector,
    dag=dag
)

#creating table promoted_policy_transactions in core_insurance dataset
policy_transactions_gcs_to_bq = GCSToBigQueryOperator(
    task_id='policy_transactions_extract_to_bq',
    bucket=base_bucket,
    source_objects=[folder_prefix+'core_insurance/data/{{ ds_nodash }}/export.csv'],
    destination_project_dataset_table='{project}.{dataset}.promoted_policy_transactions'.format(project = bq_target_project , dataset = dataset_core_insurance),
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id= bq_gcp_connector ,
    schema_update_options=(),
    dag=dag
)


################################################################################
# MEMBER BENEFITS STATEMENT
################################################################################

from plugins.operators.benefit_statement import mbs_cl_attributes, mbs_claims


#creating dataset data_products_t2_sales
create_data_products_t2_sales_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_data_products_t2_sales_dataset',
        dataset_id= dataset_data_products_t2_sales,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)
#creating table nonpromoted_mbs_cl_attributes in data_products_t2_sales
attributes = PythonOperator(
    task_id='query_cl_attributes',
    python_callable=mbs_cl_attributes,
    op_kwargs={'destination_project': bq_target_project, 'destination_dataset': dataset_data_products_t2_sales, 'gcp_connection': bq_gcp_connector,'sql_connection':mssql_connection },
    dag=dag
)

#creating table nonpromoted_mbs_claims in data_products_t2_sales
claims = PythonOperator(
    task_id='query_claims',
    python_callable=mbs_claims,
    op_kwargs={'destination_project': bq_target_project, 'destination_dataset': dataset_data_products_t2_sales, 'gcp_connection': bq_gcp_connector,'sql_connection':mssql_connection },
    dag=dag
)


with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/data_products_t2_sales/benefit_statement/mbs_join.sql'),'r') as f:
    mbs_sql_file = f.read()

mbs_sql_file = mbs_sql_file.format(project = bq_target_project, dataset =  dataset_data_products_t2_sales )


#creating table nonpromoted_benefit_statement in data_products_t2_sales dataset
merge = BigQueryOperator(
    task_id='combine_attributes_and_claims',
    #sql=sql_file.format(LASTMONTH, THISMONTH),   #check with Terry
    sql= mbs_sql_file.format(LASTMONTH, THISMONTH),
    destination_dataset_table='{project}.{dataset}.nonpromoted_benefit_statement'.format(project = bq_target_project , dataset= dataset_data_products_t2_sales),
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)

#creating dataset wip_saleforce_admin_001_dataset
create_wip_saleforce_admin_001_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_wip_saleforce_admin_001_dataset',
        dataset_id= dataset_wip_saleforce_admin_001,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)

#copying table from nonpromoted_benefit_statement in data_products_t2_sales dataset to nonpromoted_jeweler_benefit_statement in _wip_saleforce_admin_001
copy_to_salesforce_holding = BigQueryToBigQueryOperator(
    task_id='copy_statement_to_salesforce_dataset',
    source_project_dataset_tables='{project}.{dataset}.nonpromoted_benefit_statement'.format(project = bq_target_project, dataset = dataset_data_products_t2_sales),
    destination_project_dataset_table='{project}.{dataset}.nonpromoted_jeweler_benefit_statement'.format(project = bq_target_project , dataset = dataset_wip_saleforce_admin_001),
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    dag=dag
)

################################################################################
# CL LOCATIONS
################################################################################
with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/core_insurance_cl/CL_Locations.sql'),'r') as f:
    CL_Locations_sql= f.read()

#creating dataset core_insurance_cl
create_core_insurance_cl_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_core_insurance_cl_dataset',
        dataset_id= dataset_core_insurance_cl,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)

#creating cl_locations object with data and schema folder in semi_managed_reporting
cl_locations_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
                    task_id='cl_locations',
                    sql=CL_Locations_sql.format(LASTMONTH, THISMONTH),
                    bucket=base_bucket,
                    filename=folder_prefix+'cl_locations/data/{{ ds_nodash }}/export.json',
                    schema_filename=folder_prefix+'cl_locations/schemas/{{ ds_nodash }}/export.json',
                    mssql_conn_id= mssql_connection,
                    google_cloud_storage_conn_id=base_gcp_connector,
                    dag=dag
)

#creating table promoted_pc_cl_locations_core in core_insurance_cl dataset
cl_locations_gcs_to_bq = GCSToBigQueryOperator(
                    task_id='cl_locations_to_bq',
                    bucket=base_bucket,
                    source_objects=[folder_prefix+'cl_locations/data/{{ ds_nodash }}/export.json'],
                    destination_project_dataset_table='{project}.{dataset}.promoted_pc_cl_locations_core'.format(project = bq_target_project,dataset = dataset_core_insurance_cl ),
                    schema_fields=None,
                    schema_object=folder_prefix+'cl_locations/schemas/{{ ds_nodash }}/export.json',
                    source_format='NEWLINE_DELIMITED_JSON',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=0,
                    write_disposition='WRITE_TRUNCATE',
                    gcp_conn_id=bq_gcp_connector,
                    schema_update_options=(),
                    dag=dag
)


################################################################################
# CL OFFERS / QUOTES DASHBOARD
################################################################################


with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed/data_products_t1_quotes_offers/remapping_cl_offers.sql'),'r') as f:
    cl_offers_sql = f.read()


#creating dataset data_products_t1_quotes_offers
create_data_products_t1_quotes_offers_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_data_products_t1_quotes_offers_dataset',
        dataset_id= dataset_data_products_t1_quotes_offers,
        bigquery_conn_id=bq_gcp_connector,
        dag=dag)

#creating object cl_offers with data and schema folder in semi_managed_reporting
cl_offers_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
    task_id='extract_cl_offers',
    sql=cl_offers_sql,
    bucket=base_bucket,
    export_format='JSON',
    filename=folder_prefix+'cl_offers/data/{{ ds_nodash }}/export.json',
    schema_filename=folder_prefix+'cl_offers/schema/{{ ds_nodash }}/export.json',
    mssql_conn_id=  mssql_connection,
    google_cloud_storage_conn_id=base_gcp_connector,
    dag=dag)

#creating table wip_cl_offers in data_products_t1_quotes_offers
cl_offers_gcs_to_bq = GCSToBigQueryOperator(
    task_id='cl_offers_to_bq',
    bucket=base_bucket,
    source_objects=[folder_prefix+'cl_offers/data/{{ ds_nodash }}/export.json'],
    source_format='NEWLINE_DELIMITED_JSON',
    destination_project_dataset_table='{project}.{dataset}.wip_cl_offers'.format(dataset = dataset_data_products_t1_quotes_offers, project = bq_target_project),
    schema_object=folder_prefix+'cl_offers/schema/{{ ds_nodash }}/export.json',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=0,
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    schema_update_options=(),
    dag=dag)

with open(os.path.join(configuration.get('core', 'dags_folder'), r'semi-managed//data_products_t1_quotes_offers/bq_final_select.sql'),'r') as f:
    bq_sql = f.read()

bq_sql = bq_sql.format(project=bq_target_project, dataset = dataset_data_products_t1_quotes_offers)

#creating table non_promoted_cl_offers in data_products_t1_quotes_offers
reconfigure_cl_offers = BigQueryOperator(
    task_id='tableau_cl_offers_table',
    sql=bq_sql,
    destination_dataset_table='{project}.{dataset}.nonpromoted_cl_offers'.format(project = bq_target_project , dataset =  dataset_data_products_t1_quotes_offers ),
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)

#deleting table non_promoted_cl_offers in data_products_t1_quotes_offers
polish_cl_offers = BigQueryOperator(
    task_id='cleanup_cl_offers',
    sql='DROP TABLE `{project}.{dataset}.wip_cl_offers`;'.format(project = bq_target_project, dataset = dataset_data_products_t1_quotes_offers),
    gcp_conn_id= bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)




file_sensor>>create_core_insurance_dataset>>policy_transactions_to_gcs >> policy_transactions_gcs_to_bq

file_sensor>>create_data_products_t2_sales_dataset>>[attributes, claims] >> merge >> create_wip_saleforce_admin_001_dataset>> copy_to_salesforce_holding

file_sensor>>create_core_insurance_cl_dataset >>cl_locations_to_gcs >> cl_locations_gcs_to_bq

file_sensor>>create_data_products_t1_quotes_offers_dataset>>cl_offers_to_gcs >> cl_offers_gcs_to_bq >> reconfigure_cl_offers >> polish_cl_offers

