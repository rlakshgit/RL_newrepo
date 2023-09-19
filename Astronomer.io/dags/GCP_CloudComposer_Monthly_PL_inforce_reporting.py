###Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLServertoBQ_inforce import SQLServertoBQOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
import os
from airflow.operators.empty import EmptyOperator

#####Initializing Variables ########
env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix = 'GNS_DEV_'
    # project = 'jm-dl-landing'
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
    # project = 'jm-dl-landing'
    prod_gcp_connector = 'qa_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_qa01_edl_lnd'


elif env.lower() == 'prod':
    base_bucket = 'semi-managed-reporting'
    dataset_prefix = ''
    # project = 'semi-managed-reporting'
    prod_gcp_connector = 'prod_edl'
    base_gcp_connector = 'semi_managed_gcp_connection'
    bq_gcp_connector = 'semi_managed_gcp_connection'
    bq_target_project = 'semi-managed-reporting'
    folder_prefix = ''
    mssql_connection = 'instDW_PROD'
    trigger_bucket = 'jm_prod_edl_lnd'

destination_dataset = folder_prefix + 'core_insurance_inforce_history'
destination_pl_inforce_itemlevel_table = 'pl_items_monthly_inforce'
destination_pl_inforce_policylevel_table = 'pl_policy_monthly_inforce'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'nreddy@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
trigger_object = 'triggers/inbound/dw_complete.json'

try:
    load_type = Variable.get('PL_Inforce_fullload')
except:
    Variable.set('PL_Inforce_fullload', 'True')
    load_type = Variable.get('PL_Inforce_fullload')

try:
    PL_Itemlevel_filter = Variable.get('PL_Itemlevel_filter').split(',')
except:
    Variable.set('PL_Itemlevel_filter','inforce.DateKey = {{ ds_nodash }}')
    PL_Itemlevel_filter = Variable.get('PL_Itemlevel_filter').split(',')

with DAG('semimanaged_pl_monthly_inforce_dag',
         max_active_runs=1,
         schedule_interval='30 10 1 * *',
         default_args=default_args,
         is_paused_upon_creation=True,
         catchup=False
         ) as dag:
    ##GCS Trigger sensor
    file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id=prod_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=9 * 60
                                                 , deferrable = True)

    # creating dataset core_insurance_inforce_history
    create_core_insurance_inforce_history_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_core_insurance_inforce_history_dataset',
        dataset_id=destination_dataset,
        bigquery_conn_id=bq_gcp_connector)

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/PL_and_CL_inforce/v_Inforce_Summary.sql'), 'r') as summary:
        summary_sql = summary.read()

    if load_type == 'True':
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               r'ETL/PL_and_CL_inforce/PL_Itemlevel_fullload.sql'), 'r') as f:
            sql = f.read()

    else:
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               r'ETL/PL_and_CL_inforce/PL_Itemlevel.sql'), 'r') as f:
            sql = f.read()
            PL_Itemlevel_filter = ['Delta_load']

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/PL_and_CL_inforce/Create_Inforce_table_PL_Itemlevel.sql'), 'r') as f1:
        create_table_sql = f1.read()

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/PL_and_CL_inforce/Create_Inforce_table_PL_Policylevel.sql'), 'r') as f2:
        create_policy_table_sql = f2.read()

    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/PL_and_CL_inforce/PL_Policylevel.sql'), 'r') as f3:
        load_policy_table_sql = f3.read()

    # Policy Count View - open the sql file
    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'ETL/PL_and_CL_inforce/PL_Policy_Monthly_Count.sql'), 'r') as f4:
        create_policy_count_view_sql = f4.read()


    # creating PL Item level table
    create_PL_Itemlevel_table = runbqscriptoperator(task_id='create_monthly_pl_items_inforce_table',
                                                    sql=create_table_sql.format(project=bq_target_project,
                                                                                dataset=destination_dataset),
                                                    bigquery_conn_id=bq_gcp_connector)
    # Creating PL Policy level table
    create_PL_Policylevel_table = runbqscriptoperator(task_id='create_monthly_pl_policy_inforce_table',
                                                      sql=create_policy_table_sql.format(project=bq_target_project,
                                                                                         dataset=destination_dataset),
                                                      bigquery_conn_id=bq_gcp_connector)


    # creating PL Inforce Summary View
    create_Summary_PL_Itemlevel_view = runbqscriptoperator(task_id='create_summary_pl_items_inforce_view',
                                                           sql=summary_sql.format(project=bq_target_project,
                                                                                  dataset=destination_dataset,
                                                                                  date="{{ ds }}"),
                                                           bigquery_conn_id=bq_gcp_connector)

    # creating PL monthly count view
    create_pl_monthly_count_view = runbqscriptoperator(task_id='create_pl_monthly_count_view',
                                                       sql=create_policy_count_view_sql.format(
                                                           project=bq_target_project,
                                                           dataset=destination_dataset,
                                                           date="{{ ds }}"),
                                                       bigquery_conn_id=bq_gcp_connector)

    load_policy_table = BigQueryOperator(task_id='load_monthly_pl_policy_inforce_table',
                                         sql=load_policy_table_sql.format(project=bq_target_project,
                                                                          dataset=destination_dataset, date="{{ ds }}"),
                                         destination_dataset_table='{base_dataset}.{base_table}${date}'.format(
                                             project=bq_target_project,
                                             base_dataset=destination_dataset,
                                             base_table=destination_pl_inforce_policylevel_table,
                                             date="{{ ds_nodash }}"
                                         ),
                                         write_disposition='WRITE_TRUNCATE',
                                         create_disposition='CREATE_IF_NEEDED',
                                         gcp_conn_id=bq_gcp_connector,
                                         allow_large_results=True,
                                         use_legacy_sql=False,
                                         time_partitioning={"type": "DAY", 'field': 'InforceDate'})
    
    complete_loading = EmptyOperator(task_id = 'complete_loading')
    complete_setup = EmptyOperator(task_id = 'complete_setup')

    counter = 0
    for datefilter in PL_Itemlevel_filter:
        load_item_table = SQLServertoBQOperator(
            task_id='load_monthly_pl_items_inforce_table_{counter}'.format(counter=counter),
            sql=sql.format(date_filter=datefilter,date="{{ ds }}"),
            mssql_conn_id=mssql_connection,
            bigquery_conn_id=bq_gcp_connector,
            destination_dataset=destination_dataset,
            destination_table=destination_pl_inforce_itemlevel_table,
            project=bq_target_project)
        

        counter = counter + 1

        file_sensor >> create_core_insurance_inforce_history_dataset >>  [ create_PL_Itemlevel_table , create_PL_Policylevel_table] >> complete_setup >> [ load_item_table] >> complete_loading >> [
            load_policy_table, create_Summary_PL_Itemlevel_view]
        load_policy_table >> create_pl_monthly_count_view 
