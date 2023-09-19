import logging
import datetime as dt
import calendar
import time
import sys
import os

from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow import DAG, macros
from airflow.models import Variable

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,12,12),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

env = Variable.get('ENV')

source = 'KimberliteMergeScripts'
source_abbr = 'kimberlitemergescripts'

if env.lower() == 'dev':
    bq_gcp_connector = 'dev_edl'
    bq_source_project = 'dev-edl'
    bq_dataset_prefix = 'GN_DEV_'

elif env.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_source_project = 'qa-edl'
    bq_dataset_prefix = 'B_QA_'

elif env.lower() == 'prod':
    bq_gcp_connector = 'prod_edl'
    bq_source_project = 'prod-edl'
    bq_dataset_prefix = ''

source_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
destination_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
source_pc_dataset = '{prefix}ref_pc_current'.format(prefix=bq_dataset_prefix)
core_dataset = '{prefix}ref_kimberlite_core'.format(prefix=bq_dataset_prefix)

delete_partition_date = '{{ macros.ds_add(ds,-2) }}'

with DAG(
        'Kimberlite_MergeScripts_dag',
        schedule_interval='0 14 * * *',  # '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
        
    # task_sensor_core = ExternalTaskSensorAsync(
        # task_id='check_core_tables_complete',
        # external_dag_id='Kimberlite_core_dag',
     # #   external_task_id='set_source_bit',
        # external_task_id='complete_load',
        # execution_delta=timedelta(hours=0),
        # timeout=7200)        

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')
                                 
    create_kimberlite_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_kimberlite',
        dataset_id=destination_dataset,
        bigquery_conn_id=bq_gcp_connector)

    # Now create the Kimberlite MergeScript queries with or without dependencies.
    query_list_dir = [
                      "ASL.sql",
                      "BusinessType.sql",
                      "CauseOfLossStatsReporting.sql",
                      "DOLStates.sql",
                    #  "JewelerCleanupPAS.sql", OBSOLETE
                      "CauseOfLossDesc.sql",
                      "ClosedClaimOutcome.sql",
                      "ConformedCancelReasonDescription.sql",
                      "ItemClassCrosswalk.sql",
                      "LOB.sql",
                      "PolicyTransactionType.sql",
                      "StatTransactionID.sql",
                      "NameValue.sql",
                      "ReportInfo.sql",
                      "Lookup.sql",
                      "PremiumTaxStateLookups.sql",
                      "Treaty.sql",
                      "UniquePolicies.sql",
                      "ConstructionCode.sql",
                      "ExtractTransactionType.sql",
                      "ProtectionCode.sql",
                      "StateCodes.sql",
                      "TopAgents.sql",
                      "CoverageType.sql", #??
                      "DistrictDefinitions.sql", #??
                      "Geography.sql", #??   
                      "PolicyTypeDescriptions.sql", #??  
                      "t_lkup_Product.sql",
                      "t_lkup_ProductClass.sql",   
                      "t_lkup_ProductType.sql", 
                      "BusinessUnit.sql",
                      "v_ProductHierarchy.sql",
                      "t_lkup_LegalEntity.sql",
                      "t_lkup_StrategicPartners.sql"               
                     ]
    
    # Read the list of merge scripts to process from a variable; default is the query_list_dir directly above
    # This allows addition of new scripts without modifying this DAG code.
    ENABLED_SCRIPTS = Variable.get('Kimberlite_enabled_merge_scripts', default_var=', '.join(query_list_dir)).split(', ')            
        
   # for sql_group in query_list_dir:

   #    if sql_group not in ENABLED_SCRIPTS:
   #         continue
   
    for sql_group in ENABLED_SCRIPTS:        
        sql = sql_group
        table_name = sql.replace('.sql', '')
        

        with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/Merge_scripts/',
                               sql)) as f:
            sql_read_sql = f.readlines()

        sql_sql = "\n".join(sql_read_sql)
        sql_sql = sql_sql.format(project=bq_source_project, 
                                 source_dataset=source_dataset, 
                                 dest_dataset=destination_dataset,
                                 core_dataset=core_dataset,
                                 partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                 pc_dataset = source_pc_dataset,
                                 date="{{ ds }}",
                                 rbracket='}',
                                 lbracket='{')

        # Creating Kimberlite tables
        create_kimberlite_tables = runbqscriptoperator(task_id='{query}_script'.format(query=table_name),
                                                       sql=sql_sql,
                                                       bigquery_conn_id=bq_gcp_connector)
       
                        
        create_kimberlite_dataset >> create_kimberlite_tables >> bit_set                                                       
