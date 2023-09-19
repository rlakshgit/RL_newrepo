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
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
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

source = 'KimberliteBuildingBlocksDQ'
source_abbr = 'kimberlitebuildingblocksdq'

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

destination_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
source_pc_dataset = '{prefix}ref_pc_current'.format(prefix=bq_dataset_prefix)
source_bc_dataset = '{prefix}ref_bc_current'.format(prefix=bq_dataset_prefix)
dq_dataset = '{prefix}ref_kimberlite_DQ'.format(prefix=bq_dataset_prefix)


delete_partition_date = '{{ macros.ds_add(ds,-2) }}'

with DAG(
        'Kimberlite_BuildingBlocksDQ_dag',
        schedule_interval='0 16 * * *',  # '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')
                                 
    task_sensor_bb = ExternalTaskSensorAsync(
        task_id='check_bb_tables_complete',
        external_dag_id='Kimberlite_BuildingBlocks_dag',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=1),
        timeout=7200)

    # Now create the Kimberlite BuildingBlocks queries with or without dependencies.
    query_list_dir = [        
        "TransactionConfiguration.sql",
        "ProductAndRiskConfiguration.sql",
        "PolicyLevelAttributes.sql",
        "CLRiskBusinessOwnersAttributes.sql",
        "CLRiskInLandMarineAttributes.sql",
        "PJRiskLevelAttributes.sql",
        "PAJewelryCoverageLevelAttributes.sql",
        "PJCoverageLevelAttributes.sql",
        "CLCoverageLevelAttributes.sql",
        "PAJewelryRiskLevelAttributes.sql",
        "ClaimBOPDirect.sql",   
        "ClaimBOPCeded.sql",        
        "ClaimPJDirect.sql",        
        "ClaimPJCeded.sql",        
        "ClaimPADirect.sql",        
        "ClaimIMDirect.sql"    
        "ClaimIMCeded.sql",
        "AccountAttributes.sql",
        "ContactAttributes.sql",
        "AccountContactRoleAttributes.sql",
        "ClaimAttributes.sql"   
    ]
                         
    DQ_list = [
              "Dupes",
              "MissingByX",
              "LocnStock",
              "TransxLocn",
              "SCHTransCovCode",
              "TransCovCode"
              ]

    ENABLED_BUILDING_BLOCKS = Variable.get('Kimberlite_enabled_building_blocks',
                                           default_var=', '.join(query_list_dir)).split(', ')

    for sql_group in query_list_dir:

        if sql_group not in ENABLED_BUILDING_BLOCKS:
            continue

        sql = sql_group
        table_name = sql.replace('.sql', '')
   
        for dq_check in DQ_list:
            dq_filename = 'BBDQ_' + sql.replace('.sql', '_' + dq_check + '.sql')
            dq_tablename = dq_filename.replace('.sql', '').replace('BBDQ_', '')

                
            try:
                with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/DQ/', dq_filename)) as f:
                    sql_read_sql = f.readlines()

                    sql_sql = "\n".join(sql_read_sql)
                    sql_sql = sql_sql.format(project=bq_source_project, 
                                             dataset=source_pc_dataset, 
                                             pc_dataset=source_pc_dataset,
                                             bc_dataset=source_bc_dataset,
                                             dest_dataset=destination_dataset,
                                             partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                             date="{{ ds }}")
            except:
                sql_sql = ''
                               
            if (len(sql_sql) == 0): # or (dq_tablename not in ENABLED_CHECKS):
                dq_check = EmptyOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename))
                dq_status = EmptyOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename))
            else:
                dq_check = BigQueryOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename),
                                            sql=sql_sql,
                                            destination_dataset_table='{project}.{dataset}.{table}'.format(
                                                                project=bq_source_project,
                                                                dataset=dq_dataset,
                                                                table=dq_tablename
                                                                ),
                                            write_disposition='WRITE_TRUNCATE',
                                            gcp_conn_id=bq_gcp_connector,
                                            use_legacy_sql=False)

                success_sql = '''SELECT
                                    CASE WHEN EXISTS 
                                    (
                                        SELECT * FROM `{project}.{dest_dataset}.{table}` WHERE DATE(bq_load_date)=DATE({partition_date})
                                    )
                                    THEN FALSE
                                    ELSE TRUE
                                END
                              '''
                success_sql = success_sql.format(project=bq_source_project, 
                                         dataset=source_pc_dataset, 
                                         pc_dataset=source_pc_dataset,
                                         bc_dataset=source_bc_dataset,
                                         dest_dataset=dq_dataset,
                                         partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                         date="{{ ds }}",
                                         table=dq_tablename)                         
                              
                dq_status = BigQueryCheckOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename),
                                                                     sql=success_sql,
                                                                     gcp_conn_id=bq_gcp_connector,
                                                                     use_legacy_sql=False)
                                    

            task_sensor_bb >> dq_check >> dq_status >> bit_set