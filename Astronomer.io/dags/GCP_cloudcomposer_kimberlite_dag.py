import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from airflow.models import Variable

####This Dag can be used to create create or update kimberlite table########
ts = calendar.timegm(time.gmtime())
logging.info(ts)


env = Variable.get('ENV')


if env.lower() == 'dev':
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'GN_DEV_'

elif env.lower() == 'qa':
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'

elif env.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''

source = 'kimberlite'
source_abbr = 'kimberlite'

#is kimberlite just policy center?
#source_dataset_id = '{prefix}ref_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
kimberlite_core_dataset = '{prefix}ref_kimberlite_core'.format(prefix=folder_prefix)
kimberlite_config_dataset_id = '{prefix}ref_{source_abbr}_config'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
kimberlite_dataset = '{prefix}ref_kimberlite'.format(prefix=folder_prefix)
kimberlite_DQ_dataset = '{prefix}ref_kimberlite_DQ'.format(prefix=folder_prefix)
kimberlite_core_DQ_dataset = '{prefix}ref_kimberlite_core_DQ'.format(prefix=folder_prefix)
dataproducts_dataset = '{prefix}gld_kimberlite'.format(prefix=folder_prefix)
#Are the kimberlite tables going to be partitioned tables?


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# It should be dependent on Policy center prioritized dag
#Try using dagcompleteoperator

with DAG(
        'Kimberlite_config_dag',
        schedule_interval= '@once', #"@daily",#dt.timedelta(days=1), #
        catchup=False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        
        check_for_kimberlite_config_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_dataset_{source}_config'.format(source=source_abbr),
            dataset_id=kimberlite_config_dataset_id,
            bigquery_conn_id=bq_gcp_connector)
        
        check_for_kimberlite_core_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_kimberlite_core',
            dataset_id=kimberlite_core_dataset,
            bigquery_conn_id=bq_gcp_connector)

        check_for_kimberlite_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_kimberlite_dataset',
            dataset_id=kimberlite_dataset,
            bigquery_conn_id=bq_gcp_connector)

        check_for_kimberlite_DQ_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_kimberlite_DQ_dataset',
            dataset_id=kimberlite_DQ_dataset,
            bigquery_conn_id=bq_gcp_connector)
        
        check_for_kimberlite_core_DQ_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_kimberlite_core_DQ_dataset',
            dataset_id=kimberlite_core_DQ_dataset,
            bigquery_conn_id=bq_gcp_connector)

        create_kimberlite_dataproduct_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_dataset_kimberlite_dataproducts',
            dataset_id=dataproducts_dataset,
            bigquery_conn_id=bq_gcp_connector)



        #creating config tables
        base_etl_folder = r'ETL/{source}/Kimberlite_scripts/Kimberlite_config.sql'.format(source=source)


        with open(os.path.join(configuration.get('core', 'dags_folder'),base_etl_folder)) as f:
            read_file_data =f.readlines()
        cleaned_file_data = "\n".join(read_file_data)
        sql = cleaned_file_data.format(project=bq_target_project, dataset=kimberlite_config_dataset_id, core_dataset=kimberlite_core_dataset)
        build_kimberlite_config_tables = runbqscriptoperator(
                                                        task_id='create_kimberlite_config_tables',
                                                        sql = sql,
                                                        bigquery_conn_id = bq_gcp_connector)

        #creating kimberlite tables
        create_table_scripts = ['Kimberlite_create_table_script.sql', 'SourceRecordExceptions.sql']
        for script in create_table_scripts:

            base_etl_folder = r'ETL/{source}/Kimberlite_scripts/{script}'.format(source=source, script=script)

            with open(os.path.join(configuration.get('core', 'dags_folder'), base_etl_folder)) as f:
                read_file_data = f.readlines()
            cleaned_file_data = "\n".join(read_file_data)

            sql = cleaned_file_data.format(project=bq_target_project, core_dataset=kimberlite_core_dataset, dataset=kimberlite_dataset,dest_dataset=kimberlite_DQ_dataset,dp_dataset=dataproducts_dataset, date="{{ ds_nodash }}")
            build_kimberlite_tables=runbqscriptoperator(
                task_id='create_kimberlite_tables_{file}'.format(file=script),
                sql=sql,
                bigquery_conn_id=bq_gcp_connector)

            create_kimberlite_dataproduct_dataset>>check_for_kimberlite_DQ_dataset>>check_for_kimberlite_core_DQ_dataset>>check_for_kimberlite_dataset>>check_for_kimberlite_core_dataset>>check_for_kimberlite_config_dataset >> [build_kimberlite_config_tables,build_kimberlite_tables ]

