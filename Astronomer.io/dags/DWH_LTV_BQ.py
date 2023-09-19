import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import json
import os
from airflow import configuration
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.ltv_load_data_operator import SQLServertoBQOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator


ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,9,12),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'rlaksh@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDW_STAGE'
    prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDW_STAGE'
    # mssql_connection_target = 'db_QA6_dwh'
    prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'
    mssql_connection_target = 'instDW_PROD'
    prefix = ''

source = 'LTV'
source_abbr = 'LTV'
dataset = prefix + 'jm_model'
source_table_name = 't_ltv_source'
allversion_table_name = 't_ltv_allversions'
currentVersion_table_name = 't_ltv_currentversions'

FULL_LOAD = Variable.get('ltv_fullload')
 
if FULL_LOAD == 'Y':
    # Construct a list 
    end = default_dag_args['start_date']
    start_date = datetime.strptime('20120101', '%Y%m%d')
    time_difference = relativedelta(end, start_date)
    time_difference = int(time_difference.years)
    end_date = start_date + relativedelta(years = time_difference)
    date_list = []
    while int(start_date.year) <= int(end_date.year):
        date_list.append(start_date.strftime("%Y%m%d"))
        start_date = start_date + relativedelta(years = 1)
    date_list.append(end.strftime('%Y%m%d'))

    
else:
    start = '{{ ds_nodash }}'
    end = '{{ yesterday_ds_nodash }}'  
    date_list = [end,start]

# Variable.set('ltv_fullload', 'N')
source_extract_sql_path = r'ETL/LTV/01_LoadData_FromDW.sql'
all_version_sql_path = r'ETL/LTV/02_Retention_PolicyProfile_AllTransactions.sql'
current_version_sql_path = r'ETL/LTV/03_LTV_CurrentVersion.sql'
dq_check_sql_path =  r'ETL/LTV/04_DQ Checks.sql'
current_version_view_sql_path = r'ETL/LTV/05_LTV_Latest_current_view.sql'
create_source_table_sql_path = r'ETL/LTV/create_source_table.sql'
create_allversions_table_sql_path  = r'ETL/LTV/create_allversions_table.sql'
create_currentversions_table_sql_path  = r'ETL/LTV/create_currentversion_table.sql'

# create source table
with open(os.path.join(configuration.get('core', 'dags_folder'), create_source_table_sql_path)) as f:
    read_file_data = f.readlines()
create_source_table_sql = "\n".join(read_file_data)

# create all versions table
with open(os.path.join(configuration.get('core', 'dags_folder'), create_allversions_table_sql_path)) as f:
    read_file_data = f.readlines()
create_allversions_table_sql = "\n".join(read_file_data)

# create current versions table
with open(os.path.join(configuration.get('core', 'dags_folder'), create_currentversions_table_sql_path)) as f:
    read_file_data = f.readlines()
create_currentversions_table_sql = "\n".join(read_file_data)

# source extract sql
with open(os.path.join(configuration.get('core', 'dags_folder'), source_extract_sql_path)) as f:
    read_file_data = f.readlines()
load_data_sql = "\n".join(read_file_data)

# all version sql query  
with open(os.path.join(configuration.get('core', 'dags_folder'), all_version_sql_path)) as f:
    read_file_data = f.readlines()
LTV_all_version_sql = "\n".join(read_file_data)

# current version sql
with open(os.path.join(configuration.get('core', 'dags_folder'), current_version_sql_path)) as f:
    read_file_data = f.readlines()
LTV_current_version_sql = "\n".join(read_file_data)

# dq checks sql
with open(os.path.join(configuration.get('core', 'dags_folder'), dq_check_sql_path)) as f:
    read_file_data = f.readlines()
dq_check_sql = "\n".join(read_file_data)

# current verson view 
with open(os.path.join(configuration.get('core', 'dags_folder'), current_version_view_sql_path)) as f:
    read_file_data = f.readlines()
current_version_view_sql = "\n".join(read_file_data)


def _set_var(k, v):
    Variable.set(k, v)
    return f'Variable {k} is set to {v}'


with DAG(
	'LTV_dag',
    schedule_interval='0 6 * * *',  
    catchup=True,
    max_active_runs=1,
    default_args=default_dag_args) as dag:

    create_dataset= BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset',
        dataset_id=dataset,
        bigquery_conn_id=bq_gcp_connector)

    create_table = runbqscriptoperator(
        task_id='create_ltv_source_table',
        sql=create_source_table_sql.format(project = bq_target_project,dataset = dataset ,table = source_table_name),
        bigquery_conn_id=bq_gcp_connector)
    
    create_ltv_table = runbqscriptoperator(
        task_id='create_ltv_allversions_table',
        sql=create_allversions_table_sql.format(project = bq_target_project,dataset = dataset ,table = allversion_table_name),
        bigquery_conn_id=bq_gcp_connector)
    
    create_ltv_current_version_table = runbqscriptoperator(
        task_id='create_ltv_currentversion_table',
        sql=create_currentversions_table_sql.format(project = bq_target_project,dataset = dataset ,table = currentVersion_table_name),
        bigquery_conn_id=bq_gcp_connector)

    dq_check = runbqscriptoperator(
        task_id='DQ_check',
        sql=dq_check_sql.format(project=bq_target_project, dataset=dataset),
        bigquery_conn_id=bq_gcp_connector)

    current_version_view = runbqscriptoperator(
        task_id='create_current_version_view',
        sql=current_version_view_sql.format(project=bq_target_project, dataset=dataset),
        bigquery_conn_id=bq_gcp_connector)

    create_dataset >> create_table >> create_ltv_table >> create_ltv_current_version_table 

    complete_load = EmptyOperator(task_id = 'complete_loading')
    
    for x in date_list[:-1]:
        start_year = x
        end_year = date_list[date_list.index(x)+1]
        
        load_data_from_MSSql_BQ = SQLServertoBQOperator(
            task_id='load_data_from_DataWarehouse_to_BQ_{num}'.format(num=date_list.index(x)),
            mssql_conn_id=mssql_connection_target,
            bigquery_conn_id=bq_gcp_connector,
            sql=load_data_sql.format(StartDate=start_year, EndDate=end_year, date="{{ ds_nodash }}"),
            destination_dataset=dataset,
            destination_table=source_table_name,
            project=bq_target_project,
            pool='singel_task')
        
        create_ltv_current_version_table >> load_data_from_MSSql_BQ >> complete_load

    LOAD_STATIC = Variable.get('ltv_load_static_tables')

    if LOAD_STATIC == 'Y':

        complete_load_static_tables = EmptyOperator(task_id = 'complete_load_static_tables')

        set_full_load_var = PythonOperator( task_id = 'set_history_load_var',
                                            python_callable = _set_var,
                                            op_kwargs = {"k" : "ltv_fullload", "v": "N"} )


        set_load_static = PythonOperator( task_id = 'set_static_tables_var',
                                            python_callable = _set_var,
                                            op_kwargs = {"k" : "ltv_load_static_tables", "v": "N"} )

        load_all_version_table = BigQueryOperator(task_id='Load_all_version_table',
                                        sql=LTV_all_version_sql.format(
                                            project=bq_target_project,
                                            dataset=dataset, 
                                            date="{{ ds }}"),
                                        destination_dataset_table='{base_dataset}.{base_table}${date}'.format(
                                            project=bq_target_project,
                                            base_dataset=dataset,
                                            base_table=allversion_table_name,
                                            date="{{ ds_nodash }}"
                                        ),
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        allow_large_results=True,
                                        use_legacy_sql=False,
                                        time_partitioning={"type": "DAY", 'field': 'allversion_load_date'})

        load_ltv_current_version = BigQueryOperator(task_id='create_ltv_current_version',
                                                    sql=LTV_current_version_sql.format(
                                                        project=bq_target_project,
                                                        dataset=dataset, date="{{ ds }}", 
                                                        all_version_table=allversion_table_name),
                                                    destination_dataset_table='{base_dataset}.{base_table}${date}'.format(
                                                        project=bq_target_project,
                                                        base_dataset=dataset,
                                                        base_table=currentVersion_table_name,
                                                        date="{{ ds_nodash }}"
                                                    ),
                                                    write_disposition='WRITE_TRUNCATE',
                                                    create_disposition='CREATE_IF_NEEDED',
                                                    gcp_conn_id=bq_gcp_connector,
                                                    allow_large_results=True,
                                                    use_legacy_sql=False,
                                                    time_partitioning={"type": "DAY", 'field': 'currentversion_load_date'})

        static_table_list = ['ref_ltv_ltv.csv', 'ref_territory_group.csv', 'ref_ltv_polvaluelabel.csv', 'ref_ltv_cntvalue.csv', 'ref_ltv_age.csv', 'ref_ltv_itemcount.csv', 'ref_pp_policyprofile.csv', 'ref_pp_territory.csv', 'ref_pp_region.csv', 'ref_pp_itemtype.csv', 'ref_pp_valuelabel.csv', 'ref_pp_itemvalue.csv', 'ref_pp_cntlabel.csv', 'ref_pp_itemcount.csv', 'ref_pp_polvaluelabel.csv', 'ref_pp_cntvalue.csv', 'ref_pp_agelabel.csv', 'ref_pp_itemage.csv']

        for table in static_table_list:

            static_table_name = table[:-4]
            schema_file_name = static_table_name + '.json'

            load_static_data = GoogleCloudStorageToBigQueryOperator(
                    task_id='load_static_tables_{table_name}'.format(table_name=static_table_name),                                                                          
                    bucket=base_bucket,
                    source_objects=['LTV/static_tables/{table}'.format(table=table)],
                    destination_project_dataset_table='{project}.{dataset}.{table_name}'.format(
                    project=bq_target_project,
                    dataset=dataset,
                    table_name=static_table_name),
                    schema_object='LTV/static_tables_schema/{schema_file}'.format(schema_file=schema_file_name),
                    source_format='CSV',
                    compression='NONE',
                    ignore_unknown_values=True,
                    allow_quoted_newlines =True,
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=1,
                    write_disposition='WRITE_TRUNCATE',
                    max_bad_records=0,
                    bigquery_conn_id=bq_gcp_connector,
                    google_cloud_storage_conn_id=base_gcp_connector,
                    # schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION'],
                    file_prefix=prefix,
                    #src_fmt_configs=None,
                    # autodetect=True,
                    gcs_to_bq = 'True')


            complete_load >>  load_static_data >> complete_load_static_tables >> load_all_version_table >> load_ltv_current_version >> dq_check >> current_version_view >> set_full_load_var >> set_load_static

    else:
        load_all_version_table = BigQueryOperator(task_id='Load_all_version_table',
                                        sql=LTV_all_version_sql.format(
                                            project=bq_target_project,
                                            dataset=dataset, 
                                            date="{{ ds }}"),
                                        destination_dataset_table='{base_dataset}.{base_table}${date}'.format(
                                            project=bq_target_project,
                                            base_dataset=dataset,
                                            base_table=allversion_table_name,
                                            date="{{ ds_nodash }}"
                                        ),
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        allow_large_results=True,
                                        use_legacy_sql=False,
                                        time_partitioning={"type": "DAY", 'field': 'allversion_load_date'})

        current_ltv_current_version = BigQueryOperator(task_id='create_ltv_current_version',
                                                    sql=LTV_current_version_sql.format(
                                                        project=bq_target_project,
                                                        dataset=dataset, date="{{ ds }}", 
                                                        all_version_table=allversion_table_name),
                                                    destination_dataset_table='{base_dataset}.{base_table}${date}'.format(
                                                        project=bq_target_project,
                                                        base_dataset=dataset,
                                                        base_table=currentVersion_table_name,
                                                        date="{{ ds_nodash }}"
                                                    ),
                                                    write_disposition='WRITE_TRUNCATE',
                                                    create_disposition='CREATE_IF_NEEDED',
                                                    gcp_conn_id=bq_gcp_connector,
                                                    allow_large_results=True,
                                                    use_legacy_sql=False,
                                                    time_partitioning={"type": "DAY", 'field': 'currentversion_load_date'})


        set_full_load_var = PythonOperator( task_id = 'set_history_load_var',
                                            python_callable = _set_var,
                                            op_kwargs = {"k" : "ltv_fullload", "v": "N"} )

        
        complete_load >> load_all_version_table >> current_ltv_current_version >> dq_check >> current_version_view >> set_full_load_var 


