import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import json
import os
from airflow import configuration
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from airflow.operators.bash_operator import BashOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_SQLServertoBQ_Retention_hubspot import SQLServertoBQOperator
from plugins.operators.jm_RUN_SQLServerQuery import RUNSQLServerQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import BranchPythonOperator


ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,4,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'rlaksh@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDataRetention_STAGE'
    prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDataRetention_STAGE'
    prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'
    mssql_connection_target = 'instDataRetention_PROD'
    prefix = ''

source = 'Hubspot'
source_abbr = 'hubspot_v2'

purge_90_day_dataset = 'ref_90_day_purge'
purge_90_day_table = 'ref_90_day_hubspot_purge_audit'

def _check_data_availability():
    '''
    This function is used by the Branch operator to determine whether to 
    run the purge tasks to skip them 

    The load_email_sql tasks sets purge_records_available variable to 0 or 1 based 
    on data availability 
    '''
    data_availability = Variable.get('purge_records_available')
    print('data_availability = ', data_availability)
    
    if int(data_availability) == 0:
        print('No data to purge - Skipping Purge Tasks')
        return 'check_mixpanel'
    else:
        print('data available for purging')
        return 'load_email_address'
    

with DAG(
	'GTE_90_day_purge_dag_hubspot',
    schedule_interval='0 6 * * 4',  
    catchup=False,
    max_active_runs=1,
    default_args=default_dag_args) as dag:


    create_purge_90_day_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_purge_90_day_dataset',
        dataset_id=purge_90_day_dataset,
        bigquery_conn_id=bq_gcp_connector)


    base_etl_folder = r'ETL/DataRetention/hubspot/hubspot_purge_query.sql'
    with open(os.path.join(configuration.get('core', 'dags_folder'), base_etl_folder)) as f:
        read_file_data = f.readlines()
    get_purgeid_sql = "\n".join(read_file_data)


    create_table_sql = """ CREATE TABLE if not exists `{project}.{dataset}.{table}`
                           (
                                          ScrubRunID STRING,
                                          DatabaseName STRING,
                                          TableSchema STRING,
                                          TableName STRING,
                                          PrimaryKey01 STRING,
                                          EntityPK01Value STRING,
                                          PrimaryKey02 STRING,
                                          EntityPK02Value STRING,
                                          PurgeFlag STRING,
                                          PurgeDate STRING,
                                          Email STRING,
                                          bq_load_date TIMESTAMP						  
                            )
                            PARTITION BY
                            DATE(bq_load_date);""".format(project = bq_target_project,dataset = purge_90_day_dataset,table = purge_90_day_table)

    load_email_sql = """Begin Transaction; 

                        UPDATE `{project}.{dataset}.{table}` t1
                        SET t1.Email = t2.properties_email_value
                        FROM (SELECT DISTINCT(vid), properties_email_value FROM `{project}.{prefix}{source_abbr}.t_{source_abbr}_{source_table}`) t2
                        where t2.vid = t1.EntityPK01Value;

                        commit Transaction
    """.format(project = bq_target_project,dataset = purge_90_day_dataset,source_table='contacts_details_updates', table = purge_90_day_table, source_abbr=source_abbr, prefix=prefix,tbl_pk = 'properties_email_value', date = "{{ds}}")


    create_90_day_purge_table = runbqscriptoperator(
        task_id='create_90_day_purge_table',
        sql=create_table_sql,
        bigquery_conn_id=bq_gcp_connector)


    load_data_from_MSSql_BQ = SQLServertoBQOperator(
        task_id='load_data_from_MSSql_BQ',
        mssql_conn_id=mssql_connection_target,
        bigquery_conn_id=bq_gcp_connector,
        sql=get_purgeid_sql,
        destination_dataset=purge_90_day_dataset,
        destination_table=purge_90_day_table,
        project=bq_target_project)

    purge_data_availability = BranchPythonOperator(
        task_id = 'purge_data_availability',
        python_callable = _check_data_availability,
        do_xcom_push=False
    )

    load_email_address = runbqscriptoperator(
        task_id='load_email_address',
        sql=load_email_sql,
        bigquery_conn_id=bq_gcp_connector)

    task_start = EmptyOperator(task_id='task_start')

    

    table_list = ['contacts_details_updates', 'emailEvents'] # add other tables to this list




    
    update_sql = """
        UPDATE `{project}.{dataset_audit}.{table_audit}`
        SET PurgeFlag = 'Y', 
            PurgeDate	= '{date}',
            Email = ''
        WHERE DATE(bq_load_date) ='{date}';
        """.format(project = bq_target_project, dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")

    update_audit_table = runbqscriptoperator(
        task_id='update_audit_table',
        sql=update_sql,
        bigquery_conn_id=bq_gcp_connector,
        retries=1,
        retry_delay=timedelta(seconds=60))

    task_sensor_mixpanel = ExternalTaskSensorAsync(
        task_id='check_mixpanel',
        external_dag_id='GTE_90_day_purge_dag_mixpanel',
        external_task_id='update_audit_table',
        execution_delta=timedelta(hours=0),
        timeout=7200,
        trigger_rule='none_failed')

    update_scrub_id_sql = """
                            BEGIN TRANSACTION;
                            use [DataRetention_Home];
                            EXEC [dbo].[s_Scrub_Run_Update_DataLake] @ScrubRunID={scrub_id};
                            COMMIT TRANSACTION;

                            """

    update_scrub_run_status = RUNSQLServerQuery(
        task_id='update_scrub_run_status',
        mssql_conn_id=mssql_connection_target,
        sql=update_scrub_id_sql)

    task_end = EmptyOperator(task_id='task_end')


    for table in table_list:
        if table == 'emailEvents':
            tbl_pk = 'recipient'

            delete_sql = """
                        Begin Transaction;
                        Delete from `{project}.{prefix}{source_abbr}.t_{source_abbr}_{table}` 
                        where {tbl_pk} IN(SELECT distinct(Email) FROM `{project}.{dataset_audit}.{table_audit}`
                                            WHERE DATE(bq_load_date) = '{date}');
                        commit Transaction
                        """.format(project=bq_target_project, prefix=prefix, source_abbr=source_abbr, table=table, tbl_pk=tbl_pk ,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")


    
        elif table == 'contacts_details_updates':
            tbl_pk = 'vid'

            delete_sql = """
                        Begin Transaction;
                        Delete from `{project}.{prefix}{source_abbr}.t_{source_abbr}_{table}` 
                        where {tbl_pk} IN(SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
                                            WHERE DATE(bq_load_date) = '{date}');
                        commit Transaction
                        """.format(project=bq_target_project, prefix=prefix, source_abbr=source_abbr, table=table, tbl_pk=tbl_pk ,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")

        purge_data = runbqscriptoperator(
            task_id='purge_table_{table_name}'.format(table_name = table),
            sql=delete_sql,
            bigquery_conn_id=bq_gcp_connector)

        audit_sql = """
        select
        Case when not exists
        (
            select * from `{project}.{prefix}{source_abbr}.t_{source_abbr}_{table}` 
            where '{tbl_pk}' in (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
				                WHERE DATE(bq_load_date) = '{date}')
        )
        then True
        else False
        end
        """.format(project = bq_target_project,prefix=prefix,source_abbr=source_abbr, tbl_pk = tbl_pk, table = table, 
		           dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")
		
        Audit_status = BigQueryCheckOperator(task_id='Audit_{dq_check}_success'.format(dq_check=table),
                                          sql=audit_sql,
                                          gcp_conn_id=bq_gcp_connector,
                                          use_legacy_sql=False)
        
        create_purge_90_day_dataset >> create_90_day_purge_table >> load_data_from_MSSql_BQ >> purge_data_availability >> load_email_address >> task_start >> purge_data >> Audit_status >> task_end>>update_audit_table >>task_sensor_mixpanel >> update_scrub_run_status

    

