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
from plugins.operators.jm_SQLServertoBQ_Retention_mxpnl import SQLServertoBQOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.empty import EmptyOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,4,15),
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
    folder_prefix = 'DEV_'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDataRetention_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDataRetention_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    folder_prefix = ''
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'
    mssql_connection_target = 'instDataRetention_PROD'

purge_90_day_dataset = 'ref_90_day_purge'
purge_90_day_table = 'ref_90_day_mixpanel_purge_audit'


with DAG(
	'GTE_90_day_purge_dag_mixpanel',
    schedule_interval='0 6 * * 4',  # weekly 6 AM UTC thursdays
    catchup=False,
    max_active_runs=1,
    default_args=default_dag_args) as dag:


    create_purge_90_day_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_purge_90_day_dataset',
        dataset_id=purge_90_day_dataset,
        bigquery_conn_id=bq_gcp_connector)


    base_etl_folder = r'ETL/DataRetention/mixpanel/mixpanel_purge_query.sql'
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
                                          bq_load_date TIMESTAMP						  
                            )
                            PARTITION BY
                            DATE(bq_load_date);""".format(project = bq_target_project,dataset = purge_90_day_dataset,table = purge_90_day_table)


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

    task_start = EmptyOperator(task_id='task_start')

    table_list = ['gld_digital_dashboard.t_Mixpanel_UsageCountSummary.date_distinct_id'
,'gld_mixpanel.t_mixpanel_events_all.distinct_id'
,'gld_fraud_ring_model.t_fraudringmodel_mixpanel_data.distinct_id'
,'gld_mixpanel_conversion_funnel.mixpanel_session_funnel.distinct_id'
,'gld_mixpanel_conversion_funnel.mixpanel_session_funnel_with_people.distinct_id'
,'gld_mixpanel_conversion_funnel.mixpanel_session_group.distinct_id'
,'gld_pl_personalization.t_pl_personalization_profile.distinctid'
,'gld_fraud_ring_model.t_fraudringmodel_final.distinctid'
,'ref_mixpanel.t_mixpanel_events_all.distinct_id'
,'ref_mixpanel_people.t_mixpanel_people.distinct_id'
]

    update_sql = """
        UPDATE `{project}.{dataset_audit}.{table_audit}`
        SET PurgeFlag	 = 'Y', PurgeDate	= '{date}'
        WHERE DATE(bq_load_date) ='{date}';
        """.format(project = bq_target_project, dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table,  date = "{{ds}}")

    update_audit_table = runbqscriptoperator(
        task_id='update_audit_table',
        sql=update_sql,
        bigquery_conn_id=bq_gcp_connector,
        retries=1,
        retry_delay=timedelta(seconds=60))

    task_end = EmptyOperator(task_id='task_end')


    for tables in table_list:
        list = tables.split('.')
        schema = list[0]
        table = list[1]
        tbl_pk = list[2]

        if table != 't_Mixpanel_UsageCountSummary':
            delete_sql = """
                        Begin Transaction;
                        Delete from `{project}.{dataset}.{table}` 
                        where {tbl_pk} in (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
                                           WHERE DATE(bq_load_date) = '{date}');
                        commit Transaction
                        """.format(project = bq_target_project,dataset =schema,table = table, tbl_pk =tbl_pk ,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")
        else:
            delete_sql = """
                        Begin Transaction;
                        Delete from `{project}.{dataset}.{table}` 
                        where substr({tbl_pk}, 1, length(date_distinct_id)-10) in 
                            (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
                             WHERE DATE(bq_load_date) = '{date}');
                        commit Transaction
                        """.format(project = bq_target_project,dataset =schema,table = table, tbl_pk =tbl_pk ,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")

        purge_data = runbqscriptoperator(
            task_id='purge_table_{schema}_{table_name}'.format(table_name = table, schema = schema),
            sql=delete_sql,
            bigquery_conn_id=bq_gcp_connector)

        audit_sql = """
        select
        Case when not exists
        (
            select * from `{project}.{dataset}.{table}` 
            where {tbl_pk} in (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
				                WHERE DATE(bq_load_date) = '{date}')
        )
        then True
        else False
        end
        """.format(project = bq_target_project,dataset =schema, tbl_pk = tbl_pk, table = table, 
		           dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")
		
        Audit_status = BigQueryCheckOperator(task_id='Audit_{schema}_{dq_check}_success'.format(dq_check=table, schema=schema),
                                          sql=audit_sql,
                                          gcp_conn_id=bq_gcp_connector,
                                          use_legacy_sql=False)


        create_purge_90_day_dataset >> create_90_day_purge_table >> load_data_from_MSSql_BQ >> task_start >> purge_data >> Audit_status >> task_end>>update_audit_table
