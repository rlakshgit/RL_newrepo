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
from plugins.operators.jm_SQLServertoBQ_Retention_plecom import SQLServertoBQOperator
from plugins.operators.jm_RUN_SQLServerQuery_plecom import RUNSQLServerQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import BranchPythonOperator 

ts = calendar.timegm(time.gmtime())
logging.info(ts)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,5,11),
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
    mssql_connection_target = 'instDataRetention12_STAGE'

elif AIRFLOW_ENV.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    mssql_connection_target = 'instDataRetention12_STAGE'

elif AIRFLOW_ENV.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    folder_prefix = ''
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'
    mssql_connection_target = 'instDataRetention12_PROD'

purge_90_day_dataset = 'ref_90_day_purge'
purge_90_day_table = 'ref_90_day_plecom_purge_audit'

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
        return 'update_scrub_run_status'
    else:
        print('data available for purging')
        return 'task_start'

with DAG(
        'GTE_90_day_purge_dag_plecom',
        schedule_interval='0 6 * * 4',  # weekly 6 AM UTC thursdays
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


    create_purge_90_day_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_purge_90_day_dataset',
        dataset_id=purge_90_day_dataset,
        bigquery_conn_id=bq_gcp_connector)

    """
    step 1: Copy the Retention IDS and other details from SQLserver to BQ
    """

    base_etl_folder = r'ETL/DataRetention/PLEcom/plecom_purge_query.sql'
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

    purge_data_availability = BranchPythonOperator(
        task_id = 'purge_data_availability',
        python_callable = _check_data_availability,
        do_xcom_push=False
    )

    task_start = EmptyOperator(task_id='task_start')

    table_list = ['JewelersCut.Submission.SubmissionId.None',
'QuoteApp.t_Address.AddressId.None',
'QuoteApp.t_JewelryPurchaseInfo.JewelryPurchaseInfoId.None',
'QuoteApp.t_Person.PersonId.None',
'QuoteApp.t_Application.ApplicationId.None',
'QuoteApp.t_SavedQuote.SavedQuote_ID.None',
'QuoteApp.t_UnderwritingInformation.UnderwritingInformationId.None',
'JewelersCut.AppraisalFile.Id.None',
'JewelersCut.Item.ItemId.None',
'QuoteApp.t_ApplicationItemJewelerPaymentTracker.ApplicationItemJewelerPaymentTracker_ID.None',
'QuoteApp.t_ApplicationOptIn.ApplicationId.OptInId',
'QuoteApp.t_ApplicationPayment.ApplicationPaymentId.None',
'QuoteApp.t_ApplicationQuote.ApplicationQuoteId.None',
'QuoteApp.t_QuotedJewelryItem.QuotedJewelryItemId.None',
'QuoteApp.t_LossHistoryEvent.LossHistoryEventId.None',
'QuoteApp.t_SavedQuoteItem.SavedQuoteItem_ID.None']

    update_sql = """
      UPDATE `{project}.{dataset_audit}.{table_audit}`
      SET PurgeFlag	 = 'Y', PurgeDate	= '{date}'
      WHERE  DATE(bq_load_date) ='{date}';
      """.format(project=bq_target_project, dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date="{{ds}}")

    update_audit_table = runbqscriptoperator(
        task_id='update_audit_table',
        sql=update_sql,
        bigquery_conn_id=bq_gcp_connector,
        retries=1,
        retry_delay=timedelta(seconds=60))

    task_end = EmptyOperator(task_id='task_end')
    
    update_scrub_id_sql = """
                            BEGIN TRANSACTION;
                            use [DataRetention_Support_12];
                            EXEC [dbo].[s_Scrub_Run_Update_DataLake] @ScrubRunID={scrub_id};
                            COMMIT TRANSACTION;
                        """
        
    update_scrub_run_status = RUNSQLServerQuery(
                                    task_id='update_scrub_run_status', 
                                    mssql_conn_id=mssql_connection_target,
                                    sql=update_scrub_id_sql,
                                    trigger_rule='none_failed')

    for table in table_list:
        list = table.split('.')
        schema = list[0].lower()
        table = list[1]
        pk = list[2]
        pk2= list[3]
        #interval = list[4]

        if table != 't_ApplicationOptIn':

            delete_sql = """
            Begin Transaction;
            Delete from 
            `{project}.{prefix}ref_plecom_{dataset}.{table}` 
            where cast({pk} as string) in 
            (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
             WHERE tablename = '{table}' and DATE(bq_load_date) = '{date}');
             
             
             Delete from 
            `{project}.{prefix}ref_plecom_{dataset}_current.{table}` 
            where cast({pk} as string) in 
            (SELECT distinct(EntityPK01Value) FROM `{project}.{dataset_audit}.{table_audit}`
             WHERE tablename = '{table}' and DATE(bq_load_date) = '{date}');
             
             
        
             commit Transaction
            """.format(project = bq_target_project,prefix = folder_prefix,dataset =schema,pk = pk, table = table,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")

        else:

            delete_sql = """
                        Begin Transaction;
                        MERGE `{project}.{prefix}ref_plecom_{dataset}.{table}` AS Target
                        USING (SELECT * FROM `{project}.{dataset_audit}.{table_audit}`
                                WHERE tablename = '{table}' and DATE(bq_load_date) = '{date}')  AS Source
                        on CAST(Target.{pk} as String) = source.EntityPK01Value and Cast(Target.{pk2} as String) = source.EntityPK02Value
                        WHEN MATCHED THEN
                        DELETE;
                        
                         MERGE `{project}.{prefix}ref_plecom_{dataset}_current.{table}` AS Target
                        USING (SELECT * FROM `{project}.{dataset_audit}.{table_audit}`
                                WHERE tablename = '{table}' and DATE(bq_load_date) = '{date}')  AS Source
                        on CAST(Target.{pk} as String) = source.EntityPK01Value and Cast(Target.{pk2} as String) = source.EntityPK02Value
                        WHEN MATCHED THEN
                        DELETE;
                        Commit Transaction;
                       
                        """.format(project=bq_target_project, prefix=folder_prefix, dataset=schema, pk=pk, table=table,pk2=pk2,dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table,date="{{ds}}")



        purge_data = runbqscriptoperator(
            task_id='purge_table_{schema}_{table_name}'.format(table_name = table, schema = schema),
            sql=delete_sql,
            bigquery_conn_id=bq_gcp_connector)

        audit_sql = """
        select
        Case when not exists
        (
            select * from 
            `{project}.{prefix}ref_plecom_{dataset}.{table}` 
            where cast({pk} as string) in (SELECT distinct(EntityPK01Value) 
            FROM `{project}.{dataset_audit}.{table_audit}` WHERE tablename = '{table}'))
        then True
        else False
        end
        """.format(project = bq_target_project,prefix = folder_prefix,dataset =schema,pk = pk, table = table, 
		           dataset_audit=purge_90_day_dataset, table_audit=purge_90_day_table, date = "{{ds}}")
        Audit_status = BigQueryCheckOperator(task_id='Audit_{schema}_{dq_check}_success'.format(dq_check=table, schema=schema),
                                          sql=audit_sql,
                                          gcp_conn_id=bq_gcp_connector,
                                          use_legacy_sql=False)

        create_purge_90_day_dataset >>create_90_day_purge_table>> load_data_from_MSSql_BQ >> purge_data_availability >>task_start>> purge_data >> Audit_status >>task_end>> update_audit_table >> update_scrub_run_status

