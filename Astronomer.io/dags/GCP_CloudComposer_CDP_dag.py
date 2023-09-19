import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_CDPAuditOperator import CDPAuditOperator
from plugins.operators.jm_CDPSFTPOperator import CDPtoSFTPOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
import os
from airflow import configuration
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

ts = calendar.timegm(time.gmtime())
logging.info("rlaksh ")
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

##
AIRFLOW_ENV = Variable.get('ENV')

trigger_object = 'triggers/inbound/dw_complete.json'

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    trigger_bucket = 'jm_dev01_edl_lnd'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    recast_project = 'dev-edl'
    folder_prefix = 'DEV_'
    sftp_connector = 'acquia-sftp-test'

elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    recast_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    sftp_connector = 'acquia-sftp-test'

elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    recast_project = 'semi-managed-reporting'
    folder_prefix = ''
    sftp_connector = 'acquia-sftp-prod'

source = 'CDP'
source_abbr = 'CDP'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)
sql_list_dir = os.listdir(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/CDP/'))

# source_dag

with DAG(
        '{source}_dag'.format(source='CDP'),
        schedule_interval='0 14 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    try:
        sftp_folder = Variable.get('cdp_sftp_folder')
    except:
        if AIRFLOW_ENV.lower() == 'prod':
            sftp_folder = ''
        else:
            sftp_folder = 'Sample/'

    # add pc_priortized trigger and add tables to pc_priortized variable

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='CDP',
                                 mode='SET')

    task_sensor = ExternalTaskSensorAsync(
        task_id='check_pc_priortized_dag',
        external_dag_id='PolicyCenter_Prioritized_Tables',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    task_sensor2 = ExternalTaskSensorAsync(
        task_id='check_plecom_dag',
        external_dag_id='PLEcom_dag',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    task_sensor3 = ExternalTaskSensorAsync(
        task_id='check_nice_dag',
        external_dag_id='Nice_dag',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=7),
        timeout=7200)

    task_sensor4 = ExternalTaskSensorAsync(
        task_id='check_bc_prioritized_dag',
        external_dag_id='BillingCenter_Prioritized_Tables',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    task_sensor5 = ExternalTaskSensorAsync(
        task_id='check_hubspot_gold',
        external_dag_id='Hubspot_Gold',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=6),
        timeout=7200)

    task_sensor6 = ExternalTaskSensorAsync(
        task_id='check_recast',
        external_dag_id='semimanaged_PL_tracking_daily_dag_new',
        external_task_id='run_RECAST_V6.0',
        execution_delta=timedelta(hours=2),
        timeout=7200)

    try:
        CDP_FILE_LIST = Variable.get('cdp_file_list')
    except:
        Variable.set('cdp_file_list', 10)

    logging.info("rlaksh CDP_FILE_LIST")
    logging.info(CDP_FILE_LIST)
    
    # See if this is a delta or historical load
    try:
        sql_delta_load = Variable.get('CDP_DeltaLoad')
    except:
        sql_delta_load = 'True'

    try:
        CDP_RECAST_TABLE = Variable.get('cdp_recast_table')
    except:
        Variable.set('cdp_recast_table', 'financial_year.pl_recast_after_2014')
        CDP_RECAST_TABLE = 'financial_year.pl_recast_after_2014'

    if sql_delta_load == 'True':
        sql_final_list = [x for x in sql_list_dir if '_Delta' in x]
    else:
        sql_final_list = [x for x in sql_list_dir if '_Delta' not in x]

    logging.info("rlaksh sql_final_list ",sql_final_list)

    sql_file_count = 0
    for sql in sql_final_list:
        table_name = sql.replace('.sql', '')

        logging.info("rlaksh table_name 170", table_name)
        sql_filter_list = []
        if sql_delta_load == 'False':
            # See if there is a data filter for historical loading
            try:
                sql_filter_list = Variable.get(table_name.replace('_Delta', '') + '_DataFilterList').split(',')
            except:
                sql_filter_list = []

        if len(sql_filter_list) > 0:
            sql_file_count += len(sql_filter_list)
        else:
            sql_file_count += 1

        logging.info("rlaksh sql_filter_list 170", sql_filter_list)

    sql_file_audit = CDPAuditOperator(task_id='sql_file_count_audit',
                                      sql_file_count=len(sql_final_list))

    sftp_file_audit = CDPAuditOperator(task_id='sftp_file_count_audit',
                                       sql_file_count=sql_file_count,
                                       sftp_destination_path=sftp_folder,
                                       sftp_conn_id=sftp_connector)

    source_pc_dataset = 'ref_pc_current'
    source_bc_dataset = 'ref_bc_current'
    source_plecom_dataset = 'ref_plecom_quoteapp_current'
    source_nice_dataset = 'ref_nice'
    source_hubspot_dataset = folder_prefix + 'gld_hubspot'
    for sql in sql_final_list:
        table_name = sql.replace('.sql', '')

        sql_filter_list = []
        if sql_delta_load == 'False':
            # See if there is a data filter for historical loading
            try:
                sql_filter_list = Variable.get(table_name.replace('_Delta', '') + '_DataFilterList').split(',')
            except:
                sql_filter_list = []

        with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/CDP/', sql)) as f:
            read_sql = f.readlines()
        full_sql = "\n".join(read_sql)

        logging.info("rlaksh full_sql 202", full_sql)
        if 'NICE' not in table_name:
            full_sql = full_sql.format(date="{{ ds }}",
                                       ydate="{{ prev_ds }}",
                                       prefix=folder_prefix,
                                       pc_dataset=source_pc_dataset,
                                       bc_dataset=source_bc_dataset,
                                       cdp_recast_table=CDP_RECAST_TABLE,
                                       recast_project=recast_project,
                                       hubspot_dataset=source_hubspot_dataset,
                                       plecom_dataset=source_plecom_dataset)
            # if 'hubspot' in table_name.lower():
            #     filter_sql = full_sql.replace('filter_date',"{{ ds }}")


        filename = sql.replace('.sql', '')

        logging.info("rlaksh len sql_filter_list 233", len(sql_filter_list))
        logging.info("rlaksh sql_filter_list 233", sql_filter_list)

        if len(sql_filter_list) > 0:
            filter_counter = 0
            for one_filter in sql_filter_list:
                filter_counter += 1
                if 'hubspot' in sql.lower():
                    filter_sql = full_sql.replace('filter_date',one_filter)
                else:

                    filter_sql = full_sql + one_filter

                logging.info("rlaksh 245 filter_sql", filter_sql) 
                store_data = CDPtoSFTPOperator(
                    task_id='export_{source}_data_for_{sql}_{counter}'.format(source=source_abbr,
                                                                              sql=table_name.replace('_Delta', ''),
                                                                              counter=filter_counter),
                    sftp_dest_path=sftp_folder,
                    destination_bucket=base_bucket,
                    destination_path=base_file_location + "{{ ds_nodash }}",
                    gcp_conn_id=base_gcp_connector,
                    bigquery_conn_id=bq_gcp_connector,
                    sftp_conn_id=sftp_connector,
                    sql=filter_sql,
                    file_name=filename + '_' + str(filter_counter),
                    sql_filter_list=sql_filter_list)

                [task_sensor, task_sensor2, task_sensor3,
                 task_sensor4] >> sql_file_audit >> store_data >> sftp_file_audit >> bit_set
        else:
            logging.info("rlaksh 263 else ") 
            logging.info("rlaksh 263 else bq_gcp_connector ", bq_gcp_connector) 
            store_data = CDPtoSFTPOperator(task_id='export_{source}_data_for_{sql}'.format(source=source_abbr,
                                                                                           sql=table_name.replace(
                                                                                               '_Delta', '')),
                                           sftp_dest_path=sftp_folder,
                                           destination_bucket=base_bucket,
                                           destination_path=base_file_location + "{{ ds_nodash }}",
                                           gcp_conn_id=base_gcp_connector,
                                           bigquery_conn_id=bq_gcp_connector,
                                           sftp_conn_id=sftp_connector,
                                           sql=full_sql,
                                           file_name=filename + '_',
                                           sql_filter_list=sql_filter_list)

            [task_sensor, task_sensor2, task_sensor3,
             task_sensor4, task_sensor5, task_sensor6] >> sql_file_audit >> store_data >> sftp_file_audit >> bit_set
