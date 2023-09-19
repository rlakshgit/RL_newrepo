import logging
import datetime as dt
import calendar
import time
from airflow import configuration
import os
import airflow
from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_BigQueryUDFOperator import BigQueryUDFOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync


ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    prefix = 'DEV_'
    source_project_prefix = 'dev-edl.DEV_jm_'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    prefix = 'B_QA_'
    source_project_prefix = 'qa-edl.B_QA_'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    mssql_connection_target = 'instDW_PROD'
    prefix = ''
    source_project_prefix = 'prod-edl.'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'



source = 'QuickQuote'
source_abbr = source.lower()
with DAG(
        'QuickQuote_Gold',
        schedule_interval= '30 13 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
        ##
        check_pc_priority_dependency = ExternalTaskSensorAsync(
                                            task_id='check_pc_priority_tables_complete',
                                            external_dag_id='PolicyCenter_Prioritized_Tables',
                                            external_task_id='set_source_bit',
                                            execution_delta= timedelta(hours=1,minutes=30),
                                            timeout = 7200)

        check_ratabase_dependency = ExternalTaskSensorAsync(
                                            task_id='check_ratabase_complete',
                                            external_dag_id='ratabase_v1_dag',
                                            external_task_id='set_source_bit_pa',
                                            execution_delta=timedelta(hours=7,minutes=0),
                                            timeout=7200)


        source_base_dataset = 'ref_{source}'.format(source=source.lower())
        base_dataset = 'gld_{source}'.format(source=source.lower())

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id='{prefix}{base_dataset}'.format(
                base_dataset=base_dataset, prefix=prefix),
            bigquery_conn_id=bq_gcp_connector)

        get_file_list = os.listdir(os.path.join(configuration.get('core', 'dags_folder'),r'ETL/QuickQuote'))
        targeted_filelist = []
        for f in get_file_list:
            if f.startswith('gld_QQ_'):
                targeted_filelist.append(f)
            else:
                continue

        target_f = 'QQ_Conversion.sql'
        target_f_id = target_f.replace('.sql', '').replace('QQ_', '')
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               r'ETL/QuickQuote/{file}'.format(file=target_f))) as f:
            read_file_data = f.readlines()
        cleaned_file_data = "\n".join(read_file_data)
        cleaned_file_data = cleaned_file_data.format(source_project_prefix=source_project_prefix,
                                                     date='{{ ds }}',
                                                     base_dataset='{prefix}{base_dataset}'.format(base_dataset=base_dataset, prefix=prefix),
													 lbracket='{',
													 rbracket='}',
                                                     project=bq_target_project)

        build_conversion_data = BigQueryOperator(task_id='quick_quote_{file}'.format(file=target_f_id),
                                           sql=cleaned_file_data,
                                           destination_dataset_table='{project}.{prefix}{base_dataset}.t_quickquote_{file_id}${date}'.format(
                                               project=bq_target_project,
                                               file_id=target_f_id.lower(),
                                               prefix=prefix,
                                               base_dataset=base_dataset,
                                               source_abbr=source_abbr.lower(),
                                               date="{{ ds_nodash }}"),
                                           write_disposition='WRITE_TRUNCATE',
                                           create_disposition='CREATE_IF_NEEDED',
                                           gcp_conn_id=bq_gcp_connector,
                                           allow_large_results=True,
                                           use_legacy_sql=False,
                                           time_partitioning={"type": "DAY"},
                                           schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                  'ALLOW_FIELD_RELAXATION'])

        target_f = 'QQ_Summary.sql'
        target_f_id = target_f.replace('.sql', '').replace('QQ_', '')
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               r'ETL/QuickQuote/{file}'.format(file=target_f))) as f:
            read_file_data = f.readlines()
        cleaned_file_data = "\n".join(read_file_data)
        cleaned_file_data = cleaned_file_data.format(source_project_prefix=source_project_prefix,
                                                     date='{{ ds }}',
                                                     lbracket='{',
                                                     rbracket='}',
                                                     base_dataset='{prefix}{base_dataset}'.format(
                                                         base_dataset=base_dataset, prefix=prefix),
                                                     project=bq_target_project)

        build_summary_data = BigQueryOperator(task_id='quick_quote_{file}'.format(file=target_f_id),
                                                 sql=cleaned_file_data,
                                                 destination_dataset_table='{project}.{prefix}{base_dataset}.t_quickquote_{file_id}${date}'.format(
                                                     project=bq_target_project,
                                                     file_id=target_f_id.lower(),
                                                     prefix=prefix,
                                                     base_dataset=base_dataset,
                                                     source_abbr=source_abbr.lower(),
                                                     date="{{ ds_nodash }}"),
                                                 write_disposition='WRITE_TRUNCATE',
                                                 create_disposition='CREATE_IF_NEEDED',
                                                 gcp_conn_id=bq_gcp_connector,
                                                 allow_large_results=True,
                                                 use_legacy_sql=False,
                                                 time_partitioning={"type": "DAY"},
                                                 schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                        'ALLOW_FIELD_RELAXATION'])

        ###
        for target_f in targeted_filelist:
            target_f_id = target_f.replace('.sql','').replace('gld_QQ_','')
            with open(os.path.join(configuration.get('core', 'dags_folder'),r'ETL/QuickQuote/{file}'.format(file=target_f))) as f:
                read_file_data =f.readlines()
            cleaned_file_data = "\n".join(read_file_data)
            if 'ratabase' in target_f_id.lower():
                base_dataset_set = 'ref_ratabase'
            else:
                base_dataset_set = 'ref_pc_current'
            cleaned_file_data = cleaned_file_data.format(source_project_prefix=source_project_prefix,
                                                                         prefix='',
                                                                         date = '{{ ds }}',
                                                                         #prefix=prefix,
                                                                         base_dataset=base_dataset_set,
                                                                         #base_dataset=source_base_dataset,
                                                                         project='prod-edl')
                                                                         #project=bq_target_project)

            build_gold_data = BigQueryOperator(task_id='quick_quote_{file}'.format(file=target_f_id),
                                                           sql=cleaned_file_data,
                                                           destination_dataset_table='{project}.{prefix}{base_dataset}.t_quickquote_{file_id}${date}'.format(
                                                               project=bq_target_project,
                                                               file_id = target_f_id.lower(),
                                                               prefix=prefix,
                                                               base_dataset=base_dataset,
                                                               source_abbr=source_abbr.lower(),
                                                                date="{{ ds_nodash }}"),
                                                           write_disposition='WRITE_TRUNCATE',
                                                           create_disposition='CREATE_IF_NEEDED',
                                                           gcp_conn_id=bq_gcp_connector,
                                                           allow_large_results=True,
                                                           use_legacy_sql=False,
                                                           time_partitioning={"type": "DAY"},
                                                            schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                            'ALLOW_FIELD_RELAXATION'])




            [check_pc_priority_dependency,check_ratabase_dependency] >> create_dataset >> build_gold_data >> build_conversion_data>>build_summary_data
