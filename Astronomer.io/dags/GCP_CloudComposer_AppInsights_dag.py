import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_AppInsight_to_gcs import AppInsightToGCS
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator

###Variables

ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')
source = 'AppInsight'
source_abbr = 'appinsight'

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'GN_DEV_'
    api_connection_id = 'APPInsight_Connector'

elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector ='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_'
    api_connection_id =  'APPInsight_Connector'

elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector ='prod_edl'
    bq_target_project = 'prod-edl'
    base_gcs_folder = ''
    api_connection_id =  'APPInsight_Connector'

#Landing location
base_file_location = '{base}l1/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)

#Normalized data location
base_normalized_location = '{base}l1_norm/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)

#schema location
base_schema_location = '{base}l1_schema/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)

#Audit Location
audit_filename = '{base_gcs_folder}l1_audit/{source}'.format(source=source_abbr.lower(),
                                                             base_gcs_folder=base_gcs_folder)

#Metadata location
metadata_filename = '{base_gcs_folder}l1/{source}'.format(source=source_abbr.lower(),
                                                          base_gcs_folder=base_gcs_folder)

#refined Dataset name
refine_dataset = '{prefix}ref_{source}_zing'.format(prefix=base_gcs_folder,
                                               source=source_abbr.lower())    ###

try:
    AppInsight_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
except:
    Variable.set(source + '_gcs_to_bq', 'True')
    AppInsight_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')

try:
    HISTORY_CHECK = Variable.get('appinsight_history_check')
except:
    Variable.set('appinsight_history_check', 'True')
    HISTORY_CHECK = Variable.get('appinsight_history_check')

if HISTORY_CHECK.lower() == 'true':
    history_check = True
else:
    history_check = False

###DAG Parameters
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,2,15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'jm_AppInsight_dag',
        schedule_interval='0 6 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    #queries = ['browserTimings', 'availabilityResults', 'customEvents', 'customMetrics', 'pageViews','performanceCounters']
    try:
        queries = Variable.get("appinsight_tables").replace("[","").replace("]","").replace(" ","").replace("'","").split(",")
    except:
        Variable.set('appinsight_tables',"['browserTimings', 'availabilityResults', 'customEvents', 'customMetrics', 'pageViews','performanceCounters','requests']")
        queries = Variable.get("appinsight_tables").replace("[", "").replace("]", "").replace(" ", "").replace("'","").split(",")
    ###check dataset exists operator
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id='check_for_dataset_{table}'.format(table = source),
                                                        dataset_id='{prefix_value}ref_{source_abbr}'.format(
                                                            source_abbr=source_abbr,
                                                            prefix_value=base_gcs_folder),
                                                        bigquery_conn_id=bq_gcp_connector)
    
    bit_set = CompletionOperator(task_id='set_source_bit',
                                     source=source,
                                     mode='SET')

    for query in queries:
        ####AppInsighttoGCSoperator
        landing_task =AppInsightToGCS(task_id='land_{source}_data_{table}'.format(source=source_abbr,table = query),
                                     base_gcs_folder=base_file_location,
                                     google_cloud_storage_conn_id=base_gcp_connector,
                                     target_gcs_bucket=base_bucket,
                                     normalized_location=base_normalized_location,
                                     schema_location=base_schema_location,
                                     metadata_filename=metadata_filename,
                                     history_check=history_check,
                                     api_connection_id=api_connection_id,
                                     query = query,
                                     source_name = source_abbr,
                                     begin_pull="{{ ds }}",
                                     end_pull="{{ tomorrow_ds }}")

        ####Landing Audit
        landing_audit_task = APIAuditOperator(task_id='landing_audit_{table}'.format(table=query),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=refine_dataset,
                                         base_gcs_folder=None,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                         metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                             base=metadata_filename,
                                             table=query,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                             base=audit_filename,
                                             table=query,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         check_landing_only=True,
                                         table=query)

        ###BigqueryOperator
        refine_data_task = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{source_abbr}_{table}'.format(source_abbr=source_abbr,
                                                               table=query),
            bucket=base_bucket,

            source_objects=['{location_base}{table}/{date}/l1_norm_{source}_{table}*.json'.format(
                location_base=base_normalized_location,
                source=source_abbr,
                table=query,
                date="{{ ds_nodash }}")],
            destination_project_dataset_table='{project}.{prefix_value}ref_{source_abbr}.{table}${date}'.format(
                project=bq_target_project,
                source_abbr=source_abbr,
                table=query,
                date="{{ ds_nodash }}",
                prefix_value=base_gcs_folder),
            schema_fields=[],
            schema_object='{schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                schema_location=base_schema_location,
                table=query,
                date="{{ ds_nodash }}",
                source=source_abbr),
            source_format='NEWLINE_DELIMITED_JSON',
            compression='NONE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=0,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=0,
            bigquery_conn_id=bq_gcp_connector,
            google_cloud_storage_conn_id=base_gcp_connector,
            schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
            gcs_to_bq=AppInsight_BQ_PROCESS)

    ###AuditOperator
        if AppInsight_BQ_PROCESS.upper() == 'TRUE':
            refine_audit_task = APIAuditOperator(task_id='refine_audit_{table}'.format(table=query),
                                            bucket=base_bucket,
                                            project=bq_target_project,
                                            dataset=refine_dataset,
                                            base_gcs_folder=None,
                                            target_gcs_bucket=base_bucket,
                                            google_cloud_storage_conn_id=base_gcp_connector,
                                            source_abbr=source_abbr,
                                            source=source,
                                            metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                                base=metadata_filename,
                                                table=query,
                                                date="{{ ds_nodash }}",
                                                source=source_abbr),
                                            audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                                base=audit_filename,
                                                table=query,
                                                date="{{ ds_nodash }}",
                                                source=source_abbr),
                                            check_landing_only=False,
                                            table=query)
        else:
            refine_audit_task = EmptyOperator(task_id='refine_audit_{table}'.format(table=query))

        create_dataset_task >> landing_task >>landing_audit_task>>refine_data_task>>refine_audit_task>>bit_set