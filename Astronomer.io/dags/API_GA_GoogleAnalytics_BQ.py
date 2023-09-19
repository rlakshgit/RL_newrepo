import logging
import datetime as dt
import calendar
import time
import json
import os
import ast
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Import the code that will be run for this dag
from plugins.operators.jm_ga_to_gcs import GoogleAnalyticsToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable


ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')
if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    base_prefix = 'DEV_'
    ga_connection_target = 'ga_dev_edl'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    base_prefix = 'B_QA_'
    ga_connection_target = 'ga_qa_edl'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'prod_edl'
    bq_project = 'prod-edl'
    base_gcs_folder = 'l1'
    base_prefix = ''
    ga_connection_target = 'ga_prod_edl'


source = 'ga'
source_full = 'GoogleAnalytics'
dataset_name = '{base}ref_{source}'.format(source=source.lower(),
                                              base=base_prefix)
gld_dataset = '{base}gld_digital_dashboard'.format(base=base_prefix)
gld_table = 't_GA_digital_dashboard${{ ds_nodash }}'
event_table_name = 'client_session_all'

base_file_location = '{base}/{source}'.format(base=base_gcs_folder,
                                              source=source)
base_norm_location = '{base}_norm/{source}'.format(base=base_gcs_folder,
                                                   source=source)
base_schema_location = '{base}_schema/{source}/{date}'.format(base=base_gcs_folder,
                                                              source=source,
                                                              date='{{ ds_nodash }}')
base_audit_location = '{base}_audit/{source}'.format(base=base_gcs_folder,
                                                     source=source)


with DAG(
        '{source}_dag'.format(source=source_full),
        schedule_interval='0 7 * * *',  # '@daily',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    ga_view_id = '95708224'

    confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'ga', r'ini', r'ga_airflow_config.ini')
    with open(confFilePath, 'r') as configFile:
        SOURCE_TABLES = json.load(configFile)

    confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'ga', r'ini', r'ga_airflow_gold_schema.ini')
    with open(confFilePath, 'r') as configFile:
        gold_sql_schema = json.load(configFile)

    confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'ga', r'ini', r'ga_airflow_rename.ini')
    with open(confFilePath, 'r') as configFile:
        bigquery_rename = json.load(configFile)

    event_tables = [
            'ga_user_1',
            'ga_session',
            'ga_trafficsource',
            'ga_adwords',
            'ga_platformordevice',
            'ga_pagetracking',
            'ga_geonetwork',
            'ga_system',
            'ga_time',
            'ga_socialinteractions',
            'ga_eventtracking',
            'ga_sitespeed',
            'ga_adexchange',
            'ga_apptracking',
            'ga_channelgrouping',
            'ga_contentgrouping',
            'ga_goalconversions',
            'ga_internalsearch',
            'ga_customdimensions',
    ]

    gold_duplicate_check_partition = ['clientid',
                                      'sessionid']

    gold_duplicate_check_order_by = ['clientid',
                                     'sessionid',
                                     'hittimestamp']

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')

    try:
        HISTORY_CHECK = Variable.get('ga_history_check')
    except:
        Variable.set('ga_history_check', 'True')
        HISTORY_CHECK = Variable.get('ga_history_check')
        
    # Check if the gold dataset exists in BigQuery
    create_gld_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_gld_dataset_digital_dashboard',
        dataset_id=gld_dataset,
        bigquery_conn_id=bq_gcp_connector
    )

    for table_details in SOURCE_TABLES:
        table = table_details['table_name']
        metrics_list = table_details['metrics']
        dimension_list = table_details['dimensions']
        table_norm = table_details['table_norm']
        table_refine = table_details['table_refine']
        for_normalize = table_details['to_normalize']
        for_refinement = table_details['refine']
        if for_refinement == 'true':
            for_refinement = True
        else:
            for_refinement = False

        combine_all = False
        file_path_table_refine = table_refine.replace('t_','')
        folder_norm = file_path_table_refine + '/' 
        if table_refine == 't_client_session_all':
            combine_all = True
            file_path_table_refine = 'client_session_all'
            folder_norm = ''

        file_name_prep = '{location_base}/{date}/l1_data_{table}'.format(location_base=base_file_location,
                                                                         table=table,
                                                                         date='{{ ds_nodash }}')

        norm_file_name_prep = '{location_base}/{date}/{folder_norm}l1_norm_data_{table}'.format(location_base=base_norm_location,
                                                                                                folder_norm=folder_norm,
                                                                                                table=table_norm,
                                                                                                date='{{ ds_nodash }}')

        combined_file_name_prep = '{location_base}/{date}/{folder_norm}/l1_norm_data_{table}'.format(location_base=base_norm_location,
                                                                                                     folder_norm=file_path_table_refine,
                                                                                                     table=file_path_table_refine,
                                                                                                     date='{{ ds_nodash }}')

        norm_folder_prep = '{location_base}/{date}/l1_data_{table}'.format(location_base=base_file_location,
                                                                           table=table_norm,
                                                                           date='{{ ds_nodash }}')

        combined_folder_prep = '{location_base}/{date}/l1_norm_data_'.format(location_base=base_norm_location,
                                                                             date='{{ ds_nodash }}')

        audit_file_name_prep = '{location_base}/{date}/l1_audit_results_{table}.json'.format(location_base=base_audit_location,
                                                                                             table=table,
                                                                                             date='{{ ds_nodash }}')

        audit_norm_file_name_prep = '{location_base}/{date}/l1_audit_results_{table}.json'.format(location_base=base_audit_location,
                                                                                                  table=table_norm,
                                                                                                  date='{{ ds_nodash }}')

        audit_combined_file_name_prep = '{location_base}/{date}/{folder_norm}/l1_audit_results_{table}.json'.format(location_base=base_audit_location,
                                                                                                                    folder_norm=file_path_table_refine,
                                                                                                                    table=file_path_table_refine,
                                                                                                                    date='{{ ds_nodash }}')

        metadata_file_name_prep = '{location_base}/{date}/l1_metadata_{table}.json'.format(location_base=base_file_location,
                                                                                           table=table,
                                                                                           date='{{ ds_nodash }}')

        metadata_norm_file_name_prep = '{location_base}/{date}/l1_metadata_{table}.json'.format(location_base=base_norm_location,
                                                                                                table=table_norm,
                                                                                                date='{{ ds_nodash }}')

        metadata_combined_file_name_prep = '{location_base}/{date}/{folder_norm}/l1_metadata_{table}.json'.format(location_base=base_norm_location,
                                                                                                                  folder_norm=file_path_table_refine,
                                                                                                                  table=file_path_table_refine,
                                                                                                                  date='{{ ds_nodash }}')

        if for_refinement:
            metadata_norm_file_name_prep = metadata_combined_file_name_prep
            audit_norm_file_name_prep = audit_combined_file_name_prep

        # Load data: bring each individual report into L1 directory for date
        load_data = GoogleAnalyticsToGoogleCloudStorageOperator(
            task_id='get_{source}_data_{table}'.format(source=source,
                                                       table=table),
            google_analytics_conn_id=ga_connection_target,
            view_id=ga_view_id,
            since="{{ ds }}",
            until="{{ ds }}",
            dimensions=dimension_list,
            metrics=metrics_list,
            gcs_conn_id=base_gcp_connector,
            gcs_bucket=base_bucket,
            gcs_filename='{}.json'.format(file_name_prep),
            table_name=table,
            bq_dataset='',
            bq_table='',
            gcp_project=project,
            prefix_norm=norm_folder_prep,
            target_schema_base='',
            event_tables=event_tables,
            event_table_name='',
            bq_rename=bigquery_rename,
            metadata_file=metadata_file_name_prep,
            history_check=HISTORY_CHECK,
            normalize=False,
            page_size=10000,
            include_empty_rows=False,
            sampling_level=None,
            #pool='ga-pool'
            )

        # L1 audit - check source record count against landed record count in L1
        landing_audit = APIAuditOperator(task_id='landing_audit_{table}'.format(table=table),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=dataset_name,
                                         base_gcs_folder=None,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source,
                                         source=source,
                                         metadata_filename=metadata_file_name_prep,
                                         audit_filename=audit_file_name_prep,
                                         check_landing_only=True,
                                         table=table_refine)

        # Normalize data - bring together source files that belong together as specified by ini file into L1_norm
        if for_normalize == 'false':
            normalize_data = EmptyOperator(task_id='normalize_data_{table}'.format(table=table))
        else:
            normalize_data = GoogleAnalyticsToGoogleCloudStorageOperator(task_id='normalize_data_{table}'.format(table=table),
                                                                         google_analytics_conn_id=ga_connection_target,
                                                                         view_id=ga_view_id,
                                                                         since="{{ ds }}",
                                                                         until="{{ tomorrow_ds }}",
                                                                         dimensions=dimension_list,
                                                                         metrics=metrics_list,
                                                                         gcs_conn_id=base_gcp_connector,
                                                                         gcs_bucket=base_bucket,
                                                                         gcs_filename='{}.json'.format(norm_file_name_prep),
                                                                         table_name=table,
                                                                         bq_dataset=dataset_name,
                                                                         bq_table=table_refine,
                                                                         gcp_project=project,
                                                                         prefix_norm=norm_folder_prep,
                                                                         for_refinement=for_refinement,
                                                                         target_schema_base='{base}/l1_schema_{table}.json'.format(base=base_schema_location, table=file_path_table_refine),
                                                                         event_tables=event_tables,
                                                                         event_table_name=event_table_name,
                                                                         bq_rename=bigquery_rename,
                                                                         metadata_file=metadata_norm_file_name_prep,
                                                                         history_check=HISTORY_CHECK,
                                                                         normalize=True,
                                                                         page_size=10000,
                                                                         include_empty_rows=True,
                                                                         sampling_level=None)

        # Audit L1 norm files to match record count
        normalize_audit = APIAuditOperator(task_id='normalize_audit_{table}'.format(table=table),
                                           bucket=base_bucket,
                                           project=project,
                                           dataset=dataset_name,
                                           base_gcs_folder=None,
                                           target_gcs_bucket=base_bucket,
                                           google_cloud_storage_conn_id=base_gcp_connector,
                                           source_abbr=source,
                                           source=source,
                                           metadata_filename=metadata_norm_file_name_prep,
                                           audit_filename=audit_norm_file_name_prep,
                                           check_landing_only=True,
                                           table=table_refine)

        # Check if the refined dataset exists
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
            dataset_id=dataset_name,
            bigquery_conn_id=bq_gcp_connector
        )

        # Refine tables as specified by ini file using GCStoBQOperator and prepared schema files
        refined_file_name_prep = norm_file_name_prep
        if combine_all:
            refined_file_name_prep = combined_file_name_prep

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
            bucket=base_bucket,
            source_objects=['{}.json'.format(refined_file_name_prep), ],
            destination_project_dataset_table='{project}.{dataset_name}.{table}${date}'.format(project=bq_project,
                                                                                               dataset_name=dataset_name,
                                                                                               table=table_refine,
                                                                                               date="{{ ds_nodash }}"),
            schema_fields=[],
            schema_object='{schema_location}/l1_schema_{table}.json'.format(schema_location=base_schema_location,
                                                                            table=file_path_table_refine),
            source_format='NEWLINE_DELIMITED_JSON',
            compression='NONE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=0,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=0,
            bigquery_conn_id=bq_gcp_connector,
            google_cloud_storage_conn_id=base_gcp_connector,
            schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
            src_fmt_configs={},
            autodetect=True)

        # Audit refined table to match BQ record counts to L1_norm record counts
        audit_data = APIAuditOperator(task_id='refine_audit_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
                                      bucket=base_bucket,
                                      project=bq_project,
                                      dataset=dataset_name,
                                      base_gcs_folder=None,
                                      target_gcs_bucket=base_bucket,
                                      google_cloud_storage_conn_id=base_gcp_connector,
                                      source_abbr=source,
                                      source=source,
                                      metadata_filename=metadata_combined_file_name_prep,
                                      audit_filename=audit_combined_file_name_prep,
                                      check_landing_only=False,
                                      table=table_refine)

        # For the bulk of the data, take an extra combination step
        # This goes after normalization and before BigQuery and outputs into l1_norm/client_session_all folder
        if combine_all:

            combine_data = GoogleAnalyticsToGoogleCloudStorageOperator(task_id='combine_data_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
                                                                       google_analytics_conn_id=ga_connection_target,
                                                                       view_id=ga_view_id,
                                                                       since="{{ ds }}",
                                                                       until="{{ tomorrow_ds }}",
                                                                       dimensions=dimension_list,
                                                                       metrics=metrics_list,
                                                                       gcs_conn_id=base_gcp_connector,
                                                                       gcs_bucket=base_bucket,
                                                                       gcs_filename='{}.json'.format(combined_file_name_prep),
                                                                       table_name=table,
                                                                       bq_dataset=dataset_name,
                                                                       bq_table=table_refine,
                                                                       gcp_project=project,
                                                                       prefix_norm=combined_folder_prep,
                                                                       for_refinement=True,
                                                                       target_schema_base='{base}/l1_schema_{table}.json'.format(base=base_schema_location, table=file_path_table_refine),
                                                                       event_tables=event_tables,
                                                                       event_table_name=event_table_name,
                                                                       bq_rename=bigquery_rename,
                                                                       metadata_file=metadata_combined_file_name_prep,
                                                                       history_check=HISTORY_CHECK,
                                                                       normalize=True,
                                                                       page_size=10000,
                                                                       include_empty_rows=True,
                                                                       sampling_level=None)

            # Audit the combined data file information against source record counts
            combine_audit_data = APIAuditOperator(task_id='combine_audit_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
                                                  bucket=base_bucket,
                                                  project=project,
                                                  dataset=dataset_name,
                                                  base_gcs_folder=None,
                                                  target_gcs_bucket=base_bucket,
                                                  google_cloud_storage_conn_id=base_gcp_connector,
                                                  source_abbr=source,
                                                  source=source,
                                                  metadata_filename=metadata_combined_file_name_prep,
                                                  audit_filename=audit_combined_file_name_prep,
                                                  check_landing_only=True,
                                                  table=table_refine)

            # Build the gold layer query from the ini file schema and use the BQOperator
            column_list = []
            for one_field in gold_sql_schema:
                column_list.append('SAFE_CAST({field} as {datatype}) AS {field_name}'.format(field=one_field['select_field'],
                                                                                             datatype=one_field['field_type'],
                                                                                             field_name=one_field['field_name']))

            gld_sql = '''with cte as (select {column_list},
                         SAFE_CAST(DATE("{date}") as TIMESTAMP) as run_date,
                         rank() over (partition by {partition_fields} order by {order_by_fields}) as r
                         from `{project}.{dataset}.{table}`
                         where DATE(_PARTITIONTIME) = "{date}")
                         select * except(r) from cte where r=1
            '''.format(column_list=', '.join(column_list),
                       project=bq_project,
                       dataset=dataset_name,
                       table=table_refine,
                       date="{{ ds }}",
                       partition_fields=', '.join(gold_duplicate_check_partition),
                       order_by_fields=', '.join(gold_duplicate_check_order_by))

            gld_data = BigQueryOperator(task_id='create_gld_digital_dashboard_{table}_{table_refine}'.format(table=table, table_refine=file_path_table_refine),
                                        sql=gld_sql,
                                        destination_dataset_table='{project}.{dataset}.{table_partition}'.format(
                                            project=bq_project,
                                            dataset=gld_dataset,
                                            table_partition=gld_table),
                                        write_disposition='WRITE_APPEND',
                                        gcp_conn_id=bq_gcp_connector,
                                        use_legacy_sql=False,
                                        time_partitioning={'type': 'DAY', 'field': 'run_date'},
                                        schema_update_options=('ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION'))

            load_data >> landing_audit >> normalize_data >> normalize_audit >> combine_data >> combine_audit_data >> create_dataset >> refine_data >> audit_data >> bit_set >> create_gld_dataset >> gld_data
        else:
            load_data >> landing_audit >> normalize_data >> normalize_audit >> create_dataset >> refine_data >> audit_data >> bit_set
