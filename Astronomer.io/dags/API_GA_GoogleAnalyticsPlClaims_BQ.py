"""
Author: Gayathri Narayanasamy
Date: 2/20/2022
Source: GA - PLclaims
Description: This dag ingest data from Gaoogle Analytics - Plclaims  source into GCP storage and then into BigQuery. At present Business wants to ingest
dat from three metrics - PageViews,Sessions and events. we can add more metrics and dimesions dynamically by adding it to the variable 'ga_plclaims_source_tables'
in Airflow

"""
#Import needed Packages
from airflow import DAG
from datetime import datetime, timedelta
from plugins.operators.jm_plclaims_ga_to_gcs import GoogleAnalyticsToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.models import Variable
import json
from pandas.io.json import json_normalize


#Variable declarations
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,12,31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#Environment Variables declaration
AIRFLOW_ENV = Variable.get('ENV')
if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    base_prefix = 'DEV_'
    ga_connection_target = 'ga_plclaims_dev'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    base_prefix = 'B_QA_'
    ga_connection_target = 'ga_plclaims_qa'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'prod_edl'
    bq_project = 'prod-edl'
    base_gcs_folder = 'l1'
    base_prefix = ''
    ga_connection_target = 'ga_plclaims_prod'

##Reading ga_plclaims config from variable
try:
    SOURCE_TABLES = json.loads(Variable.get('ga_plclaims_source_tables'))
except:
    Variable.set('ga_plclaims_source_tables', json.dumps(
{"sessions": {"table_name": "t_sessions", "metrics": [{"expression": "ga:sessions"}], "dimensions": [{"name": "ga:date"}, {"name": "ga:year"}, {"name": "ga:month"}]},
"pageviews": {"table_name": "t_pageviews", "metrics": [{"expression": "ga:pageviews"},{"expression": "ga:uniquePageviews"}], "dimensions": [{"name": "ga:date"}, {"name": "ga:year"}, {"name": "ga:month"}]},
"events": {"table_name": "t_events", "metrics": [{"expression": "ga:totalEvents"},{"expression": "ga:uniqueEvents"}], "dimensions": [{"name": "ga:date"}, {"name": "ga:year"}, {"name": "ga:month"}, {"name": "ga:eventCategory"}, {"name": "ga:eventAction"}]}}))
SOURCE_TABLES = json.loads(Variable.get('ga_plclaims_source_tables'))

SOURCE_TABLES_KEYS = SOURCE_TABLES.keys()
source = 'ga_plclaims'
source_full = 'GoogleAnalytics_PLClaims'
dataset_name = '{base}ref_{source}'.format(source=source.lower(),
                                              base=base_prefix)


base_file_location = '{base}/{source}'.format(base=base_gcs_folder,
                                              source=source)

base_schema_location = '{base}_schema/{source}/{date}'.format(base=base_gcs_folder,
                                                              source=source,
                                                              date='{{ ds_nodash }}')
base_audit_location = '{base}_audit/{source}'.format(base=base_gcs_folder,
                                                     source=source)

with DAG(
        '{source}_dag'.format(source=source_full),
        schedule_interval='0 8 * * *', #daily run
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    ga_view_id = '227969531'


        # Check if the refined dataset exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=dataset_name,
        bigquery_conn_id=bq_gcp_connector
    )

    #For each metrics in config file get the data from GA api
    for SOURCE in SOURCE_TABLES_KEYS:
        table = SOURCE_TABLES[SOURCE]['table_name'].split("_")[1]
        metrics_list = SOURCE_TABLES[SOURCE]['metrics']
        dimension_list = SOURCE_TABLES[SOURCE]['dimensions']

        file_name_prep = '{location_base}/{date}/l1_data_{table}'.format(location_base=base_file_location,table=table,date='{{ ds_nodash }}')
        audit_file_name_prep = '{location_base}/{date}/l1_audit_results_{table}.json'.format(location_base=base_audit_location,table=table,
                                                                                             date='{{ ds_nodash }}')
        metadata_file_name_prep = '{location_base}/{date}/l1_metadata_{table}.json'.format(location_base=base_file_location,
                                                                                           table=table,
                                                                                           date='{{ ds_nodash }}')


        # Load data into Storage
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
            target_schema_base='{base}/l1_schema_{table}.json'.format(base=base_schema_location,
                                                                      table=table),
            gcp_project=project,
            metadata_file=metadata_file_name_prep,
            sampling_level=None)

        #Landing Audit
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
                                         table=table)
        #Loading data into BQ
        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{table}'.format(table=table),
            bucket=base_bucket,
            source_objects=['{}.json'.format(file_name_prep), ],
            destination_project_dataset_table='{project}.{dataset_name}.{table}${date}'.format(project=bq_project,
                                                                                               dataset_name=dataset_name,
                                                                                               table=table,
                                                                                               date="{{ ds_nodash }}"),
            schema_fields=[],
            schema_object='{schema_location}/l1_schema_{table}.json'.format(schema_location=base_schema_location,
                                                                            table=table),
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

        #BQ audit
        refine_audit = APIAuditOperator(task_id='refine_audit_{table}'.format(table=table),
                                      bucket=base_bucket,
                                      project=bq_project,
                                      dataset=dataset_name,
                                      base_gcs_folder=None,
                                      target_gcs_bucket=base_bucket,
                                      google_cloud_storage_conn_id=base_gcp_connector,
                                      source_abbr=source,
                                      source=source,
                                      metadata_filename=metadata_file_name_prep,
                                      audit_filename=audit_file_name_prep,
                                      check_landing_only=False,
                                      table=table)


        create_dataset >> load_data >> landing_audit>>refine_data>>refine_audit


