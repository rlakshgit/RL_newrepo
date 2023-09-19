#Import needed Packages
from airflow import DAG
from datetime import datetime, timedelta
from plugins.operators.jm_careplan_ga_to_gcs import GoogleAnalyticsToGoogleCloudStorageOperator
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
    'start_date': datetime(2023,9,1),
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
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    prefix = 'DEV_'
    # mssql_connection_target = 'instDW_STAGE'
    ga_connection_target = 'dev_ga_careplan'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'QA_'
    ga_connection_target = 'ga_qa_edl'
    # mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    prefix = ''
    # mssql_connection_target = 'instDW_PROD'
    ga_connection_target = 'ga_prod_edl'

####
source = 'GA_CarePlan'
source_abbr = 'ga_careplan'

l1_storage_location = '{prefix}l1/{source}/'.format(prefix=prefix,source=source_abbr)
l2_storage_location = '{prefix}l2/{source}/'.format(prefix=prefix,source=source_abbr)
metadata_storage_location = '{prefix}metadata/{source}/'.format(prefix=prefix,source=source_abbr)
schema_storage_location = '{prefix}schema/{source}/'.format(prefix=prefix,source=source_abbr)
audit_storage_location = '{prefix}audit/{source}/'.format(prefix=prefix,source=source_abbr) 


# try:
#     SOURCE_TABLES = json.loads(Variable.get('ga_careplan_source_tables'))
# except:
#     Variable.set('ga_careplan_source_tables', json.dumps({"sessions":{"table_name":"t_sessions","metrics":[{"expression":"ga:sessions"}],"dimensions":[{"name":"ga:date"},{"name":"ga:year"},{"name":"ga:month"}]}}))
#     SOURCE_TABLES = json.loads(Variable.get('ga_careplan_source_tables'))

# SOURCE_TABLES_KEYS = SOURCE_TABLES.keys()
# source = 'ga_careplan'
# source_full = 'GoogleAnalytics_CarePlan'
dataset_name = '{base}{source}'.format(source=source.lower(), base=prefix)


# base_file_location = '{base}/{source}'.format(base=base_gcs_folder,
#                                               source=source)

# base_schema_location = '{base}_schema/{source}/{date}'.format(base=base_gcs_folder,
#                                                               source=source,
#                                                               date='{{ ds_nodash }}')
# base_audit_location = '{base}_audit/{source}'.format(base=base_gcs_folder,
#                                                      source=source)
# ga_creds = Variable.get('ga_careplan_creds')

with DAG(
        '{source}_dag'.format(source=source),
        schedule_interval='0 0 1 * *', #monthly run
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    ga_view_id = '227969531'


    
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset_careplan_ga',
        dataset_id=dataset_name,
        bigquery_conn_id=bq_gcp_connector
    )

    # for SOURCE in SOURCE_TABLES_KEYS:
    # table = SOURCE_TABLES[SOURCE]['table_name'].split("_")[1]
    # metrics_list = SOURCE_TABLES[SOURCE]['metrics']
    # dimension_list = SOURCE_TABLES[SOURCE]['dimensions']
    table = 't_monthly_sessions_and_views'
    metrics_list = ['year', 'month']
    dimension_list = ['year', 'month']

    # file_name_prep = '{location_base}/{date}/l1_data_{table}'.format(location_base=base_file_location,table=table,date='{{ ds_nodash }}')
    audit_file_name_prep = '{audit_storage_location}{date}/{source}_{date}.json'.format(audit_storage_location=audit_storage_location, source=source, date="{ds_nodash}")

    metadata_file_name_prep = '{metadata_storage_location}{date}/{source}_{date}.json'.format(metadata_storage_location=metadata_storage_location, source=source, date="{ds_nodash}")

    
    # Load data: bring each individual report into L1 directory for date
    land_data = GoogleAnalyticsToGoogleCloudStorageOperator(
        task_id='get_{source}_data'.format(source=source),
        google_analytics_conn_id=ga_connection_target,
        view_id=ga_view_id,
        since="{{ ds }}",
        until="{{ ds }}",
        dimensions=dimension_list,
        metrics=metrics_list,
        gcs_conn_id=base_gcp_connector,
        gcs_bucket=base_bucket,
        source = source,
        l1_storage_location = l1_storage_location,
        l2_storage_location = l2_storage_location,
        metadata_filename=metadata_file_name_prep,
        schema_storage_location = schema_storage_location,
        audit_storage_location = audit_storage_location,
        gcp_project=project,
        sampling_level=None)


    land_audit = APIAuditOperator(task_id='landing_audit_{source}'.format(source=source),
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




    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_{source}'.format(source=source),
        bucket=base_bucket,
        source_objects=['{l2_storage_location}{date}/{source}_{date}.json'.format(
            l2_storage_location=l2_storage_location,
            source=source,
            date="{{ ds_nodash }}" )],
        destination_project_dataset_table='{project}.{dataset_name}.{table}${date}'.format(project=bq_target_project,
                                                                                            dataset_name=dataset_name,
                                                                                            table=table,
                                                                                            date="{{ ds_nodash }}"),
        schema_fields=[],
        schema_object='{schema_storage_location}{date}/{source}_{date}.json'.format(schema_storage_location=schema_storage_location,
                                                                                    source=source,
                                                                                    date="{{ ds_nodash }}"),
        source_format='CSV',
        compression='NONE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=0,
        bigquery_conn_id=bq_gcp_connector,
        google_cloud_storage_conn_id=base_gcp_connector,
        schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
        src_fmt_configs={},
        autodetect=True)

    load_audit = APIAuditOperator(task_id='bq_audit_{source}'.format(source=source),
                                    bucket=base_bucket,
                                    project=bq_target_project,
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

    create_dataset >> land_data >> land_audit >> load_data >> load_audit 


