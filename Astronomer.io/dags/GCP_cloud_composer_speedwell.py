####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import os
import json


#####Variables Section ########
AIRFLOW_ENV = Variable.get('ENV')
if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'speedwell_data_upload'
    source_project = 'jm-dl-landing'
    gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    prefix= 'DEV_'
    base_gcs_folder = ''


elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'speedwell_data_upload'
    source_project = 'jm-dl-landing'
    gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
    base_gcs_folder = ''


elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'speedwell_data_upload'
    source_project = 'jm-dl-landing'
    bq_target_project = 'prod-speedwell'
    gcp_connector='prod_speedwell'
    prefix=''
    base_gcs_folder = ''


source='speedwell'
source_abbr = 'speedwell'
refine_dataset = '{prefix}ref_{source}'.format(prefix=prefix,source=source)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,6,20),
    # 'end_date': datetime(2022,3,10),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    #'retries': 4,
    #'retry_delay': timedelta(minutes=1),
}

table_list = ['weather','earthquake']
with DAG(
        'speedwell_loading_dag',
        schedule_interval= '@once',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source_abbr),
        dataset_id= refine_dataset,
        bigquery_conn_id=gcp_connector)

    for item in table_list:
        storage_location = base_gcs_folder + '{source}/'.format(source=item)

        #schema files
        file_path = os.path.join(configuration.get('core', 'dags_folder'),
                                         r'speedwell/ini/{table}_schema.json'.format(table=item))
        with open(file_path, 'rb') as inputJSON:
            schemaList = json.load(inputJSON)

        #SQL - create table script files

        sql_path = os.path.join(configuration.get('core', 'dags_folder'),
                                 r'speedwell/create_tables_script/{table}.sql'.format(table=item))
        with open(sql_path) as inputpath:
            read_file_data = inputpath.readlines()
        cleaned_file_data = "\n".join(read_file_data)
        sql = cleaned_file_data.format(project=bq_target_project,dataset=refine_dataset,table=item)


        ##Views script
        sql_view_path = os.path.join(configuration.get('core', 'dags_folder'),
                                     r'speedwell/views/create_{table}_view.sql'.format(table=item))
        with open(sql_view_path) as inputpath:
            file_data = inputpath.readlines()
        new_file_data = "\n".join(file_data)
        view_sql = new_file_data.format(project=bq_target_project, dataset=refine_dataset)
        ##Create speedwell empty tables

        create_table = runbqscriptoperator(
            task_id='create_{table}_table'.format(table=item),
            sql=sql,
            bigquery_conn_id=gcp_connector )

        ###Load data from L1 storage to BQ
        load_data_to_BQ = GoogleCloudStorageToBigQueryOperator(
                        task_id='load_data_{source}_{api_name}'.format(source=source_abbr,
                                                                       api_name=item),
                        bucket=base_bucket,
                        source_objects=['{storage_location}*.csv'.format(storage_location=storage_location)],
                        destination_project_dataset_table='{project}.{dataset}.t_{table}'.format(
                        project=bq_target_project,
                            dataset = refine_dataset,
                        source_abbr=source_abbr.lower(),
                        table=item),
                        schema_fields=schemaList,
                        #schema_object=base_schema_location+'{api_name}/{date}/l1_schema_{source}_{api_name}.json'.format(
                                                                                             # api_name=table,
                                                                                              #source = source,
                                                                                              #date="{{ ds_nodash }}"),
                        source_format='CSV',
                        compression='NONE',
                        ignore_unknown_values=False,
                        allow_quoted_newlines =True,
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=1,
                        write_disposition='WRITE_TRUNCATE',
                        max_bad_records=0,
                        bigquery_conn_id=gcp_connector,
                        google_cloud_storage_conn_id=gcp_connector,
                        #schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION']
                        )

        create_view = runbqscriptoperator(
            task_id='create_{table}_view'.format(table=item),
            sql=view_sql,
            bigquery_conn_id=gcp_connector)




        create_dataset >> create_table>> load_data_to_BQ>>create_view
