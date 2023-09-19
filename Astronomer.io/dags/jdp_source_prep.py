import logging
import datetime as dt
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.dummy_operator import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_mssql_to_gcs_JDP import JM_MsSqlToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    bq_target_project = 'dev-edl'
    bq_gcp_connector = 'dev_edl'
    folder_prefix = ''
    mssql_connection = 'instDW_STAGE'

elif AIRFLOW_ENV.lower() == 'qa':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    base_bucket = 'jm-edl-landing-wip'
    bq_target_project = 'qa-edl'
    bq_gcp_connector = 'qa_edl'
    folder_prefix = ''
    mssql_connection = 'instDW_STAGE'

elif AIRFLOW_ENV.lower() == 'prod':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    base_bucket = 'jm-edl-landing-prod'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    folder_prefix = ''
    mssql_connection = 'instDW_PROD'


# source_dag##

with DAG(
        'jdp_source_prep',
        schedule_interval='0 13 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:



    try:
        SOURCE_INFO = json.loads(Variable.get('jdp_source_prep'))
    except:
        Variable.set('jdp_source_prep', json.dumps({
            "careplan": {"dataset": "ref_careplan", "dag_id": "CarePlan_DAIS_dag", "external_task_id": "set_source_bit", "interval": "7"},
            "jmservices": {"dataset": "ref_jmservices_platinumpoints_current", "dag_id": "JMServices_dag", "external_task_id": "set_source_bit", "interval": "-1"},
            "plecom": {"dataset": "ref_plecom_jewelerscut_current", "dag_id": "PLEcom_dag", "external_task_id": "set_source_bit", "interval": "4"},
            "salesforce": {"dataset": "ref_salesforce", "dag_id": "Salesforce_grouped_dag", "external_task_id": "set_source_bit", "interval": "7"},
            "claimcenter": {"dataset": "ref_cc_current", "dag_id":"ClaimCenter_Prioritized_Tables","external_task_id": "set_source_bit", "interval": "3"},
            "wexler_active_xlsx": {"dataset": "ref_agency_information", "dag_id": "agency_information_dag", "external_task_id": "set_source_bit", "interval": "1"},
            "lunar_active_xlsx": {"dataset": "ref_agency_information", "dag_id": "agency_information_dag", "external_task_id": "set_source_bit", "interval": "1"},
            "jmis_active_xlsx": {"dataset": "ref_agency_information", "dag_id": "agency_information_dag", "external_task_id": "set_source_bit", "interval": "1"},
            "GWPC": {"dataset": "None", "dag_id": "Get_Tables_Lists","external_task_id": "gcs_trigger_sensor", "interval": "1"}
        }))


        SOURCE_INFO = json.loads(Variable.get('JDPSources'))

    sources = SOURCE_INFO.keys()

    JDP_gld_dataset = '{prefix}gld_JDP'.format(prefix=folder_prefix)

    create_JDP_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_JDP_dataset',
        dataset_id=JDP_gld_dataset,
        bigquery_conn_id=bq_gcp_connector)

    for source in sources:

        dataset = SOURCE_INFO[source]["dataset"]
        external_dag_id = SOURCE_INFO[source]["dag_id"]
        external_task_id = SOURCE_INFO[source]["external_task_id"]
        interval= int(SOURCE_INFO[source]["interval"])
        source = source.lower()
        destination_table_name = 't_JDP_{source}'.format(source=source)
        source_project = 'prod-edl'
        with open(os.path.join(configuration.get('core', 'dags_folder'),
                               r'JDP/source_prep_SQL/JDP_{source}.sql'.format(source = source))) as f:
            sql_list = f.readlines()
        sql = "\n".join(sql_list)

        gld_sql = sql.format(project=source_project
                             , dataset=dataset
                             ,date = '{{ ds }}')


        check_data_source = ExternalTaskSensorAsync(
            task_id='check_{source}_data'.format(source=source),
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_delta=timedelta(hours=interval),
            timeout=36000)  # timeout in seconds



        if source != 'gwpc':
            load_data_to_storage = EmptyOperator(
                task_id='land_data_{source}'.format(source=source)
            )
            JDP_Source_tables = BigQueryOperator(task_id='load_JDP_{source}_table'.format(source=source),
                                                        sql=gld_sql,
                                                        destination_dataset_table='{project}.{dataset}.{table_name}'.format(
                                                            project=bq_target_project,
                                                            dataset=JDP_gld_dataset,
                                                            table_name=destination_table_name),
                                                        write_disposition='WRITE_TRUNCATE',
                                                        gcp_conn_id=base_gcp_connector,
                                                        use_legacy_sql=False)

        else:
            load_data_to_storage = JM_MsSqlToGoogleCloudStorageOperator(
                task_id='land_data_{source}'.format(source=source),
                sql=gld_sql,
                bucket=base_bucket,
                filename=folder_prefix + 'JDP/gwpc/data/{{ ds_nodash }}/export.json',
                schema_filename=folder_prefix + 'JDP/gwpc/schemas/{{ ds_nodash }}/export.json',
                mssql_conn_id=mssql_connection,
                google_cloud_storage_conn_id=base_gcp_connector
            )

            # creating table promoted_pc_cl_locations_core in core_insurance_cl dataset
            JDP_Source_tables = GCSToBigQueryOperator(
                task_id='load_JDP_{source}_table'.format(source=source),
                bucket=base_bucket,
                source_objects=[folder_prefix + 'JDP/gwpc/data/{{ ds_nodash }}/export.json'],
                destination_project_dataset_table='{project}.{dataset}.{table}'.format(
                    project=bq_target_project, dataset=JDP_gld_dataset, table = destination_table_name),
                schema_fields=None,
                schema_object=folder_prefix + 'JDP/gwpc/schemas/{{ ds_nodash }}/export.json',
                source_format='NEWLINE_DELIMITED_JSON',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                gcp_conn_id=bq_gcp_connector,
                schema_update_options=()
            )




        create_JDP_dataset >>check_data_source>>load_data_to_storage>> JDP_Source_tables
        #create_JDP_dataset >> JDP_Source_tables

