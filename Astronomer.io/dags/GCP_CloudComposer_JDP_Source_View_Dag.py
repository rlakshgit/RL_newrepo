import logging
import datetime as dt
import calendar
import time
import json
import os
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync


ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_target_project = 'dev-edl'
    bq_gcp_connector = 'dev_edl'
    folder_prefix = ''

elif AIRFLOW_ENV.lower() == 'qa':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_target_project = 'qa-edl'
    bq_gcp_connector = 'qa_edl'
    folder_prefix = ''

elif AIRFLOW_ENV.lower() == 'prod':
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    folder_prefix = ''


# source_dag##

with DAG(
        'JDP_source_view_prep',
        schedule_interval='0 16 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:



    try:
        SOURCE_INFO = json.loads(Variable.get('JDPSources'))
    except:
        Variable.set('JDPSources', json.dumps({
            "careplan": {"dataset": "ref_careplan", "dag_id": "CarePlan_DAIS_dag", "external_task_id": "set_source_bit", "interval": "10"},
            "jmservices": {"dataset": "ref_jmservices_platinumpoints_current", "dag_id": "JMServices_dag", "external_task_id": "set_source_bit", "interval": "-1"},
            "plecom": {"dataset": "ref_plecom_jewelerscut_current", "dag_id": "PLEcom_dag", "external_task_id": "set_source_bit", "interval": "4"},
            "salesforce": {"dataset": "ref_salesforce", "dag_id": "Salesforce_grouped_dag", "external_task_id": "set_source_bit", "interval": "10"},
            "claimcenter": {"dataset": "ref_cc_current", "dag_id":"ClaimCenter_Prioritized_Tables","external_task_id": "set_source_bit", "interval": "3"},
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
                               r'ETL/JDP/JDP_{source}.sql'.format(source = source))) as f:
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




        JDP_Source_tables = BigQueryOperator(task_id='create_JDP_{source}_table'.format(source=source),
                                                    sql=gld_sql,
                                                    destination_dataset_table='{project}.{dataset}.{table_name}'.format(
                                                        project=bq_target_project,
                                                        dataset=JDP_gld_dataset,
                                                        table_name=destination_table_name),
                                                    write_disposition='WRITE_TRUNCATE',
                                                    gcp_conn_id=base_gcp_connector,
                                                    use_legacy_sql=False)



        create_JDP_dataset >>check_data_source>> JDP_Source_tables
        #create_JDP_dataset >> JDP_Source_tables

