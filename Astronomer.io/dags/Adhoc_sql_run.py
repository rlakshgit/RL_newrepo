import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from airflow.models import Variable

ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    prefix = 'DEV_'
elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    prefix = ''


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

"""try:
    sql= Variable.get("Adhoc_sql")
except:
    Variable.set('Adhoc_sql',"")
    sql=Variable.get("Adhoc_sql")"""

with DAG(
        'Adhoc_run_sql',
        schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #
        catchup=False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/Adhoc_SQL/Adhoc_sql.sql')) as f:
        read_sql = f.readlines()
    sql = "\n".join(read_sql)
    if sql == "":
        raise Exception("SQL file empty. Please check")
    else:
        pass

    adhoc_sql_run = runbqscriptoperator(task_id='adhoc_sql_run',
                                                  sql=sql,
                                                  bigquery_conn_id=bq_gcp_connector)
    adhoc_sql_run