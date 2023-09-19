import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}



#source_dag

with DAG(
        'EnvironmentReset',
        schedule_interval= '0 5 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=False,
		max_active_runs=1,
        default_args=default_dag_args) as dag:

    bit_set = CompletionOperator(task_id='reset_source_states',
                                   source='ALL',
                                   mode='RESET')

