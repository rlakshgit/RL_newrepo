#library imports
import logging
import datetime
import pandas as pd
import calendar
import time
import json
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.models import Variable
import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)


#DAG - default args
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,7,26),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 30,
    'retry_delay': timedelta(minutes=2),
}

#DAG - Initialization
with DAG(
        'Backfill_trigger_dag',
        schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #
        catchup=False,
        is_paused_upon_creation = False,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


 # get source information from variable. this variable has to set/adjust manually in Airflow UI
    SOURCES_INFO =Variable.get('Backfill_sources')
    json_data = json.loads(SOURCES_INFO)
    sources = json_data.keys()
    for source in sources:
        dag_id = json_data[source]["dag_id"]
        start_date = json_data[source]["start_date"]
        end_date = json_data[source]["end_date"]
        enable = json_data[source]["enable"]

        try:
            start_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
            end_date = pd.to_datetime(end_date).strftime("%Y-%m-%d")
        except:
            raise
        # check the dates to make sure the start date is before end date
        if start_date >= end_date:
            raise Exception("start date should be before end date..please check dates")

        # if the enable flag is set to True for a source - then run Bash operator otherwise run Dummy operator
        if(enable.upper() == "TRUE"):

            Backfill_Task = BashOperator(
                task_id='Backfill_{source}'.format(source=dag_id),
                bash_command="airflow dags backfill --run-backwards {dag_id} --rerun-failed-tasks --start-date {start_date} --end-date {end_date} ".format(dag_id = dag_id, start_date = start_date,end_date = end_date))
        else:
            Backfill_Task   = EmptyOperator(task_id='Backfill_{source}'.format(source=dag_id))


        Backfill_Task