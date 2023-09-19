from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow import configuration
from airflow.hooks.base import BaseHook
from google.oauth2 import service_account
import pandas_gbq
import logging
import pandas as pd
import os
import json

from datetime import datetime, timedelta

# this dag gets the data from BQ and populates a given table in an SQL db.
# during population, it also generates a master key for each record and assumes the status of all records as active ("OK")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('jdp_places_refresh_1',
          max_active_runs=1,
          catchup=False,
          schedule_interval='@weekly',  # '0 3 0 * *',
          default_args=default_args)


# method to get data and push each record using pandas-gbq

def group_tasks():
    place_key_tasks = {}
    #    service_account_file = os.path.join(configuration.get('core', 'dags_folder'), r'common/semi-managed-reporting.json')
    #    credentials_smr = service_account.Credentials.from_service_account_file(service_account_file, )
    #	credentials_smr = service_account.Credentials.from_service_account_file('semi-managed-reporting.json')
    bq_connection = BaseHook.get_connection('semi_managed_gcp_connection')
    client_secrets = json.loads(bq_connection.extra_dejson['keyfile_dict'])
    credentials_smr = service_account.Credentials.from_service_account_info(client_secrets)
    project_id_smr = "semi-managed-reporting"
    sql = '''WITH    current_a AS (
						SELECT placeKey
								, MAX(update_time) as update_time
						 FROM `semi-managed-reporting.core_JDP.placeKey_table`
             WHERE current_pid NOT IN (SELECT IFNULL(previous_pid, 'x') previous_pid FROM `semi-managed-reporting.core_JDP.placeKey_table` )
                  AND current_pid != 'NOT_FOUND'
						GROUP BY placeKey),
           

					target AS (
						 SELECT placeKey
								, current_pid
								, update_time
						 FROM `semi-managed-reporting.core_JDP.placeKey_table`
						 WHERE current_pid != 'NOT_FOUND'
                )
                
              
			SELECT DISTINCT placeKey, update_time
				FROM current_a LEFT JOIN target
				USING (placeKey, update_time)'''

    logging.info('LOADING PLACE IDS...')
    df = pandas_gbq.read_gbq(sql, project_id=project_id_smr, credentials=credentials_smr, dialect="standard")
    existing_pid_sql = '''SELECT current_pid FROM `semi-managed-reporting.core_JDP.placeKey_table` GROUP BY current_pid HAVING current_pid != 'NOT_FOUND' '''
    logging.info('LOADING EXISTING PLACE IDS')
    df_existing_pid = pandas_gbq.read_gbq(existing_pid_sql, project_id=project_id_smr, credentials=credentials_smr,
                                          dialect="standard")

    logging.info('Creating tasks...')
    i = 1
    j = 0
    temp_list = []
    for pk in df.placeKey.to_list():
        temp_list.append(pk)
        if i % 1000 == 0:
            place_key_tasks[f'td_{j}'] = temp_list
            j += 1
            temp_list = []
        i += 1

    Variable.set("jdp_place_key_tasks", place_key_tasks)
    Variable.set("jdp_place_id_existing", df_existing_pid.current_pid.to_list())


placekey_group_tasks = PythonOperator(
    task_id='placekey_group_tasks',
    python_callable=group_tasks,
    dag=dag,
)

t_wait = TimeDeltaSensorAsync(
    task_id='wait_15_mins',
    delta=timedelta(seconds=900),
    dag=dag
)

trigger = TriggerDagRunOperator(
    task_id="trigger_refresh",
    trigger_dag_id="jdp_places_refresh_2",
    dag=dag,
)

# placekey_group_tasks >> trigger
placekey_group_tasks >> t_wait >> trigger
