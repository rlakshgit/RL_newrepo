from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from plugins.operators.jm_risk_model_irpm import risk_model_report
#from plugins.operators.load_sp_data import get_sp_data
import json

# dw_creds = Variable.get("dw_secret")
# sql_creds = json.loads(dw_creds)
# {"userid": "alangsner", "password": ""}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 19),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('CL_UNDERWRITING_MODEL_DAG',
          max_active_runs=1,
          schedule_interval='0 14 * * *',
          default_args=default_args,
          catchup=False)


risk_model = PythonOperator(
    task_id='cl_risk_model_reporting',
    python_callable=risk_model_report,
    email=['alangsner@jminsure.com'],
    # op_kwargs={'sql_creds': sql_creds},
    dag=dag)

risk_model


