"""
--This Dag archives(copy dataset/tables from source to destination and delete the source) inside the same project
--We have to pass project,source_datset,destination_dataset,table_search string, destination dataset expiration time,delete_source
  through airflow variable called Archival_variable in Airflow web frontend

-- we have three options - we can copy entire dataset or copy list of tables inside a dataset or copy tables starts with some search string
   It can be achieved by manipulating search_string in Archival_variable through Airflow web frontend
   if search string ='' ----> copy entire dataset to destination dataset
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"","dataset_expiration_time":864000}
   if search string ='[table1,table2,table3]' -----> copy only list of tables in a source dataset to destination dataset
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"[table1,table2,table3]","dataset_expiration_time":864000}
   if search string = 'sometext' ------> copy only those tables which starts with search string
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"pctl","dataset_expiration_time":864000}
Note:
    To delete the source table/dataset -->"delete_source" in archival variable has set to be True.
    dataset_expiration_time field is set in seconds (for example "dataset_expiration_time":864000 ) 864000 = 10 days
"""
import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow import configuration
from airflow.operators.bash_operator import BashOperator
from plugins.operators.jm_bq_archive import BigQueryArchiveOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator


from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)
AIRFLOW_ENV = Variable.get('ENV')
# Airflow Environment Variables
if AIRFLOW_ENV.lower() == 'dev':
    base_gcp_connector = 'dev_edl'

elif AIRFLOW_ENV.lower() == 'qa':
    base_gcp_connector = 'qa_edl'

elif AIRFLOW_ENV.lower() == 'prod':
    base_gcp_connector = 'jm_landing_prod'


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Getting values from archival_variable and parse it into other variables
SOURCES_INFO =Variable.get('Archival_Variable')
json_data = json.loads(SOURCES_INFO)
project = json_data["project"]
source_dataset = json_data["source_dataset"]
destination_dataset = json_data["destination_dataset"]
table_search_string = json_data["table_search_string"]
dataset_expiration_time = json_data["dataset_expiration_time"]
delete_source=json_data["delete_source"]
archive=json_data["archive"]
datasets_to_delete=json_data["datasets_to_delete"]

#params = '{"source_dataset_id":"{source_dataset}","source_project_id":"{source_project}","overwrite_destination_table":"true"}'.format(source_dataset = source_dataset,source_project=source_project )
with DAG(
        'Archive_Dataset_or_Table_DAG',
        schedule_interval='@once',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        if archive=='False':
            create_destination_dataset = EmptyOperator(task_id='create_destination_dataset')

        else:


            # This task creates Destination dataset for archiving
            create_destination_dataset = BashOperator(
                task_id='create_destination_dataset',
                bash_command='exists=$(bq ls -n 1000 -d {project}:| grep -w "{destination_dataset}");if [ -n "$exists" ]; then echo "Not creating dataset since it already exists";else bq mk --dataset --default_table_expiration {dataset_expiration_time} --description "Archive table" {project}:{destination_dataset}; fi'.format(
                                                                                                                            dataset_expiration_time=dataset_expiration_time,
                                                                                                                            destination_dataset=destination_dataset,
                                                                                                                            project = project))
        # This task copies dataset/tables from source to destination dataset and delete the dataset/tables if delete_source is set to True
        bigquery_transfer = BigQueryArchiveOperator(
            task_id = 'Archive_job',
            base_gcp_connector = base_gcp_connector,
            project = project,
            source_dataset = source_dataset,
            destination_dataset = destination_dataset,
            table_search_string = table_search_string,
            delete_source = delete_source,
            archive=archive,
            datasets_to_delete = datasets_to_delete
        )

        create_destination_dataset>> bigquery_transfer


