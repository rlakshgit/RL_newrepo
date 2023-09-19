####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_mongodb_to_gcs_memo import Mongodbtogcs
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.operators.python import PythonOperator
import json 
import os


#####Initializing Variables ########
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mongo_db_conn_id='azure_membership_uat2'
    base_gcs_folder = 'DEV_l1'
    prefix_folder = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mongo_db_conn_id = 'azure_membership_uat2'
    base_gcs_folder = 'B_QA_l1'
    prefix_folder = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    mongo_db_conn_id = 'azure_membership_prod'
    base_gcs_folder = 'l1'
    prefix_folder = ''

source='memo'
source_abbr = 'memo'
collection='memodocuments'
database_name='platform-memo'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source_abbr)
base_norm_location = folder_prefix + 'l2/{source_abbr}/'.format(source_abbr=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)

metadata_filename = '{base_gcs_folder}/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)

refine_dataset = '{prefix}ref_zing_{source}'.format(prefix=prefix_folder,source=source)
audit_filename = '{base_gcs_folder}_audit/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,1,13),
    #'end_date': datetime(2022,6,8),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'smaguluri@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'memo', r'ini', r"memo_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())
    
def _set_var(k, v):
    Variable.set(k, v)
    return f'Variable {k} is set to {v}'

################################################################################
# INSTANTIATE DAG TO RUN once (6AM CDT)
################################################################################

with DAG(
        'zing_memo_landing_dag',
        schedule_interval='0 10 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_args) as dag:
    
    refine_dataset = folder_prefix + 'ref_zing_' + source_abbr
    
    entity_list = ['lookupentity','preference','memo','memo_Items','memo_Shipments','memo_StatusLogs','memo_Reminders']
    
    try:
        HISTORY_CHECK = Variable.get('memo_history_check')
    except:
        Variable.set('memo_history_check', 'True')
        HISTORY_CHECK = Variable.get('memo_history_check')
    

    start_task = EmptyOperator(task_id = 'start_task'  )
    end_task = EmptyOperator(task_id='end_task')
    complete_landing = EmptyOperator(task_id='complete_landing')

    for entity in entity_list: 
        if '_' not in entity:
            land_data = Mongodbtogcs(task_id='land_{entity}_files_to_gcs'.format(entity=entity),
                                            project=project,
                                            source=source,
                                            source_abbr=source_abbr,
                                            entity=entity,
                                            collection=collection,
                                            database_name=database_name,
                                            target_gcs_bucket=base_bucket,
                                            google_cloud_storage_conn_id =base_gcp_connector,
                                            base_gcs_folder=base_file_location,
                                            base_schema_folder=base_schema_location,
                                            base_norm_folder = base_norm_location,
                                            metadata_filename= base_file_location,
                                            mongo_db_conn_id = mongo_db_conn_id,
                                            confJSON = confJSON)

        
        
        
            landing_audit = APIAuditOperator(task_id='landing_audit_{table}'.format(table=entity),
                                            bucket=base_bucket,
                                            project=project,
                                            dataset=refine_dataset,
                                            base_gcs_folder=None,
                                            target_gcs_bucket=base_bucket,
                                            google_cloud_storage_conn_id=base_gcp_connector,
                                            source_abbr=source_abbr,
                                            source=source,
                                            metadata_filename='{base_folder}{entity}/{date_nodash}/l1_metadata_{source}_{entity}.json'.format(
                                                base_folder=base_file_location,
                                                source=source_abbr,
                                                date_nodash="{{ ds_nodash }}",
                                                entity=entity),
                                            audit_filename='{base_audit_folder}{entity}/{date_nodash}/l1_audit_{source}_{entity}.json'.format(
                                                base_audit_folder=base_audit_location,
                                                source=source_abbr,
                                                date_nodash="{{ ds_nodash }}",
                                                entity=entity),
                                            check_landing_only=True,
                                            table=entity)


            

        else:

            landing_audit = APIAuditOperator(task_id='landing_audit_{table}'.format(table=entity),
                                            bucket=base_bucket,
                                            project=project,
                                            dataset=refine_dataset,
                                            base_gcs_folder=None,
                                            target_gcs_bucket=base_bucket,
                                            google_cloud_storage_conn_id=base_gcp_connector,
                                            source_abbr=source_abbr,
                                            source=source,
                                            metadata_filename='{base_folder}{entity}/{date_nodash}/l1_metadata_{source}_{entity}.json'.format(
                                                base_folder=base_file_location,
                                                source=source_abbr,
                                                date_nodash="{{ ds_nodash }}",
                                                entity=entity),
                                            audit_filename='{base_audit_folder}{entity}/{date_nodash}/l1_audit_{source}_{entity}.json'.format(
                                                base_audit_folder=base_audit_location,
                                                source=source_abbr,
                                                date_nodash="{{ ds_nodash }}",
                                                entity=entity),
                                            check_landing_only=True,
                                            table=entity)


        start_task >> land_data >> landing_audit >> end_task
        
    set_full_load_var = PythonOperator( task_id = 'set_history_load_var',
                                                python_callable = _set_var,
                                                op_kwargs = {"k" : "memo_history_load", "v": "N"} )

    end_task >> set_full_load_var