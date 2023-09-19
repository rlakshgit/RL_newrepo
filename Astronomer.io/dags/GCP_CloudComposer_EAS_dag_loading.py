import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
#from plugins.operators.jm_hubspot_search_to_gcs import HubspotSearchToGoogleCloudStorageOperator
#from plugins.operators.jm_hubspot_api_to_gcs import HubspotAPIToGoogleCloudStorageOperator
#from plugins.operators.jm_hubspot_flatten_data import HubspotFlattenDataOperator
from plugins.operators.jm_gcs_to_bq_eas import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_eas_staging import EASStagingOperator
from plugins.operators.jm_eas_data_availability_check import EASDataAvailabilityCheckOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
#from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.contrib.operators.bigquery_operator import  BigQueryOperator
from plugins.operators.jm_BQSchemaGenerationOperator import BQSchemaGenerationOperator
from airflow.models import Variable
import json
from airflow.operators.python import BranchPythonOperator 
import logging

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    folder_prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'RL_QA_l1'
    folder_prefix = 'RL_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    base_gcs_folder = 'l1'
    folder_prefix = ''

source='EAS'
source_abbr = 'EAS'
l2_storage_location = folder_prefix + 'l1_norm/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)
staging_file_location = folder_prefix+'staging/{source}/'.format(source=source_abbr)
metadata_storage_location = '{base_gcs_folder}/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
#refine_dataset = '{prefix}{source_abbr}_ingestion'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
#gld_dataset = '{prefix}{source_abbr}_snapshot'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
refine_dataset = '{prefix}ref_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
gld_dataset  = '{prefix}gld_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
table = 'Vendor_Data'
table1 = 'Vendor_Information'
source_table_vd = 't_eas_Vendor_Data'
source_table_vi = 't_eas_Vendor_Information'

source_table=['t_eas_Vendor_Data.Vendor_Data','t_eas_Vendor_Information.Vendor_Information']

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'rlaksh@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

confFilePath = os.path.join(configuration.get('core','dags_folder'), r'EAS', r'ini', r"eas_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())

    eas_tables = confJSON.keys()

##
with DAG(
        'eas_loading_zone_dag_v2',
        schedule_interval= '0 6 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source_abbr),
        dataset_id= refine_dataset,
        bigquery_conn_id=bq_gcp_connector)

    #Sensor task to ensure landing job has finished before starting this loading job 
    task_sensor_core = ExternalTaskSensorAsync(
            task_id='check_{source}_update_dag'.format(source=source_abbr),
            external_dag_id='eas_landing_zone_dag_v2',
            external_task_id='end_task',
            execution_delta=timedelta(hours=0),
            timeout=7200)

    #Dummy operators to control parallelism 
    task_start = EmptyOperator(task_id='task_start')
    task_end = EmptyOperator(task_id='task_end')
    process_finish = EmptyOperator(task_id='process_finish')

    #To check existence of gold dataset and create if needed 
    check_for_gld_dataset = BigQueryCreateEmptyDatasetOperator(
                task_id='check_gld_dataset_{source}'.format(source=source_abbr),
                dataset_id=gld_dataset,
                bigquery_conn_id=bq_gcp_connector)

    #sql query to build gold layer table 
    transactions_gold_sql = '''  
        SELECT DISTINCT T.COMP as Company, T.ACCT as AccountNbr, NULL AS PaymentNumber, T.AP_DATA AS AP, T.CNV_AMT AS Amount, T.DR_CR as DC , T.INS_DAT AS TransDate, T.ACCT_PER as Period, T.DESCR as Description, NULL AS SuspStat, NULL AS MatchID, NULL AS SuspProcDate, NULL AS SuspMatch, T.STAT_IND as JEDetailStatus, T.SRCE as Source, T.BAS as Basis, T.REF_DATA as RefID, T.ENT_OPER as EntryOper, T.ENT_DAT as EntryDate, T.UPD_DAT as UpdateDate, T.APV_OPER as ApprovalOper, T.TRAN_ID as TranID, T.TR_DT_ID as TranDtlID, T.VND_NAM as PayeeName, T.VND_ID as PayeeCode, NULL AS NetPaymentAmount, T.CURR as OperCurrency, T.ORG_AMT as OrigAmount, T.EXCH_RT as ExchgRate, T.BU, T.DEPT, DIV, PROV, PURP, SPTYP, T.STATE_2 as STATE, ACT.ACCOUNT_NAME as AccountName, V.User_Def_Category as VMO
          from (select * from `{project}.{dataset}.{table_1}` ) T 
                  left join `{project}.{dataset}.t_eas_Vendor_Accounts_Lookup` act on t.acct = act.account_id
                  join (select Vendor_ID, trim (Vendor_name) as vendor_name, trim(User_Def_Category) as User_Def_Category, rnk_it
                               from (select *, rank() over (partition by vendor_id order by DATE(_PARTITIONTIME) desc) rnk_it                               
                                       from `{project}.{dataset}.{table_2}`  
                                     )a  where rnk_it = 1
                        ) as V ON T.VND_ID = V.Vendor_ID AND rnk_it = 1 AND upper(substr(User_Def_Category,0,3)) = 'VMO' '''.format(
        project=bq_target_project,
        dataset=refine_dataset,
        table_1=source_table_vd,
        table_2=source_table_vi,
        date="{{ds}}")

   
    destination_dataset_table = '{project}.{dataset}.{table}'.format(
        project=bq_target_project,table=source_table_vd,date="{{ ds_nodash }}",dataset=gld_dataset,source_abbr=source_abbr.lower())

    #Build gold layer table 
    transactions_gold_data = BigQueryOperator(task_id='Transactions_gld',
                                       sql=transactions_gold_sql,
                                       destination_dataset_table=destination_dataset_table,
                                       write_disposition='WRITE_TRUNCATE',
                                       create_disposition='CREATE_IF_NEEDED',
                                       gcp_conn_id=bq_gcp_connector,
                                       allow_large_results=True,
                                       use_legacy_sql=False)

    #Shipments gold layer table query
    gl_gold_sql = '''SELECT *,DATE(_PARTITIONTIME) as bq_load_date  FROM `{project}.{dataset}.{table}` 
                      where ACCT in ('66000','66100')   '''.format(
        project=bq_target_project,
        dataset=refine_dataset,
        table=source_table_vd,
        date="{{ds}}")


    destination_dataset_table = '{project}.{dataset}.{table}${date}'.format(
        project=bq_target_project,
        table='GL_shipments',
        date="{{ ds_nodash }}",
        dataset=gld_dataset,
        source_abbr=source_abbr.lower())

    #Shipments gold layer table data 
    GL_shipments_gold_data = BigQueryOperator(task_id='GL_shipments_gld',
                                       sql=gl_gold_sql,
                                       destination_dataset_table=destination_dataset_table,
                                       write_disposition='WRITE_TRUNCATE',
                                       create_disposition='CREATE_IF_NEEDED',
                                       gcp_conn_id=bq_gcp_connector,
                                       allow_large_results=True,
                                       use_legacy_sql=False,
                                       time_partitioning={"type": "DAY"})

    #Check if files available in l1_norm after flattening in landing job 
    #This is needed as flattening step has some account filters and possibility of no rows/files 
    files_availability_check = EASDataAvailabilityCheckOperator(task_id = 'files_presence_check_{source}'.format(source = source),
                                             source = source_abbr.lower(),
                                             table = 'Vendor_Data',
                                             config_file = confJSON,
                                             google_cloud_storage_conn_id=base_gcp_connector,
                                             l2_storage_location=l2_storage_location,
                                             target_gcs_bucket=base_bucket
                                             )

    #if no files, proceed to final step skipping the load operators 
    skip_or_process_decision = BranchPythonOperator(
        task_id = 'skip_or_process_decision',
        python_callable = lambda : 'task_start' if (int(Variable.get('eas_files_availability_eas')) == 1) else 'process_finish' ,
        do_xcom_push=False
    )

    #Processing both Vendor-Data and Vendor-Information files in parallel until Gold layer start 
    for items in source_table:
	
        list = items.split('.')
        item = list[0]
        schema_item = list[1]
        #task_id_to = 'load_data_{source}_{api_name}'.format(source=source_abbr, api_name=schema_item)
        eas_files_availability='eas_files_availability_{schema_item_br}'.format(schema_item_br=schema_item)
        print("eas_files_availability ", eas_files_availability)
        #print("lambda ",lambda x : 'process_finish' if (int(Variable.get(eas_files_availability)) == 0) else 'load_data_{source}_{api_name}'.format(source=source_abbr, api_name=schema_item))
        print(schema_item, " ramesh inside for - schema_item")					
                
        generate_schema = BQSchemaGenerationOperator(
			task_id = 'generate_schema_{source}_{table}'.format(source = source_abbr , table = schema_item ),
			project = project,
			config_file = confJSON,
			source = source_abbr,
			table = schema_item,
			target_gcs_bucket = base_bucket,
			schema_location = base_schema_location,
			google_cloud_storage_conn_id=base_gcp_connector
			)
			
        stage_files = EASStagingOperator(task_id = 'stage_files_{source}_{table}'.format(source = source , table = schema_item ),
                                                 source = source_abbr,
                                                 table = schema_item,
                                                 config_file = confJSON,
                                                 google_cloud_storage_conn_id=base_gcp_connector,
                                                 l2_storage_location=l2_storage_location,
                                                 staging_file_location=staging_file_location,
                                                 target_gcs_bucket=base_bucket
                                                 )
												 												 
        load_data_to_BQ = GoogleCloudStorageToBigQueryOperator(
                        task_id='load_data_{source}_{api_name}'.format(source=source_abbr,
                                                                       api_name=schema_item),
                        bucket=base_bucket,
                        source_objects=[
                        '{staging_file_location}{api_name}/{date}/staging_{source}_{api_name}*'.format(staging_file_location=staging_file_location,
                                                                                                       source = source,
                                                                                        api_name=schema_item,
                                                                                        date="{{ ds_nodash }}")],
                        destination_project_dataset_table='{project}.{dataset}.t_{source_abbr}_{table}${date}'.format(
                                          project=bq_target_project, dataset = refine_dataset, source_abbr=source_abbr.lower(),
                                          table=schema_item, date="{{ ds_nodash }}"),
                        #schema_fields=None,
                        schema_object=base_schema_location+'{api_name}/{date}/l1_schema_{source}_{api_name}.json'.format(
                                                                                              api_name=schema_item,
                                                                                              source = source,
                                                                                              date="{{ ds_nodash }}"),
                        source_format='CSV',
                        compression='NONE',
                        ignore_unknown_values=True,
                        allow_quoted_newlines =True,
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=1,
                        write_disposition='WRITE_TRUNCATE',
                        max_bad_records=0,
                        bigquery_conn_id=bq_gcp_connector,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        schema_update_options=['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION'],
                        schema_item=schema_item)
						
        create_dataset >> task_sensor_core >> files_availability_check >> skip_or_process_decision >> task_start >> generate_schema >> stage_files >> load_data_to_BQ >> task_end >> check_for_gld_dataset>> transactions_gold_data >> GL_shipments_gold_data >> process_finish
		