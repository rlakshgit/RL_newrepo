####Libraries/Operators import
import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable



ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'rjha@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#####Initializing Variables ########
AIRFLOW_ENV = Variable.get('ENV')

trigger_object = 'triggers/inbound/dw_complete.json'

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    base_gcs_folder = 'DEV_l1'
    mssql_connection_target = 'zing_marketplace_nonprod_db'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    base_gcs_folder = 'B_QA_l1'
    mssql_connection_target = 'zing_marketplace_nonprod_db'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    base_gcs_folder = 'l1'
    mssql_connection_target = 'zing_marketplace_prod_db'

source = 'mp'
sql_source_db = 'marketplacedb'
sql_source_db_abbr = 'mp'
source_abbr = 'mp'
ct_enable = True

##Storage File locations
base_file_location = folder_prefix + 'l1/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)

metadata_filename = '{base_gcs_folder}/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)

refine_dataset = '{prefix}ref_zing_{source}'.format(prefix=folder_prefix,source=source)
audit_filename = '{base_gcs_folder}_audit/{source}/'.format(
            source=source_abbr.lower(),
            base_gcs_folder=base_gcs_folder)


################################################################################
# INSTANTIATE DAG TO RUN once
################################################################################

with DAG(
        'Zing_azure_marketplace_landing_dag',
        schedule_interval='0 10 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    # READ INI FILE FOR TABLE LIST table_list =
    # confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'gw', r'ini', r'bc_kimberlite_tables.ini')
    # with open(confFilePath, 'r') as configFile:
    #     SOURCE_SCHEMA_TABLES = json.load(configFile)##

    try:
        MARKET_TABLES = Variable.get('prioritized_mp_tables')
    except:
        Variable.set('prioritized_mp_tables','dbo.OrderDetails,dbo.ProductDetails,dbo.Country,dbo.Company')
        MARKET_TABLES = Variable.get('prioritized_mp_tables')


    MARKET_TABLES = MARKET_TABLES.split(',')
    SQL_SOURCE_CT_CHECK = json.loads(Variable.get('SQL_SOURCE_CT_STATUS'))
    values = SQL_SOURCE_CT_CHECK[sql_source_db]
    values_split = values.split('.')
    ct_enable = values_split[0]
    if ct_enable.lower() == 'false':
        ct_enable = False
    else:
        ct_enable = True
    sql_source_db = values_split[1]
    table_list = []
    
    end_task = EmptyOperator(task_id='end_task')
    complete_landing = EmptyOperator(task_id='complete_landing')

    for table in MARKET_TABLES:  # [:400]:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')

        if table_split[0] == 'vw':
            continue

        table_list.append(table_split[1])



        # This must be pre-set, if you attempt to use a format with the file name you can not set a arguement for multiple files
        file_name_prep = '{location_base}{schema}/{table}/{date}/l1_data_{table}'.format(
            location_base=base_file_location,
            schema=table_split[0].lower(),
            table=table_split[1], date="{{ ds_nodash }}",
            file_date=file_dt)

        # Base file name for the audit file
        audit_file_name_prep = '{location_base}{date}/l1_audit_results_{schema}_{table}.json'.format(
            location_base=base_audit_location,
            schema=table_split[0].lower(),
            table=table_split[1], date="{{ ds_nodash }}")

        # Base file name for the landed file
        landed_file_name = '{location_base}{schema}/{table}/'.format(
            location_base=base_file_location,
            schema=table_split[0].lower(),
            table=table_split[1], date="{{ ds_nodash }}")

        # Base dataset name variable to reduce the clustering of information multiple attempts.
        dataset_name = '{prefix_value}ref_zing_{sql_source_db_abbr}_{schema}'.format(
            sql_source_db_abbr=sql_source_db_abbr.lower(),
            schema=table_split[0].lower(),
            prefix_value=folder_prefix)

        land_data = MsSqlToGoogleCloudStorageOperator(
            task_id='land_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                             schema=table_split[0].lower(),
                                                                             table=table_split[1]),
            sql='''SELECT *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [{sql_source_db}].[{schema}].[{table}];'''.format(
                sql_source_db=sql_source_db,
                schema=table_split[0],
                table=table_split[1]),
            bucket=base_bucket,
            gzip=False,
            filename=file_name_prep + '_{}.json',
            schema_filename='{schema_location}{schema}/{table}/l1_schema_{table}.json'.format(
                schema_location=base_schema_location,
                schema=table_split[0].lower(),
                table=table_split[1]),
            mssql_conn_id=mssql_connection_target,
            google_cloud_storage_conn_id=base_gcp_connector,
            change_tracking_check=ct_enable,
            primary_key_check=True,
            google_cloud_bq_config={'project': bq_target_project, 'dataset': dataset_name, 'table': table_split[1],
                                    'db': sql_source_db},
            metadata_filename='{location_base}{schema}/{table}/{date}/l1_metadata_{schema}_{table}.json'.format(
                location_base=base_file_location,
                schema=table_split[0].lower(),
                table=table_split[1],
                date="{ds_nodash}")
        )

           
        land_data >> complete_landing>>end_task