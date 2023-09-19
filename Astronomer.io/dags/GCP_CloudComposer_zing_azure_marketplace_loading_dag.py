import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from plugins.operators.jm_CurrentVersionAzureOperator import CurrentVersionOperator
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_SQLFinalAudit import SQLFinalAuditOperator

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
    mssql_connection_target = 'zing_marketplace_nonprod_db'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'zing_marketplace_nonprod_db'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'zing_marketplace_prod_db'


sql_source_db = 'marketplacedb'
sql_source_db_abbr = 'mp'
ct_enable = True
base_file_location = folder_prefix + 'l1/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)

# source_dag##

with DAG(
        'Zing_azure_marketplace_loading_dag',
        schedule_interval='0 11 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    # READ INI FILE FOR TABLE LIST table_list =
    # confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'gw', r'ini', r'bc_kimberlite_tables.ini')
    # with open(confFilePath, 'r') as configFile:
    #     SOURCE_SCHEMA_TABLES = json.load(configFile)##
    
    task_sensor_core = ExternalTaskSensorAsync(
        task_id='check_{source}_landing_task'.format(source=sql_source_db_abbr),
        external_dag_id='Zing_azure_marketplace_landing_dag',
        external_task_id='end_task',
        execution_delta=timedelta(minutes=60),
        timeout=7200)
    
    try:
        MARKET_TABLES = Variable.get('prioritized_mp_tables')
    except:
        Variable.set('prioritized_mp_tables','')
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

    audit_sql = '''USE {source};

                                                                select t.name TableName, i.rows sql_count
                                                                from sysobjects t, sysindexes i
                                                                where t.xtype = 'U' and i.id = t.id and i.indid in (0,1)
                                                                order by TableName;'''.format(
        source=sql_source_db)

    final_audit = SQLFinalAuditOperator(
        task_id='final_audit_{sql_source}'.format(sql_source=sql_source_db),
        sql=audit_sql,
        table_list=table_list,
        destination_dataset='{prefix_value}ref_{sql_source_db_abbr}_current'.format(
            prefix_value=folder_prefix,
            sql_source_db_abbr=sql_source_db_abbr.lower()),
        destination_table='audit_results',
        project=bq_target_project,
        mssql_conn_id=mssql_connection_target,
        bigquery_conn_id=bq_gcp_connector
        )

    for table in MARKET_TABLES:  # [:400]:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')

        if table_split[0] == 'vw':
            continue

        table_list.append(table_split[1])

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            dataset_id='{prefix_value}ref_zing_{sql_source_db_abbr}_{schema}'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix),
            bigquery_conn_id=bq_gcp_connector
        )

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
        dataset_name = '{prefix_value}ref_{sql_source_db_abbr}_{schema}'.format(
            sql_source_db_abbr=sql_source_db_abbr.lower(),
            schema=table_split[0].lower(),
            prefix_value=folder_prefix)

        landing_audit_data = SQLAuditOperator(
            task_id='landing_audit_health_{sql_source_db}_{schema}_{table}'.format(sql_source_db=sql_source_db,
                                                                                   schema=table_split[0],
                                                                                   table=table_split[1]),
            filename=audit_file_name_prep,
            landed_filename=landed_file_name,
            sql='SELECT count(*) as count FROM [{sql_source_db}].[{schema}].[{table}];'.format(
                sql_source_db=sql_source_db,
                schema=table_split[0],
                table=table_split[1]),
            bucket=base_bucket,
            mssql_conn_id=mssql_connection_target,
            google_cloud_storage_conn_id=base_gcp_connector,
            change_tracking_check=ct_enable,
            primary_key_check=True,
            google_cloud_bq_config={'project': bq_target_project, 'dataset': dataset_name,
                                    'table': table_split[1], 'db': sql_source_db,
                                    'source_abbr': sql_source_db_abbr,
                                    'schema': table_split[0]},
            check_landing_only=True)

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                               schema=table_split[0].lower(),
                                                                               table=table_split[1]),
            bucket=base_bucket,
            source_objects=[
                '{location_base}{schema}/{table}/{date}/l1_data_{table}_*'.format(location_base=base_file_location,
                                                                                  destination_bucket=base_bucket,
                                                                                  schema=table_split[0].lower(),
                                                                                  table=table_split[1],
                                                                                  date="{{ ds_nodash }}")],
            destination_project_dataset_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}.{table}${date}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_split[1],
                schema=table_split[0].lower(),
                date="{{ ds_nodash }}",
                prefix_value=folder_prefix),
            schema_fields=[],
            schema_object='{schema_location}{schema}/{table}/l1_schema_{table}.json'.format(
                schema_location=base_schema_location,
                schema=table_split[
                    0].lower(),
                table=table_split[1]),
            source_format='NEWLINE_DELIMITED_JSON',
            compression='NONE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=0,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=0,
            bigquery_conn_id=bq_gcp_connector,
            google_cloud_storage_conn_id=base_gcp_connector,
            schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
            src_fmt_configs={},
            # autodetect=True
            )

        audit_data = SQLAuditOperator(
            task_id='audit_health_{sql_source_db}_{schema}_{table}'.format(sql_source_db=sql_source_db,
                                                                           schema=table_split[0], table=table_split[1]),
            filename=audit_file_name_prep,
            landed_filename=landed_file_name,
            sql='SELECT count(*) as count FROM [{sql_source_db}].[{schema}].[{table}];'.format(
                sql_source_db=sql_source_db, schema=table_split[0], table=table_split[1]),
            bucket=base_bucket,
            mssql_conn_id=mssql_connection_target,
            google_cloud_storage_conn_id=base_gcp_connector,
            change_tracking_check=ct_enable,
            primary_key_check=True,
            google_cloud_bq_config={'project': bq_target_project, 'dataset': dataset_name,
                                    'table': table_split[1], 'db': sql_source_db,
                                    'source_abbr': sql_source_db_abbr,
                                    'schema': table_split[0]})
        create_currentversion_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_currentversion_dataset_{schema}_{table}'.format(table=table_split[1],
                                                                               schema=table_split[0].lower()),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}_current'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix).replace('dbo_', ''),
            bigquery_conn_id=bq_gcp_connector
        )

        # noinspection PyDeprecation
        if table.replace('dbo.', '').startswith('bctl_'):
            refine_current_version = CurrentVersionOperator(
                task_id='bq_build_current_version_{schema}_{table}'.format(table=table_split[1],
                                                                           schema=table_split[0].lower()),
                bigquery_conn_id=bq_gcp_connector,
                gcp_conn_id=bq_gcp_connector,
                use_legacy_sql=False,
                write_disposition='WRITE_TRUNCATE',
                allow_large_results=True,
                destination_dataset_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_current.{table}'.format(
                    project=bq_target_project,
                    sql_source_db_abbr=sql_source_db_abbr.lower(),
                    table=table_split[1],
                    #schema=table_split[0].lower(),
                    prefix_value=folder_prefix
                    ).replace('dbo_', ''),
                build_out_source_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}.{table}'.format(
                    project=bq_target_project,
                    sql_source_db_abbr=sql_source_db_abbr.lower(),
                    table=table_split[1],
                    schema=table_split[0].lower(),
                    prefix_value=folder_prefix),
                source_task='get_primary_key_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
                #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                sql_table=table_split[1],
                source=sql_source_db,
                mssql_conn_id=mssql_connection_target,
                change_tracking_check=ct_enable,
                #time_partitioning={"type": "DAY"}
                )
        else:
            refine_current_version = CurrentVersionOperator(
            task_id='bq_build_current_version_{schema}_{table}'.format(table=table_split[1],
                                                                       schema=table_split[0].lower()),
            bigquery_conn_id=bq_gcp_connector,
            gcp_conn_id=bq_gcp_connector,
            use_legacy_sql=False,
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            destination_dataset_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_current.{table}${date}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_split[1],
                #schema=table_split[0].lower(),
                prefix_value=folder_prefix,
                date="{{ ds_nodash }}").replace('dbo_', ''),
            build_out_source_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}.{table}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_split[1],
                schema=table_split[0].lower(),
                prefix_value=folder_prefix),
            source_task='get_primary_key_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
            sql_table=table_split[1],
            source=sql_source_db,
            mssql_conn_id=mssql_connection_target,
            time_partitioning={"type": "DAY"},
            change_tracking_check=ct_enable)

        task_sensor_core >> landing_audit_data >> create_dataset >> refine_data >> audit_data >> create_currentversion_dataset >> refine_current_version >> final_audit # >>gold_data>>normalize_source_build]