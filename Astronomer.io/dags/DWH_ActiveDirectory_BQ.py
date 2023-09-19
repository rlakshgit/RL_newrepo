import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CurrentVersionOperator import CurrentVersionOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLFinalAudit import SQLFinalAuditOperator

############################################################################################################
#                      MODIFICATION LOG 
############################################################################################################
#  NAME                   DATE                   DESCRIPTION                 
############################################################################################################
#  RAMESH L               2023-09-10             AIRFLOW 2 COMPATIBILITY CHANGES 
#  RAMESH L               2023-09-13             MIGRATION TO ASTRO
#
############################################################################################################
 
  

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


##
AIRFLOW_ENV = Variable.get('ENV')

trigger_object = 'triggers/inbound/dw_complete.json'



if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    trigger_bucket = 'jm_dev01_edl_lnd'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'


sql_source_db = 'DW_SOURCE'
sql_source_db_abbr = 'ad'
ct_enable = False
base_file_location = folder_prefix + 'l1/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)

#source_dag

with DAG(
        '{sql_source_db}_dag'.format(sql_source_db='ActiveDirectory'),
        schedule_interval= '0 13 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
		max_active_runs=1,
        default_args=default_dag_args) as dag:

    bit_set = CompletionOperator(task_id='set_source_bit',
                                   source='ActiveDirectory',
                                   mode='SET')

    file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id=bq_gcp_connector
                                                 , timeout=3 * 60 * 60
                                                 , poke_interval=10 * 60, deferrable = True)

    get_table_list = MsSqlToGoogleCloudStorageOperator(
                                                    task_id='get_tablelist_{center}'.format(center=sql_source_db.lower()),
                                                    sql='''SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM [{sql_source_db}].information_schema.tables 
                                                            where TABLE_NAME like 'AD%' or TABLE_NAME like 'v_AD%' '''.format(sql_source_db=sql_source_db),
                                                    bucket=base_bucket,
                                                    filename=base_file_location + 'l1_data_{}.json',
                                                    schema_filename=base_schema_location + 'l1_schema_{center}.json'.format(center=sql_source_db),
                                                    mssql_conn_id=mssql_connection_target,
                                                    google_cloud_storage_conn_id=base_gcp_connector,
                                                    airflow_var_set='{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
                                                    )

    ##Read the table list from the the system Variable.
    #Standardize format naming....

    source_target_table_list = '{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
    try:
        SOURCE_SCHEMA_TABLES = Variable.get(source_target_table_list).split(',')
    except:
        Variable.set(source_target_table_list,'test.test_load1,test.test_load2')
        SOURCE_SCHEMA_TABLES = Variable.get(source_target_table_list).split(',')
    table_list = []

    #This is a Dummy Operator for the flattening process..
    # normalize_source_build = EmptyOperator(task_id='flat_{sql_source_db_abbr}_build'.format(sql_source_db_abbr=sql_source_db_abbr.lower()))

    # Addded to fix the duplicated task_id
    table_split = SOURCE_SCHEMA_TABLES[0].split('.')
    table_split[0] = table_split[0].replace(' ', '')
    
    audit_sql = '''USE {source};
                    (select t.name TableName, i.rows sql_count
                    from sysobjects t, sysindexes i
                    where t.xtype = 'U' and i.id = t.id and i.indid in (0,1))
                    union 
                    (select 'v_ADView' as TableName, count(*) as sql_count from [DW_SOURCE].[bief_src].[v_ADView])'''.format(source=sql_source_db)

    final_audit = SQLFinalAuditOperator(task_id='final_audit_{sql_source}'.format(sql_source=sql_source_db),
                                        sql=audit_sql,
                                        table_list=table_list,
                                        destination_dataset='{prefix_value}ref_{sql_source_db_abbr}_{schema}_current'.format(
                                            prefix_value=folder_prefix,schema=table_split[0].lower(),
                                            sql_source_db_abbr=sql_source_db_abbr.lower()),
                                        destination_table='audit_results',
                                        project=bq_target_project,
                                        mssql_conn_id=mssql_connection_target,
                                        bigquery_conn_id=bq_gcp_connector,
                                        )

    for table in SOURCE_SCHEMA_TABLES:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')
        table_strp = table_split[1].replace('$', '')
        if table_split[0] == 'vw':
            continue
        table_list.append(table_split[1])

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{schema}_{table}'.format(schema=table_split[0].lower(), table=table_strp),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix),
            bigquery_conn_id=bq_gcp_connector,
            project_id=bq_target_project
            )

        # This must be pre-set, if you attempt to use a format with the file name you can not set a arguement for multiple files
        file_name_prep = '{location_base}{schema}/{table}/{date}/l1_data_{table}'.format(
            location_base=base_file_location,
            schema=table_split[0].lower(),
            table=table_strp, date="{{ ds_nodash }}")

        # Base file name for the audit file
        audit_file_name_prep = '{location_base}{date}/l1_audit_results_{schema}_{table}.json'.format(
            location_base=base_audit_location,
            schema=table_split[0].lower(),
            table=table_strp, date="{{ ds_nodash }}")

        # Base file name for the landed file
        landed_file_name = '{location_base}{schema}/{table}/'.format(
            location_base=base_file_location,
            schema=table_split[0].lower(),
            table=table_strp, date="{{ ds_nodash }}")

        # Base dataset name variable to reduce the clustering of information multiple attempts.
        dataset_name = '{prefix_value}ref_{sql_source_db_abbr}_{schema}'.format(
            sql_source_db_abbr=sql_source_db_abbr.lower(),
            schema=table_split[0].lower(),
            prefix_value=folder_prefix)

        load_data = MsSqlToGoogleCloudStorageOperator(
            task_id='land_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                             table=table_strp,
                                                                             schema=table_split[0].lower()),
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
                table=table_strp),
            mssql_conn_id=mssql_connection_target,
            google_cloud_storage_conn_id=base_gcp_connector,
            change_tracking_check=ct_enable,
            primary_key_check=True,
            google_cloud_bq_config={'project': project, 'dataset': dataset_name, 'table': table_split[1],
                                    'db': sql_source_db},
            metadata_filename='{location_base}{schema}/{table}/{date}/l1_metadata_{schema}_{table}.json'.format(
                location_base=base_file_location,
                schema=table_split[0].lower(),
                table=table_strp,
                date="{ds_nodash}")
        )

        landing_audit_data = SQLAuditOperator(
            task_id='landing_audit_health_{sql_source_db}_{schema}_{table}'.format(sql_source_db=sql_source_db,
                                                                                   schema=table_split[0],
                                                                                   table=table_strp),
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
            google_cloud_bq_config={'project': project, 'dataset': dataset_name,
                                    'table': table_split[1], 'db': sql_source_db,
                                    'source_abbr': sql_source_db_abbr,
                                    'schema': table_split[0]},
            check_landing_only=True)

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                               table=table_strp,
                                                                               schema=table_split[0].lower()),
            bucket=base_bucket,
            project_id=bq_target_project,
            source_objects=[
                '{location_base}{schema}/{table}/{date}/l1_data_{table}_*'.format(location_base=base_file_location,
                                                                                 destination_bucket=base_bucket,
                                                                                 schema=table_split[0].lower(),
                                                                                 table=table_strp,
                                                                                 date="{{ ds_nodash }}")],
            destination_project_dataset_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}.{table}${date}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_strp,
                schema=table_split[0].lower(),
                date="{{ ds_nodash }}",
                prefix_value=folder_prefix),
            schema_fields=[],
            schema_object='{schema_location}{schema}/{table}/l1_schema_{table}.json'.format(
                schema_location=base_schema_location,
                schema=table_split[
                    0].lower(),
                table=table_strp),
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
                                                                           schema=table_split[0], table=table_strp),
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
            task_id='check_for_currentversion_dataset_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}_current'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix).replace('dbo_',''),
            bigquery_conn_id=bq_gcp_connector
        )

        # noinspection PyDeprecation

        refine_current_version = CurrentVersionOperator(
            task_id='bq_build_current_version_{schema}_{table}'.format(table=table_split[1],
                                                                       schema=table_split[0].lower()),
            bigquery_conn_id=bq_gcp_connector,
            gcp_conn_id=bq_gcp_connector,
            use_legacy_sql=False,
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            destination_dataset_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}_current.{table}${date}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_split[1],
                schema=table_split[0].lower(),
                prefix_value=folder_prefix,
                date="{{ ds_nodash }}").replace('dbo_',''),
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
            time_partitioning = {"type": "DAY"})

        file_sensor >> get_table_list >> load_data >> landing_audit_data >> create_dataset >> refine_data >> audit_data >> create_currentversion_dataset >> refine_current_version >> final_audit >> bit_set  # >>gold_data>>normalize_source_build]
