import logging
import datetime as dt
import calendar
import time
import json
import re
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_CurrentVersionOperator import CurrentVersionOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

ts = calendar.timegm(time.gmtime())
logging.info(ts)

#test me

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
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


sql_source_db = 'ContactManager'
sql_source_db_abbr = 'cm'
#sql_source_db_abbr2 = 'cm'
#ct_enable = True
base_file_location = folder_prefix + 'l1/gw/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/gw/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/gw/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
non_partition_list = ['abtl_abcontact','abtl_state','abtl_primaryphonetype','abtl_maritalstatus','abtl_addresstype',
                      'abtl_address','abtl_country','abtl_addressstatus_jmic']

#source_dag

with DAG(
        '{sql_source_db}_Group_Two_dag_v2'.format(sql_source_db=sql_source_db),
        schedule_interval= '0 13 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
		max_active_runs=1,
        default_args=default_dag_args) as dag:

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=sql_source_db,
                                 mode='SET')

    task_sensor = ExternalTaskSensorAsync(
                                    task_id='check_{center}_table_list'.format(center=sql_source_db.lower()),
                                    external_dag_id='Get_Tables_Lists',
                                    external_task_id='get_tablelist_{center}'.format(center=sql_source_db.lower()),
                                    execution_delta=timedelta(hours=1),
                                    # execution_date_fntimedelta(hours=5),
                                    timeout=7200)


    SQL_SOURCE_CT_CHECK = json.loads(Variable.get('SQL_SOURCE_CT_STATUS'))
    values = SQL_SOURCE_CT_CHECK[sql_source_db]
    values_split = values.split('.')
    ct_enable = values_split[0]
    if ct_enable.lower() == 'false':
        ct_enable = False
    else:
        ct_enable = True
    sql_source_db = values_split[1]

    ##Read the table list from the the system Variable.
    #Standardize format naming....


    source_target_table_list = '{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
    SOURCE_SCHEMA_TABLES = Variable.get(source_target_table_list).split(',')

    ##New variable created to load only 4 tables requested by Bryan ##
    src_target_table_list2 = '{sql_source_db_abbr}_table_list_2'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
    SOURCE_SCHEMA_TABLES_2 = Variable.get(src_target_table_list2).split(',')
    ## End##
    #This is a Dummy Operator for the flattening process..
    #normalize_source_build = EmptyOperator(task_id='flat_{sql_source_db_abbr}_build'.format(sql_source_db_abbr=sql_source_db_abbr.lower()))

    for table in SOURCE_SCHEMA_TABLES_2:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')

        temp_split = table_split[1].split('_')
        # temp_string = temp_split[1]
        # first_char = temp_string[0]
        # if re.match('[a-j]', first_char.lower()) == None:
        #     continue
        # else:
        #     pass
        # pass

        if table_split[0] == 'vw':
            continue

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}'.format(
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

        load_data = MsSqlToGoogleCloudStorageOperator(
            task_id='land_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                             schema=table_split[0].lower(),
                                                                             table=table_split[1]),
            sql='''SELECT *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [{sql_source_db}].[{schema}].[{table}];'''.format(sql_source_db=sql_source_db,schema=table_split[0],table=table_split[1]),
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
            google_cloud_bq_config={'project': project, 'dataset': dataset_name,
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

        # dataset name changes :
        create_currentversion_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_currentversion_dataset_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}_current'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix).replace('dbo_',''),
            bigquery_conn_id=bq_gcp_connector
        )

        if table_split[1] in non_partition_list:
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
                prefix_value=folder_prefix),
            build_out_source_table='{project}.{prefix_value}ref_{sql_source_db_abbr}_{schema}.{table}'.format(
                project=bq_target_project,
                sql_source_db_abbr=sql_source_db_abbr.lower(),
                table=table_split[1],
                schema=table_split[0].lower(),
                prefix_value=folder_prefix),
            source_task='get_primary_key_{schema}_{table}'.format(table=table_split[1], schema=table_split[0].lower()),
            sql_table=table_split[1],
            source=sql_source_db,
            mssql_conn_id=mssql_connection_target,
            change_tracking_check=ct_enable)
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
            time_partitioning = {"type": "DAY"},
            change_tracking_check=ct_enable)


        # gold_data = EmptyOperator(task_id='gold_data_{sql_source_db_abbr}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,table=table_split[1]))

        task_sensor >> load_data >> landing_audit_data >> create_dataset >> refine_data >> audit_data >> create_currentversion_dataset >> refine_current_version >> bit_set  # >>gold_data>>normalize_source_build]
