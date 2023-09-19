import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_PLEcom_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_CurrentVersionOperator import CurrentVersionOperator
from airflow.models import Variable
import json
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLFinalAudit_withSchema import SQLFinalAuditOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_MSTeams_webhook_operator import MSTeamsWebhookOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

# test me

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,4,22),
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
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mssql_connection_target = 'dbPLEcom_STAGE_RO'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_qa01_edl_lnd'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'dbPLEcom_STAGE_RO'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    trigger_bucket = 'jm_prod_edl_lnd'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'

sql_source_db = 'PLEcom'
sql_source_db_abbr = 'plecom'
ct_enable = True
base_file_location = folder_prefix + 'l1/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)

##
# source_dag

with DAG(
        dag_id='{sql_source_db}_dag'.format(sql_source_db=sql_source_db),
        schedule_interval='0 12 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=sql_source_db,
                                 mode='SET')

    file_sensor = GCSObjectExistenceSensor(task_id='gcs_trigger_sensor'
                                                 , bucket=trigger_bucket
                                                 , object=trigger_object
                                                 , google_cloud_conn_id=bq_gcp_connector, deferrable = True)

    SQL_SOURCE_CT_CHECK = json.loads(Variable.get('SQL_SOURCE_CT_STATUS'))
    values = SQL_SOURCE_CT_CHECK[sql_source_db]
    values_split = values.split('.')
    ct_enable = values_split[0]
    if ct_enable.lower() == 'false':
        ct_enable = False
    else:
        ct_enable = True
    sql_source_db = values_split[1]

    """get_table_list = MsSqlToGoogleCloudStorageOperator(
                                                    task_id='get_tablelist_{center}'.format(center=sql_source_db.lower()),
                                                    sql='''SELECT DISTINCT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM [{sql_source_db}].information_schema.tables'''.format(sql_source_db=sql_source_db),
                                                    bucket=base_bucket,
                                                    filename=base_file_location + 'l1_data_{}.json',
                                                    schema_filename=base_schema_location + 'l1_schema_{center}.json'.format(center=sql_source_db),
                                                    mssql_conn_id=mssql_connection_target,
                                                    google_cloud_storage_conn_id=base_gcp_connector,
                                                    airflow_var_set='{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
                                                    )"""

    ##Read the table list from the the system Variable.
    # Standardize format naming....

    source_target_table_list = '{sql_source_db_abbr}_table_list'.format(sql_source_db_abbr=sql_source_db_abbr.lower())
    try:
        SOURCE_SCHEMA_TABLES = Variable.get(source_target_table_list).split(',')
    except:
        Variable.set(source_target_table_list,'JewelersCut.AppraisalFile, JewelersCut.Country, JewelersCut.IncompleteReason, JewelersCut.Item, JewelersCut.Jeweler, JewelersCut.MaterialDetail, JewelersCut.Payment, JewelersCut.PaymentMethod, JewelersCut.PaymentRule, JewelersCut.Program, JewelersCut.ProgramDescription, JewelersCut.ProgramDetail, JewelersCut.ProgramStatus, JewelersCut.QuoteResult, JewelersCut.State, JewelersCut.Submission, JewelersCut.SubmissionRule, JewelersCut.TransactionDescription, Partner.t_ErrorCodeLookup, Partner.t_Partner, Partner.t_PartnerProgram, Partner.t_PartnerProgramLink, Partner.t_PartnerPurchaseKey, Partner.t_UserDisplayError, PolicyService.lkup_SubmissionQueueStatus, PolicyService.SubmissionQueue, Portal.Notification, Portal.NotificationAction, Portal.NotificationActionType, Portal.NotificationType, QuoteApp.t_Address, QuoteApp.t_AgencyExpressPartner, QuoteApp.t_AgencyExpressPartnerSource, QuoteApp.t_Alarm, QuoteApp.t_AlarmUsage, QuoteApp.t_Application, QuoteApp.t_ApplicationItemJewelerPaymentTracker, QuoteApp.t_ApplicationOptIn, QuoteApp.t_ApplicationPayment, QuoteApp.t_ApplicationQuote, QuoteApp.t_ApplicationSource, QuoteApp.t_Brand, QuoteApp.t_ContactPreference, QuoteApp.t_Conviction, QuoteApp.t_Country, QuoteApp.t_DistributionSource, QuoteApp.t_DistributionSourceSubType, QuoteApp.t_Gender, QuoteApp.t_JewelerItemPaymentRule, QuoteApp.t_JewelerNoPayReason, QuoteApp.t_JewelerPaymentRule, QuoteApp.t_Jewelry, QuoteApp.t_JewelryCrossSell, QuoteApp.t_JewelryItemSubTypeInformation, QuoteApp.t_JewelryPurchaseInfo, QuoteApp.t_JewelrySubType, QuoteApp.t_JewelrySubTypeBrandInformation, QuoteApp.t_LossHistoryEvent, QuoteApp.t_Month, QuoteApp.t_OptIn, QuoteApp.t_PaymentMethod, QuoteApp.t_Person, QuoteApp.t_PriorLoss, QuoteApp.t_QuotedJewelryItem, QuoteApp.t_ReferringJeweler, QuoteApp.t_Relationship, QuoteApp.t_Safe, QuoteApp.t_SafeWeightClass, QuoteApp.t_SavedQuote, QuoteApp.t_SavedQuoteItem, QuoteApp.t_SecurityPresence, QuoteApp.t_StateProvince, QuoteApp.t_StorageLocation, QuoteApp.t_Travel, QuoteApp.t_TravelPrecaution, QuoteApp.t_UnderwritingInformation, QuoteApp.t_Wearing, sqlpublish._OneTimeScripts')
        SOURCE_SCHEMA_TABLES = Variable.get(source_target_table_list).split(',')

    # This is a Dummy Operator for the flattening process..
    # normalize_source_build = EmptyOperator(task_id='flat_{sql_source_db_abbr}_build'.format(sql_source_db_abbr=sql_source_db_abbr.lower()))

    table_list = []

    audit_sql = '''USE {source};

                                SELECT
                                SCHEMA_NAME(sOBJ.schema_id) as  [schema], sOBJ.name AS [TableName], SUM(sPTN.Rows) AS [sql_count]
                                FROM 
                                      sys.objects AS sOBJ
                                      INNER JOIN sys.partitions AS sPTN
                                            ON sOBJ.object_id = sPTN.object_id
                                WHERE
                                      sOBJ.type = 'U'
                                      AND sOBJ.is_ms_shipped = 0x0
                                      AND index_id < 2 -- 0:Heap, 1:Clustered
                                GROUP BY 
                                      sOBJ.schema_id
                                      , sOBJ.name'''.format(
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
        bigquery_conn_id=bq_gcp_connector,
        )

    send_alerts = MSTeamsWebhookOperator(
        task_id='send_alerts',
        http_conn_id='msteams-webhook-url',
        message='Schema changed',
        subtitle="From {sql_source_db}_dag".format(
            sql_source_db=sql_source_db),
        theme_color="00FF00",
        button_text="Go to failed dag",
        button_url="https://qc27aec3d8f964692p-tp.appspot.com/admin/airflow/tree?dag_id={dag_id}".format(
            dag_id=dag.dag_id))

    for table in SOURCE_SCHEMA_TABLES:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')
        table_strp = table_split[1].replace('$', '')
        if table_split[0] == 'vw':
            continue


        table_list.append(table)


        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{schema}_{table}'.format(schema=table_split[0].lower(), table=table_strp),
            dataset_id='{prefix_value}ref_{sql_source_db_abbr}_{schema}'.format(
                sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                prefix_value=folder_prefix),
            bigquery_conn_id=bq_gcp_connector
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

        if table_strp == 't_SavedQuote':
            load_data = MsSqlToGoogleCloudStorageOperator(
                task_id='land_data_{sql_source_db_abbr}_{schema}_{table}'.format(sql_source_db_abbr=sql_source_db_abbr,
                                                                                 table=table_strp,
                                                                                 schema=table_split[0].lower()),
                sql='''SELECT SavedQuote_ID,GoogleAnalyticsClientID,PostalCode,County,State,CountryID,cast(Timestamp as datetime2(7)) as Timestamp,DistributionSourceId,DistributionSourceSubTypeId,AgencyExpressPartner_ID,UserId,ApplicationSourceId,GuidewireProducerCode,ReferringJewelerId,ReferralCode,ExternalApplicationKey,Address1,Address2,City,FirstName,LastName,PhoneNumber,EmailAddress,SessionId,JewelerProgramName,OrderNumber,Obfuscated, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [{sql_source_db}].[{schema}].[{table}];'''.format(
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
                google_cloud_bq_config={'project': bq_target_project, 'dataset': dataset_name, 'table': table_split[1],
                                        'db': sql_source_db},
                metadata_filename='{location_base}{schema}/{table}/{date}/l1_metadata_{schema}_{table}.json'.format(
                    location_base=base_file_location,
                    schema=table_split[0].lower(),
                    table=table_strp,
                    date="{ds_nodash}"),
                table=table_strp)

        else:
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
                google_cloud_bq_config={'project': bq_target_project, 'dataset': dataset_name, 'table': table_split[1],
                                        'db': sql_source_db},
                metadata_filename='{location_base}{schema}/{table}/{date}/l1_metadata_{schema}_{table}.json'.format(
                    location_base=base_file_location,
                    schema=table_split[0].lower(),
                    table=table_strp,
                    date="{ds_nodash}"),
                table=table_strp
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
            schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
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
            sql_table=table_split[1],
            source=sql_source_db,
            mssql_conn_id=mssql_connection_target,
            change_tracking_check=ct_enable,
            time_partitioning = {"type": "DAY"})

        delete_partition_date = '{{ macros.ds_add(ds,-2) }}'
        delete_sql = "Delete  from `{project}.{dataset}.{table}` where bq_load_date <= '{delete_date}'".format(
            project=bq_target_project,
            dataset='{prefix_value}ref_{sql_source_db_abbr}_{schema}_current'.format(
                    sql_source_db_abbr=sql_source_db_abbr.lower(), schema=table_split[0].lower(),
                    prefix_value=folder_prefix).replace('dbo_',''),
            table=table_split[1],
            delete_date=delete_partition_date)

        ##Keep 2 days snapshot in BQ current layer and deleting the older ones
        delete_previous_partition_from_tables = BigQueryOperator(
            task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_split[1]),
            sql=delete_sql,
            gcp_conn_id=bq_gcp_connector,
            use_legacy_sql=False)
        
        file_sensor >> load_data >> landing_audit_data >> create_dataset >> refine_data >> audit_data  >> create_currentversion_dataset >> delete_previous_partition_from_tables >> refine_current_version >> final_audit >> bit_set 
        
        alert_trigger = Variable.get('send_alert')

        if alert_trigger == 'True':
            bit_set  >> send_alerts
        

       
    