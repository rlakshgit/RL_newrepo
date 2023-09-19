import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from plugins.operators.jm_CurrentVersionOperator import CurrentVersionOperator
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
    'start_date': datetime(2021,4,22),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

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


sql_source_db = 'PolicyCenter'
sql_source_db_abbr = 'pc'
ct_enable = True
base_file_location = folder_prefix + 'l1/gw/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_audit_location = folder_prefix + 'l1_audit/gw/{sql_source_db_abbr}/'.format(sql_source_db_abbr=sql_source_db_abbr)
base_schema_location = folder_prefix + 'l1_schema/gw/{sql_source_db_abbr}/'.format(
    sql_source_db_abbr=sql_source_db_abbr)
non_partition_list = ['pc_history']

# source_dag##

with DAG(
        '{sql_source_db}_Prioritized_Tables'.format(sql_source_db=sql_source_db),
        schedule_interval='0 12 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    # READ INI FILE FOR TABLE LIST table_list =
    # confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'gw', r'ini', r'bc_kimberlite_tables.ini')
    # with open(confFilePath, 'r') as configFile:
    #     SOURCE_SCHEMA_TABLES = json.load(configFile)##

    try:
        KIMBERLITE_PC_TABLES = Variable.get('prioritized_pc_tables')
    except:
        Variable.set('prioritized_pc_tables','dbo.pctl_floodzone_jmic,dbo.pctl_grouptype,dbo.pctl_clarityenhanced_jmic_pl,\
                     dbo.pctl_jpadamagetype_jm,dbo.pctl_pearltype_jmic,dbo.pctl_inventorytype_jm,dbo.pctl_cut_jmic_pl,\
                     dbo.pctl_jpaartacquisition_jm,dbo.pctl_jwlryfeatdetail_jmic_pl,dbo.pctl_jpaarticlesafestatus_jm,\
                     dbo.pctl_jpainsuredbyothercomp_jm,dbo.pctl_jpacost_jm,dbo.pctl_publicutilities_jmic,dbo.pctl_earthquakezone_jmic,\
                     dbo.pctl_firmindicator_jmic,dbo.pctl_limittovalueprcnt_jmic,dbo.pctl_jpavaluationtype_jm,dbo.pctl_billingmethod,\
                     dbo.pctl_bopbuildingeffgrade_jmic,dbo.pcx_ilmonetimecredit_jmic_v2,dbo.pcx_ilmstkcededpremtrans_jmic,\
                     dbo.pcx_ilmsublinecededpremiumjmic,dbo.pcx_ilmsubstkcededpremiumjmic,dbo.pcx_jewelrystockexcl_jmic,\
                     dbo.pctl_watchbrands_jm,dbo.pcx_jewelrystockcond_jmic,dbo.pcx_ilmsublncededpremtran_jmic,\
                     dbo.pcx_ilmsubstkcededpremtranjmic,dbo.pcx_ilmstockcededpremiumjmic,dbo.pc_acctholderedge,\
                     dbo.pcx_bopundinformation_jmic,dbo.pcx_floodinformation_JMIC,dbo.pcx_ilmcededpremiumjmic,\
                     dbo.pcx_bopriskinformation_JMIC,dbo.pcx_ilmcededpremiumtrans_jmic,dbo.pcx_watchbrandcontainer_jm,dbo.pc_organization,\
                     dbo.pcx_jewelrystockpropcat_jmic,dbo.pctl_bopconstructiontype,dbo.pctl_gemcert_jmic_pl,dbo.pctl_addressstatus_jmic,\
                     dbo.pctl_addresstype,dbo.pctl_chargepattern,dbo.pctl_currency,dbo.pctl_gendertype,dbo.pctl_buildingclass_jmic,\
                     dbo.pctl_cancellationsource,dbo.pctl_boppresafersposition_jmic,dbo.pctl_bopcoastal_jmic,\
                     dbo.pctl_ilmlocationratedas_jmic,dbo.pctl_jewelryitemstyle_jmic_pl,dbo.pctl_buildingimprtype,dbo.pctl_contact,\
                     dbo.pctl_country,dbo.pctl_bopcoastalzones_jmic,dbo.pctl_accountcontactrole,dbo.pctl_classcodetype_jmic_pl,\
                     dbo.pctl_apptakenby_jmic_pl,dbo.pctl_accountstatus,dbo.pctl_featuretype_jmic,dbo.pctl_brandtype_jmic_pl,\
                     dbo.pctl_boplocationtype_jmic,dbo.pctl_fireprotectclass,dbo.pctl_jpaitemtype_jm,dbo.pctl_reasoncode,\
                     dbo.pctl_prerenewaldirection,dbo.pctl_riagreement,dbo.pcx_ilmsubloc_jmic,dbo.pctl_wherestored_jmic,\
                     dbo.pcx_jpatransaction_jm,dbo.pctl_rewritetype,dbo.pcx_bopcovcededpremium,dbo.pctl_nottakencode_jmic,\
                     dbo.pctl_jpamaterialtype_jm,dbo.pctl_windorhaildedpercent_jmic,dbo.pcx_jpalocation_jm,dbo.pctl_valuationtype_jmic_pl,\
                     dbo.pctl_policychangerea_jmic_pl,dbo.pcx_jpacededpremium_jm,dbo.pctl_onetimecreditreason_jmic,dbo.pctl_rateamounttype,\
                     dbo.pctl_sprinklered,dbo.pcx_bopsubloccov_jmic,dbo.pctl_renewalcode,dbo.pctl_reinstatecode,dbo.pctl_policyperiodstatus,\
                     dbo.pcx_boplocationcond_jmic,dbo.pctl_policycontactrole,dbo.pcx_bopsubloc_jmic,dbo.pcx_ilmsubloccov_jmic,\
                     dbo.pcx_bopcovcededpremtransaction,dbo.pcx_bopsublinecov_jmic,dbo.pcx_boponetimecredit_jmic_v2,dbo.pctl_policyline,\
                     dbo.pcx_bopbuildingcededpremtrans,dbo.pctl_segment,dbo.pctl_jpaitembrand_jm,dbo.pctl_nonrenewalcode,\
                     dbo.pctl_uwcompanycode,dbo.pctl_personalarticle_jm,dbo.pctl_jpastoragetype_jm,dbo.pctl_preferredmethodcomm_jmic,\
                     dbo.pctl_primaryphonetype,dbo.pcx_bopsubline_jmic,dbo.pcx_jpacededpremiumtrans_jm,dbo.pctl_specifiedcarrier_jmic,\
                     dbo.pctl_jewelrystockpctype_jmic,dbo.pctl_jpaitemgendertype_jm,dbo.pctl_state,dbo.pctl_jurisdiction,\
                     dbo.pctl_ricoveragegrouptype,dbo.pctl_source_jmic_pl,dbo.pctl_stones_jmic_pl,dbo.pctl_mounting_jmic_pl,\
                     dbo.pctl_jpaitemsubtype_jm,dbo.pctl_quotetype,dbo.pctl_job,dbo.pctl_jpaitemstyle_jm,dbo.pctl_uwcompanystatus,\
                     dbo.pcx_ilmlocationexcl_jmic,dbo.pcx_bopbuildingcededpremium,dbo.pcx_ilmsubstock_jmic,dbo.pcx_commonbuilding_jmic,\
                     dbo.pcx_jpacost_jm,dbo.pcx_ilmlocationcond_jmic,dbo.pcx_ilmsubstockcov_jmic,dbo.pcx_jeweler_jm,\
                     dbo.pcx_commonlocation_jmic,dbo.pcx_ilmlocationcov_jmic,dbo.pcx_ilmsubline_jmic,dbo.pcx_ilmlocation_jmic,\
                     dbo.pcx_jewelrystock_jmic,dbo.pcx_ilmsublinecov_jmic,dbo.pcx_ilmtransaction_jmic,dbo.pcx_boplocationexcl_jmic,\
                     dbo.pcx_jmpersonallinecov,dbo.pcx_ilmlinecov_jmic,dbo.pcx_ilmcost_jmic,dbo.pcx_jewelrystockcov_jmic,\
                     dbo.pcx_jmtransaction,dbo.pcx_jewelryitem_jmic_pl,dbo.pcx_JwlryFeatDetail_JMIC_PL,dbo.pc_uwcompany,\
                     dbo.pc_questionlookup,dbo.pc_group,dbo.pcx_umbcededpremiumtrans_jmic,dbo.pcx_personalarticlecov_jm,\
                     dbo.pcx_umbtransaction_jmic,dbo.pc_bopclasscode,dbo.pcx_personalartcllinecov_jm,dbo.pc_reinsuranceagreement,\
                     dbo.pc_ricoveragegroup,dbo.pcx_umbcededpremiumjmic,dbo.pc_producercode,dbo.pc_user,dbo.pc_groupproducercode,\
                     dbo.pcx_umbrellalinecov_jmic,dbo.pc_contactaddress,dbo.pcx_personalarticle_jm,dbo.pc_bopcededpremiumtransaction,\
                     dbo.pcx_plcededpremiumtrans_jmic,dbo.pc_bopcededpremium,dbo.pcx_umbcost_jmic,dbo.pc_territorycode,dbo.pc_building,\
                     dbo.pcx_cost_jmic,dbo.pcx_policystatus_jmic,dbo.pc_account,dbo.pc_boplocation,dbo.pc_policyterm,dbo.pc_policy,\
                     dbo.pc_bopbuilding,dbo.pc_buildingimpr,dbo.pc_accountcontactrole,dbo.pc_paymentplansummary,dbo.pc_accountcontact,\
                     dbo.pc_bopbuildingcov,dbo.pc_job,dbo.pc_policyline,dbo.pc_boptransaction,dbo.pc_bopcost,dbo.pc_businessownerscov,\
                     dbo.pc_effectivedatedfields,dbo.pc_policylocation,dbo.pcx_jwryitemcov_jmic_pl,dbo.pcx_plcededpremiumjmic,dbo.pc_address,\
                     dbo.pc_locationanswer,dbo.pc_boplocationcov,dbo.pc_policyperiod,dbo.pcx_JwlryFeature_JMIC_PL,dbo.pc_policycontactrole,\
                     dbo.pc_contact,dbo.pctl_uwreviewstatus_jm,dbo.pcx_jwlrymodjmicpl,dbo.pctl_alarm_jmic_pl,dbo.pc_activitypattern,\
                     dbo.pctl_activitystatus,dbo.pc_activity,dbo.pctl_terroracceptreject_jmic,dbo.pc_jobgroup,dbo.pctl_jobgroup,\
                     dbo.pctl_bindoption,dbo.pc_jobuserroleassign,dbo.pc_policyuserroleassign,dbo.pc_history,dbo.pctl_nvcrdtcancelreason_jmic,\
                     dbo.pctl_customhistorytype,dbo.pc_userrole,\
                     dbo.pcx_NVCreditCancelReason_JMIC,dbo.pctl_businesstype,dbo.pctl_producerstatus,dbo.pc_role,dbo.pc_userproducercode')
        KIMBERLITE_PC_TABLES = Variable.get('prioritized_pc_tables')


    KIMBERLITE_PC_TABLES = KIMBERLITE_PC_TABLES.split(',')
    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='PolicyCenter_Prioritized_Tables',
                                 mode='SET')
    task_sensor = ExternalTaskSensorAsync(
                                    task_id='check_{center}_table_list'.format(center=sql_source_db.lower()),
                                    external_dag_id='Get_Tables_Lists',
                                    external_task_id='get_tablelist_{center}'.format(center=sql_source_db.lower()),
                                    execution_delta=timedelta(hours=0),
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
        bigquery_conn_id=bq_gcp_connector,
        )

    for table in KIMBERLITE_PC_TABLES:  # [:400]:
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        table_split = table.split('.')
        table_split[0] = table_split[0].replace(' ', '')

        if table_split[0] == 'vw':
            continue
        table_list.append(table_split[1])
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
        if table.replace('dbo.', '').startswith('pctl_') or table.replace('dbo.', '') in non_partition_list:
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

        task_sensor >> load_data >> landing_audit_data >> create_dataset >> refine_data >> audit_data >> create_currentversion_dataset >> refine_current_version >> final_audit >> bit_set  # >>gold_data>>normalize_source_build]