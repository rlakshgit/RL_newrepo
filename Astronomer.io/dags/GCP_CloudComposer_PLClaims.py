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
from plugins.operators.jm_PLClaims_to_gcs import plclaimsGraphQLToGCS
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_PLClaimsAuditOperator import APIAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')

source = 'PLClaims'
source_abbr = 'plclaims'

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_'
    api_connection_id = 'PLClaimsGraphQLID_DEV'
    access_connection_id = 'PLClaimsGraphQLAccessID_DEV'
elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_'
    api_connection_id = 'PLClaimsGraphQLID_DEV'
    access_connection_id = 'PLClaimsGraphQLAccessID_DEV'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    base_gcs_folder = ''
    api_connection_id = 'PLClaimsGraphQLID_PROD'
    access_connection_id = 'PLClaimsGraphQLAccessID_PROD'

base_file_location = '{base}l1/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)
base_normalized_location = '{base}l1_norm/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)
base_schema_location = '{base}l1_schema/{source_abbr}/'.format(base=base_gcs_folder, source_abbr=source_abbr)

refine_dataset = '{prefix}ref_{source}'.format(prefix=base_gcs_folder,
                                               source=source_abbr.lower())
audit_filename = '{base_gcs_folder}l1_audit/{source}'.format(source=source_abbr.lower(),
                                                             base_gcs_folder=base_gcs_folder)
metadata_filename = '{base_gcs_folder}l1/{source}'.format(source=source_abbr.lower(),
                                                          base_gcs_folder=base_gcs_folder)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'mgiddings@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'PLClaims_DAIS_dag',
        schedule_interval='0 6 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    try:
        SOURCE_LIST = Variable.get('PLClaimsQueries').replace(']', '').replace('[', '').replace("'", '').replace(' ',
                                                                                                                 '').split(
            ',')
    except:
        Variable.set('PLClaimsQueries',
                     'claimByExternalId,claimsByDateRange,claimSyncByClaimExternalId,syncLogsByDateRange')
        SOURCE_LIST = Variable.get('PLClaimsQueries').split(',')


    '''This below variable is used for 3 differnet purposes:
        1. First split is used as indicator if its detail api or not
        2. Second split gives the path for extraction from JSON data
        3rd and 4th are required only for detail objects 
        3. Thrid split gives the parent/child object
        4. Fourth split gives the column id used for extracting details'''


    try:
        TYPE_MAPPING = json.loads(Variable.get('PLClaimsTypeMapping'))
    except:
        Variable.set('PLClaimsTypeMapping', json.dumps({
            "claimByExternalId": "True.claimByExternalId.claimsByDateRange.external_claim_id",
            "claimsByDateRange": "False.claims.claimSyncByClaimExternalId",
            "claimSyncByClaimExternalId": "True.claimSyncByClaimExternalId.claimsByDateRange.external_claim_id",
            "syncLogsByDateRange": "False.syncLogs.NA"
        }))
        TYPE_MAPPING = json.loads(Variable.get('PLClaimsTypeMapping'))

    try:
        HISTORY_CHECK = Variable.get('plclaims_history_check')
    except:
        Variable.set('plclaims_history_check', 'True')
        HISTORY_CHECK = Variable.get('plclaims_history_check')


    detail_object= json.dumps({"claimsByDateRange":"claimSyncByClaimExternalId"})

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')
    start = EmptyOperator(
        task_id='starting_data_ingestion')



    for query2 in SOURCE_LIST:
        query_type = TYPE_MAPPING.get(query2, 'NA')

        if query_type.split(".")[0].upper() == 'TRUE':
            continue

        sql = query2 + '.sql'
        with open(os.path.join(configuration.get('core', 'dags_folder'), r'PLClaims/', sql)) as f:
            read_sql = f.readlines()
        sql_query = "\n".join(read_sql)
        get_data = plclaimsGraphQLToGCS(task_id='get_data_for_{query}'.format(query=query2),
                                        base_gcs_folder=base_file_location,
                                        google_cloud_storage_conn_id=base_gcp_connector,
                                        target_gcs_bucket=base_bucket,
                                        normalized_location=base_normalized_location,
                                        schema_location=base_schema_location,
                                        metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                            base=metadata_filename,
                                            table=query2,
                                            date="{{ ds_nodash }}",
                                            source=source_abbr),
                                        query=sql_query,
                                        table_name=query2,
                                        history_check=HISTORY_CHECK,
                                        query_type=TYPE_MAPPING.get(query2, 'NA'),
                                        api_connection_id=api_connection_id,
                                        http_access_request_conn_id=access_connection_id,
                                        api_configuration={},
                                        target_env='DEV',
                                        bigquery_conn_id=bq_gcp_connector,
                                        bq_project=bq_target_project,
                                        ref_dataset=refine_dataset,
                                        airflow_queries_var_set='NA',
                                        begin_pull="{{ ds }}",
                                        end_pull="{{ tomorrow_ds }}")

        # L1 audit - check source record count against landed record count in L1
        landing_audit = APIAuditOperator(task_id='landing_audit_{table}'.format(table=query2),
                                         bucket=base_bucket,
                                         project=project,
                                         dataset=refine_dataset,
                                         base_gcs_folder=None,
                                         target_gcs_bucket=base_bucket,
                                         google_cloud_storage_conn_id=base_gcp_connector,
                                         source_abbr=source_abbr,
                                         source=source,
                                         metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                             base=metadata_filename,
                                             table=query2,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                             base=audit_filename,
                                             table=query2,
                                             date="{{ ds_nodash }}",
                                             source=source_abbr),
                                         check_landing_only=True,
                                         table=query2)

        create_dataset = BigQueryCreateEmptyDatasetOperator(task_id='check_for_dataset_{table}'.format(table=query2),
                                                            dataset_id='{prefix_value}ref_{source_abbr}'.format(
                                                                source_abbr=source_abbr,
                                                                prefix_value=base_gcs_folder),
                                                            bigquery_conn_id=bq_gcp_connector)

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{source_abbr}_{table}'.format(source_abbr=source_abbr,
                                                               table=query2),
            bucket=base_bucket,
            source_objects=['{location_base}{table}/{date}/l1_norm_{source}_{table}_*'.format(
                location_base=base_normalized_location,
                source=source_abbr,
                table=query2,
                date="{{ ds_nodash }}")],
            destination_project_dataset_table='{project}.{prefix_value}ref_{source_abbr}.{table}${date}'.format(
                project=bq_target_project,
                source_abbr=source_abbr,
                table=query2,
                date="{{ ds_nodash }}",
                prefix_value=base_gcs_folder),
            schema_fields=[],
            schema_object='{schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                schema_location=base_schema_location,
                table=query2,
                date="{{ ds_nodash }}",
                source=source_abbr),
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
            autodetect=False)

        refine_audit = APIAuditOperator(task_id='refine_audit_{table}'.format(table=query2),
                                        bucket=base_bucket,
                                        project=bq_target_project,
                                        dataset=refine_dataset,
                                        base_gcs_folder=None,
                                        target_gcs_bucket=base_bucket,
                                        google_cloud_storage_conn_id=base_gcp_connector,
                                        source_abbr=source_abbr,
                                        source=source,
                                        metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                            base=metadata_filename,
                                            table=query2,
                                            date="{{ ds_nodash }}",
                                            source=source_abbr),
                                        audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                            base=audit_filename,
                                            table=query2,
                                            date="{{ ds_nodash }}",
                                            source=source_abbr),
                                        check_landing_only=False,
                                        table=query2)

        detail_object = TYPE_MAPPING.get(query2, 'NA').split('.')[2]
        if detail_object != 'NA':


            query2 = detail_object
            sql = query2 + '.sql'
            with open(os.path.join(configuration.get('core', 'dags_folder'), r'PLClaims/', sql)) as f:
                read_sql = f.readlines()
            sql_query = "\n".join(read_sql)
            get_data_detail = plclaimsGraphQLToGCS(task_id='get_data_for_{query}'.format(query=query2),
                                            base_gcs_folder=base_file_location,
                                            google_cloud_storage_conn_id=base_gcp_connector,
                                            target_gcs_bucket=base_bucket,
                                            normalized_location=base_normalized_location,
                                            schema_location=base_schema_location,
                                            metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                                base=metadata_filename,
                                                table=query2,
                                                date="{{ ds_nodash }}",
                                                source=source_abbr),

                                            query=sql_query,
                                            table_name=query2,
                                            history_check=HISTORY_CHECK,
                                            query_type=TYPE_MAPPING.get(query2, 'NA'),
                                            api_connection_id=api_connection_id,
                                            http_access_request_conn_id=access_connection_id,
                                            api_configuration={},
                                            target_env='DEV',
                                            bigquery_conn_id=bq_gcp_connector,
                                            bq_project=bq_target_project,
                                            ref_dataset=refine_dataset,
                                            airflow_queries_var_set='NA',
                                            begin_pull="{{ ds }}",
                                            end_pull="{{ tomorrow_ds }}")

            # L1 audit - check source record count against landed record count in L1
            landing_audit_detail = APIAuditOperator(task_id='landing_audit_{table}'.format(table=query2),
                                             bucket=base_bucket,
                                             project=project,
                                             dataset=refine_dataset,
                                             base_gcs_folder=None,
                                             target_gcs_bucket=base_bucket,
                                             google_cloud_storage_conn_id=base_gcp_connector,
                                             source_abbr=source_abbr,
                                             source=source,
                                             metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                                 base=metadata_filename,
                                                 table=query2,
                                                 date="{{ ds_nodash }}",
                                                 source=source_abbr),
                                             audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                                 base=audit_filename,
                                                 table=query2,
                                                 date="{{ ds_nodash }}",
                                                 source=source_abbr),
                                             check_landing_only=True,
                                             table=query2)



            refine_data_detail = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source_abbr}_{table}'.format(source_abbr=source_abbr,
                                                                   table=query2),
                bucket=base_bucket,
                source_objects=['{location_base}{table}/l1_norm_{source}_{table}_*'.format(
                    location_base=base_normalized_location,
                    source=source_abbr,
                    table=query2,
                    date="{{ ds_nodash }}")],
                destination_project_dataset_table='{project}.{prefix_value}ref_{source_abbr}.{table}'.format(
                    project=bq_target_project,
                    source_abbr=source_abbr,
                    table=query2,
                    date="{{ ds_nodash }}",
                    prefix_value=base_gcs_folder),
                schema_fields=[],
                schema_object='{schema_location}{table}/{date}/l1_schema_{source}_{table}.json'.format(
                    schema_location=base_schema_location,
                    table=query2,
                    date="{{ ds_nodash }}",
                    source=source_abbr),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                #time_partitioning={'field': TYPE_MAPPING.get(query2, 'NA').split('.')[3]},
                src_fmt_configs={},
                autodetect=False)

            refine_audit_detail = APIAuditOperator(task_id='refine_audit_{table}'.format(table=query2),
                                            bucket=base_bucket,
                                            project=bq_target_project,
                                            dataset=refine_dataset,
                                            base_gcs_folder=None,
                                            target_gcs_bucket=base_bucket,
                                            google_cloud_storage_conn_id=base_gcp_connector,
                                            source_abbr=source_abbr,
                                            source=source,
                                            metadata_filename='{base}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
                                                base=metadata_filename,
                                                table=query2,
                                                date="{{ ds_nodash }}",
                                                source=source_abbr),
                                            audit_filename='{base}/{date}/{table}/l1_audit_{source}_{table}.json'.format(
                                                base=audit_filename,
                                                table=query2,
                                                date="{{ ds_nodash }}",
                                                source=source_abbr),
                                            check_landing_only=False,
                                            table=query2)
        else:
            get_data_detail = EmptyOperator(task_id='get_data_for_{query}_detail'.format(query=query2))
            refine_audit_detail = EmptyOperator(task_id='refine_audit_{table}_detail'.format(table=query2))
            refine_data_detail = EmptyOperator(
                task_id='refine_data_{source_abbr}_{table}_detail'.format(source_abbr=source_abbr,
                                                                   table=query2))
            landing_audit_detail = EmptyOperator(task_id='landing_audit_{table}_detail'.format(table=query2))





        start >> get_data >> landing_audit >> create_dataset >> refine_data >> refine_audit >> get_data_detail  >> landing_audit_detail >>  refine_data_detail >> refine_audit_detail  >> bit_set

