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
from plugins.operators.jm_nice_to_gcs import NiceToGoogleCloudStorageOperator,NiceSupportingAPIToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_NiceAPIAuditOperator import APIAuditOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.operators.bigquery_operator import  BigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json


############################################################################################################
#                      MODIFICATION LOG 
############################################################################################################
#  NAME                   DATE                   DESCRIPTION                 
############################################################################################################
#  RAMESH L               2023-09-10             AIRFLOW 2 COMPATIBILITY CHANGES 
#  RAMESH L               2023-09-13             MIGRATION TO ASTRO
#
#
#
#
############################################################################################################

ts = calendar.timegm(time.gmtime())
logging.info(ts)


env = Variable.get('ENV')


if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    source_bq_target_project = 'dev-edl'
elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    source_bq_target_project = 'qa-edl'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''
    source_bq_target_project = 'prod-edl'


source = 'Nice'
source_abbr = 'nice'
ct_enable = False
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source_abbr)
normalize_file_location = folder_prefix + 'l1_norm/{source_abbr}/'.format(source_abbr=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)


# base_file_location = 'bl_l1_norm/{source}/'.format(source=source_abbr)
# base_schema_location = 'bl_l1_schema/{source}/'.format(source=source_abbr)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

completed=[]

problem_apis = []

niceAPIs = ['team_performance_total','skill_sla_reporting','skills_call_data','dispositions_classifications','agents_skills',
            'agents_states','agent_patterns','agents_quick_replies','agents_skill_data','agent_reporting',
            'skill_reporting','call_list_jobs','call_lists','dnc_groups','groups','address_books',
            'contact_state_descriptions','teams','skills_campaigns','skills_dispositions_skills','contact_reporting',
            'agents','skills','wfm_management_media_1','wfm_management_media_3','wfm_management_media_4',
            'wfm_management_media_5','wfm_management_media_6','wfm_management_media_7','wfm_management_media_8',
            'wfm_agents','wfm_skils_dialer_contact_media_1','wfm_skils_dialer_contact_media_3',
            'wfm_skils_dialer_contact_media_4','wfm_skils_dialer_contact_media_5','wfm_skils_dialer_contact_media_6',
            'wfm_skils_dialer_contact_media_7','wfm_skils_dialer_contact_media_8','wfm_agents_schedule_adherence',
            'wfm_agents_scorecards','wfm_agents_performance']


#niceAPIs = ['contact_reporting']
niceAPIsWithDetail = {
                        'agent_reporting' : 'agent_interaction_recent;agent_login_history;agent_state_history;agent_interaction_history',
                        'skill_reporting' : 'skills_skill_sla_summary;skills_skill_summary',
                        'call_lists' : 'call_lists_details;call_lists_attempts',
                        'dnc_groups' : 'dnc_groups_details;dnc_groups_contributing_skills;dnc_groups_records;dnc_groups_scrubbed_skills',
                        'groups' : 'groups_detail;groups_detail_agents',
                        'address_books' : 'address_books_dynamic_entries;address_books_entries',
              
                        'teams' : 'teams_address_books;teams_detail;teams_agents_detail;teams_unavailable_codes',
                        'skills_campaigns' : 'campaigns_address_books;skills_campaign_detail',
                        'skills_dispositions_skills' : 'skills_dispositions_skills_detail',
                        'contact_reporting' : 'contact_chat_transcripts;contact_email_transcripts;contact_details;contact_state_history;contact_call_quality;contact_custom_data;contact_files;contact_hierarchy',
                        'agents' : 'agent_detail;agent_detail_groups;agent_detail_skills;agent_detail_skills_unassigned;agent_detail_skill_data;agent_detail_quick_replies;agent_detail_messages;agent_detail_indicators;agents_address_books;agent_scheduled_callbacks',
                        'skills' : 'skills_detail;skills_thankyou_detail;skills_agents_detail;skills_agents_unassigned_detail;skills_call_data_detail;skills_disposition_detail;skills_disposition_unassigned_detail;skills_tags_detail;skills_param_general_settings_detail;skills_param_cpa_mgnt_detail;skills_param_xs_settings_detail;skills_param_delivery_prefer_detail;skills_param_retry_settings_detail;skills_param_schedule_settings_detail;skills_address_book;skills_scheduled_callbacks',

                      }


disabled_niceAPIsWithDetail = {
                                'call_list_jobs' : 'call_lists_jobs_details',
                              }

non_partitioned_apis = ['t_dispositions_classifications','t_agents_quick_replies','t_agents_states','t_agent_patterns','t_groups'
                        ,'t_groups_detail','t_groups_detail_agents','t_address_books','t_address_books_dynamic_entries','t_address_books_entries'
                        ,'t_contact_state_descriptions','t_skills_campaigns',
                        't_campaigns_address_books','t_skills_campaign_detail']
####for testing

##For Composer.....
# file_path = os.path.join('/home/airflow/gcs/dags',
#                                  r'ini/mixpanel_digital_dashboard_schema.json')

confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'nice', r'ini', r"nice_ingestion_airflow_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())


with open(os.path.join(configuration.get('core', 'dags_folder'), r'nice/sql/claims_dashboard_nice_data.sql')) as f:
    nice_claims_dashboard_sql_list = f.readlines()
nice_claims_dashboard_sql = "\n".join(nice_claims_dashboard_sql_list)
nice_claims_dashboard_sql = nice_claims_dashboard_sql.format(source_project=source_bq_target_project,
                                                            source_dataset=folder_prefix + 'ref_nice')


with DAG(
        'Nice_dag',
        schedule_interval= '0 7 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


    source_base_dataset = 'ref_{source}'.format(source=source.lower())
    base_dataset = 'gld_{source}'.format(source=source.lower())

    create_gld_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gld_dataset_{source}'.format(source=source_abbr),
        dataset_id='{prefix}{base_dataset}'.format(
            base_dataset=base_dataset, prefix=folder_prefix),
        bigquery_conn_id=bq_gcp_connector)


    nice_claims_dashboard = BigQueryOperator(task_id='nice_claims_dashboard',
                                             sql=nice_claims_dashboard_sql,
                                             destination_dataset_table='{project}.{prefix}{base_dataset}.t_nice_claims_dashboard'.format(
                                                 project=bq_target_project,
                                                 prefix=folder_prefix,
                                                 base_dataset=base_dataset,
                                                 source_abbr=source_abbr.lower(),
                                                 date="{{ ds_nodash }}"),
                                             write_disposition='WRITE_TRUNCATE',
                                             create_disposition='CREATE_IF_NEEDED',
                                             gcp_conn_id=bq_gcp_connector,
                                             allow_large_results=True,
                                             use_legacy_sql=False)




    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='Nice',
                                 mode='SET')

    try:
        HISTORY_CHECK = Variable.get('nice_history_check')
    except:
        Variable.set('nice_history_check', 'True')
        HISTORY_CHECK = Variable.get('nice_history_check')

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source_abbr),
        dataset_id='{prefix}ref_{source_abbr}'.format(
            source_abbr=source_abbr.lower(),prefix=folder_prefix),
        bigquery_conn_id=bq_gcp_connector)


    print('*' * 20)
    print('Total API Count.....',len(niceAPIs))
    print('*' * 20)



    for api in niceAPIs:

        try:
            detail_list = niceAPIsWithDetail[api].split(';')
        except:
            detail_list = ['skip']


        land_data = NiceToGoogleCloudStorageOperator(task_id='landing_data_{source}_{api}'.format(source=source_abbr,api=api),
                                                     api_configuration=confJSON,
                                                     api_name=api,
                                                     google_cloud_storage_conn_id=base_gcp_connector,
                                                     base_gcs_folder = base_file_location,
                                                     target_gcs_bucket=base_bucket,
                                                     normalized_location=normalize_file_location,
                                                     schema_location=base_schema_location,
                                                     history_check = HISTORY_CHECK,
                                                     metadata_filename=base_audit_location + 'date_nodash/{api_name}/l1_metadata_nice_{api_name}.json'.format(api_name=api))

        if 't_' + api in non_partitioned_apis:

            destination_project_dataset_table = '{project}.{prefix}ref_{source_abbr}.{table}'.format(
            project=bq_target_project,
            source_abbr=source_abbr.lower(),
            table='t_' + api,
            prefix=folder_prefix)
            partitioned_table = False
            refine_data = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source}_{api}'.format(source=source_abbr,
                                                            api=api),
                bucket=base_bucket,
                source_objects=[
                    '{base_file_location}{api}/{date}/l1_data_nice_{api}_*'.format(
                        base_file_location=normalize_file_location,
                        api=api,
                        date="{{ ds_nodash }}")],
                destination_project_dataset_table=destination_project_dataset_table,
                schema_fields=None,
                schema_object='{schema_location}{api}/{date}/l1_schema_nice_{api}.json'.format(
                    schema_location=base_schema_location,
                    api=api,
                    date="{{ ds_nodash }}"),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs=None,
                # autodetect=True,
                # gcs_to_bq = HUBAPI_BQ_PROCESS
                )

        else:
            destination_project_dataset_table = '{project}.{prefix}ref_{source_abbr}.{table}${date}'.format(
                project=bq_target_project,
                source_abbr=source_abbr.lower(),
                table='t_' + api,
                prefix=folder_prefix,
                date="{{ ds_nodash }}")
            partitioned_table = True
            refine_data = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{source}_{api}'.format(source=source_abbr,
                                                            api=api),
                bucket=base_bucket,
                source_objects=[
                    '{base_file_location}{api}/{date}/l1_data_nice_{api}_*'.format(
                        base_file_location=normalize_file_location,
                        api=api,
                        date="{{ ds_nodash }}")],
                destination_project_dataset_table=destination_project_dataset_table,
                schema_fields=None,
                schema_object='{schema_location}{api}/{date}/l1_schema_nice_{api}.json'.format(
                    schema_location=base_schema_location,
                    api=api,
                    date="{{ ds_nodash }}"),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs=None,
                # autodetect=True,
                # gcs_to_bq = HUBAPI_BQ_PROCESS
                )

        land_audit = APIAuditOperator(
                                    task_id='landing_audit_data_{source}_{api}'.format(source=source_abbr, api=api),
                                    project=bq_target_project,
                                    dataset=None,
                                    table=None,
                                    source=source,
                                    source_abbr=source_abbr,
                                    target_gcs_bucket=base_bucket,
                                    partitioned_table=partitioned_table,
                                    bucket=base_bucket,
                                    google_cloud_storage_conn_id=base_gcp_connector,
                                    check_landing_only=True,
                                    metadata_filename=base_audit_location + "{ds_nodash}" + '/{api_name}'.format(api_name=api) +  '/l1_metadata_nice_{api_name}.json'.format(api_name=api),
                                    audit_filename=base_audit_location +  "{ds_nodash}" + '/{api_name}'.format(api_name=api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=api)
                                    )


        refine_audit = APIAuditOperator(
            task_id='refine_audit_data_{source}_{api}'.format(source=source_abbr, api=api),
            project=bq_target_project,
            dataset='{prefix}ref_{source_abbr}'.format(prefix=folder_prefix,source_abbr=source_abbr),
            table='t_' + api,
            source=source,
            source_abbr=source_abbr,
            target_gcs_bucket=base_bucket,
            bucket=base_bucket,
            partitioned_table=partitioned_table,
            google_cloud_storage_conn_id=base_gcp_connector,
            check_landing_only=False,
            metadata_filename=base_audit_location + "{ds_nodash}" + '/{api_name}'.format(
                api_name=api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=api),
            audit_filename=base_audit_location + "{ds_nodash}" + '/{api_name}'.format(
                api_name=api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=api)
        )


        for detail_api in detail_list:
            if detail_api == 'skip':
                supporting_api_land_data = EmptyOperator(
                    task_id='supporting_api_land_data_{api}_{detail_api}'.format(detail_api=detail_api, api=api))
                supporting_api_refine_data = EmptyOperator(
                    task_id='supporting_api_refine_data_{api}_{detail_api}'.format(detail_api=detail_api, api=api))
                supporting_api_land_audit = EmptyOperator(
                    task_id='supporting_landing_audit_data_{api}_{detail_api}'.format(detail_api=detail_api, api=api))
                supporting_api_refine_audit = EmptyOperator(
                    task_id='supporting_api_refine_audit_data_{api}_{detail_api}'.format(detail_api=detail_api, api=api))

            else:
                supporting_api_land_data = NiceSupportingAPIToGoogleCloudStorageOperator(task_id='supporting_api_land_data_{source}_{api}_{detail_api}'.format(source=source_abbr,api=api,detail_api=detail_api),
                                                     api_configuration=confJSON,
                                                     api_name=detail_api,
                                                     base_api_name=api,
                                                     base_api_location=normalize_file_location,
                                                     google_cloud_storage_conn_id=base_gcp_connector,
                                                     base_gcs_folder = base_file_location,
                                                     target_gcs_bucket=base_bucket,
                                                     normalized_location=normalize_file_location,
                                                     schema_location=base_schema_location,
                                                     metadata_filename=base_audit_location + 'date_nodash/{api_name}/l1_metadata_nice_{api_name}.json'.format(api_name=detail_api))

                if 't_' + detail_api in non_partitioned_apis:

                    destination_project_dataset_table = '{project}.{prefix}ref_{source_abbr}.{table}'.format(
                        project=bq_target_project,
                        source_abbr=source_abbr.lower(),
                        table='t_' + detail_api,
                        prefix=folder_prefix)
                    partitioned_table = False
                    supporting_api_refine_data = GoogleCloudStorageToBigQueryOperator(
                        task_id='supporting_api_refine_data_{source}_{api}'.format(source=source_abbr,
                                                                                   api=detail_api),
                        bucket=base_bucket,
                        source_objects=[
                            '{base_file_location}{api}/{date}/l1_data_nice_{api}_*'.format(
                                base_file_location=normalize_file_location,
                                api=detail_api,
                                date="{{ ds_nodash }}")],
                        destination_project_dataset_table=destination_project_dataset_table,
                        schema_fields=None,
                        schema_object='{schema_location}{api}/{date}/l1_schema_nice_{api}.json'.format(
                            schema_location=base_schema_location,
                            api=detail_api,
                            date="{{ ds_nodash }}"),
                        source_format='NEWLINE_DELIMITED_JSON',
                        compression='NONE',
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=0,
                        write_disposition='WRITE_TRUNCATE',
                        max_bad_records=0,
                        bigquery_conn_id=bq_gcp_connector,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        #schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                        src_fmt_configs=None,
                        # autodetect=True,
                        # gcs_to_bq = HUBAPI_BQ_PROCESS
                        )
                else:
                    destination_project_dataset_table = '{project}.{prefix}ref_{source_abbr}.{table}${date}'.format(
                        project=bq_target_project,
                        source_abbr=source_abbr.lower(),
                        table='t_' + detail_api,
                        prefix=folder_prefix,
                        date="{{ ds_nodash }}")
                    partitioned_table = True
                    supporting_api_refine_data = GoogleCloudStorageToBigQueryOperator(
                        task_id='supporting_api_refine_data_{source}_{api}'.format(source=source_abbr,
                                                                                   api=detail_api),
                        bucket=base_bucket,
                        source_objects=[
                            '{base_file_location}{api}/{date}/l1_data_nice_{api}_*'.format(
                                base_file_location=normalize_file_location,
                                api=detail_api,
                                date="{{ ds_nodash }}")],
                        destination_project_dataset_table=destination_project_dataset_table,
                        schema_fields=None,
                        schema_object='{schema_location}{api}/{date}/l1_schema_nice_{api}.json'.format(
                            schema_location=base_schema_location,
                            api=detail_api,
                            date="{{ ds_nodash }}"),
                        source_format='NEWLINE_DELIMITED_JSON',
                        compression='NONE',
                        create_disposition='CREATE_IF_NEEDED',
                        skip_leading_rows=0,
                        write_disposition='WRITE_TRUNCATE',
                        max_bad_records=0,
                        bigquery_conn_id=bq_gcp_connector,
                        google_cloud_storage_conn_id=base_gcp_connector,
                        schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                        src_fmt_configs=None,
                        # autodetect=True,
                        # gcs_to_bq = HUBAPI_BQ_PROCESS
                        )


                supporting_api_land_audit = APIAuditOperator(
                                    task_id='supporting_landing_audit_data_{source}_{api}'.format(source=source_abbr, api=detail_api),
                                    project=bq_target_project,
                                    dataset=None,
                                    table=None,
                                    source=source,
                                    source_abbr=source_abbr,
                                    target_gcs_bucket=base_bucket,
                                    partitioned_table=partitioned_table,
                                    bucket=base_bucket,
                                    google_cloud_storage_conn_id=base_gcp_connector,
                                    check_landing_only=True,
                                    metadata_filename=base_audit_location +  "{ds_nodash}" + '/{api_name}'.format(api_name=detail_api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=detail_api),
                                    audit_filename=base_audit_location +  "{ds_nodash}" + '/{api_name}'.format(api_name=detail_api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=detail_api)
                                    )
                supporting_api_refine_audit = APIAuditOperator(
            task_id='supporting_api_refine_audit_data_{source}_{api}'.format(source=source_abbr, api=detail_api),
            project=bq_target_project,
            dataset='{prefix}ref_{source_abbr}'.format(prefix=folder_prefix,source_abbr=source_abbr),
            table='t_' + detail_api,
            source=source,
            source_abbr=source_abbr,
            target_gcs_bucket=base_bucket,
            partitioned_table = partitioned_table,
            bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            check_landing_only=False,
            metadata_filename=base_audit_location + "{ds_nodash}" + '/{api_name}'.format(
                api_name=detail_api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=detail_api),
            audit_filename=base_audit_location + "{ds_nodash}" + '/{api_name}'.format(
                api_name=detail_api) + '/l1_metadata_nice_{api_name}.json'.format(api_name=detail_api)
            )

            create_dataset >> land_data >> land_audit >> refine_data >> refine_audit >> supporting_api_land_data >> supporting_api_land_audit >> supporting_api_refine_data >> supporting_api_refine_audit>>bit_set >> create_gld_dataset >> nice_claims_dashboard
