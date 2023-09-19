import logging
import datetime as dt
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import  BigQueryOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_GetNiceTableListOperator import GetNiceTables
from airflow.models import Variable
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)


env = Variable.get('ENV')


if env.lower() == 'dev':
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'

elif env.lower() == 'qa':
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'

elif env.lower() == 'prod':
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    folder_prefix = ''

source = 'Nice'
source_abbr = 'nice'
source_dataset_id = '{prefix}ref_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)
destination_dataset_id = '{prefix}gld_{source_abbr}'.format(source_abbr=source_abbr.lower(), prefix=folder_prefix)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        'nice_gold',
        schedule_interval= '0 9 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        check_Nice_Completion = ExternalTaskSensorAsync(
            task_id='check_Nice_Completion',
            external_dag_id='Nice_dag',
            external_task_id='set_source_bit',
            execution_delta=timedelta(hours=2),
            timeout=7200)

        get_nice_refined_tables = GetNiceTables(task_id='get_nice_refined_tables',
                                       gcp_connection_id=bq_gcp_connector,
                                       project_id=bq_target_project,
                                       dataset_id= source_dataset_id)

        """check_for_gld_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_gld_dataset_{source}'.format(source=source_abbr),
            dataset_id=destination_dataset_id,
            bigquery_conn_id=bq_gcp_connector)"""

        non_partitioned_apis = ['t_dispositions_classifications', 't_agents_quick_replies', 't_agents_states',
                                't_agent_patterns', 't_groups', 't_groups_detail', 't_groups_detail_agents',
                                't_address_books', 't_address_books_dynamic_entries','t_address_books_entries'
                                ,'t_contact_state_descriptions', 't_contact_state_descriptions_detail',
                                't_skills_campaigns','t_campaigns_address_books', 't_skills_campaign_detail']

        NICE_TABLES = Variable.get('Nice_refined_tables_list', default_var = 't_address_books, t_address_books_dynamic_entries, t_address_books_entries, t_agent_detail, t_agent_detail_messages, t_agent_detail_quick_replies, t_agent_detail_skill_data, t_agent_detail_skills, t_agent_detail_skills_unassigned, t_agent_interaction_history, t_agent_interaction_recent, t_agent_login_history, t_agent_reporting, t_agent_scheduled_callbacks, t_agent_state_history, t_agents, t_agents_address_books, t_agents_quick_replies, t_agents_skill_data, t_agents_skills, t_agents_states, t_call_list_jobs, t_contact_call_quality, t_contact_chat_transcripts, t_contact_details, t_contact_files, t_contact_hierarchy, t_contact_reporting, t_contact_state_descriptions, t_contact_state_descriptions_detail, t_contact_state_history, t_dispositions_classifications, t_dnc_groups, t_dnc_groups_contributing_skills, t_dnc_groups_details, t_groups, t_groups_detail, t_skill_reporting, t_skill_sla_reporting, t_skills, t_skills_agents_detail, t_skills_agents_unassigned_detail, t_skills_call_data, t_skills_call_data_detail, t_skills_campaign_detail, t_skills_campaigns, t_skills_detail, t_skills_disposition_detail, t_skills_disposition_unassigned_detail, t_skills_dispositions_skills, t_skills_dispositions_skills_detail, t_skills_param_cpa_mgnt_detail, t_skills_param_delivery_prefer_detail, t_skills_param_general_settings_detail, t_skills_param_retry_settings_detail, t_skills_param_schedule_settings_detail, t_skills_param_xs_settings_detail, t_skills_scheduled_callbacks, t_skills_skill_sla_summary, t_skills_skill_summary, t_skills_tags_detail, t_skills_thankyou_detail, t_team_performance_total, t_teams, t_teams_address_books, t_teams_agents_detail, t_teams_detail, t_teams_unavailable_codes, t_wfm_agents, t_wfm_agents_performance, t_wfm_agents_scorecards, t_wfm_management_media_1, t_wfm_management_media_3, t_wfm_management_media_4, t_wfm_management_media_5, t_wfm_management_media_6, t_wfm_management_media_7, t_wfm_management_media_8')


        NICE_TABLES = NICE_TABLES.replace(" ","").split(',')

        for table in NICE_TABLES:

            if table in non_partitioned_apis:

                nice_gold_sql = '''  
                    SELECT *, DATE('{date}') as bq_load_date FROM `{project}.{dataset}.{table}`  '''.format(
                project=bq_target_project,
                dataset=source_dataset_id,
                table=table,
                date = "{{ds}}")
                destination_dataset_table = '{project}.{dataset}.{table}'.format(
                    project=bq_target_project,
                    table=table,
                    dataset=destination_dataset_id,
                    source_abbr=source_abbr.lower())
                build_gold_data = BigQueryOperator(task_id='Nice_gld_{table}'.format(table=table),
                                                   sql=nice_gold_sql,
                                                   destination_dataset_table=destination_dataset_table,
                                                   write_disposition='WRITE_TRUNCATE',
                                                   create_disposition='CREATE_IF_NEEDED',
                                                   gcp_conn_id=bq_gcp_connector,
                                                   allow_large_results=True,
                                                   use_legacy_sql=False,
                                                   #time_partitioning={"type": "DAY"}
                                                   )
            else:
                nice_gold_sql = '''  
                                    SELECT *, DATE(_PARTITIONTIME) as bq_load_date FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = '{date}' '''.format(
                    project=bq_target_project,
                    dataset=source_dataset_id,
                    table=table,
                    date="{{ds}}")
                destination_dataset_table = '{project}.{dataset}.{table}${date}'.format(
                    project=bq_target_project,
                    table=table,
                    dataset=destination_dataset_id,
                    source_abbr=source_abbr.lower(),
                    date="{{ ds_nodash }}")
                build_gold_data = BigQueryOperator(task_id='Nice_gld_{table}'.format(table=table),
                                                   sql=nice_gold_sql,
                                                   destination_dataset_table=destination_dataset_table,
                                                   write_disposition='WRITE_TRUNCATE',
                                                   create_disposition='CREATE_IF_NEEDED',
                                                   gcp_conn_id=bq_gcp_connector,
                                                   allow_large_results=True,
                                                   use_legacy_sql=False,
                                                   time_partitioning={"type": "DAY"},
                                                   schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                          'ALLOW_FIELD_RELAXATION'])





            check_Nice_Completion>>get_nice_refined_tables >> build_gold_data