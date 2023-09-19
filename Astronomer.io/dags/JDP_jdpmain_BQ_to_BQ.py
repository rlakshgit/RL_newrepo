import logging
import datetime as dt
import calendar
import time
import sys
import hashlib
import fuzzywuzzy
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow import configuration
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.gcs_hook import GCSHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import EmptyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from plugins.operators.place_key_operator import PlaceKeyOperator
from plugins.operators.experian_key_operator import ExperianKeyOperator
from plugins.operators.JDPProductsAuditOperator import JDPProductsAuditOperator
from plugins.operators.skipoperator import TaskSkippingOperator
# from plugins.hooks.jm_google_places_hook import GooglePlacesHttpHook


from airflow.models import Variable
import numpy as np

import os
from airflow.operators.subdag_operator import SubDagOperator
import base64
import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

AIRFLOW_ENV = Variable.get('ENV')

# Max_allowed_places_look_up_count
Max_allowed_places_look_up_count = Variable.get('Max_allowed_places_look_up_count', default_var=500)

# Set some project-specific variables that are used in building the tasks.
if AIRFLOW_ENV.lower() == 'dev':
    jdp_project = 'dev-edl'
    src_project = jdp_project
    jdp_dataset = 'core_JDP'
    destination_connection = 'dev_edl'
    catchup = False
    limit = 10

elif AIRFLOW_ENV.lower() == 'qa':
    jdp_project = 'qa-edl'
    src_project = jdp_project
    jdp_dataset = 'core_JDP'
    destination_connection = "qa_edl"
    catchup = False
    limit = 10

elif AIRFLOW_ENV.lower() == 'prod':
    jdp_project = 'semi-managed-reporting'
    src_project = 'prod-edl'
    jdp_dataset = 'core_JDP'
    destination_connection = "semi_managed_gcp_connection"
    catchup = False
    limit = 999999

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'max_active_runs': 1,
    'start_date': datetime(2022, 1, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'alangsner@jminsure.com',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'write_disposition': "WRITE_TRUNCATE",
    'bigquery_conn_id': destination_connection,
    'use_legacy_sql': False,
    'time_partitioning': {'type': 'DAY', 'field': 'date_created'},
    'schema_update_options': ('ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION')
}

# Build a list of columns for use in JDP SQL statements.
master_fields = ['PlaceKey',
                 'BusinessKey', ]
master_new = ["'' as PlaceKey",
              "'' as BusinessKey", ]
stage_fields = ['PlaceID_Lookup',
                'ExperianBIN_Lookup',
                'input_name',
                'input_address', ]

# Read source information from a configuration file (to build the internal_ series of tables)

confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'JDP', r'ini', r"JDP_source_config.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())
source_list = list(confJSON.keys())
internal_stage_sql_parts = []
all_source_keys = []
source_tasks = []

# Go through the source list once to build lists of all fields across sources.
for source in source_list:
    source_key = '{source}_LocationKey'.format(source=source)
    all_source_keys = all_source_keys + [source_key]

# Create a SQL template for querying one of the JDP tables to gather run metrics.
# This will be copied as needed and formatted with appropriate variables.

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                       'health_template.sql'), 'r') as f:
    health_template_sql = f.read()

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                       'health_template_ex.sql'), 'r') as f:
    health_template_sql_ex = f.read()

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                       'health_template_pk.sql'), 'r') as f:
    health_template_sql_pk = f.read()

# This is the format of the health table
# data_output_template_sql = '''SELECT '{task_name}' as task_name
#                                  , '{dataset_name}' as dataset_name
#                                  , '{table_name}' as table_name
#                                  , {loop_num} as iteration_loop
#                                  , count(*) as record_count
#                                  , 0 as incomplete_key_count
#                                  , {dag_exec_date} as dag_execution_date
#                                  , CURRENT_TIMESTAMP() as update_time
#                              FROM {query_table}
#                           '''

# DAG Definition
with models.DAG(
        'jdp_main',
        schedule_interval='0 17 * * *',
        catchup=catchup,
        default_args=default_args) as dag:

    task_start = EmptyOperator(task_id="task_start", dag=dag)

    sources_complete = EmptyOperator(task_id="sources_complete", dag=dag)

    # Build variable strings to be used in the queries, dynamically updated by sources.
    query_stage_fields = all_source_keys
    query_master_fields = query_stage_fields
    query_stage_fields_string = ', '.join(query_stage_fields)
    query_stage_fields_labeled = ', '.join(['stage.' + x for x in query_stage_fields])
    query_gem_digger_fields_labeled = ', '.join([" '' " + x for x in query_stage_fields])
    query_join_fields_labeled = ' AND '.join(
        ["IFNULL(master." + x + ", 'NULL')=IFNULL(stage." + x + ", 'NULL')" for x in query_stage_fields])
    query_nullif_fields = ', '.join(["NULLIF(" + x + ", '')" for x in query_stage_fields])
    query_nullif_fields_var = ', '.join(["NULLIF(" + x + ", " + x + ")" for x in query_stage_fields])
    query_master_fields_string = ', '.join(query_master_fields)

    health_query_null_keys = ' OR '.join([x + ' IS NULL ' for x in all_source_keys])
    health_query_blank_keys = ' OR '.join([x + "='' " for x in all_source_keys])

    # ------------------------------------------------------------------------------------------------------
    # Create the appropriate operator task and a health check task for each item in the task_details list.
    # Processing begins and ends with a dummy operator.
    # ------------------------------------------------------------------------------------------------------

    def health_check(task_id, table_name, sql=health_template_sql, project=jdp_project, dataset_name=jdp_dataset,
                     partition_field='date_created', health_query_null_keys=health_query_null_keys,
                     health_query_blank_keys=health_query_blank_keys, loop_num=1, dag=dag):
        return BigQueryOperator(task_id=f'health_{task_id}',
                                sql=sql.format(task_name=task_id,
                                               project=project,
                                               dataset_name=dataset_name,
                                               table_name=table_name,
                                               loop_num=loop_num,
                                               dag_exec_date='DATE("{{ ds }}")',
                                               partition_field=partition_field,
                                               query_null_keys=health_query_null_keys,
                                               query_blank_keys=health_query_blank_keys),
                                destination_dataset_table=f'{project}.{dataset_name}.jdp_task_health${{{{ds_nodash}}}}',
                                write_disposition='WRITE_APPEND',
                                time_partitioning={"type": 'DAY', "field": 'dag_execution_date'},
                                schema_update_options=('ALLOW_FIELD_RELAXATION',),
                                dag=dag)

    # Now build tasks for each internal source.
    for source in source_list:
        task_name = 'internal_source_{source}'.format(source=source)
        task_sql = '{source}_source_sql'.format(source=source.lower())
        task_table = task_name
        task_table_partition = task_table + '${{{{ ds_nodash }}}}'
        source_key = '{source}_LocationKey'.format(source=source)
        internal_stage_select = confJSON[source]['internal_stage_select'].split(',')
        source_project = src_project  # confJSON[source]['source_project']
        source_dataset = confJSON[source]['source_dataset']
        source_table = confJSON[source]['source_table']
        task_partition_field = confJSON[source]['source_partition']
        task_sql_file = confJSON[source]['source_sql']
        with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/ini/', task_sql_file), 'r') as f:
            task_sql = f.read()

        task_sql = task_sql.format(source_project=source_project, source_dataset=source_dataset, source_table=source_table,
                                   project=jdp_project, dataset_jdp=jdp_dataset, table=task_table)

        select_list = internal_stage_select + master_new + stage_fields


        internal_stage_sql_parts += ['''{source} as (
                                      SELECT DISTINCT {select_list}
                                      FROM `{project}.{source_dataset}.{task_table}`
                                      WHERE {source_partition} <= DATE("{{{{ ds }}}}")
                                      AND (
                                        {source_key} NOT IN (SELECT DISTINCT(IFNULL({source_key}, 'NULL')) FROM `{project}.{source_dataset}.internal_stage` WHERE date_created < DATE("{{{{ ds }}}}"))
                                        OR {source_key} NOT IN (SELECT DISTINCT(IFNULL({source_key}, 'NULL')) FROM `{project}.{source_dataset}.business_master` WHERE date_created < DATE("{{{{ ds }}}}"))
                                        )
                                      LIMIT {limit}  
                                    )'''.format(source=source,
                                                select_list=', '.join(select_list),
                                                project=jdp_project,
                                                source_dataset=jdp_dataset,
                                                task_table=task_table,
                                                source_partition=task_partition_field,
                                                source_key=source_key,
                                                limit=limit)]

        ###### DYNAMIC SOURCE TASK DEFINITIONS ######
        task_id = task_name
        table_name = task_id
        task = BigQueryOperator(task_id=task_id,
                                sql=task_sql,
                                destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ds_nodash}}}}",
                                time_partitioning={'type': 'DAY', 'field': f'{task_partition_field}'},
                                dag=dag)

        task_start >> task >> sources_complete


    internal_stage_sql_parts += [
        'joined as ( ' + ' UNION DISTINCT '.join(['SELECT * FROM ' + x for x in source_list]) + ' ) ']
    join_parts = ', '.join(internal_stage_sql_parts)
    internal_stage_sql = '''WITH {join_parts}
                            SELECT *, DATE("{{{{ ds }}}}") as date_created
                            FROM joined'''.format(join_parts=join_parts)

    # List the task details so they can be dynamically generated.  Sequence is very important here.





    # ---------------------------------------------------------------------------------------------------
    # Loop 2 GuideWire stage query
    # Bring source fields (in addition to key fields) forward (PlaceID_Lookup, ExperianBIN_Lookup)
    # Create date_created field from dag schedule exec date
    # TODO: Implement union for other source tables within this JewelerMaster
    # ----------------------------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'loop2_internal_stg.sql'), 'r') as f:
        loop2_internal_stg_sql = f.read().format(project=jdp_project,
                                                 source_dataset=jdp_dataset,
                                                 query_stage_fields=query_stage_fields_string,
                                                 query_master_fields=query_master_fields_string,
                                                 query_stage_fields_labeled=query_stage_fields_labeled,
                                                 query_join_fields=query_join_fields_labeled)

    # ---------------------------------------------------------------------------------------------------
    # Internal master query
    # Bring key fields forward
    # Create date_created field from dag schedule exec date
    # ----------------------------------------------------------------------------------------------------

    internal_master_sql = '''SELECT {query_master_fields}
                                    , PlaceKey
                                    , BusinessKey
                                    , DATE("{{{{ ds }}}}") as date_created
						     FROM (
						       SELECT {query_master_fields}
                                      , PlaceKey
                                      , BusinessKey
                               FROM `{project}.{source_dataset}.internal_stage`
					           WHERE date_created = DATE("{{{{ ds }}}}")                      
                             )'''.format(project=jdp_project,
                                         source_dataset=jdp_dataset,
                                         query_master_fields=query_master_fields_string)

    loop2_internal_master_sql = '''SELECT * FROM `{project}.{source_dataset}.business_master` WHERE date_created = DATE("{{{{ ds }}}}")'''.format(
        project=jdp_project, source_dataset=jdp_dataset)

    # -------------------------------------------------------------------------------
    # Places stage1 query
    # This currently contains deltas from internal_stage.
    # Bring source fields (in addition to key fields) forward (PlaceID_Lookup, ExperianBIN_Lookup)
    # -------------------------------------------------------------------------------

    places_stage_1_sql = '''SELECT DISTINCT *
                               FROM `{project}.{source_dataset}.internal_stage` 
                               WHERE date_created = DATE("{{{{ ds }}}}")
                            '''.format(project=jdp_project, source_dataset=jdp_dataset)

    places_stage_2_clean_sql = '''SELECT DISTINCT *
                               FROM `{project}.{source_dataset}.places_stage_2` 
                               WHERE DATE(date_created) = "2099-01-01"
                            '''.format(project=jdp_project, source_dataset=jdp_dataset)
    # -------------------------------------------------------------------------------
    # Places ID lookup query
    # This currently contains deltas from places_stage.
    # -------------------------------------------------------------------------------

    places_stage_2_sql = '''SELECT * FROM `{project}.{source_dataset}.places_stage_1`  WHERE date_created = DATE("{{{{ ds }}}}") AND PlaceID_Lookup NOT IN (SELECT PlaceID_Lookup FROM `{project}.{source_dataset}.places_stage_2` WHERE date_created = "{{{{ ds }}}}" )'''.format(
        project=jdp_project, source_dataset=jdp_dataset)
    # places_lookup_sql = "SELECT * FROM  `dev-edl.GN_JDP.places_stage` WHERE PlaceID_Lookup = 'Robert Palma Designs LLC 1750 Kalakaua Ave, Suite 3702 Honolulu Hawaii 96826' "
    # -------------------------------------------------------------------------------------------------
    # PlaceKey_table query.
    # This currently contains deltas from places_stage_2 merged with placeKey_table.
    # Do not bring foreign keys forward (PlaceID_Lookup, ExperianBIN_Lookup)
    # -------------------------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'placeKey_table.sql'), 'r') as f:
        placeKey_table_sql = f.read().format(project=jdp_project, source_dataset=jdp_dataset)

    # ---------------------------------------------------------------------------------------------------
    # Places master query
    # Create date_created field from dag schedule exec date
    # ----------------------------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'places_master.sql'), 'r') as f:
        places_master_sql = f.read().format(project=jdp_project,
                                            source_dataset=jdp_dataset,
                                            query_master_fields=query_master_fields_string,
                                            query_gem_digger_fields=query_gem_digger_fields_labeled,
                                            query_stage_fields=query_stage_fields_labeled,
                                            query_nullif_fields=query_nullif_fields)

    loop2_places_master_sql = '''SELECT * FROM `{project}.{source_dataset}.business_master` WHERE date_created = DATE("{{{{ ds }}}}")'''.format(
        project=jdp_project, source_dataset=jdp_dataset)

    # -------------------------------------------------------------------------------
    # Business stage1 query
    # This currently contains deltas from places_master.
    # Potential for multiple records from each individual to provide lookup for both GW name/address and Places name/address.
    # Bring source fields (in addition to key fields) forward (PlaceID_Lookup, ExperianBIN_Lookup)
    # TODO: Implement union for other source tables within this JewelerMaster.
    # -------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'business_stage1.sql'), 'r') as f:
        business_stage_1_sql = f.read().format(project=jdp_project,
                                               source_dataset=jdp_dataset,
                                               query_master_fields=query_master_fields_string,
                                               query_stage_fields_labeled=query_stage_fields_labeled,
                                               query_join_fields=query_join_fields_labeled,
                                               query_nullif_fields=query_nullif_fields_var)

    business_stage_2_clean_sql = '''SELECT DISTINCT *
                                   FROM `{project}.{source_dataset}.business_stage_2` 
                                   WHERE DATE(date_created) = "2099-01-01"
                                '''.format(project=jdp_project, source_dataset=jdp_dataset)

    business_stage_2_sql = '''SELECT * FROM `{project}.{source_dataset}.business_stage_1`  WHERE date_created = DATE("{{{{ ds }}}}") AND ExperianBIN_Lookup NOT IN (SELECT ExperianBIN_Lookup FROM `{project}.{source_dataset}.business_stage_2` WHERE date_created = "{{{{ ds }}}}" )'''.format(
        project=jdp_project, source_dataset=jdp_dataset)



    # -------------------------------------------------------------------------------------------------
    # BusinessKey_table query.
    # This currently contains deltas from business__ merged with businessKey_table.
    # Do not bring foreign keys forward (PlaceID_Lookup, ExperianBIN_Lookup)
    # -------------------------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'businessKey_table.sql'), 'r') as f:
        businessKey_table_sql = f.read().format(project=jdp_project, source_dataset=jdp_dataset)

    # ---------------------------------------------------------------------------------------------------
    # Business master query
    # Create date_created field from dag schedule exec date
    # ----------------------------------------------------------------------------------------------------

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/jdp_main_SQL/',
                           'business_master.sql'), 'r') as f:
        business_master_sql = f.read().format(project=jdp_project,
                                              source_dataset=jdp_dataset,
                                              query_master_fields=query_master_fields_string,
                                              query_stage_fields_labeled=query_stage_fields_labeled)


    ###### INTERNAL STAGE L1 #####

    task_id = 'internal_stage'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    audit_internal_stage_sql = '''SELECT * FROM `{project}.{dataset}.jdp_task_health` where table_name = 'internal_stage' and dag_execution_date ="{{{{ ds }}}}"and record_count >={Max_allowed_places_look_up_count}'''.format(
        Max_allowed_places_look_up_count=Max_allowed_places_look_up_count,
        project=jdp_project,
        dataset=jdp_dataset
    )
    audit_task = JDPProductsAuditOperator(
        task_id=f"audit_{task_id}",
        sql=audit_internal_stage_sql,
        jdp_products=False,
        dag=dag
    )


    sources_complete >> task >> health_task >> audit_task
    task_start = audit_task

    ###### INTERNAL MASTER L1 #####

    task_id = 'internal_master'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACES STAGE 1 - L1 #####

    task_id = 'places_stage_1'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACES STAGE 2 CLEAN - L1 #####

    task_id = 'places_stage_2_clean'
    table_name = 'places_stage_2'
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)

    task_start >> task
    task_start = task

    ###### places_stage_2 (places lookup) - L1 #####

    task_id = 'places_stage_2'
    table_name = task_id
    task = PlaceKeyOperator(task_id=task_id,
                            source_sql=eval(task_id + '_sql'),
                            destination_table=f"{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            google_places_conn_id="GooglePlacesDefault",
                            write_disposition='append',
                            # TODO should be "replace" but not compatible with partitioned tables
                            dag=dag)
    health_task = health_check(task_id, table_name, partition_field='DATE(date_created)')

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACEKEY TABLE - L1 #####

    task_id = 'placeKey_table'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name, sql=health_template_sql_pk, partition_field='DATE(date_created)')

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACES MASTER TABLE - L1 #####

    task_id = 'places_master'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    task_start >> task >> health_task
    task_start = health_task

    ###### BUSINESS STAGE 1 - L1 #####

    task_id = 'business_stage_1'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACES STAGE 2 CLEAN - L1 #####

    task_id = 'business_stage_2_clean'
    table_name = 'business_stage_2'
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)

    task_start >> task
    task_start = task

    ###### BUSINESS STAGE 2 (business_key_lookup) - L1 #####

    task_id = 'business_stage_2'
    table_name = task_id
    task = ExperianKeyOperator(task_id=task_id,
                               source_sql=eval(task_id + '_sql'),
                               destination_project=jdp_project,
                               destination_dataset=jdp_dataset,
                               destination_table=f"{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                               experian_conn_id='ExperianDefault',
                               write_disposition='append',
                               # TODO should be "replace" but not compatible with partitioned tables
                               dag=dag)
    health_task = health_check(task_id, table_name, partition_field='DATE(date_created)')

    task_start >> task >> health_task
    task_start = health_task

    ###### BUSINESS KEY TABLE - L1 #####

    task_id = 'businessKey_table'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name, sql=health_template_sql_ex, )

    task_start >> task >> health_task
    task_start = health_task

    ###### BUSINESS MASTER - L1 #####

    task_id = 'business_master'
    table_name = task_id
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name)

    task_start >> task >> health_task
    task_start = health_task

    ###### INTERNAL MASTER - L2 #####

    task_id = 'loop2_internal_master'
    table_name = 'internal_master'
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name, loop_num=2)

    task_start >> task >> health_task
    task_start = health_task

    ###### PLACES MASTER - L2 #####

    task_id = 'loop2_places_master'
    table_name = 'places_master'  # needed when task_id doesn't match table name
    task = BigQueryOperator(task_id=task_id,
                            sql=eval(task_id + '_sql'),
                            destination_dataset_table=f"{jdp_project}.{jdp_dataset}.{table_name}${{{{ ds_nodash }}}}",
                            dag=dag)
    health_task = health_check(task_id, table_name, loop_num=2)

    task_start >> task >> health_task
    task_start = health_task

    task_end = EmptyOperator(task_id="task_end", dag=dag)
    task_end.set_upstream(task_start)
