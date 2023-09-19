import logging
import datetime as dt
import calendar
import time
import json
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.operators.jm_mssql_to_gcs import MsSqlToGoogleCloudStorageOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLAuditOperator import SQLAuditOperator
from airflow.models import Variable
from plugins.operators.jm_CompletionOperator import CompletionOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# just get variable
AIRFLOW_ENV = Variable.get('ENV')


if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    destination_project = 'aquialift-dev'
    destination_gcp_connector = 'acquia_dev_edl'
    prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    mssql_connection_target = 'instDW_STAGE'
    base_gcp_connector = 'jm_landing_dev'
    destination_project = 'aquialift-dev'
    destination_gcp_connector = 'acquia_dev_edl'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    mssql_connection_target = 'instDW_PROD'
    base_gcp_connector = 'jm_landing_prod'
    destination_project = 'external-acquia-prod'
    destination_gcp_connector = 'acquia_prod_edl'
    bq_gcp_connector = 'prod_edl'
    bq_target_project = 'prod-edl'
    prefix = ''





#source_list = {'mixpanel': 'mixpanel_dag_local', 'mixpanel_people': 'mixpanel_people_dag_local_version_2'}
source='pl_personalization'
people_dataset = '{prefix}ref_{source}'.format(prefix=prefix, source='mixpanel_people')    # change dataset name
people_table = 't_mixpanel_people'
event_dataset = '{prefix}ref_{source}'.format(prefix=prefix, source='mixpanel')
event_table = 't_mixpanel_events_all'
gold_dataset = '{prefix}gld_pl_personalization'.format(prefix=prefix)   # change the dataset name
gold_table = 't_pl_personalization_profile'
dest_view = 'v_pl_personalization_profile'
audit_table = 't_pl_personalization_profile_audit'
required_table_list = 'pl_personalization_table_list'

#just get variables
try:
    PL_PERSONALIZATION_SOURCE_LIST = Variable.get(required_table_list).split(',')
except:
    Variable.set(required_table_list, "mixpanel:Mixpanel_dag,mixpanel_people:Mixpanel_People_dag")
    PL_PERSONALIZATION_SOURCE_LIST = Variable.get(required_table_list).split(',')

#PL_PERSONALIZATION_SOURCE_LIST = json.loads(PL_PERSONALIZATION_SOURCE_LIST)

# Creating DAG Object
with DAG(
        'PL_Personalization_dag',
        schedule_interval='0 7 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    # Task - create dataset in dev-edl for the pl personalization tables
    create_dataset_pl_personalization_gld = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gld_pl_personalization_dataset',
        dataset_id='{dataset}'.format(dataset=gold_dataset),
        bigquery_conn_id=bq_gcp_connector)

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')

    gcp_source_bq_hook = BigQueryHook(gcp_conn_id=bq_gcp_connector)
    # variable returns boolean value based on Table exist check
    PL_PERSONALIZATION_TABLE_EXIST_CHECK = gcp_source_bq_hook.table_exists(bq_target_project, gold_dataset, gold_table)

    # Full load Query
    sql_full_load = '''WITH CTE_EVENT 
            AS
            (
            SELECT DISTINCT
            distinct_id,
            event,
            time_central,
            CASE    ---  Logic to rank events
            WHEN event ="Quote Started" THEN 1
            WHEN event ="Quote Finished" THEN 2
            WHEN event ="Application Started" THEN 3
            WHEN event ="Application Saved" THEN 4
            WHEN event ="Application Finished" THEN 5
            END event_rank
            FROM `{project_name}.{source_dataset_name_event}.{source_table_name_event}` 
            WHERE event IN ("Application Finished","Application Saved","Application Started","Quote Finished","Quote Started") -- enent Filter
            AND 
            distinct_id NOT IN    -----Filtering agents or brokers 
            (
            SELECT distinct_id FROM `{project_name}.{source_dataset_name_event}.{source_table_name_event}`
            WHERE event in ("Application Finished")
            GROUP BY distinct_id ,event HAVING  COUNT(event)>1
            )
            ),
            CTE_PEOPLE
            as
            (
            SELECT DISTINCT
            distinct_id,
            acquialiftTrackingIds,
            acquisition_channel,
            last_seen,
            item_types ,
            item_values,
            item_values_new,
            case  ------Logic to find customer Type
            when num_rings=num_items THEN 'RING'
            When num_watches = num_items then 'WATCH'
            WHEN num_rings > 0 and num_watches > 0 then 'JEWELRY'
            WHEN num_items >4 and num_rings  != num_items and num_watches != num_items then 'JEWELRY'
            WHEN num_rings > 0 and num_watches = 0 and num_items <= 4 then 'RING'
            ELSE 'JEWELRY'
            end
            as customer_type
            FROM   ------Table Expression
            (SELECT 
            distinct_id,
            acquisition_channel,
            num_submitted_applications,
            last_seen,
            item_types ,
            item_values,
            LENGTH(item_types) - LENGTH(REGEXP_REPLACE(item_types, 'Ring', REPEAT(' ',3))) AS num_rings, ---calculated column to get number of Rings 
            LENGTH(item_types) - LENGTH(REGEXP_REPLACE(item_types, 'Watch', REPEAT(' ', 4))) AS num_watches, ---calculated column to get number of watches
            LENGTH(item_types) - LENGTH(REGEXP_REPLACE(item_types, ',', '')) + 1 AS num_items, ---- calculated column to get number of itemscount number of commas and add one = num_items
            RANK() OVER (PARTITION BY distinct_id ORDER BY last_seen DESC ) as Rank,  ---Rank distinct id by last_seen to filter max last_seen date for distinct_id
            (
                 SELECT format('%t',ARRAY_AGG( DISTINCT acquia_id))
                 FROM UNNEST(split(REPLACE(REPLACE(acquia_lift_ids,'[', ''), ']', ''), ',')) AS acquia_id  --Logic to get unique Acquia ID(unnest the list and get distinct Acquia id)
             )acquialiftTrackingIds,
             (
                 SELECT sum(case when item_values != 'None' and  item_values != 'nan' and item_values != '' then cast(item_values as int64) END)
                 FROM UNNEST(split(REPLACE(REPLACE(item_values,'[', ''), ']', ''), ',')) AS item_values  --Logic to get unique Acquia ID(unnest the list and get distinct Acquia id)
             )item_values_new
            FROM `{project_name}.{source_dataset_name_people}.{source_table_name_people}`
            WHERE  ( if(LENGTH(item_types)=0,'None',item_types) != 'None' AND replace(item_types,'nan','None')!='None' AND replace(item_types,"[u'']",'None')!='None') --item_type filter
            AND ( if(LENGTH(item_values)=0,'None',item_values) != 'None' AND replace(item_values,'nan','None')!= 'None' AND replace(item_values,"[u'']",'None')!='None' )--item_value filter
            AND (num_submitted_applications='1.0' OR replace(num_submitted_applications,'nan','None')='None') --- num_submitted_applications filter
            AND lower(acquisition_channel) IN ("web","express") ---acquisition_channel filter
            AND (acquia_lift_ids IS NOT NULL and acquia_lift_ids != 'nan' AND acquia_lift_ids != 'None') ---acquia_lift_id filter
            ) TEMP_TABLE
            WHERE TEMP_TABLE.rank=1 -- filter the table by max last_seen
            ),
            CTE_FINAL
            as
            (
            SELECT 
            PEOPLE.last_seen,
            #replace(acquialiftTrackingIds,"'",'')acquialiftTrackingIds,
            replace(acquialiftTrackingIds,"'",'')acquialiftTrackingIds,
            PEOPLE.distinct_id as distinctId ,
            EVENTS.event as lastStepCompleted,
            PEOPLE.item_types,
            PEOPLE.item_values,
            PEOPLE.item_values_new,
            PEOPLE.customer_type,
            PEOPLE.acquisition_channel as acquisitionChannel,
            max(EVENTS.time_central) as  eventTimeCentral
            FROM CTE_PEOPLE PEOPLE,unnest(split(REPLACE(REPLACE(PEOPLE.acquialiftTrackingIds,'[', ''), ']', ''), ', ')) AS acquialiftTrackingIds
            LEFT JOIN CTE_EVENT EVENTS on PEOPLE.distinct_id = EVENTS.distinct_id 
            WHERE EVENTS.event_rank = (SELECT MAX(event_rank) from CTE_EVENT WHERE distinct_id = EVENTS.distinct_id) 
            GROUP BY
            PEOPLE.last_seen,acquialiftTrackingIds,PEOPLE.distinct_id,EVENTS.event,PEOPLE.item_types,PEOPLE.item_values,PEOPLE.item_values_new,PEOPLE.customer_type,PEOPLE.acquisition_channel)
            ,cte_distinctacquiaID
            as
            (
            select *, rank() over (partition by acquialiftTrackingIds order by eventTimeCentral desc) acquia_rank  from cte_final
            )
            select 
            ID.acquialiftTrackingIds as acquialiftTrackingId,
            ID.distinctId,
            ID.lastStepCompleted,
            ID.item_types as itemTypes,
            ID.item_values as itemValues,
            ID.item_values_new as itemValueTotal,
            ID.customer_type as itemTypeCategory,
            ID.acquisitionChannel,
            ID.eventTimeCentral,
            ID.last_seen as lastSeen ,
            DATE('{date}') as load_bq_date
            from cte_distinctacquiaID ID where acquia_rank=1'''.format(
        project_name=bq_target_project,
        source_dataset_name_event=event_dataset,
        source_dataset_name_people=people_dataset,
        source_table_name_event=event_table,
        source_table_name_people=people_table,
        date="{{ ds }}"
    )

    # incremental load Query
    sql_incremental_load = '''{sql} AND DATE(ID.last_seen) >='{SD}' and DATE(ID.last_seen) <'{ED}' '''.format(
        sql=sql_full_load, SD="{{ds}}", ED="{{next_ds}}")

    # create personalization gld layer table in dev-edl
    pl_personalization_gld_table = BigQueryOperator(task_id='create_gld_layer_pl_personalization_table',
                                                    sql=sql_incremental_load if PL_PERSONALIZATION_TABLE_EXIST_CHECK else sql_full_load,
                                                    destination_dataset_table='{project}.{dataset}.{table_partition}${date}'.format(
                                                        project=bq_target_project,
                                                        dataset=gold_dataset,
                                                        table_partition=gold_table,
                                                        date="{{ ds_nodash }}"
                                                    ),
                                                    write_disposition='WRITE_TRUNCATE',
                                                    gcp_conn_id=bq_gcp_connector,
                                                    use_legacy_sql=False,
                                                    time_partitioning={'type': 'DAY', 'field': 'load_bq_date'},
                                                    # schema_update_options=('ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION')
                                                    )

    # Audit pl_personalization query
    # audit_gld_pl_personalization_sql = '''select SAFE_CAST(DATE("{start_date}") as TIMESTAMP)  as Run_Date, count(*) count from  `{project_name}.{source_dataset_name}.{source_table_name}`'''.format(
    #     project_name=bq_target_project,
    #     source_dataset_name=gold_dataset,
    #     source_table_name=gold_table,
    #     start_date="{{ds}}"
    # )
    # task - creates audit table in dev-edl project. Counts the number of records in pl personalization gold layer table and write it to BigQuery table.
    # audit_pl_personalization_gld_table = BigQueryOperator(task_id='create_audit_gld_pl_personalization_table',
    #                                                       sql=audit_gld_pl_personalization_sql,
    #                                                       destination_dataset_table='{project}.{dataset}.{table_partition}${date}'.format(
    #                                                           project=bq_target_project,
    #                                                           dataset=gold_dataset,
    #                                                           table_partition=audit_table,
    #                                                           date="{{ ds_nodash }}"
    #                                                       ),
    #                                                       write_disposition='WRITE_TRUNCATE',
    #                                                       bigquery_conn_id=bq_gcp_connector,
    #                                                       use_legacy_sql=False,
    #                                                       time_partitioning={'type': 'DAY', 'field': 'Run_Date'},
    #                                                       # schema_update_options=('ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION'),
    #                                                       dag=dag)

    create_dataset_in_acquia = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gld_pl_personalization_dataset_in_acquia',
        dataset_id='{dataset}'.format(dataset=gold_dataset),
        bigquery_conn_id=destination_gcp_connector)

    view_pl_personalization_sql = '''  CREATE VIEW IF NOT EXISTS 
        `{destination_project}.{dataset}.{view_name}`
        AS
                            SELECT 
                            acquialiftTrackingId,
                            distinctId,
                            lastStepCompleted,
                            itemTypes,
                            itemTypeCategory,
                            itemValueTotal,
                            acquisitionChannel,
                            eventTimeCentral, 
                            lastSeen,
                            load_bq_date as loadBQDate
                            FROM `{project}.{dataset}.{table}` '''.format(project=bq_target_project,
                                                                          destination_project=destination_project,
                                                                          dataset=gold_dataset,
                                                                          table=gold_table,
                                                                          view_name=dest_view)

    create_view_in_aquia = BigQueryOperator(task_id='create_pl_personalization_view',
                                            sql=view_pl_personalization_sql,
                                            gcp_conn_id=destination_gcp_connector,
                                            use_legacy_sql=False)

    for item in PL_PERSONALIZATION_SOURCE_LIST:
        item_split = item.split(':')
        check_data_source = ExternalTaskSensorAsync(
            task_id='check_{source}_data'.format(source=item_split[0]),
            external_dag_id=item_split[1],
            external_task_id='audit_ref_data_{source}'.format(source=item_split[0]),
            execution_delta=timedelta(hours=1),
            timeout=7200) #timeout in seconds

        [check_data_source >> create_dataset_pl_personalization_gld >> pl_personalization_gld_table  >>create_dataset_in_acquia>> create_view_in_aquia>>bit_set]
