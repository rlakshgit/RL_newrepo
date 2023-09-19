import logging
from datetime import date
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.GCS_File_Check import GCSFileCheck
from plugins.hooks.jm_gcs import GCSHook
from plugins.operators.jm_PostgresSQLtoBQ import PostgresToBQOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.models import Variable
from plugins.operators.jm_CompletionOperator import CompletionOperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceAsyncSensor

import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    prefix = 'DEV_'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    prefix = ''

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'bnagandla@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# dags to be in the sla & storage - entered through variable
try:
    dags = Variable.get('Dag_names')
except:
    Variable.set('Dag_names', "'BillingCenter_Prioritized_Tables','ClaimCenter_Prioritized_Tables','Hubspot_Gold','Mixpanel_dag','Mixpanel_People_dag','nice_gold','PolicyCenter_Prioritized_Tables','PLEcom_dag','PL_Personalization_dag','QuickQuote_Gold','ratabase_dag','experian_dag_local','CDP_dag'")
    dags = Variable.get('Dag_names')

# datasets & time intervals for each dag

try:
    app_dict = json.loads(Variable.get("dag_dict"))
except:
    Variable.set('dag_dict', json.dumps({"BillingCenter_Prioritized_Tables": {"dataset": "ref_bc_dbo,ref_bc_current", "dag_id": "BillingCenter_Prioritized_Tables"},"ClaimCenter_Prioritized_Tables": {"dataset": "ref_cc_dbo,ref_cc_current", "dag_id": "ClaimCenter_Prioritized_Tables"},"Hubspot_Gold": {"dataset": "ref_hubspot,gld_hubspot", "dag_id": "Hubspot_Gold"},"Mixpanel_dag": {"dataset": "ref_mixpanel,gld_mixpanel", "dag_id": "Mixpanel_dag"},"Mixpanel_People_dag": {"dataset": "ref_mixpanel_people", "dag_id": "Mixpanel_People_dag"},"nice_gold": {"dataset": "ref_nice,gld_nice", "dag_id": "nice_gold"},"PolicyCenter_Prioritized_Tables": {"dataset": "ref_pc_dbo,ref_pc_current", "dag_id": "PolicyCenter_Prioritized_Tables"},"PLEcom_dag": {"dataset": "ref_plecom_partner,ref_plecom_sqlpublish,ref_plecom_dbo,ref_plecom_policyservice,ref_plecom_quoteapp,ref_plecom_portal,ref_plecom_jewelerscut,ref_plecom_jewelerscut_current,ref_plecom_policyservice_current,ref_plecom_current,ref_plecom_portal_current", "dag_id": "PLEcom_dag"},"PL_Personalization_dag": {"dataset": "gld_pl_personalization", "dag_id": "PL_Personalization_dag"},"QuickQuote_Gold": {"dataset": "gld_quickquote", "dag_id": "QuickQuote_Gold"},"ratabase_dag": {"dataset": "ref_ratabase", "dag_id": "ratabase_dag"},"experian_dag_local": {"dataset": "ref_experian", "dag_id": "experian_dag_local"},"CDP_dag":{"dataset": "", "dag_id": "CDP_dag"}}))
    app_dict = json.loads(Variable.get("dag_dict"))

sources = app_dict.keys()

# dataset_dict = json.loads(Variable.get("dag_dataset_dict"))

# parameter for BQ storage data, for full load, it should be 1 = 1
try:
    BQ_metadata_param = Variable.get("bq_metadata_param_DL")
except:
    Variable.set('bq_metadata_param_DL', "partition_id = '{partition}'")
    BQ_metadata_param = Variable.get("bq_metadata_param_DL")

# parameter for sla data, for full load , it should be 1 = 1
try:
    sla_parm = Variable.get("sla_dag_run_id")
except:
    Variable.set("sla_dag_run_id", "run_id like 'scheduled__{runid}%'")
    sla_parm = Variable.get("sla_dag_run_id")

# Keeping this variable to get the required historical data to the sla_metrics data, it should be {{ds}} ideally, you can provide the date in YYYY-MM-DD format as start date
try:
    sla_metrics_start_date_parm = Variable.get("sla_metrics_start_date")
except:
    Variable.set("sla_metrics_start_date", "{{ds}}")
    sla_metrics_start_date_parm = Variable.get("sla_metrics_start_date")

try:
    Experian_file_Check = Variable.get("Experian_File_Check")
except:
    Variable.set("Experian_File_Check", "False")
    Experian_file_Check = Variable.get("Experian_File_Check")

target_dataset = 'dl_metrics'

file_location = '{prefix}l1/{source}/'.format(source="experian", prefix=prefix)

with DAG(
        'DL_Metrics',
        schedule_interval='0 6-20 * * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_{dataset}'.format(dataset=target_dataset),
        dataset_id='{dataset}'.format(dataset=target_dataset.lower()),
        bigquery_conn_id=bq_gcp_connector)
    Experian_check = GCSFileCheck(task_id = "Experian_check",
                                  project = project,
                                  bucket = base_bucket,
                                  google_cloud_storage_conn_id = base_gcp_connector,
                                  prefix_for_filename_search = file_location + "{{ ds_nodash }}")

    bq_metadata_sql = ""

    for source in sources:
        dataset = app_dict[source]["dataset"].split(",")
        #external_dag_id = app_dict[source]["dag_id"]
        #external_task_id = app_dict[source]["external_task_id"]
        #interval_hours = int(app_dict[source]["interval_hours"])
        #interval_minutes = int(app_dict[source]["interval_minutes"])
        source = source.lower()
        #task_sensor = ExternalTaskSensorAsync(
        #    task_id='check_{dag_name}'.format(dag_name=source),
        #    external_dag_id='{dag_name}'.format(dag_name=external_dag_id),
        #    external_task_id=external_task_id,
        #    execution_delta=timedelta(hours=interval_hours,minutes=interval_minutes),
        #    timeout=7200)

        for dataset_name in dataset:
            if dataset_name == "":
                continue
            else:
                bq_metadata_sql = str(bq_metadata_sql) + """select 
                                                            '{app}' as app,
                                                            case
                                                            when left(table_schema,4) = 'ref_' and right(table_schema,8) = '_current' then "refined_current"
                                                            when left(table_schema,4) = 'ref_' and right(table_schema,8) <> '_current' then "refined"
                                                            else "gold"
                                                            end as dataset_type,
                                                            table_schema as dataset,
                                                            partition_id,
                                                            parse_date('%Y%m%d',partition_id) as partition_date,
                                                            sum(total_rows) as count,
                                                            sum(total_logical_bytes) as bytes
                                                            from 
                                                            `{project}.{prefix}{dataset}.INFORMATION_SCHEMA.PARTITIONS` 
                                                            where {param}
                                                            group by 
                                                            table_schema,
                                                            partition_id""".format(app=source.upper(),
                                                                                   prefix = prefix,
                                                                                   project=bq_target_project,
                                                                                   dataset=dataset_name,
                                                                                   param=BQ_metadata_param.format(
                                                                                       partition="{{ds_nodash}}"))

                bq_metadata_sql = bq_metadata_sql + "\n\nUNION ALL\n\n"

    if Experian_file_Check==True:
        bq_metadata_sql = bq_metadata_sql + """Select "Experian" as app, 
                                                "refined" as dataset_type, 
                                                table_schema as dataset, 
                                                Coalesce(partition_id,cast({partition} as string)),
                                                parse_date('%Y%m%d',cast({partition} as string)) as partition_date,
                                                sum(total_rows) as count,
                                                sum(total_logical_bytes) as bytes 
                                                from 
                                                `{project}.{prefix}ref_experian.INFORMATION_SCHEMA.PARTITIONS`
                                                group by table_schema,partition_id""".format(prefix = prefix,
                                                                                             project=bq_target_project,
                                                                                             partition="{{ds_nodash}}")
    else:
        bq_metadata_sql = bq_metadata_sql[:-13]

    bq_metadata_sql_final = """
                            with metadata as
                            (""" + bq_metadata_sql + ")\n" + """select * from 
                            (select app,dataset_type,partition_date,count,bytes from metadata)
                            pivot( sum(count) as total_count, sum(bytes) as total_bytes for dataset_type in ("refined_current","refined","gold"))"""

    runid = str(date.today())[:-3]

    sla_sql = """select dag_id,execution_date,state,run_id,end_date,start_date from dag_run
                    where dag_id in ({dags})
                    and state <> 'running'
                    and {sla_param}""".format(dags=dags, sla_param=sla_parm.format(runid=str(runid)))

    dag_run_data = PostgresToBQOperator(task_id='sla_data',
                                     sql=sla_sql,
                                     project=bq_target_project,
                                     postgres_conn_id='airflow_db',
                                     destination_dataset=target_dataset,
                                     destination_table='t_dl_dag_runs',
                                     bigquery_conn_id=bq_gcp_connector)

    storage_data = BigQueryOperator(task_id='Storage_data',
                                    sql=bq_metadata_sql_final,
                                    destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                    project=bq_target_project,
                                    dataset=target_dataset,
                                    table='dl_bq_storage_metrics',
                                    date="{{ ds_nodash }}"
                                    ),
                                    time_partitioning={'type': 'DAY'},
                                    write_disposition='WRITE_TRUNCATE',
                                    create_disposition='CREATE_IF_NEEDED',
                                    gcp_conn_id=bq_gcp_connector,
                                    use_legacy_sql=False
                                    )

    dag_sla_sql = """SELECT *, 
                        CASE 
                        WHEN sla_notes = 'MET' then 'No-Delay'
                        when date(execution_date) = last_day(execution_date) then 'DELAYS DUE TO MONTHEND'
                        else 'longrunning/waiting on upstream dependencies'
                        end as sla_remarks
                        from 
                        (
                        SELECT 
                        dag_id,
                        execution_date,
                        state,
                        run_id,
                        end_date,
                        start_date,
                        case
                        when state = 'failed' then 'NOT MET DUE TO DAG FAILURE'
                        when dag_id = 'experian_dag_local' then 'MET'
                        when Time_diff(Time(end_date),Sla_Time,Minute) =0 then 'MET'
                        when Time_diff(Time(end_date),Sla_Time,Minute) > 0 and Time_diff(Time(end_date),Sla_Time,Minute) < 60 then 'LESS THAN 1 HOUR'
                        when Time_diff(Time(end_date),Sla_Time,Minute) > 60 and Time_diff(Time(end_date),Sla_Time,Minute) < 120 then 'DELAY OF 1-2 HOURS'
                        when Time_diff(Time(end_date),Sla_Time,Minute) > 120 and Time_diff(Time(end_date),Sla_Time,Minute) < 180 then 'DELAY OF 2-3 HOURS'
                        when Time_diff(Time(end_date),Sla_Time,Minute) > 180 and Time_diff(Time(end_date),Sla_Time,Minute) < 240 then 'DELAY OF 3-4 HOURS'
                        when Time_diff(Time(end_date),Sla_Time,Minute) > 240 then 'DELAY MORE THANS 5 HOURS'
                        else 'MET'
                        end as sla_notes
                        from `{project}.{dataset}.t_dl_dag_runs` dag_run
                        left outer join `{project}.{dataset}.dl_dag_sla` sla
                        on sla.Dag_Name = dag_run.Dag_id
                        where date(execution_date) >= DATE("{start_date}")
                        and date(execution_date) <= DATE("{end_date}")
                        )""".format(project=bq_target_project, dataset=target_dataset, start_date=sla_metrics_start_date_parm, end_date = "{{tomorrow_ds}}")

    dl_dag_sla_data = BigQueryOperator(task_id='dl_dag_sla_data',
                                       sql=dag_sla_sql,
                                       destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                           project=bq_target_project,
                                           dataset=target_dataset,
                                           table='dl_dag_sla_metrics',
                                           date = "{{ ds_nodash }}"
                                       ),
                                       time_partitioning={'type': 'DAY'},
                                       write_disposition='WRITE_TRUNCATE',
                                       create_disposition='CREATE_IF_NEEDED',
                                       gcp_conn_id=bq_gcp_connector,
                                       use_legacy_sql=False)



    create_dataset >> dag_run_data >> dl_dag_sla_data >> Experian_check >> storage_data
