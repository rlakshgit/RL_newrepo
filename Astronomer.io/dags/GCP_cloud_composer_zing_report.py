import logging
from datetime import date
import calendar
import time
import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
#from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
#from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceAsyncSensor

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
    'start_date': datetime(2022, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
#   'email': 'rjha@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

target_dataset = 'gld_zing_report'

with DAG(
        'Zing_Report',
        schedule_interval=None,
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_{dataset}'.format(dataset=target_dataset),
        dataset_id='{dataset}'.format(dataset=target_dataset.lower()),
        bigquery_conn_id=bq_gcp_connector)
		

    event_sql = """select
                            date(timestamp) as EventTime
                        ,count(distinct user_AuthenticatedId) as active_users

                    from `{project}.{dataset}.customEvents` as events
                    where
                        name in ("ProductActive-Appraisal",
                                    "ProductActive-PersonalLines",
                                    "ProductActive-CarePlan",
                                    "ProductActive-Shipping",
                                    "ProductActive-BusinessInsurance",
                                    "ProductActive-Marketplace",
                                    --"ProductActive-PartnerGateway",
                                    "ProductActive-JewelerMap",
                                    "ProductActive-ZingDashboard"
                                    )
                        and date(_PARTITIONTIME) >= '2021-01-01'
                        group by date(timestamp)""".format(project=bq_target_project, dataset='ref_appinsight')

    event_dag_load_data = BigQueryOperator(task_id='event_dag_load_data',
                                        sql=event_sql,
                                        destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                        project=bq_target_project,
                                        dataset=target_dataset,
                                        table='t_active_users',
                                        date = "{{ ds_nodash }}"
                                        ),
                                        time_partitioning={'type': 'DAY'},
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        use_legacy_sql=False)                                       									   

    page_view_sql = """select
                        date(timestamp) AS EventTime
                        ,count(*) as page_views

                    from `{project}.{dataset}.pageViews` as views
                    where
                            date(_PARTITIONTIME) >= '2021-01-01'
                            and not regexp_contains(url, r".*com/login.*")
                            group by date(timestamp)""".format(project=bq_target_project, dataset='ref_appinsight')

    pageViews_dag_load_data = BigQueryOperator(task_id='pageViews_dag_load_data',
                                        sql=page_view_sql,
                                        destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                        project=bq_target_project,
                                        dataset=target_dataset,
                                        table='t_page_views',
                                        date = "{{ ds_nodash }}"
                                        ),
                                        time_partitioning={'type': 'DAY'},
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        use_legacy_sql=False)

    unique_sessions_sql = """select
                            date(timestamp) as EventTime
                            ,count(distinct user_AuthenticatedId) as users
                            ,count(distinct session_Id) as sessions
                            ,count(*) as instances
                    from `{project}.{dataset}.pageViews`
                    where
                                    operation_SyntheticSource =""
                                    and date(_PARTITIONTIME) >= '2021-01-01'
                                    group by date(timestamp)""".format(project=bq_target_project, dataset='ref_appinsight')

    uniqueSessions_dag_load_data = BigQueryOperator(task_id='uniqueSessions_dag_load_data',
                                        sql=unique_sessions_sql,
                                        destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                        project=bq_target_project,
                                        dataset=target_dataset,
                                        table='t_unique_sessions',
                                        date = "{{ ds_nodash }}"
                                        ),
                                        time_partitioning={'type': 'DAY'},
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        use_legacy_sql=False)

    registered_users_sql = """WITH usr AS
    -- Get the User ID, Name, Conmpany Name From Person and Company Tables
    (
    SELECT DISTINCT p.ReferenceId
                ,p.FirstName
                ,p.LastName
                ,p.email
                ,c.name AS CompanyName
                ,CompanyId
                ,c._id
                ,DATE(p.CreatedOn) as CreatedOn
        FROM     `{project}.{dataset1}.t_membership_person`  p 
    INNER JOIN     `{project}.{dataset1}.t_membership_company` c
            ON      p.CompanyId = c._id
            AND     DATE(c._PARTITIONTIME) >= '2020-12-01'
        WHERE     c.name    IS NOT NULL
                AND DATE(p._PARTITIONTIME) >= '2020-12-01'
                AND LOWER(c.name) NOT IN ("duplicate","allstate","geico","eggliner","goosehead",'liberty mutual',"lovelace","none")
                AND p.IsDeleted = 'False'
                AND p.ReferenceId NOT IN
                        ( 
                            SELECT DISTINCT usr.ReferenceId 
                            FROM `{project}.{dataset1}.t_membership_person`  usr
                            WHERE 
                                (
                                    usr.email  LIKE '%test%'   
                                    OR usr.email LIKE '%@synechron.com%'
                                    OR usr.email LIKE '%zingtet%' 
                                    OR usr.email LIKE '%zing%' 
                                    OR usr.email LIKE '%jewelersmutual.com%'
                                    OR usr.email LIKE '%jmtestuser%' 
                                    OR usr.email IS NULL  
                                    OR usr.LastName IS NULL
                                    ) 
                                    AND (usr.email NOT LIKE ('%whitestone%') OR usr.email  NOT LIKE ('%statestreet%'))
                                    AND  usr.ReferenceId !='None' 
                                    AND  usr.ReferenceId IS NOT NULL
                            )
    ) --End of "usr"

    ,reg AS 
    --Get registered users from custom events (This table contains data from 2020 Dec)
    (
    SELECT            user_AuthenticatedId,ARRAY_AGG(name) AS services
    FROM              `{project}.{dataset}.customEvents` 
    WHERE         DATE(_PARTITIONTIME) >= '2020-12-01'
                AND name LIKE 'ProductRegistered%' 
                AND name NOT IN ('ProductRegistered-PartnerGateway')
    GROUP BY          user_AuthenticatedId
    ) --End of reg

    SELECT   CreatedOn 
            ,COUNT(DISTINCT  usr.email) cnt 
    FROM      usr 
    INNER     JOIN reg  
    ON        usr.ReferenceId = reg.user_AuthenticatedId
    WHERE     DATE(CreatedOn) >= '2020-12-01'
    GROUP BY  CreatedOn
    ORDER BY  CreatedOn""".format(project=bq_target_project, dataset='ref_appinsight',dataset1='ref_zing_membership')

    registeredUsers_dag_load_data = BigQueryOperator(task_id='registeredUsers_dag_load_data',
                                        sql=unique_sessions_sql,
                                        destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                        project=bq_target_project,
                                        dataset=target_dataset,
                                        table='t_registered_users',
                                        date = "{{ ds_nodash }}"
                                        ),
                                        time_partitioning={'type': 'DAY'},
                                        write_disposition='WRITE_TRUNCATE',
                                        create_disposition='CREATE_IF_NEEDED',
                                        gcp_conn_id=bq_gcp_connector,
                                        use_legacy_sql=False)
                                        
                                       
    create_dataset >> event_dag_load_data >> pageViews_dag_load_data >> uniqueSessions_dag_load_data >> registeredUsers_dag_load_data