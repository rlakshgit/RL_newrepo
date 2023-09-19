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
from plugins.operators.jm_hubspot_search_to_gcs import HubspotSearchToGoogleCloudStorageOperator
from plugins.operators.jm_hubspot_api_batch_to_gcs import HubspotAPIToGoogleCloudStorageOperator
from plugins.operators.jm_hubspot_api_detail_to_gcs import HubspotDetailToGoogleCloudStorageOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.models import Variable


import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

##
AIRFLOW_ENV = Variable.get('ENV')



if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='dev_edl'
    bq_target_project = 'dev-edl'
    prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector='qa_edl'
    bq_target_project = 'qa-edl'
    prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector='prod_edl'
    prefix = ''
    mssql_connection_target = 'instDW_PROD'

####
source = 'Hubspot'
source_abbr = 'hubspot'

l1_storage_location = '{prefix}l1/{source}/'.format(prefix=prefix,source=source_abbr)
l2_storage_location = '{prefix}l2/{source}/'.format(prefix=prefix,source=source_abbr)
offset_storage_location = '{prefix}offset/{source}/'.format(prefix=prefix,source=source_abbr)
metadata_storage_location = '{prefix}metadata/{source}/'.format(prefix=prefix,source=source_abbr)
schema_storage_location = '{prefix}schema/{source}/'.format(prefix=prefix,source=source_abbr)
audit_storage_location = '{prefix}audit/{source}/'.format(prefix=prefix,source=source_abbr) 

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': True,
    'email': 'mkadhim@jminsure.com',
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

completed=[]

## Move thses 2 lists to external file or env vars
hubspotAPIs = ['contacts', 'emailEvents','campaigns','marketingEmails','emailSubscriptionTypes']
huspotSearchAPIs = ['contacts','marketingEmails','emailSubscriptionTypes']
hubspotAPIsWithDetail = ['contacts_details_updates']
hubspotAPIsWithDetail_single = ['campaigns_details_updates']


confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'hubspot', r'ini', r"hubspot_ingestion_airflow_config_update.ini")
with open(confFilePath, 'r') as configFile:
    confJSON = json.loads(configFile.read())


with DAG(
        'Hubspot_Landing_DAG',
        schedule_interval= '0 6 * * *', #"@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


    for hub_api in hubspotAPIs:
        try:
            HUBAPI_BQ_PROCESS = Variable.get(hub_api + '_gcs_to_bq')
        except:
            Variable.set(hub_api + '_gcs_to_bq', 'True')


        try:
            CHECK_HISTORY = Variable.get(hub_api + '_check')
        except:
            Variable.set(hub_api + '_check', 'True')
            CHECK_HISTORY = 'True'

        if hub_api in huspotSearchAPIs:
            land_data = HubspotSearchToGoogleCloudStorageOperator(task_id='land_data_{source}_{api_name}'.format(source=source_abbr,api_name=hub_api),
                                                                api_connection_id='HubspotAPI',
                                                                api_name=hub_api,
                                                                api_configuration=confJSON,
                                                                google_cloud_storage_conn_id=base_gcp_connector,
                                                                l1_storage_location=l1_storage_location,
                                                                l2_storage_location=l2_storage_location,
                                                                offset_storage_location=offset_storage_location,
                                                                schema_storage_location=schema_storage_location,
                                                                metadata_storage_location=metadata_storage_location,
                                                                api_key_encryped=True,
                                                                target_gcs_bucket=base_bucket,
                                                                target_env=AIRFLOW_ENV.upper(),
                                                                history_check = CHECK_HISTORY,
                                                                metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                                                                location_base=metadata_storage_location,
                                                                api_name=hub_api,
                                                                date="{ds_nodash}"),
                                                                file_prefix=prefix,
                                                                #pool='hubspot-pool'
                                                                )
                                        
            landing_audit_check = APIAuditOperator(
                task_id='landing_audit_{source}_{api_name}'.format(source=source_abbr, api_name=hub_api),
                project=project,
                dataset='{prefix}ref_{source_abbr}'.format(prefix=prefix, source_abbr=source_abbr),
                table='t_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=hub_api),
                source=source,
                source_abbr=source_abbr,
                base_gcs_folder=None,
                target_gcs_bucket=base_bucket,
                bucket=base_bucket,
                google_cloud_storage_conn_id=base_gcp_connector,
                check_landing_only=True,
                metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                    location_base=metadata_storage_location,
                    api_name=hub_api,
                    date="{ds_nodash}"),
                audit_filename='{location_base}{api_name}/{date}/l1_audit_{source}_{api_name}.json'.format(
                    location_base=audit_storage_location,
                    source=source_abbr.lower(),
                    api_name=hub_api,
                    date="{ds_nodash}",
                    prefix=prefix)
                )

        else:
            land_data = HubspotSearchToGoogleCloudStorageOperator(task_id='land_data_{source}_{api_name}'.format(source=source,api_name=hub_api),
                                                                api_connection_id='HubspotAPI',
                                                                api_name=hub_api,
                                                                api_configuration=confJSON,
                                                                google_cloud_storage_conn_id=base_gcp_connector,
                                                                l1_storage_location=l1_storage_location,
                                                                l2_storage_location=l2_storage_location,
                                                                offset_storage_location=offset_storage_location,
                                                                schema_storage_location=schema_storage_location,
                                                                metadata_storage_location=metadata_storage_location,
                                                                api_key_encryped=True,
                                                                target_gcs_bucket=base_bucket,
                                                                target_env=AIRFLOW_ENV.upper(),
                                                                history_check = CHECK_HISTORY,
                                                                metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                                                                location_base=metadata_storage_location,
                                                                api_name=hub_api,
                                                                date="{ds_nodash}"),
                                                                file_prefix=prefix,
                                                                #pool='hubspot-pool'
                                                                )   

            landing_audit_check = APIAuditOperator(
                task_id='landing_audit_{source}_{api_name}'.format(source=source_abbr, api_name=hub_api),
                project=project,
                dataset='{prefix}ref_{source_abbr}'.format(prefix=prefix, source_abbr=source_abbr),
                table='t_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=hub_api),
                source=source,
                source_abbr=source_abbr,
                base_gcs_folder=None,
                target_gcs_bucket=base_bucket,
                bucket=base_bucket,
                google_cloud_storage_conn_id=base_gcp_connector,
                check_landing_only=True,
                metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                    location_base=metadata_storage_location,
                    api_name=hub_api,
                    date="{ds_nodash}"),
                audit_filename='{location_base}{api_name}/{date}/l1_audit_{source}_{api_name}.json'.format(
                    location_base=audit_storage_location,
                    source=source_abbr.lower(),
                    api_name=hub_api,
                    date="{ds_nodash}",
                    prefix=prefix)
                )


        detail_api = hub_api + '_details_updates'
        if detail_api in hubspotAPIsWithDetail:
        

            land_details = HubspotAPIToGoogleCloudStorageOperator(task_id='land_details_{source}_{detail_api}'.format(source=source,api_name=detail_api,detail_api=detail_api),
                                                                api_connection_id='HubspotAPI',
                                                                api_name=detail_api,
                                                                api_configuration=confJSON,
                                                                google_cloud_storage_conn_id=base_gcp_connector,
                                                                l1_storage_location=l1_storage_location,
                                                                l2_storage_location=l2_storage_location,
                                                                offset_storage_location=offset_storage_location,
                                                                schema_storage_location=schema_storage_location,
                                                                metadata_storage_location=metadata_storage_location,
                                                                api_key_encryped=True,
                                                                target_gcs_bucket=base_bucket,
                                                                target_env=AIRFLOW_ENV.upper(),
                                                                history_check = CHECK_HISTORY,
                                                                metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                                                                location_base=metadata_storage_location,
                                                                api_name=detail_api,
                                                                date="{ds_nodash}"),
                                                                file_prefix=prefix,
                                                                #pool='hubspot-pool'
                                                                )
            landing_details_audit_check = APIAuditOperator(
            task_id='landing_details_audit_{source}_{api_name}'.format(source=source_abbr, api_name=detail_api),
            project=project,
            dataset='{prefix}ref_{source_abbr}'.format(prefix=prefix, source_abbr=source_abbr),
            table='t_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=detail_api),
            source=source,
            source_abbr=source_abbr,
            base_gcs_folder=None,
            target_gcs_bucket=base_bucket,
            bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            check_landing_only=True,
            metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                location_base=metadata_storage_location,
                api_name=detail_api,
                date="{ds_nodash}"),
            audit_filename='{location_base}{api_name}/{date}/l1_audit_{source}_{api_name}.json'.format(
                location_base=audit_storage_location,
                source=source_abbr.lower(),
                api_name=detail_api,
                date="{ds_nodash}",
                prefix=prefix)
            )

            end_task = EmptyOperator(task_id='End_Task_{detail_api}'.format(detail_api=detail_api))

        elif detail_api in hubspotAPIsWithDetail_single:

            land_details = HubspotDetailToGoogleCloudStorageOperator(task_id='land_details_{source}_{detail_api}'.format(source=source,api_name=detail_api,detail_api=detail_api),
                                                                    api_connection_id='HubspotAPI',
                                                                    api_name=detail_api,
                                                                    parent_api=hub_api,
                                                                    api_configuration=confJSON,
                                                                    google_cloud_storage_conn_id=base_gcp_connector,
                                                                    l1_storage_location=l1_storage_location,
                                                                    l2_storage_location=l2_storage_location,
                                                                    offset_storage_location=offset_storage_location,
                                                                    schema_storage_location=schema_storage_location,
                                                                    metadata_storage_location=metadata_storage_location,
                                                                    api_key_encryped=True,
                                                                    target_gcs_bucket=base_bucket,
                                                                    target_env=AIRFLOW_ENV.upper(),
                                                                    history_check = CHECK_HISTORY,
                                                                    metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                                                                    location_base=metadata_storage_location,
                                                                    api_name=detail_api,
                                                                    date="{ds_nodash}"),
                                                                    file_prefix=prefix,
                                                                    #pool='hubspot-pool'
                                                                    )


            landing_details_audit_check = APIAuditOperator(
            task_id='landing_details_audit_{source}_{api_name}'.format(source=source_abbr, api_name=detail_api),
            project=project,
            dataset='{prefix}ref_{source_abbr}'.format(prefix=prefix, source_abbr=source_abbr),
            table='t_{source_abbr}_{table}'.format(source_abbr=source_abbr, table=detail_api),
            source=source,
            source_abbr=source_abbr,
            base_gcs_folder=None,
            target_gcs_bucket=base_bucket,
            bucket=base_bucket,
            google_cloud_storage_conn_id=base_gcp_connector,
            check_landing_only=True,
            metadata_filename='{location_base}{api_name}/{date}/l1_metadata_data_hubspot_{api_name}.json'.format(
                location_base=metadata_storage_location,
                api_name=detail_api,
                date="{ds_nodash}"),
            audit_filename='{location_base}{api_name}/{date}/l1_audit_{source}_{api_name}.json'.format(
                location_base=audit_storage_location,
                source=source_abbr.lower(),
                api_name=detail_api,
                date="{ds_nodash}",
                prefix=prefix)
            )

            end_task = EmptyOperator(task_id='End_Task_{detail_api}'.format(detail_api=detail_api))

            
        
        else:
            land_details = EmptyOperator(task_id='NO_DETAIL_{detail_api}'.format(detail_api=detail_api))
            landing_details_audit_check = EmptyOperator(task_id='NO_DETAIL_AUDIT_{detail_api}'.format(detail_api=detail_api))
            end_task = EmptyOperator(task_id='End_Task_{detail_api}'.format(detail_api=hub_api))
        
   

   

        land_data  >> landing_audit_check >> land_details >> landing_details_audit_check >> end_task 



