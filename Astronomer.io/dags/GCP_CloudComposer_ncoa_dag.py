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
from plugins.operators.jm_ncoa_to_gcs import NCOAToGCSOperator
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SQLServertoBQ import SQLServertoBQOperator
from plugins.operators.jm_APIAuditOperator import APIAuditOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_ncoa_gld import NCOAAddressMatchOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow.models import Variable
import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

#variables section
AIRFLOW_ENV= Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    #base_bucket = 'jm_dev01_edl_lnd'
    #project = 'dev-edl'
    #base_gcp_connector = 'dev_edl'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = 'DEV_'
    mssql_connection_target = 'instDW_STAGE'

elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = 'B_QA_'
    mssql_connection_target = 'instDW_STAGE'

elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    folder_prefix = ''
    mssql_connection_target = 'instDW_PROD'

#Folder Details for landing and Dataset Details for refinement
source = 'ncoa'
source_abbr = 'ncoa'

base_gcs_folder=folder_prefix+'l1'
base_file_location = folder_prefix + 'l1/{source_abbr}/'.format(source_abbr=source_abbr)
normalize_file_location = folder_prefix + 'l1_norm/{source_abbr}/'.format(source_abbr=source_abbr)
base_audit_location = folder_prefix + 'l1_audit/{source_abbr}/'.format(source_abbr=source_abbr)
base_schema_location = folder_prefix + 'l1_schema/{source_abbr}/'.format(source_abbr=source_abbr)
refine_dataset = folder_prefix+'ref_{source}'.format(source=source)
refine_table = 't_{source}'.format(source=source)
gold_dataset = folder_prefix+'gld_{source}'.format(source=source)
matched_tablename = 't_ncoa_matched'
unmatched_tablename = 't_ncoa_unmatched'

#pc_ncoa dataset
pc_ncoa_dataset_name = folder_prefix+'ref_pc_ncoa'
pc_ncoa_table_name = 't_pc_ncoa'


#SQL query to create t_pc_ncoa table
sql='''use PolicyCenter;
       select distinct account.AccountNumber, 
       ProducerCodeOfService = ProducerCodeDetails.Code, 
       ProducerCodeOfServiceName = ProducerCodeDetails.SubagencyName_JMIC, 
       ProducerCodeDetails.ProductCode, 
       ProducerCodeOfRecord = ProducerCodeofRecordDetails.Code, 
       ProducerCodeOfRecordName = ProducerCodeofRecordDetails.SubagencyName_JMIC, 
       con.emailaddress1, 
       con.firstname, 
       con.lastname, 
       [FirstMailingAddressLine1] = PC_ADDRESS_MAILING_1.AddressLine1, 
       [FirstMailingAddressLine2] = PC_ADDRESS_MAILING_1.AddressLine2, 
       [FirstMailingAddressLine3] = PC_ADDRESS_MAILING_1.AddressLine3, 
       [FirstMailingAddressCity] = PC_ADDRESS_MAILING_1.City, 
       [FirstMailingAddressCounty] = PC_ADDRESS_MAILING_1.County, 
       [FirstMailingAddressState] = PCTL_STATE_MAILING_1.DESCRIPTION, 
       [FirstlkpMailingAddressGeogState] = PCTL_STATE_MAILING_1.TYPECODE, 
       [FirstMailingAddressPostalCode] = PC_ADDRESS_MAILING_1.PostalCode, 
       [FirstMailingAddressCountry] = CASE PCTL_COUNTRY_MAILING_1.TYPECODE WHEN 'US' THEN 'US' 
                                                                           WHEN 'CA' THEN 'CA' 
                                                                           ELSE PCTL_COUNTRY_MAILING_1.NAME END, 
       [SecondMailingAddressLine1] = PC_ADDRESS_MAILING_2.AddressLine1, 
       [SecondMailingAddressLine2] = PC_ADDRESS_MAILING_2.AddressLine2, 
       [SecondMailingAddressLine3] = PC_ADDRESS_MAILING_2.AddressLine3, 
       [SecondMailingAddressCity] = PC_ADDRESS_MAILING_2.City, 
       [SecondMailingAddressCounty] = PC_ADDRESS_MAILING_2.County, 
       [SecondMailingAddressState] = PCTL_STATE_MAILING_2.DESCRIPTION, 
       [SecondlkpMailingAddressGeogState] = PCTL_STATE_MAILING_2.TYPECODE, 
       [SecondMailingAddressPostalCode] = PC_ADDRESS_MAILING_2.PostalCode, 
       [SecondMailingAddressCountry] = CASE PCTL_COUNTRY_MAILING_2.TYPECODE WHEN 'US' THEN 'US' 
                                                                            WHEN 'CA' THEN 'CA' 
                                                                            ELSE PCTL_COUNTRY_MAILING_2.NAME END, 
       [ThirdMailingAddressLine1] = PC_ADDRESS_MAILING_3.AddressLine1, 
       [ThirdMailingAddressLine2] = PC_ADDRESS_MAILING_3.AddressLine2, 
       [ThirdMailingAddressLine3] = PC_ADDRESS_MAILING_3.AddressLine3, 
       [ThirdMailingAddressCity] = PC_ADDRESS_MAILING_3.City, 
       [ThirdMailingAddressCounty] = PC_ADDRESS_MAILING_3.County, 
       [ThirdMailingAddressState] = PCTL_STATE_MAILING_3.DESCRIPTION, 
       [ThirdlkpMailingAddressGeogState] = PCTL_STATE_MAILING_3.TYPECODE, 
       [ThirdMailingAddressPostalCode] = PC_ADDRESS_MAILING_3.PostalCode, 
       [ThirdMailingAddressCountry] = CASE PCTL_COUNTRY_MAILING_3.TYPECODE WHEN 'US' THEN 'US' 
                                                                           WHEN 'CA' THEN 'CA' 
                                                                           ELSE PCTL_COUNTRY_MAILING_3.NAME END, 
       A3.AddressLine1 as PrimaryMailingAddressLine1, 
       A3.AddressLine2 as PrimaryMailingAddressLine2, 
       A3.AddressLine3 as PrimaryMailingAddressLine3, 
       A3.city as PrimaryMailingAddressCity, 
       A3.county as PrimaryMailingAddressCounty, 
       pcstate.NAME as PrimaryMailingAddressState, 
       A3.PostalCode as PrimaryMailingAddressPostalCode, 
       pccountry.NAME as PrimaryMailingAddressCountry, 
       con.NameDenorm CompanyName 
  FROM pc_account account 
       inner join pc_accountcontact ac 
          on ac.Account >= account.id 
             AND ac.Account <= account.id 
       inner join pc_acctholderedge actcont 
          on actcont.OwnerID = account.ID 
       inner join pc_contact con 
          on ac.Contact = con.id 
             AND con.ID = actcont.ForeignEntityID 
       inner join PC_ACCOUNTCONTACTROLE acr 
          ON ac.ID = acr.AccountContact 
       inner join PCTL_AccountContactRole pctl_acr 
          ON acr.Subtype = pctl_acr.ID 
       LEFT OUTER JOIN PCTL_CONTACT 
         on CON.Subtype = PCTL_CONTACT.ID 
       LEFT OUTER JOIN (SELECT A1.AccountID, 
                               A1.ID, 
                               A1.OriginalEffectiveDate, 
                               A1.ProducerCodeOfServiceID, 
                               A1.ProductCode, 
                               D1.Code, 
                               D1.SubagencyName_JMIC 
                          FROM [PolicyCenter].[dbo].[pc_policy] A1 
                               INNER JOIN [PolicyCenter].[dbo].[pc_producercode] D1 
                                  ON A1.ProducerCodeOfServiceID = D1.ID 
                         WHERE A1.ID = (select MAX(ID) 
                                          FROM [PolicyCenter].[dbo].[pc_policy] B1 
                                         WHERE B1.AccountID = A1.AccountID 
                                           AND (B1.OriginalEffectiveDate = (select MAX(C1.OriginalEffectiveDate) 
                                                                              FROM [PolicyCenter].[dbo].[pc_policy] C1 
                                                                             WHERE C1.AccountID = A1.AccountID) 
                                                 OR B1.OriginalEffectiveDate is null))) ProducerCodeDetails 
         ON account.id = ProducerCodeDetails.AccountID 
       LEFT OUTER JOIN (SELECT A2.PolicyID, 
                               A2.ID, 
                               A2.ProducerCodeOfRecordID, 
                               D2.Code, 
                               D2.SubagencyName_JMIC 
                          FROM [PolicyCenter].[dbo].[pc_policyperiod] A2 
                               INNER JOIN [PolicyCenter].[dbo].[pc_producercode] D2 
                                  ON A2.ProducerCodeOfRecordID = D2.ID 
                         WHERE A2.ID = (select MAX(ID) 
                                          FROM [PolicyCenter].[dbo].[pc_policyperiod] B2 
                                         WHERE B2.PolicyID = A2.PolicyID 
                                           AND B2.UpdateTime = (SELECT MAX(UpdateTime) 
                                                                  FROM [PolicyCenter].[dbo].[pc_policyperiod] C2 
                                                                 WHERE C2.PolicyID = B2.PolicyID))) ProducerCodeofRecordDetails 
         ON ProducerCodeDetails.ID = ProducerCodeofRecordDetails.PolicyID 
       LEFT OUTER JOIN (SELECT PC_CONTACT1.ID ContactID, 
                               MAX(PC_ADDRESS1.ID) as MailingAddressID 
                          FROM PC_CONTACT PC_CONTACT1 
                               LEFT OUTER JOIN pc_contactaddress PC_CONTACTADDRESS1 
                                 on PC_CONTACT1.ID = PC_CONTACTADDRESS1.ContactID 
                               INNER JOIN pc_address PC_ADDRESS1 
                                  ON PC_CONTACTADDRESS1.AddressID = PC_ADDRESS1.ID 
                               INNER JOIN pctl_addresstype PCTL_ADDRESSTYPE1 
                                  ON PC_ADDRESS1.AddressType = PCTL_ADDRESSTYPE1.ID 
                         WHERE PCTL_ADDRESSTYPE1.TYPECODE = 'Mailing_FFR_JMIC' 
                         GROUP BY PC_CONTACT1.ID) FirstMailingAddress 
         on CON.ID = FirstMailingAddress.ContactID 
       LEFT OUTER JOIN PC_ADDRESS PC_ADDRESS_MAILING_1 
         ON FirstMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_1.ID 
       LEFT OUTER JOIN PCTL_STATE PCTL_STATE_MAILING_1 
         ON PC_ADDRESS_MAILING_1.State = PCTL_STATE_MAILING_1.ID 
       LEFT OUTER JOIN PCTL_COUNTRY PCTL_COUNTRY_MAILING_1 
         ON PC_ADDRESS_MAILING_1.Country = PCTL_COUNTRY_MAILING_1.ID 
       LEFT OUTER JOIN (SELECT PC_CONTACT2.ID ContactID, 
                               MAX(PC_ADDRESS2.ID) as MailingAddressID 
                          FROM PC_CONTACT PC_CONTACT2 
                               LEFT OUTER JOIN pc_contactaddress PC_CONTACTADDRESS2 
                                 on PC_CONTACT2.ID = PC_CONTACTADDRESS2.ContactID 
                               INNER JOIN pc_address PC_ADDRESS2 
                                  ON PC_CONTACT2.MailingAddress_JMIC = PC_ADDRESS2.ID 
                               INNER JOIN pctl_addresstype PCTL_ADDRESSTYPE2 
                                  ON PC_ADDRESS2.AddressType = PCTL_ADDRESSTYPE2.ID 
                         GROUP BY PC_CONTACT2.ID) SecondMailingAddress 
         on CON.ID = SecondMailingAddress.ContactID 
       LEFT OUTER JOIN PC_ADDRESS PC_ADDRESS_MAILING_2 
         ON SecondMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_2.ID 
       LEFT OUTER JOIN PCTL_STATE PCTL_STATE_MAILING_2 
         ON PC_ADDRESS_MAILING_2.State = PCTL_STATE_MAILING_2.ID 
       LEFT OUTER JOIN PCTL_COUNTRY PCTL_COUNTRY_MAILING_2 
         ON PC_ADDRESS_MAILING_2.Country = PCTL_COUNTRY_MAILING_2.ID 
       LEFT OUTER JOIN (SELECT PC_CONTACT3.ID ContactID, 
                               MAX(PC_ADDRESS3.ID) as MailingAddressID 
                          FROM PC_CONTACT PC_CONTACT3 
                               LEFT OUTER JOIN pc_contactaddress PC_CONTACTADDRESS3 
                                 on PC_CONTACT3.ID = PC_CONTACTADDRESS3.ContactID 
                               LEFT OUTER JOIN pc_address PC_ADDRESS3 
                                 on PC_CONTACTADDRESS3.AddressID = PC_ADDRESS3.ID 
                               INNER JOIN pctl_addresstype PCTL_ADDRESSTYPE3 
                                  ON PC_ADDRESS3.AddressType = PCTL_ADDRESSTYPE3.ID 
                         WHERE PCTL_ADDRESSTYPE3.TYPECODE = 'Mailing_JMIC' 
                         GROUP BY PC_CONTACT3.ID) ThirdMailingAddress 
         on CON.ID = ThirdMailingAddress.ContactID 
       LEFT OUTER JOIN PC_ADDRESS PC_ADDRESS_MAILING_3 
         ON ThirdMailingAddress.MailingAddressID = PC_ADDRESS_MAILING_3.ID 
       LEFT OUTER JOIN PCTL_STATE PCTL_STATE_MAILING_3 
         ON PC_ADDRESS_MAILING_3.State = PCTL_STATE_MAILING_3.ID 
       LEFT OUTER JOIN PCTL_COUNTRY PCTL_COUNTRY_MAILING_3 
         ON PC_ADDRESS_MAILING_3.Country = PCTL_COUNTRY_MAILING_3.ID 
       INNER JOIN dbo.pc_address A3 
          ON A3.id = con.PrimaryAddressID 
       INNER JOIN dbo.PCTL_ADDRESSTYPE atype 
          ON atype.id = A3.AddressType 
       INNER JOIN dbo.PCTL_STATE pcstate 
          ON pcstate.id = A3.State 
       INNER JOIN dbo.PCTL_COUNTRY pccountry 
          ON pccountry.ID = A3.Country 
 WHERE pctl_acr.ID in (2, 16) 
   AND acr.Subtype in (2, 16)'''


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,9,3),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        'NCOA_dag',
        schedule_interval='0 12 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    bit_set = CompletionOperator(task_id='set_source_bit_{source}'.format(source=source),
                                 source=source,
                                 mode='SET')

    create_dataset_ref = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_{source}'.format(source=source),
        dataset_id='{dataset}'.format(
            dataset=refine_dataset),
        bigquery_conn_id=bq_gcp_connector)


    try:
        NCOA_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
    except:
        Variable.set(source + '_gcs_to_bq', 'True')

    try:
        CHECK_HISTORY = Variable.get(source + '_check')
    except:
        Variable.set(source + '_check', 'True')
        CHECK_HISTORY = 'True'

    land_data = NCOAToGCSOperator(
        task_id='landing_data_{source}'.format(source=source),
        source_path='outgoing*',
        destination_bucket=base_bucket,
        destination_path = '{base_file_location}{date}/l1_data_{source}_{date}.json'.format(
                base_file_location=base_file_location,
                source = source,
                date="{{ds_nodash}}"),
        schema_file_path='{base_schema_location}{date}/l1_schema_{source}.json'.format(
            base_schema_location=base_schema_location,
            date="{{ds_nodash}}",
            source = source),

        sftp_conn_id='NCOA',
        gcp_conn_id=base_gcp_connector,
        mime_type = "application/json",
        metadata_file_name='{base_file_location}{date}/l1_metadata_{source}.json'.format(
                base_file_location=base_file_location,
                date="{{ds_nodash}}",
                source = source),
        norm_file_path='{base_norm_location}{date}/l1_norm_{source}.json'.format(
            base_norm_location=normalize_file_location,
            source=source,
            date="{{ds_nodash}}"),
        history_check=CHECK_HISTORY,
        source=source)



    NCOA_BQ_PROCESS = Variable.get(source + '_gcs_to_bq')
    if NCOA_BQ_PROCESS.upper() == 'TRUE':
        audit_data_lnd = APIAuditOperator(task_id='audit_l1_data_{source}'.format(source=source),
                                          bucket=base_bucket,
                                          project=bq_target_project,
                                          dataset=refine_dataset,
                                          base_gcs_folder=base_gcs_folder,
                                          target_gcs_bucket=base_bucket,
                                          google_cloud_storage_conn_id=base_gcp_connector,
                                          source_abbr=source_abbr,
                                          source=source,
                                          metadata_filename='{base_file_location}{date}/l1_metadata_{source}.json'.format(
                                              base_file_location=base_file_location,
                                              date="{ds_nodash}",
                                              source= source),
                                          audit_filename='{audit_filename}{date_nodash}/l1_audit_{source}.json'.format(
                                              audit_filename=base_audit_location,
                                              source= source,
                                              date_nodash="{ds_nodash}"),
                                          check_landing_only=True,
                                          table=refine_table)
    else:
        audit_data_lnd = EmptyOperator(task_id='health_{source}_lnd'.format(source=source))



    refine_data = GoogleCloudStorageToBigQueryOperator(
        task_id='refine_data_{source}'.format(source=source),
        bucket=base_bucket,
        source_objects=[
            '{base_norm_location}{date}/l1_norm_{source}.json'.format(
            base_norm_location=normalize_file_location,
            source = source,
            date="{{ ds_nodash }}")],
        destination_project_dataset_table='{project}.{dataset}.{table}${date}'.format(
            project=bq_target_project,
            dataset=refine_dataset,
            table=refine_table,
            date="{{ ds_nodash }}"),
        schema_fields=None,
        schema_object='{schema_location}{date}/l1_schema_{source}.json'.format(
            schema_location=base_schema_location,
            source=source,
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
        autodetect=False,
        gcs_to_bq=NCOA_BQ_PROCESS)


    if NCOA_BQ_PROCESS.upper() == 'TRUE':
        audit_data_refine = APIAuditOperator( task_id='audit_ref_data_{source}'.format(source=source),
                                       bucket=base_bucket,
                                       project=bq_target_project,
                                       dataset=refine_dataset,
                                       base_gcs_folder=base_gcs_folder,
                                       target_gcs_bucket=base_bucket,
                                       google_cloud_storage_conn_id = base_gcp_connector,
                                       source_abbr=source_abbr,
                                       source=source,
                                              metadata_filename='{base_file_location}{date}/l1_metadata_{source}.json'.format(
                                                  base_file_location=base_file_location,
                                                  source = source,
                                                  date="{ds_nodash}"),
                                              audit_filename='{audit_filename}{date_nodash}/l1_audit_{source}.json'.format(
                                                  audit_filename=base_audit_location,
                                                  source = source,
                                                  date_nodash="{ds_nodash}"),
                                       check_landing_only=False,
                                       table=refine_table)
    else:
        audit_data_refine = EmptyOperator(task_id='health_{source}_ref'.format(source=source))


    create_pc_ncoa_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_pc_ncoa',
        dataset_id=pc_ncoa_dataset_name,
        bigquery_conn_id=bq_gcp_connector)

    load_pc_ncoa_table = SQLServertoBQOperator(
        task_id='load_pc_ncoa_table',
        sql = sql,
        mssql_conn_id=mssql_connection_target,
        bigquery_conn_id=bq_gcp_connector,
        destination_dataset=pc_ncoa_dataset_name,
        destination_table=pc_ncoa_table_name,
        project=bq_target_project)

    create_dataset_gld = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_gld_dataset_{source}'.format(source=source),
        dataset_id='{dataset}'.format(
            dataset=gold_dataset),
        bigquery_conn_id=bq_gcp_connector)

    try:
        NCOA_ADDRESSMATCH_THRESHOLD = Variable.get(source + 'addressmatch_threshold')
    except:
        Variable.set(source + 'addressmatch_threshold', 60)

    gold_data = NCOAAddressMatchOperator(task_id='{source}_address_matching'.format(source=source),
                                   google_cloud_storage_conn_id=bq_gcp_connector,
                                   project=bq_target_project,
                                   source_dataset=refine_dataset,
                                   source_table=refine_table,
                                   destination_dataset=gold_dataset,
                                    matched_tablename=matched_tablename,
                                    unmatched_tablename=unmatched_tablename,
                                    adhoc_dataset=pc_ncoa_dataset_name,
                                    pc_table=pc_ncoa_table_name,
                                   threshold=NCOA_ADDRESSMATCH_THRESHOLD,
                                   ncoa_ref_to_gld = NCOA_BQ_PROCESS)

    [land_data >> audit_data_lnd >> create_dataset_ref >> refine_data >> audit_data_refine >>bit_set]>> create_dataset_gld  >> gold_data
    [create_pc_ncoa_dataset>>load_pc_ncoa_table] >> create_dataset_gld  >> gold_data


