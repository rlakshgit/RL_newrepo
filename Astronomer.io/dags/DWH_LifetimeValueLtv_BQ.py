import logging
import datetime as dt
import calendar
import time

from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
# from plugins.operators.jm_PolicyProfileOperator import PolicyProfileGetAccountDataMssql,PolicyProfileGetAccountListMssql
# from plugins.operators.jm_PolicyProfileOperator import PolicyProfileScoreGetData,PolicyProfileScoreData
# from plugins.operators.jm_PolicyProfileOperator import PolicyProfileFactorBuild,PolicyProfileSupportFileScoreBuild
from plugins.operators.jm_PolicyProfileOperator import LifeTimeValueGetAccountDataMssql,LifeTimeValueSupportFileScoreBuild
# from plugins.operators.jm_CreatePoolOperator import CreatePoolOperator,DeletePoolOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_gcs_trigger_file_create import GoogleCloudStorageTriggerFileCreateOperator

ts = calendar.timegm(time.gmtime())
logging.info(ts)



default_dag_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" : "alangsner@jminsure.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}




AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    folder_prefix = 'DEV_'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    target_bucket = 'jm_dev01_edl_lnd'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    mssql_connection_target = 'instDW_STAGE'
    folder_prefix = 'QA_'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    target_bucket = 'jm_qa01_edl_lnd'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    mssql_connection_target = 'instDW_PROD'
    folder_prefix = ''
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    target_bucket = 'jm_prod_edl_lnd'




source = 'LTV_LTV'
source_abbr = 'ltv_ltv'
chunk_counts = 100
score_chunk_counts = 250
base_file_location = folder_prefix + 'l1/models/{source}/'.format(source=source)
normalize_file_location = folder_prefix + 'l1_norm/models/{source}/'.format(source=source)
base_data_file_location = folder_prefix + 'l1_norm/base_data/{source}/'.format(source=source)
score_file_location = folder_prefix + 'l1_norm/models/{source}_score_data/'.format(source=source)
base_audit_location = folder_prefix + 'l1_audit/models/{source}/'.format(source=source)
base_schema_location = folder_prefix + 'l1_schema/models/{source}/'.format(source=source)



with DAG(
        'lifetime_value_ltv',
        schedule_interval= '0 20 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

        ##

        create_trigger_file = GoogleCloudStorageTriggerFileCreateOperator(task_id='create_trigger_file'
                                                     , bucket_name=target_bucket
                                                     , objects=['triggers/outbound/ltv_complete.json'
                                                                ]
                                                     , google_cloud_storage_conn_id=bq_gcp_connector)

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id='{prefix}ref_model_{source_abbr}'.format(
                source_abbr=source_abbr.lower(), prefix=folder_prefix),
            bigquery_conn_id=bq_gcp_connector)

        chunk = 0
        ltv_get_data = LifeTimeValueGetAccountDataMssql(
                task_id='ltv_get_data_chunk_{chunk}'.format(chunk=chunk),
                mssql_conn_id=mssql_connection_target,
                mssql_three_part_detail='[{server}].[{database}].[{table}]'.format(server='DW_INT',database='MarketingEmail',table='MarketingContact'),
                schema_filename=base_schema_location + '{ds_nodash}/ltv_ltv_schema.json',
                filename=base_file_location + '{ds_nodash}/ltv_base_data.json',
                google_cloud_storage_conn_id=base_gcp_connector,
                bucket=base_bucket,
                chunk_count=chunk_counts,
                chunk_value = chunk,
                extracted_data=normalize_file_location + '{ds_nodash}/ltv_ltv_data_',
                #pool='policyprofile-pool'
                )

        lifetime_value_data_load = GoogleCloudStorageToBigQueryOperator(
                task_id='lifetime_value_data_load',
                bucket=base_bucket,
                source_objects=[
                    '{normalize_file_location}{ds_nodash}/ltv_ltv_data_*'.format(normalize_file_location=normalize_file_location,ds_nodash="{{ ds_nodash }}")],
                destination_project_dataset_table='{project}.{prefix}ref_model_{source_abbr}.lifetime_value_data'.format(
                    project=bq_target_project,
                    prefix=folder_prefix,
                    source_abbr=source_abbr.lower(),
                    date="{{ ds_nodash }}"),
                schema_fields=[], 
				schema_object=base_schema_location + '{ds_nodash}/ltv_ltv_schema.json'.format(ds_nodash="{{ ds_nodash }}"),
                source_format='NEWLINE_DELIMITED_JSON',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=1,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
				autodetect=False,
               # autodetect=True,
                gcs_to_bq='True')

        ltv_classification_sql_step_one = '''select *,
                            CASE
                              WHEN BirthYear is Null THEN -1
                              WHEN (EXTRACT(MONTH from CURRENT_DATE()) - BirthMonth) < 1 THEN (EXTRACT(YEAR from CURRENT_DATE()) - BirthYear)
                              ELSE (EXTRACT(YEAR from CURRENT_DATE()) - BirthYear)+1
                            END as age,
                            CASE
                            WHEN State = "Alabama" THEN "AL"
                            WHEN State = "Alaska" THEN "AK"
                            WHEN State = "Arizona" THEN "AZ"
                            WHEN State = "Arkansas" THEN "AR"
                            WHEN State = "California" THEN "CA"
                            WHEN State = "Colorado" THEN "CO"
                            WHEN State = "Connecticut" THEN "CT"
                            WHEN State = "Delaware" THEN "DE"
                            WHEN State = "Florida" THEN "FL"
                            WHEN State = "Georgia" THEN "GA"
                            WHEN State = "Hawaii" THEN "HI"
                            WHEN State = "Idaho" THEN "ID"
                            WHEN State = "Illinois" THEN "IL"
                            WHEN State = "Indiana" THEN "IN"
                            WHEN State = "Iowa" THEN "IA"
                            WHEN State = "Kansas" THEN "KS"
                            WHEN State = "Kentucky" THEN "KY"
                            WHEN State = "Louisiana" THEN "LA"
                            WHEN State = "Maine" THEN "ME"
                            WHEN State = "Maryland" THEN "MD"
                            WHEN State = "Massachusetts" THEN "MA"
                            WHEN State = "Michigan" THEN "MI"
                            WHEN State = "Minnesota" THEN "MN"
                            WHEN State = "Mississippi" THEN "MS"
                            WHEN State = "Missouri" THEN "MO"
                            WHEN State = "Montana" THEN "MT"
                            WHEN State = "Nebraska" THEN "NE"
                            WHEN State = "Nevada" THEN "NV"
                            WHEN State = "New Hampshire" THEN "NH"
                            WHEN State = "New Jersey" THEN "NJ"
                            WHEN State = "New Mexico" THEN "NM"
                            WHEN State = "New York" THEN "NY"
                            WHEN State = "North Carolina" THEN "NC"
                            WHEN State = "North Dakota" THEN "ND"
                            WHEN State = "Ohio" THEN "OH"
                            WHEN State = "Oklahoma" THEN "OK"
                            WHEN State = "Oregon" THEN "OR"
                            WHEN State = "Pennsylvania" THEN "PA"
                            WHEN State = "Rhode Island" THEN "RI"
                            WHEN State = "South Carolina" THEN "SC"
                            WHEN State = "South Dakota" THEN "SD"
                            WHEN State = "Tennessee" THEN "TN"
                            WHEN State = "Texas" THEN "TX"
                            WHEN State = "Utah" THEN "UT"
                            WHEN State = "Vermont" THEN "VT"
                            WHEN State = "Virginia" THEN "VA"
                            WHEN State = "Washington" THEN "WA"
                            WHEN State = "West Virginia" THEN "WV"
                            WHEN State = "Wisconsin" THEN "WI"
                            WHEN State = "Wyoming" THEN "WY"
                            WHEN State = "Alberta" THEN "AB"
                            WHEN State = "British Columbia" THEN "BC"
                            WHEN State = "Manitoba" THEN "MB"
                            WHEN State = "New Brunswick" THEN "NB"
                            WHEN State = "Newfoundland" THEN "NL"
                            WHEN State = "Labrador" THEN "NL"
                            WHEN State = "Newfoundland and Labrador" THEN "NL"
                            WHEN State = "Nova Scotia" THEN "NS"
                            WHEN State = "Northwest Territories" THEN "NT"
                            WHEN State = "Nunavut" THEN "NU"
                            WHEN State = "Ontario" THEN "ON"
                            WHEN State = "Prince Edward Island" THEN "PE"
                            WHEN State = "Quebec" THEN "QC"
                            WHEN State = "Saskatchewan" THEN "SK"
                            WHEN State = "Yukon" THEN "YT"
                            ELSE "USA:Other"
                            END as state_abbr
                            FROM `{project}.{prefix}ref_model_{source_abbr}.lifetime_value_data`
                            '''.format(project=bq_target_project,prefix=folder_prefix,source_abbr=source_abbr.lower())
        ltv_classification_step_one = BigQueryOperator(task_id='ltv_classification_step_one',
                                                     sql=ltv_classification_sql_step_one,
                                                     destination_dataset_table ='{project}.{prefix}ref_model_{source_abbr}.ltv_classification_step_1'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                           source_abbr=source_abbr.lower()),
                                                     write_disposition='WRITE_TRUNCATE',
                                                     create_disposition='CREATE_IF_NEEDED',
                                                     gcp_conn_id=bq_gcp_connector,
                                                     allow_large_results=True,
                                                     use_legacy_sql=False)


        ltv_classification_sql_step_two = '''select *,
                                    CASE
                                      WHEN age = -1 THEN "MISS"
                                      WHEN age >=18 and age <= 24 THEN "18-24"
                                      WHEN age >=25 and age <= 29 THEN "25-29"
                                      WHEN age >=30 and age <= 39 THEN "30-39"
                                      WHEN age >=40 and age <= 49 THEN "40-49"
                                      WHEN age >=50 and age <= 59 THEN "50-59"
                                      WHEN age >=60 THEN "60+"
                                      ELSE "MISS"
                                    END as age_bin,
                                    CASE
                                      WHEN age = -1 THEN 99
                                      WHEN age >=18 and age <= 24 THEN 1
                                      WHEN age >=25 and age <= 29 THEN 2
                                      WHEN age >=30 and age <= 39 THEN 3
                                      WHEN age >=40 and age <= 49 THEN 4
                                      WHEN age >=50 and age <= 59 THEN 5
                                      WHEN age >=60 THEN 6
                                      ELSE 99
                                    END as age_label,
                                    CASE
                                        WHEN state_abbr = "FL" THEN "FL"
                                        WHEN state_abbr = "CA" THEN "CA"
                                        WHEN state_abbr = "NY" THEN "NY"
                                        WHEN state_abbr = "TX" THEN "TX"
                                        WHEN state_abbr = "NJ" THEN "NJ"
                                        WHEN state_abbr = "LA" THEN "LA"
                                        WHEN state_abbr = "UT" THEN "Mountain"
                                        WHEN state_abbr = "CO" THEN "Mountain"
                                        WHEN state_abbr = "AZ" THEN "Mountain"
                                        WHEN state_abbr = "NM" THEN "Mountain"
                                        WHEN state_abbr = "MT" THEN "Mountain"
                                        WHEN state_abbr = "WY" THEN "Mountain"
                                        WHEN state_abbr = "MI" THEN "MidWest"
                                        WHEN state_abbr = "IL" THEN "MidWest"
                                        WHEN state_abbr = "WI" THEN "MidWest"
                                        WHEN state_abbr = "MN" THEN "MidWest"
                                        WHEN state_abbr = "NE" THEN "MidWest"
                                        WHEN state_abbr = "IN" THEN "MidWest"
                                        WHEN state_abbr = "IA" THEN "MidWest"
                                        WHEN state_abbr = "SD" THEN "MidWest"
                                        WHEN state_abbr = "ND" THEN "MidWest"
                                        WHEN state_abbr = "PA" THEN "NorthEast"
                                        WHEN state_abbr = "MD" THEN "NorthEast"
                                        WHEN state_abbr = "MA" THEN "NorthEast"
                                        WHEN state_abbr = "CT" THEN "NorthEast"
                                        WHEN state_abbr = "VA" THEN "NorthEast"
                                        WHEN state_abbr = "OH" THEN "NorthEast"
                                        WHEN state_abbr = "KY" THEN "NorthEast"
                                        WHEN state_abbr = "RI" THEN "NorthEast"
                                        WHEN state_abbr = "NH" THEN "NorthEast"
                                        WHEN state_abbr = "DE" THEN "NorthEast"
                                        WHEN state_abbr = "DC" THEN "NorthEast"
                                        WHEN state_abbr = "ME" THEN "NorthEast"
                                        WHEN state_abbr = "WV" THEN "NorthEast"
                                        WHEN state_abbr = "VT" THEN "NorthEast"
                                        WHEN state_abbr = "MO" THEN "Plains"
                                        WHEN state_abbr = "KS" THEN "Plains"
                                        WHEN state_abbr = "OK" THEN "Plains"
                                        WHEN state_abbr = "AR" THEN "Plains"
                                        WHEN state_abbr = "MS" THEN "Plains"
                                        WHEN state_abbr = "GA" THEN "SouthEast"
                                        WHEN state_abbr = "NC" THEN "SouthEast"
                                        WHEN state_abbr = "SC" THEN "SouthEast"
                                        WHEN state_abbr = "AL" THEN "SouthEast"
                                        WHEN state_abbr = "TN" THEN "SouthEast"
                                        WHEN state_abbr = "AP" THEN "SouthEast"
                                        WHEN state_abbr = "AE" THEN "SouthEast"
                                        WHEN state_abbr = "PR" THEN "SouthEast"
                                        WHEN state_abbr = "VI" THEN "SouthEast"
                                        WHEN state_abbr = "WA" THEN "West"
                                        WHEN state_abbr = "NV" THEN "West"
                                        WHEN state_abbr = "OR" THEN "West"
                                        WHEN state_abbr = "HI" THEN "West"
                                        WHEN state_abbr = "ID" THEN "West"
                                        WHEN state_abbr = "AK" THEN "West"
                                        WHEN state_abbr = "ON" THEN "Canada"
                                        WHEN state_abbr = "QC" THEN "Canada"
                                        WHEN state_abbr = "NS" THEN "Canada"
                                        WHEN state_abbr = "NB" THEN "Canada"
                                        WHEN state_abbr = "MB" THEN "Canada"
                                        WHEN state_abbr = "BC" THEN "Canada"
                                        WHEN state_abbr = "PE" THEN "Canada"
                                        WHEN state_abbr = "SK" THEN "Canada"
                                        WHEN state_abbr = "AB" THEN "Canada"
                                        WHEN state_abbr = "NL" THEN "Canada"
                                        WHEN state_abbr = "YT" THEN "Canada"
                                        WHEN state_abbr = "NU" THEN "Canada"
                                        WHEN state_abbr = "USA:Other" THEN "USA:Other"
                                    ELSE "USA:Other"
                                    END as state_abbr_classifier,
                                    CASE
                                        WHEN NumberOfItemsInsured > 1 THEN
                                            CASE
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 6000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 10000 THEN "6K-10K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 10000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 16000 THEN "10K-16K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 16000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 25000 THEN "16K-25K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 25000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 40000 THEN "25K-40K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 40000 THEN "40K+"
                                                ELSE "0-6K"
                                            END
                                        ELSE
                                            CASE
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 3000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 4500 THEN "3K-4.5K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 4500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 6000 THEN "4.5K-6K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 6000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 7500 THEN "6K-7.5K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 7500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 10000 THEN "7.5K-10K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 10000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 12500 THEN "10K-12.5K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 12500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 15000 THEN "12.5K-15K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 15000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 20000 THEN "15K-20K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 20000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 25000 THEN "20K-25K"
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 25000 THEN "25K+"
                                                ELSE "0-3K"
                                            END
                                    END as agg_total_value_bin,
                                    CASE
                                        WHEN NumberOfItemsInsured > 1 THEN
                                            CASE
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 6000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 10000 THEN 12
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 10000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 16000 THEN 13
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 16000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 25000 THEN 14
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 25000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 40000 THEN 15
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 40000 THEN 16
                                                ELSE 11
                                            END
                                        ELSE
                                            CASE
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 3000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 4500 THEN 2
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 4500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 6000 THEN 3
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 6000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 7500 THEN 4
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 7500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 10000 THEN 5
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 10000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 12500 THEN 6
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 12500 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 15000 THEN 7
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 15000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 20000 THEN 8
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 20000 and CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  <= 25000 THEN 9
                                                WHEN CAST(REPLACE(JewelryValue, 'None', '0') AS FLOAT64)  > 25000 THEN 10
                                                ELSE 1
                                            END
                                    END as agg_total_value_label,
                                    CASE
                                        WHEN NumberOfItemsInsured = 5 THEN 4
                                        WHEN NumberOfItemsInsured > 6 THEN 6
                                        ELSE NumberOfItemsInsured
                                    END as number_of_items,
                                    (SELECT MAX(CAST(InterceptScore as FLOAT64)) FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_intercept_value`) as intercept_value,
                                    FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_classification_step_1`
                                    '''.format(project=bq_target_project, prefix=folder_prefix, source_abbr=source_abbr.lower())
        ltv_classification_step_two = BigQueryOperator(task_id='ltv_classification_step_two',
                                                       sql=ltv_classification_sql_step_two,
                                                       destination_dataset_table='{project}.{prefix}ref_model_{source_abbr}.ltv_classification_step_2'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                           source_abbr=source_abbr.lower()),
                                                       write_disposition='WRITE_TRUNCATE',
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       gcp_conn_id=bq_gcp_connector,
                                                       allow_large_results=True,
                                                       use_legacy_sql=False)

        ltv_score_support_data = LifeTimeValueSupportFileScoreBuild(task_id='ltv_score_support_data',
                                                                google_cloud_bq_conn_id=bq_gcp_connector,
                                                                bucket=base_bucket,
                                                                dataset='{prefix}ref_model_{source_abbr}'.format(
                                                                        source_abbr=source_abbr.lower(), prefix=folder_prefix))

        ltv_calculate_scoring_sql = '''SELECT
                                                    a.*,
                                                    ROUND(COALESCE(b.xbeta_float,0),3) AS age_xbeta,
                                                    ROUND(COALESCE(c.xbeta_float,0),3) AS item_xbeta,
                                                    ROUND(COALESCE(d.xbeta_float,0),3) AS policy_xbeta,
                                                    ROUND(COALESCE(e.xbeta_float,0),3) AS state_xbeta,
                                                    (ROUND(COALESCE(b.xbeta_float,0),3) + ROUND(COALESCE(c.xbeta_float,0),3) + ROUND(COALESCE(d.xbeta_float,0),3) + ROUND(COALESCE(e.xbeta_float,0),3)) as sum_xbeta,
                                                    (ROUND(COALESCE(b.xbeta_float,0),3) + ROUND(COALESCE(c.xbeta_float,0),3) + ROUND(COALESCE(d.xbeta_float,0),3) + ROUND(COALESCE(e.xbeta_float,0),3)+ a.intercept_value)  as xbeta_adj,
                                                    EXP((ROUND(COALESCE(b.xbeta_float,0),3) + ROUND(COALESCE(c.xbeta_float,0),3) + ROUND(COALESCE(d.xbeta_float,0),3) + ROUND(COALESCE(e.xbeta_float,0),3)+ a.intercept_value))  as rank_value
                                            FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_classification_step_2` a
                                            LEFT JOIN (SELECT *,CAST(Min_Index as INT64) as min_index_int,CAST(xbeta as FLOAT64) as xbeta_float  FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_age_xbeta` ) b
                                            ON a.age_label = min_index_int
                                            LEFT JOIN (SELECT *,CAST(Min_Value as INT64) as min_value_int,CAST(xbeta as FLOAT64) as xbeta_float  FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_item_count_xbeta`) c
                                            ON a.number_of_items = min_value_int
                                            LEFT JOIN (SELECT *,CAST(Min_Index as INT64) as min_index_policy_amount_int,CAST(xbeta as FLOAT64) as xbeta_float  FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_policy_amount_xbeta`) d
                                            ON a.agg_total_value_label = min_index_policy_amount_int
                                            LEFT JOIN (SELECT *,CAST(xbeta as FLOAT64) as xbeta_float  FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_state_xbeta`) e
                                            ON a.state_abbr = e.Code
                                            '''.format(project=bq_target_project, prefix=folder_prefix,
                                                       source_abbr=source_abbr.lower())
        ltv_calculate_scoring = BigQueryOperator(task_id='ltv_calculate_scoring',
                                                       sql=ltv_calculate_scoring_sql,
                                                       destination_dataset_table='{project}.{prefix}ref_model_{source_abbr}.ltv_calculate_scoring'.format(
                                                           project=bq_target_project,
                                                           prefix=folder_prefix,
                                                           source_abbr=source_abbr.lower()),
                                                       write_disposition='WRITE_TRUNCATE',
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       gcp_conn_id=bq_gcp_connector,
                                                       allow_large_results=True,
                                                       use_legacy_sql=False)

        ltv_decile_scoring_sql = '''SELECT *,
                                        CASE
                                        WHEN number_of_items = 0 THEN 0
                                        ELSE lifetime_value_decile_rank
                                        END as lifetime_value_score
                                    FROM (SELECT
                                             a.*,
                                             CAST(b.rank as INT64) as lifetime_value_decile_rank
                                             FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_calculate_scoring` a
                                             INNER JOIN (SELECT *,CAST(Min as FLOAT64) as Min_float, CAST(Max as FLOAT64) as Max_float FROM `{project}.{prefix}ref_model_{source_abbr}.ltv_decile_rankings`  ) b
                                             ON (a.rank_value between b.Min_float and b.Max_float))
                                    '''.format(project=bq_target_project, prefix=folder_prefix,
                                                               source_abbr=source_abbr.lower())
        ltv_decile_scoring = BigQueryOperator(task_id='ltv_decile_scoring',
                                                 sql=ltv_decile_scoring_sql,
                                                 destination_dataset_table='{project}.{prefix}ref_model_{source_abbr}.ltv_decile_score'.format(
                                                     project=bq_target_project,
                                                     prefix=folder_prefix,
                                                     source_abbr=source_abbr.lower()),
                                                 write_disposition='WRITE_TRUNCATE',
                                                 create_disposition='CREATE_IF_NEEDED',
                                                 gcp_conn_id=bq_gcp_connector,
                                                 allow_large_results=True,
                                                 use_legacy_sql=False)

        create_dataset >> ltv_get_data >> lifetime_value_data_load >> ltv_classification_step_one>>ltv_score_support_data>> ltv_classification_step_two>>ltv_calculate_scoring>>ltv_decile_scoring >> create_trigger_file

        #Previous timing average is 102 mins
        #new timing 12 mins