####Libraries/Operators import
from airflow import DAG
from plugins.operators.jm_mssql_to_gcs_semimanaged import JM_MsSqlToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import configuration
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from plugins.operators.jm_SQLServertoBQ import SQLServertoBQOperator
import os
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from airflow.operators.empty import EmptyOperator

#####Initializing Variables ########
env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix = 'DEV_'
    # project = 'jm-dl-landing'
    prod_gcp_connector = 'dev_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_dev01_edl_lnd'


elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    dataset_prefix = 'B_QA_'
    # project = 'jm-dl-landing'
    prod_gcp_connector = 'qa_edl'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    folder_prefix = dataset_prefix
    mssql_connection = 'instDW_STAGE'
    trigger_bucket = 'jm_qa01_edl_lnd'


elif env.lower() == 'prod':
    base_bucket = 'semi-managed-reporting'
    dataset_prefix = ''
    # project = 'semi-managed-reporting'
    prod_gcp_connector = 'prod_edl'
    base_gcp_connector = 'semi_managed_gcp_connection'
    bq_gcp_connector = 'semi_managed_gcp_connection'
    bq_target_project = 'semi-managed-reporting'
    folder_prefix = ''
    mssql_connection = 'instDW_PROD'
    trigger_bucket = 'jm_prod_edl_lnd'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,12, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'tverner@jminsure.com',
    'retries': 6,
    'retry_delay': timedelta(minutes=30),
}

partition_date = '{{ macros.ds_format("2023-1-1","%Y-%m-%d", "%Y-%m-%d")}}'
load_date = '{{ ds }}'
risk_table = 'model-cl-im-risk-score-' + load_date

dataset_fy = 'financial_year'

################################################################################
# INSTANTIATE DAG TO RUN AT UTC 11AM (6AM CDT)
################################################################################

dag = DAG('semimanaged_CL_tracking_daily_dag_new',
          max_active_runs=1,
          schedule_interval='30 16 * * *',
          default_args=default_args,
          catchup=True
          )

create_fy_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='check_for_fy_dataset',
    dataset_id=dataset_fy,
    bigquery_conn_id=bq_gcp_connector,
    dag=dag)


## CL production report final query
file = 'cl_v2023_Tracking.sql'

cl_table_name = 'cl_tracking_daily'
with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/financial_year/{file}'.format(file=file))) as f:
    read_file_data = f.readlines()
    sql = "\n".join(read_file_data)
cl_sql = sql.format(ddate='{{ ds_nodash }}', project=bq_target_project, dataset=dataset_fy)
final_table_cl = BigQueryOperator(
    task_id='run_cl_tracking',
    sql=cl_sql,
    destination_dataset_table='{project}.{dataset}.{table}'.format(project=bq_target_project,
                                                                   dataset=dataset_fy,
                                                                   table=cl_table_name),
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)

# cl_ref_risk_score_prem
# creating cl_ref_risk_score_prem_to_gcs object with data and schema folder in semi_managed_reporting
with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/references_cl_insurance_new/cl_ref_risk_score_prem.sql'), 'r') as f:
    cl_ref_risk_score_prem_sql = f.read()

cl_ref_risk_score_prem_to_gcs = JM_MsSqlToGoogleCloudStorageOperator(
    task_id='cl_ref_risk_score_prem_to_gcs',
    sql=cl_ref_risk_score_prem_sql.format(load_date=load_date),
    bucket=base_bucket,
    filename=folder_prefix + 'cl_ref_risk_score_prem/data/{{ ds_nodash }}/export.json',
    schema_filename=None,
    mssql_conn_id=mssql_connection,
    google_cloud_storage_conn_id=base_gcp_connector,
    dag=dag
)

# creating table cl_ref_risk_score_prem_gcs_to_bq in financial_year dataset
cl_ref_risk_score_prem_gcs_to_bq = GCSToBigQueryOperator(
    task_id='cl_ref_risk_score_prem_gcs_to_bq',
    bucket=base_bucket,
    source_objects=[folder_prefix + 'cl_ref_risk_score_prem/data/{{ ds_nodash }}/export.json'],
    destination_project_dataset_table='{project}.{dataset}.cl_ref_risk_score_prem'.format(project=bq_target_project,
                                                                                          dataset=dataset_fy),
    schema_fields=[{"mode": "NULLABLE", "name": "PolicyNumber", "type": "STRING"}, {"mode": "NULLABLE", "name": "TermNumber", "type": "INTEGER"}, {"mode": "NULLABLE", "name": "LocationNumber", "type": "INTEGER"}, {"mode": "NULLABLE", "name": "Country", "type": "STRING"}, {"mode": "NULLABLE", "name": "PremiumWritten", "type": "FLOAT"}],
    schema_object=None,
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=0,
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    schema_update_options=(),
    dag=dag
)

# creating table  cl_ref_model_us_im_V3 in financial_year
with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/references_cl_insurance_new/cl_ref_model_us_im_V3.sql'), 'r') as f:
    bq_sql = f.read()

bq_sql = bq_sql.format(project = bq_target_project, risk_table=risk_table)

cl_ref_model_us_im_V3 = BigQueryOperator(
    task_id='cl_ref_model_us_im_V3',
    sql=bq_sql,
    destination_dataset_table='{project}.{dataset}.cl_ref_model_us_im_V3'.format(project=bq_target_project,
                                                                                 dataset=dataset_fy),
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)


# creating table  cl_riskgroup_im in financial_year

cl_riskgroup_im_file = 'cl_v2023_riskgroup_im.sql'

with open(os.path.join(configuration.get('core', 'dags_folder'),
                       r'semi-managed/references_cl_insurance_new/{file}'.format(file=cl_riskgroup_im_file)), 'r') as f:
    bq_sql = f.read()

bq_sql = bq_sql.format(project=bq_target_project, dataset=dataset_fy)

cl_riskgroup_im = BigQueryOperator(
    task_id='cl_riskgroup_im',
    sql=bq_sql,
    destination_dataset_table='{project}.{dataset}.cl_riskgroup_im'.format(project=bq_target_project,
                                                                                   dataset=dataset_fy),
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id=bq_gcp_connector,
    use_legacy_sql=False,
    dag=dag
)

check_upstream_completion = ExternalTaskSensorAsync(
            task_id='check_for_upstream_datapull_completion',
            external_dag_id='semimanaged_PL_tracking_daily_dag_new',
            external_task_id='task_end',
            execution_delta=timedelta(hours= 4.5),
            timeout=36000,
            dag = dag)


create_fy_dataset >> [cl_ref_risk_score_prem_to_gcs , cl_ref_model_us_im_V3]
cl_ref_risk_score_prem_to_gcs >> cl_ref_risk_score_prem_gcs_to_bq
[cl_ref_risk_score_prem_gcs_to_bq , cl_ref_model_us_im_V3] >> cl_riskgroup_im >> check_upstream_completion>> final_table_cl
