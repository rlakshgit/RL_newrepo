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
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_ExperianStagingToL1 import ExperianStagingToL1
from plugins.operators.jm_ExperianSchemaGen import ExperianSchemaGen
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from plugins.operators.jm_ExperianSFTPtoGCS import ExperianSFTPtoGCS
from plugins.operators.jm_ExperianAuditOperator import ExperianAuditOperator
from airflow.operators.bash_operator import BashOperator
from plugins.operators.jm_CreatePoolOperator import CreatePoolOperator, DeletePoolOperator
from airflow.models import Variable

import json

ts = calendar.timegm(time.gmtime())
logging.info(ts)

env = Variable.get('ENV')

if env.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    env_suffix = '_wip'
    base_gcp_connector = 'jm_landing_dev'
    # prefix = 'GCP_ClComp_'
    prefix = 'DEV_'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    ref_target_project = 'dev-edl'
    platform_project = 'jm-dl-landing'
    platform_project_connector = 'jm_landing_dev'

elif env.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    env_suffix = '_wip'
    base_gcp_connector = 'jm_landing_dev'
    prefix = 'B_QA_'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    ref_target_project = 'qa-edl'
    platform_project = 'jm-dl-landing'
    platform_project_connector = 'jm_landing_dev'
elif env.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    env_suffix = ''
    base_gcp_connector = 'jm_landing_prod'
    prefix = ''
    bq_target_project = 'prod-edl'
    ref_target_project = 'semi-managed-reporting'
    bq_gcp_connector = 'prod_edl'
    platform_project = 'internal-platform-experian'
    platform_project_connector = 'internal_platform_experian'

source = 'Experian'
source_abbr = 'experian'
base_file_location = '{prefix}staging/{source}/'.format(source=source_abbr, prefix=prefix)
base_landing_file_location = '{prefix}l1/{source}/'.format(source=source_abbr, prefix=prefix)
base_norm_file_location = '{prefix}l1_norm/{source}/'.format(source=source_abbr, prefix=prefix)
base_schema_location = '{prefix}l1_schema/{source}/'.format(source=source_abbr, prefix=prefix)
base_audit_location = '{prefix}l1_audit/{source}/'.format(source=source_abbr, prefix=prefix)
dataset_references_experian = prefix + 'references_experian'
dataset_ref_experian = prefix + 'ref_experian'

#### TO BE FIXED ONCE IN CLOUD COMPOSER WITH THE APPROPRIATE SA
# target_project = 'dev-edl'
# target_prefix = 'bl_'
# target_gcp_conn_id = 'dev_edl'

target_project = project
target_prefix = prefix
target_gcp_conn_id = base_gcp_connector

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'alangsner@jminsure.com',
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}

completed = []

# confFilePath = os.path.join(configuration.get('core', 'dags_folder'), r'hubspot', r'ini', r"hubspot_ingestion_airflow_config.ini")
# with open(confFilePath, 'r') as configFile:
#     confJSON = json.loads(configFile.read())


with DAG(
        'experian_dag_local',
        schedule_interval='0 13 * * *',  # "@daily",#dt.timedelta(days=1), #
        catchup=True,
        default_args=default_dag_args) as dag:
    experian_pool_creation = CreatePoolOperator(task_id='experian_pool_creation',
                                                name='experian-pool',
                                                slots=3,
                                                description='Experian Pool')
    # create_pool = CreatePoolOperator('experian_pool',1,'Pool for Experian processing.')

    if env.upper() == 'PROD':
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr),
            dataset_id='{prefix}ref_{source_abbr}{env_suffix}'.format(
                source_abbr=source_abbr.lower(), prefix=prefix, env_suffix=env_suffix),
            bigquery_conn_id=bq_gcp_connector)

        refine_audit_check = ExperianAuditOperator(task_id='refine_audit_check',
                                                   google_cloud_storage_conn_id=base_gcp_connector,
                                                   bucket=base_bucket,
                                                   metadata_filename=base_audit_location + "{{ ds_nodash }}",
                                                   landing_file_location=base_file_location + "{{ ds_nodash }}",
                                                   check_landing_only=False,
                                                   google_cloud_bq_config={'project': bq_target_project,
                                                                           'dataset': '{prefix}ref_{source_abbr}{env_suffix}'.format(
                                                                               prefix=prefix,
                                                                               source_abbr=source_abbr.lower(),
                                                                               env_suffix=env_suffix),

                                                                           'table': 'experian_current_brick',
                                                                           'source_abbr': source_abbr})

        refine_data = GoogleCloudStorageToBigQueryOperator(
            task_id='refine_data_{source}'.format(source=source_abbr),
            bucket=base_bucket,
            source_objects=[
                '{base_file_location}{date}/P*'.format(base_file_location=base_landing_file_location,
                                                       date="{{ ds_nodash }}", )],
            destination_project_dataset_table='{project}.{prefix}ref_{source_abbr}{env_suffix}.experian_current_brick'.format(
                project=bq_target_project,
                prefix=prefix,
                source_abbr=source_abbr.lower(),
                env_suffix=env_suffix,
                date="{{ ds_nodash }}"),
            # schema_fields=schema,
            schema_object=base_schema_location + "{{ ds_nodash }}/experian_schema.json",
            source_format='NEWLINE_DELIMITED_JSON',
            # compression='GZIP',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=0,
            bigquery_conn_id=bq_gcp_connector,
            google_cloud_storage_conn_id=base_gcp_connector,
            # schema_update_options=(),#['ALLOW_FIELD_ADDITION','ALLOW_FIELD_RELAXATION'],
            # src_fmt_configs=None,
            autodetect=True,
            gcs_to_bq='True')

    else:
        create_dataset = EmptyOperator(
            task_id='check_for_dataset_{source}'.format(source=source_abbr))

        refine_audit_check = EmptyOperator(task_id='refine_audit_check')

        refine_data = EmptyOperator(
            task_id='refine_data_{source}'.format(source=source_abbr))

    source_file_list = 'experian_file_list'
    try:
        SOURCE_FILE_LIST = Variable.get(source_file_list).split(',')
    except:
        Variable.set(source_file_list, 'test_1,test_2')
        SOURCE_FILE_LIST = Variable.get(source_file_list).split(',')

    try:
        BRICK_PRESENT = Variable.get('experian_brick_present')
    except:
        Variable.set('experian_brick_present', 'True')

    experian_landing = ExperianSFTPtoGCS(task_id='staging_data_{source}'.format(source=source_abbr),
                                         source_path='/from_xpn/*',
                                         destination_bucket=base_bucket,
                                         destination_path=base_file_location + "{{ ds_nodash }}",
                                         gcp_conn_id=base_gcp_connector,
                                         sftp_conn_id='ExperianSFTP',
                                         audit_location=base_audit_location + "{{ ds_nodash }}")

    experian_staging_to_landing_filelist = ExperianStagingToL1(
        task_id='staging_to_landing_file_list_{source}'.format(source=source_abbr),
        project=project,
        get_file_list=True,
        source=source,
        source_abbr=source_abbr,
        target_gcs_bucket=base_bucket,
        base_gcs_folder=base_file_location + "{{ ds_nodash }}",
        google_cloud_storage_conn_id=base_gcp_connector,
        prefix_for_filename_search=base_file_location + "{{ ds_nodash }}")

    schema_file_gen = ExperianSchemaGen(task_id='generate_schema',
                                        project=project,
                                        source=source,
                                        source_abbr=source_abbr,
                                        target_gcs_bucket=base_bucket,
                                        google_cloud_storage_conn_id=base_gcp_connector,
                                        prefix_for_filename_search=base_landing_file_location + "{{ ds_nodash }}",
                                        schema_filename=base_schema_location + "{{ ds_nodash }}/experian_schema.json")


    landing_audit_check = ExperianAuditOperator(task_id='landing_audit_check',
                                                google_cloud_storage_conn_id=base_gcp_connector,
                                                bucket=base_bucket,
                                                metadata_filename=base_audit_location + "{{ ds_nodash }}",
                                                landing_file_location=base_file_location + "{{ ds_nodash }}",
                                                check_landing_only=True)


    BRICK_PRESENT = Variable.get('experian_brick_present')
    print('Brick Present Check --- ', BRICK_PRESENT)
    BRICK_PRESENT = 'FALSE'
    if BRICK_PRESENT.upper() == 'TRUE':
        archive_sql = '''
                        SELECT
                           *,DATE('{date}') as load_bq_date
                        FROM
                          `{project}.{prefix}ref_experian.experian_current_brick` A
                        INNER JOIN (
                          SELECT
                            DISTINCT experian_business_id_lkup
                          FROM
                            `{target_project}.{target_prefix}ref_experian.lkup_experian_cl_cust_bin`
                          WHERE
                            experian_business_id_lkup	!= "-1"
                            AND experian_business_id_lkup	 != "-2") B
                        ON
                          A.EXPERIAN_BUSINESS_ID = B.experian_business_id_lkup
                    '''.format(date="{{ ds }}", project=project, prefix=prefix, target_project=target_project,
                               target_prefix=target_prefix),
        archive_previous_brick = BigQueryOperator(task_id='archive_previous_brick',
                                                  sql=archive_sql,
                                                  destination_dataset_table="{project}.{prefix}ref_experian.experian_archive${date}".format(
                                                      project=target_project, prefix=target_prefix,
                                                      date="{{ ds_nodash }}"),
                                                  write_disposition='WRITE_TRUNCATE',
                                                  allow_large_results=True,
                                                  gcp_conn_id=target_gcp_conn_id,
                                                  use_legacy_sql=False,
                                                  create_disposition='CREATE_IF_NEEDED',
                                                  time_partitioning={'type': 'DAY', 'field': 'load_bq_date'})


    else:
        # refine_data = EmptyOperator(task_id='refine_data_{source}'.format(source=source_abbr))
        archive_previous_brick = EmptyOperator(task_id='archive_previous_brick')


    source_file_list = 'experian_file_list'
    SOURCE_FILE_LIST = Variable.get(source_file_list).split(',')
    print(SOURCE_FILE_LIST)

    with open(os.path.join(configuration.get('core', 'dags_folder'), r'Experian/Experian_lpad.sql'), 'r') as f:
        bq_sql = f.read()
        bq_sql = bq_sql.format(project=bq_target_project, dataset=dataset_ref_experian)

        # create table experian_current_brick in dataset dataset_ref_experian
        experian_current_brick = BigQueryOperator(
            task_id='experian_current_brick',
            sql=bq_sql,
            destination_dataset_table='{project}.{dataset}.experian_current_brick'.format(project=bq_target_project,
                                                                                          dataset=dataset_ref_experian),
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=bq_gcp_connector,
            use_legacy_sql=False
        )
        
    # creating dataset create_ref_experian_dataset
    create_ref_experian_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_ref_experian_dataset',
        dataset_id=dataset_ref_experian,
        bigquery_conn_id=bq_gcp_connector)
    
    # creating dataset create_references_experian_dataset
    create_references_experian_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_references_experian_dataset',
        dataset_id=dataset_references_experian,
        bigquery_conn_id=bq_gcp_connector)
    
    load_date = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m") }}'

    ##with open(os.path.join(configuration.get('core', 'dags_folder'), r'experian/experian_brick.sql'),'r') as f:
    ##    bq_sql = f.read()
    ##
    ##    bq_sql = bq_sql.format(project=bq_target_project, dataset = dataset_references_experian)

    # creating table experian_brick in dataser_ref_experian
    experian_brick = BigQueryToBigQueryOperator(
        task_id='experian_brick',
        source_project_dataset_tables='{project}.{dataset}.experian_current_brick'.format(project=bq_target_project,
                                                                                          dataset=dataset_ref_experian),
        destination_project_dataset_table='{project}.{dataset}.experian_brick_{load_date}'.format(
            project=ref_target_project, dataset=dataset_references_experian, load_date=load_date),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=bq_gcp_connector,
        # use_legacy_sql=False
        )

    experian_brick_current = BigQueryToBigQueryOperator(
        task_id='experian_brick_current',
        source_project_dataset_tables='{project}.{dataset}.experian_current_brick'.format(project=bq_target_project,
                                                                                          dataset=dataset_ref_experian),
        destination_project_dataset_table='{project}.{dataset}.experian_brick_current'.format(
            project=ref_target_project, dataset=dataset_references_experian),
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=bq_gcp_connector,
        # use_legacy_sql=False
        )

    for a in range(len(SOURCE_FILE_LIST)):
        experian_staging_to_landing = ExperianStagingToL1(
            task_id='staging_to_landing_data_{source}_file_counter_{counter}'.format(source=source_abbr,
                                                                                     counter=str(a + 1)),
            project=project,
            source=source,
            source_abbr=source_abbr,
            target_gcs_bucket=base_bucket,
            base_gcs_folder=base_file_location + "{{ ds_nodash }}",
            google_cloud_storage_conn_id=base_gcp_connector,
            prefix_for_filename_search=base_file_location + "{{ ds_nodash }}",
            target_file=SOURCE_FILE_LIST[a],
            audit_location=base_audit_location + "{{ ds_nodash }}",
            pool='experian-pool')

        [create_dataset >> experian_pool_creation >> experian_landing,
        archive_previous_brick] >> landing_audit_check >> experian_staging_to_landing_filelist >> experian_staging_to_landing >> schema_file_gen >>refine_data >> refine_audit_check

        experian_current_brick.set_upstream(refine_audit_check)
        experian_current_brick >> create_ref_experian_dataset >> create_references_experian_dataset >> [experian_brick,
                                                                                                        experian_brick_current]