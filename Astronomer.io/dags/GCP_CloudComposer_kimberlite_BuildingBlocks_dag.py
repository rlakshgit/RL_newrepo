import logging
import datetime as dt
import calendar
import time
import sys
import os

from datetime import datetime, timedelta
from airflow import configuration
from airflow.operators.empty import EmptyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_runbqscriptoperator import runbqscriptoperator
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from plugins.operators.jm_CompletionOperator import CompletionOperator
from airflow import DAG, macros
from airflow.models import Variable

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

env = Variable.get('ENV')

source = 'KimberliteBuildingBlocks'
source_abbr = 'kimberlitebuildingblocks'

if env.lower() == 'dev':
    bq_gcp_connector = 'dev_edl'
    bq_source_project = 'dev-edl'
    bq_dataset_prefix = 'GN_DEV_'

elif env.lower() == 'qa':
    bq_gcp_connector = 'qa_edl'
    bq_source_project = 'qa-edl'
    bq_dataset_prefix = 'B_QA_'

elif env.lower() == 'prod':
    bq_gcp_connector = 'prod_edl'
    bq_source_project = 'prod-edl'
    bq_dataset_prefix = ''

source_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
destination_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
core_dataset = '{prefix}ref_kimberlite_core'.format(prefix=bq_dataset_prefix)
dataproducts_dataset = '{prefix}gld_kimberlite'.format(prefix=bq_dataset_prefix)
source_cc_dataset = '{prefix}ref_cc_current'.format(prefix=bq_dataset_prefix)
source_pc_dataset = '{prefix}ref_pc_current'.format(prefix=bq_dataset_prefix)
source_bc_dataset = '{prefix}ref_bc_current'.format(prefix=bq_dataset_prefix)
source_pe_dbo_dataset = '{prefix}ref_pe_dbo'.format(prefix=bq_dataset_prefix)
source_cm_dataset='{prefix}ref_cm_current'.format(prefix=bq_dataset_prefix)
source_pe_cc_dataset = '{prefix}ref_pe_cc'.format(prefix=bq_dataset_prefix)
delete_partition_date = '{{ macros.ds_add(ds,-2) }}'

with DAG(
        'Kimberlite_BuildingBlocks_dag',
        schedule_interval='0 15 * * *',  # '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    # task_sensor_core = ExternalTaskSensorAsync(
    # task_id='check_core_tables_complete',
    # external_dag_id='Kimberlite_core_dag',
    # #   external_task_id='set_source_bit',
    # external_task_id='complete_load',
    # execution_delta=timedelta(hours=0),
    # timeout=7200)

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')

    create_kimberlite_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_kimberlite',
        dataset_id=destination_dataset,
        bigquery_conn_id=bq_gcp_connector)
    
    # Now create the Kimberlite BuildingBlocks queries with or without dependencies.
    query_list_dir = [
        "TransactionConfiguration.sql",
        "ProductAndRiskConfiguration.sql",
        "PolicyLevelAttributes.sql",
        "CLRiskBusinessOwnersAttributes.sql",
        "CLRiskInLandMarineAttributes.sql",
        "PJRiskLevelAttributes.sql",
        "PAJewelryCoverageLevelAttributes.sql",
        "PJCoverageLevelAttributes.sql",
        "CLCoverageLevelAttributes.sql",
        "PAJewelryRiskLevelAttributes.sql",
        "PremiumWritten.sql",
        "ClaimPADirect.sql",
        "ClaimPJDirect.sql",
        "ClaimPJCeded.sql",
        "ClaimBOPDirect.sql",
        "ClaimBOPCeded.sql",
        "ClaimIMDirect.sql",
        "ClaimIMCeded.sql",
        "AccountAttributes.sql",
        "ContactAttributes.sql",
        "AccountContactRoleAttributes.sql",
        "v_Date.sql",
        "ClaimAttributes.sql",
        "UserAgency.sql",
        "v_AgencyAttributes.sql",
]

    data_products_dir = [
        "PremiumWritten-View-Gold.sql",
    ]
    
    core_predecessors = {"TransactionConfiguration": "PolicyTransaction",
                         "ProductAndRiskConfiguration":  "PolicyTransaction,PolicyTransactionProduct,RiskPAJewelry,RiskJewelryItem,RiskBuilding,RiskLocationBusinessOwners,RiskLocationIM",
                         "PolicyLevelAttributes": "PolicyTransaction,PolicyTransactionProduct",
                         "CLRiskBusinessOwnersAttributes": "RiskLocationBusinessOwners,RiskBuilding,PolicyTransaction",
                         "CLRiskInLandMarineAttributes": "RiskLocationIM,RiskStockIM,PolicyTransaction",
                         "PJRiskLevelAttributes": "RiskJewelryItem,RiskJewelryItemFeature",
                         "PAJewelryCoverageLevelAttributes": "CoveragePAJewelry",
                         "PJCoverageLevelAttributes": "CoverageJewelryItem" ,
                         "CLCoverageLevelAttributes": "CoverageBOP,CoverageIM,CoverageUMB" ,
                         "PAJewelryRiskLevelAttributes":"RiskPAJewelry,RiskPAJewelryFeature,PolicyTransaction" ,
                         "PremiumWritten": "CoverageBOP,CoverageIM,CoveragePAJewelry,CoverageUMB,FinancialTransactionBOPDirect,FinancialTransactionIMDirect,FinancialTransactionPADirect,FinancialTransactionPJDirect,FinancialTransactionUMBDirect,PolicyTransaction,RiskBuilding,RiskJewelryItem,RiskLocationBusinessOwners,RiskLocationIM,RiskPAJewelry",
                         "ClaimPADirect": "ClaimFinancialTransactionLinePADirect",
                         "ClaimPJCeded": "ClaimFinancialTransactionLinePJCeded",
                         "ClaimPJDirect": "ClaimFinancialTransactionLinePJDirect",
                         "ClaimBOPDirect": "ClaimFinancialTransactionLineBOPDirect",
                         "ClaimBOPCeded": "ClaimFinancialTransactionLineBOPCeded",
                         "ClaimIMDirect":"ClaimFinancialTransactionLineIMDirect",
                         "ClaimIMCeded":"ClaimFinancialTransactionLineIMCeded",
                         "AccountAttributes":"Account",
                         "ContactAttributes":"Contact",
                         "AccountContactRoleAttributes":"AccountContactRole",
                         "v_Date": "t_Date",
                         "ClaimAttributes":"Claim",
                         "UserAgency": "ProducerUser,Agency",
                         "v_AgencyAttributes": "Agency"
                         }

    ENABLED_BUILDING_BLOCKS = Variable.get('Kimberlite_enabled_building_blocks',
                                           default_var=', '.join(query_list_dir)).split(', ')

    for data_product in data_products_dir:
        dp = data_product
        table_name = dp.replace('.sql', '')
        with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/data_products/',
                               dp)) as f:
            dp_read_sql = f.readlines()

        dp_sql = "\n".join(dp_read_sql)
        dp_sql = dp_sql.format(project=bq_source_project,
                                 source_dataset=source_dataset,
                                 dest_dataset = destination_dataset,
                                 core_dataset = core_dataset,
                                 dp_dataset=dataproducts_dataset,
                                 bc_dataset=source_bc_dataset,
                                 cc_dataset=source_cc_dataset,
                                 partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                 date="{{ ds }}",
                                 rbracket='}',
                                 lbracket='{')

        # delete_dp_sql = "Delete  from `{project}.{dataset}.{table}` where bq_load_date <= '{delete_date}'".format(
        #     project=bq_source_project,
        #     dataset=dataproducts_dataset,
        #     table=table_name,
        #     delete_date=delete_partition_date)

        # Creating data product tables
        create_dp_tables = runbqscriptoperator(task_id='{query}_script'.format(query=table_name),
                                                       sql=dp_sql,
                                                       bigquery_conn_id=bq_gcp_connector)

        # delete_dp_previous_partition_from_tables = BigQueryOperator(
        #     task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_name),
        #     sql=delete_dp_sql,
        #     bigquery_conn_id=bq_gcp_connector,
        #     use_legacy_sql=False,
        #     dag=dag)  

        for sql_group in query_list_dir:

            if sql_group not in ENABLED_BUILDING_BLOCKS:
                continue

            sql = sql_group
            table_name = sql.replace('.sql', '')

            core_table_list = core_predecessors[table_name].split(',')

            with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/BB/',
                                   sql)) as f:
                sql_read_sql = f.readlines()

            sql_sql = "\n".join(sql_read_sql)
            sql_sql = sql_sql.format(project=bq_source_project,
                                     source_dataset=source_dataset,
                                     dest_dataset=destination_dataset,
                                     core_dataset = core_dataset,
                                     cc_dataset=source_cc_dataset,
                                     cm_dataset=source_cm_dataset,
                                     bc_dataset=source_bc_dataset,
                                     pe_cc_dataset=source_pe_cc_dataset,
                                     pe_dbo_dataset=source_pe_dbo_dataset,
                                     pc_dataset=source_pc_dataset,
                                     partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                     date="{{ ds }}",
                                     rbracket='}',
                                     lbracket='{')

            delete_sql = "Delete  from `{project}.{dataset}.{table}` where bq_load_date <= '{delete_date}'".format(
                project=bq_source_project,
                dataset=destination_dataset,
                table=table_name,
                delete_date=delete_partition_date)
            

            # Creating Kimberlite tables
            if table_name == 'ProductAndRiskConfiguration':
                ProductAndRiskConfiguration_script = runbqscriptoperator(task_id='ProductAndRiskConfiguration_script',
                                                               sql=sql_sql,
                                                               bigquery_conn_id=bq_gcp_connector)
            elif table_name == 'TransactionConfiguration':
                TransactionConfiguration_script = runbqscriptoperator(
                    task_id='TransactionConfiguration_script',
                    sql=sql_sql,
                    bigquery_conn_id=bq_gcp_connector)

            elif table_name == 'PJCoverageLevelAttributes':
                PJCoverageLevelAttributes_script = runbqscriptoperator(
                    task_id='PJCoverageLevelAttributes_script',
                    sql=sql_sql,
                    bigquery_conn_id=bq_gcp_connector)

            elif table_name == 'PremiumWritten_bb':
                PremiumWritten_bb_script = runbqscriptoperator(
                    task_id='PremiumWritten_bb_script',
                    sql=sql_sql,
                    bigquery_conn_id=bq_gcp_connector)
                
            elif table_name == 'v_Date':
                create_view = BigQueryOperator(task_id='{query}_script'.format(query=table_name),
                                                sql=sql_sql,
                                                destination_dataset_table='{project}.{dataset}.{table}'.format(
                                                    project=bq_source_project,
                                                    dataset=destination_dataset,
                                                    table=table_name
                                                    ),
                                                write_disposition='WRITE_TRUNCATE',
                                                gcp_conn_id=bq_gcp_connector,
                                                use_legacy_sql=False)
        
            else:
                create_kimberlite_tables = runbqscriptoperator(task_id='{query}_script'.format(query=table_name),
                                                               sql=sql_sql,
                                                               bigquery_conn_id=bq_gcp_connector)

            if table_name.startswith('v_'):
                delete_previous_partition_from_tables = EmptyOperator(
                task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_name))
            else:
                delete_previous_partition_from_tables = BigQueryOperator(
                task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_name),
                sql=delete_sql,
                gcp_conn_id=bq_gcp_connector,
                use_legacy_sql=False)

            for pre_req in core_table_list:
                
                task_sensor_core = ExternalTaskSensorAsync(
                    task_id='check_core_tables_complete_{bb}_{query}'.format(bb=table_name, query=pre_req),
                    external_dag_id='Kimberlite_core_dag',
                    #   external_task_id='set_source_bit',
                    external_task_id='complete_load_{query}'.format(query=pre_req),
                    execution_delta=timedelta(hours=1),
                    timeout=7200)

                if table_name == 'ProductAndRiskConfiguration':
                    create_kimberlite_dataset >> task_sensor_core >> ProductAndRiskConfiguration_script >> delete_previous_partition_from_tables >> create_dp_tables >>  bit_set
                    TransactionConfiguration_script >> ProductAndRiskConfiguration_script

                elif table_name == 'TransactionConfiguration':
                    create_kimberlite_dataset >> task_sensor_core >> TransactionConfiguration_script >>  delete_previous_partition_from_tables >> create_dp_tables >> bit_set

                elif table_name == 'PremiumWritten_bb':
                    create_kimberlite_dataset >> task_sensor_core >> PremiumWritten_bb_script >> delete_previous_partition_from_tables >> create_dp_tables >> bit_set
                    PJCoverageLevelAttributes_script >> PremiumWritten_bb_script

                elif table_name == 'PJCoverageLevelAttributes':
                    create_kimberlite_dataset >> task_sensor_core >> PJCoverageLevelAttributes_script >> delete_previous_partition_from_tables >> create_dp_tables >> bit_set

                elif table_name == 'v_Date':
                    create_kimberlite_dataset >> task_sensor_core >> create_view >> delete_previous_partition_from_tables >> create_dp_tables >> bit_set

                else:
                    create_kimberlite_dataset >> task_sensor_core >> create_kimberlite_tables >> delete_previous_partition_from_tables >> create_dp_tables >> bit_set
