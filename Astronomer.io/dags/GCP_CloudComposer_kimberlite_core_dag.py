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
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
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
    'start_date': datetime(2021,12,12),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email" THEN "alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

env = Variable.get('ENV')

source = 'KimberliteQueries'
source_abbr = 'kimberlitequeries'

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

destination_dataset = '{prefix}ref_kimberlite'.format(prefix=bq_dataset_prefix)
destination_core_dataset = '{prefix}ref_kimberlite_core'.format(prefix=bq_dataset_prefix)
config_dataset = '{prefix}ref_kimberlite_config'.format(prefix=bq_dataset_prefix)
source_pc_dataset = '{prefix}ref_pc_current'.format(prefix=bq_dataset_prefix)
source_bc_dataset = '{prefix}ref_bc_current'.format(prefix=bq_dataset_prefix)
source_cc_dataset = '{prefix}ref_cc_current'.format(prefix=bq_dataset_prefix)
source_cm_dataset = '{prefix}ref_cm_current'.format(prefix=bq_dataset_prefix)
source_pe_dbo_dataset = '{prefix}ref_pe_dbo'.format(prefix=bq_dataset_prefix)
source_pe_cc_dataset = '{prefix}ref_pe_cc'.format(prefix=bq_dataset_prefix)
dq_dataset = '{prefix}ref_kimberlite_core_DQ'.format(prefix=bq_dataset_prefix)
source_ad_bief_dataset = '{prefix}ref_ad_bief_src_current'.format(prefix=bq_dataset_prefix)

delete_partition_date = '{{ macros.ds_add(ds,-2) }}'

with DAG(
        'Kimberlite_core_dag',
        schedule_interval='0 14 * * *',  # '0 11 * * *',#"@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:


    task_sensor_pc = ExternalTaskSensorAsync(
        task_id='check_pc_prioritized_table_complete',
        external_dag_id='PolicyCenter_Prioritized_Tables',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=2),
        timeout=7200)
        
    task_sensor_bc = ExternalTaskSensorAsync(
        task_id='check_bc_prioritized_table_complete',
        external_dag_id='BillingCenter_Prioritized_Tables',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=2),
        timeout=7200)        

    task_sensor_cc = ExternalTaskSensorAsync(
        task_id='check_cc_prioritized_table_complete',
        external_dag_id='ClaimCenter_Prioritized_Tables',
        external_task_id='set_source_bit',
        execution_delta=timedelta(hours=1),
        timeout=7200) 

    complete_load = EmptyOperator(task_id='complete_load')
    
    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source=source,
                                 mode='SET')
                                 
    create_kimberlite_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='check_for_dataset_kimberlite',
        dataset_id=destination_core_dataset,
        bigquery_conn_id=bq_gcp_connector)
    
    # Creating t_PrimaryRatingLocationBOP
    with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/Kimberlite_queries/t_PrimaryRatingLocationBOP.sql')) as f:
            sql_read_sql = f.readlines()
            sql_sql = "\n".join(sql_read_sql)

    table_name = 't_PrimaryRatingLocationBOP'

    sql_sql = sql_sql.format(project=bq_source_project, 
                                 pc_dataset=source_pc_dataset,
                                 bc_dataset=source_bc_dataset,
                                 cc_dataset=source_cc_dataset,
                                 cm_dataset=source_cm_dataset,
                                 pe_dbo_dataset=source_pe_dbo_dataset,
                                 pe_cc_dataset=source_pe_cc_dataset,
                                 config_dataset=config_dataset,
                                 ad_bief_dataset = source_ad_bief_dataset,
                                 reference_dataset=destination_dataset,
                                 dest_dataset=destination_core_dataset,
                                 partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                 date="{{ ds }}",
								 rbracket='}',
                                 lbracket='{')
    create_t_PrimaryRatingLocationBOP = runbqscriptoperator(task_id='{query}_script'.format(query=table_name),
                                                       sql=sql_sql,
                                                       bigquery_conn_id=bq_gcp_connector)
    
    [task_sensor_pc, task_sensor_bc, task_sensor_cc] >> create_kimberlite_dataset >> create_t_PrimaryRatingLocationBOP

    query_list_dir = [
        "RiskPAJewelryFeature.sql",
        "FinancialTransactionPJDirect.sql",
        "FinancialTransactionPJCeded.sql",
        "CoveragePAJewelry.sql",
        "CoverageJewelryItem.sql",
        "FinancialTransactionBOPDirect.sql",
        "FinancialTransactionPACeded.sql",
        "FinancialTransactionPADirect.sql",
        "FinancialTransactionUMBCeded.sql",
        "FinancialTransactionUMBDirect.sql",
        "PolicyTransaction.sql",
        "PolicyTransactionProduct.sql",
        "RiskBuilding.sql",
        "RiskJewelryItem.sql",
        "RiskJewelryItemFeature.sql",
        "RiskPAJewelry.sql"
    ]

    script_list_dir = [
        "RiskLocationBusinessOwners.sql",
        "RiskLocationIM.sql",
        "FinancialTransactionBOPCeded.sql",
        "FinancialTransactionIMCeded.sql",
        "FinancialTransactionIMDirect.sql",
        "RiskStockIM.sql",
        "ClaimStatus.sql",
        "CoverageIM.sql",
        "CoverageBOP.sql",
        "CoverageUMB.sql",
        "ClaimFinancialTransactionLinePJCeded.sql",
        "ClaimFinancialTransactionLinePJDirect.sql",
        "ClaimFinancialTransactionLinePADirect.sql",
        "ClaimFinancialTransactionLineBOPDirect.sql",
        "ClaimFinancialTransactionLineBOPCeded.sql",
        "ClaimFinancialTransactionLineIMDirect.sql",
        "ClaimFinancialTransactionLineIMCeded.sql",
        "Account.sql",
        "Address.sql",
        "Contact.sql",
        "AccountContactRole.sql",
        "LegalEntity.sql",
        "t_Date.sql",
        "Claim.sql",
        "User.sql",
        "Agency.sql",
        "ProducerUser.sql",
    ]
                      
    DQ_list = [
              "DupesOverall",
              "DupesByCoverage",
              "MissingByX",
              "MissingRisks",
              "Joins"
              ]
              
    DQ_all_list = [
                      "Account_DupesOverall",
                      "Account_MissingByX",
                      "AccountContactRole_DupesOverall",
                      "AccountContactRole_MissingByX",
                      "Address_DupesOverall",
                      "Address_MissingByX",
                      "Contact_DupesOverall",
                      "Contact_MissingByX",
                      "ClaimFinancialTransactionLinePADirect_DupesOverall",
                      "ClaimFinancialTransactionLinePADirect_MissingByX",
                      "ClaimFinancialTransactionLinePADirect_MissingRisks",
                      "ClaimFinancialTransactionLinePJDirect_MissingRisks",
                      "ClaimFinancialTransactionLinePJDirect_MissingByX",
                      "ClaimFinancialTransactionLinePJDirect_DupesOverall",
                      "ClaimFinancialTransactionLinePJDirect_DupesByCoverage",
                      "ClaimFinancialTransactionLinePJCeded_MissingRisks",
                      "ClaimFinancialTransactionLinePJCeded_MissingByX",
                      "ClaimFinancialTransactionLinePJCeded_DupesOverall",
                      "ClaimFinancialTransactionLinePJCeded_DupesByCoverage",
                      "ClaimFinancialTransactionLineBOPDirect_MissingByX",
                      "ClaimFinancialTransactionLineBOPDirect_MissingRisks",
                      "ClaimFinancialTransactionLineBOPDirect_DupesOverall",
                      "ClaimFinancialTransactionLineBOPCeded_MissingByX",
                      "ClaimFinancialTransactionLineBOPCeded_MissingRisks",
                      "ClaimFinancialTransactionLineBOPCeded_DupesOverall",
                      "CoverageBOP_DupesByCoverage", 
                      "CoverageBOP_DupesOverall", 
                      "CoverageBOP_MissingByX", 
                      "CoverageBOP_MissingRisks", 
                      "CoverageIM_DupesByCoverage",  
                      "CoverageIM_DupesOverall",  
                      "CoverageIM_MissingByX",  
                      "CoverageIM_MissingRisks",  
                      "CoverageIM_Joins", 
                      "CoverageJewelryItem_DupesByCoverage",
                      "CoverageJewelryItem_DupesOverall",
                      "CoverageJewelryItem_MissingByX",
                      "CoverageJewelryItem_MissingRisks",
                      "CoveragePAJewelry_DupesByCoverage",
                      "CoveragePAJewelry_DupesOverall",
                      "CoveragePAJewelry_MissingByX",
                      "CoveragePAJewelry_MissingRisks",
                      "CoverageUMB_DupesByCoverage",                      
                      "CoverageUMB_DupesOverall",                      
                      "CoverageUMB_MissingByX",                      
                      "CoverageUMB_MissingRisks",     
                      "FinancialTransactionBOPCeded_DupesByCoverage",					  
                      "FinancialTransactionBOPCeded_DupesOverall",
                      "FinancialTransactionBOPCeded_MissingByX",
                      "FinancialTransactionBOPCeded_MissingRisks",
                      "FinancialTransactionBOPDirect_DupesByCoverage",					  
                      "FinancialTransactionBOPDirect_DupesOverall",
                      "FinancialTransactionBOPDirect_MissingByX",
                      "FinancialTransactionBOPDirect_MissingRisks",
                      "FinancialTransactionIMCeded_DupesByCoverage", 					  
                      "FinancialTransactionIMCeded_DupesOverall",  
                      "FinancialTransactionIMCeded_MissingByX",    
                      "FinancialTransactionIMCeded_MissingRisks",
                      "FinancialTransactionIMDirect_DupesByCoverage", 					  
                      "FinancialTransactionIMDirect_DupesOverall",    
                      "FinancialTransactionIMDirect_MissingByX",    
                      "FinancialTransactionIMDirect_MissingRisks",  
                      "FinancialTransactionPACeded_DupesByCoverage",					  
                      "FinancialTransactionPACeded_DupesOverall",
                      "FinancialTransactionPACeded_MissingByX",
                      "FinancialTransactionPACeded_MissingRisks",
                      "FinancialTransactionPADirect_DupesByCoverage",					  
                      "FinancialTransactionPADirect_DupesOverall",
                      "FinancialTransactionPADirect_MissingByX",
                      "FinancialTransactionPADirect_MissingRisks",
                      "FinancialTransactionPJCeded_DupesByCoverage",					  
                      "FinancialTransactionPJCeded_DupesOverall",
                      "FinancialTransactionPJCeded_MissingByX",
                      "FinancialTransactionPJCeded_MissingRisks",
                      "FinancialTransactionPJDirect_DupesByCoverage",					  
                      "FinancialTransactionPJDirect_DupesOverall",
                      "FinancialTransactionPJDirect_MissingByX",
                      "FinancialTransactionPJDirect_MissingRisks",
                      "FinancialTransactionUMBCeded_DupesByCoverage",					  
                      "FinancialTransactionUMBCeded_DupesOverall",
                      "FinancialTransactionUMBCeded_MissingByX",
                      "FinancialTransactionUMBCeded_MissingRisks",
                      "FinancialTransactionUMBDirect_DupesByCoverage",					  
                      "FinancialTransactionUMBDirect_DupesOverall",
                      "FinancialTransactionUMBDirect_MissingByX",
                      "FinancialTransactionUMBDirect_MissingRisks",
                      "PolicyTransactionProduct_DupesOverall",   
                      "PolicyTransactionProduct_MissingByX",   
                      "PolicyTransactionProduct_MissingRisks",   
                      "PolicyTransaction_DupesOverall",
                      "PolicyTransaction_MissingByX",
                      "PolicyTransaction_MissingRisks",
                      "RiskBuilding_DupesOverall",                                        
                      "RiskBuilding_MissingByX",                                        
                      "RiskBuilding_MissingRisks",                                        
                      "RiskJewelryItemFeature_DupesOverall",
                      "RiskJewelryItemFeature_MissingByX",
                      "RiskJewelryItemFeature_MissingRisks",
                      "RiskJewelryItem_DupesOverall",
                      "RiskJewelryItem_MissingByX",
                      "RiskJewelryItem_MissingRisks",
                      "RiskLocationBusinessOwners_DupesOverall",
                      "RiskLocationBusinessOwners_MissingByX",
                      "RiskLocationBusinessOwners_MissingRisks",
                      "RiskLocationIM_DupesOverall",
                      "RiskLocationIM_MissingByX",
                      "RiskLocationIM_MissingRisks",
                      "RiskPAJewelryFeature_DupesOverall",
                      "RiskPAJewelryFeature_MissingByX",
                      "RiskPAJewelryFeature_MissingRisks",
                      "RiskPAJewelry_DupesOverall",
                      "RiskPAJewelry_MissingByX",
                      "RiskPAJewelry_MissingRisks",
                      "RiskStockIM_DupesOverall",
                      "RiskStockIM_MissingByX",                      
                      "RiskStockIM_MissingRisks",
                      "RiskStockIM_Joins",
                      "Claim_DupesOverall",
                      "Claim_MissingByX",
                      "Agency_DupesOverall",
                      "Agency_MissingByX",
                  ]
                  
    ENABLED_CHECKS = Variable.get('Kimberlite_enabled_checks', default_var=', '.join(DQ_all_list)).split(', ')            
        
    for sql_group in query_list_dir:

        sql = sql_group
        table_name = sql.replace('.sql', '')

        with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/Kimberlite_queries/',
                               sql)) as f:
            sql_read_sql = f.readlines()

        sql_sql = "\n".join(sql_read_sql)

        
        sql_sql = sql_sql.format(project=bq_source_project, 
                                #  dataset=source_pc_dataset, 
                                 pc_dataset=source_pc_dataset,
                                 bc_dataset=source_bc_dataset,
                                 cc_dataset=source_cc_dataset,
                                 cm_dataset=source_cm_dataset,
                                 pe_dbo_dataset=source_pe_dbo_dataset,
                                 pe_cc_dataset=source_pe_cc_dataset,
                                 ad_bief_dataset = source_ad_bief_dataset,
                                 config_dataset=config_dataset,
                                 reference_dataset=destination_dataset,
                                 dest_dataset=destination_core_dataset,
                                 partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                 date="{{ ds }}")


        delete_sql = "Delete  from `{project}.{dataset}.{table}` where bq_load_date <= '{delete_date}'".format(
                                                                                                project = bq_source_project,
                                                                                                dataset = destination_core_dataset,
                                                                                                table = table_name,
                                                                                                 delete_date = delete_partition_date)


        # Creating Kimberlite tables
        create_kimberlite_tables = BigQueryOperator(task_id='{query}_query'.format(query=table_name),
                                                    sql=sql_sql,
                                                    destination_dataset_table='{project}.{dataset}.{table}${date}'.format(
                                                        project=bq_source_project,
                                                        dataset=destination_core_dataset,
                                                        table=table_name,
                                                        date="{{ ds_nodash }}"
                                                        ),
                                                    write_disposition='WRITE_TRUNCATE',
                                                    gcp_conn_id=bq_gcp_connector,
                                                    use_legacy_sql=False,
                                                    #schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                           #'ALLOW_FIELD_RELAXATION'],
                                                    time_partitioning={'type': 'DAY', 'field': 'bq_load_date'})

        delete_previous_partition_from_tables = BigQueryOperator(task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_name),
                                                                 sql=delete_sql,
                                                                 gcp_conn_id=bq_gcp_connector,
                                                                 use_legacy_sql=False)
                                                                 
        complete_core_table_dq = EmptyOperator(task_id='complete_load_{query}'.format(query=table_name))                                                         
   
        for dq_check in DQ_list:
            dq_filename = sql.replace('.sql', '_' + dq_check + '.sql')
            dq_tablename = dq_filename.replace('.sql', '')

                
            try:
                with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/DQ/', dq_filename)) as f:
                    sql_read_sql = f.readlines()

                    sql_sql = "\n".join(sql_read_sql)
                    sql_sql = sql_sql.format(project=bq_source_project, 
                                            #  dataset=source_pc_dataset, 
                                             pc_dataset=source_pc_dataset,
                                             bc_dataset=source_bc_dataset,
                                             cc_dataset=source_cc_dataset,
                                             cm_dataset=source_cm_dataset,
                                             pe_dbo_dataset=source_pe_dbo_dataset,
                                             pe_cc_dataset=source_pe_cc_dataset,
                                             config_dataset=config_dataset,
                                             ad_bief_dataset = source_ad_bief_dataset,
                                             reference_dataset=destination_dataset,
                                             dest_dataset=destination_core_dataset,
                                             partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                             date="{{ ds }}")
            except:
                sql_sql = ''
                               
            if (len(sql_sql) == 0) or (dq_tablename not in ENABLED_CHECKS):
                dq_check = EmptyOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename))
                dq_status = EmptyOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename))
            else:
                dq_check = BigQueryOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename),
                                            sql=sql_sql,
                                            destination_dataset_table='{project}.{dataset}.{table}'.format(
                                                                project=bq_source_project,
                                                                dataset=dq_dataset,
                                                                table=dq_tablename
                                                                ),
                                            write_disposition='WRITE_TRUNCATE',
                                            gcp_conn_id=bq_gcp_connector,
                                            use_legacy_sql=False,
                                                            #schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                                   #'ALLOW_FIELD_RELAXATION'],
                                                            #time_partitioning={'type': 'DAY', 'field': 'bq_load_date'}
                                                            )

                success_sql = '''SELECT
                                    CASE WHEN EXISTS 
                                    (
                                        SELECT * FROM `{project}.{dest_dataset}.{table}` WHERE DATE(bq_load_date)=DATE({partition_date})
                                    )
                                    THEN FALSE
                                    ELSE TRUE
                                END
                              '''
                success_sql = success_sql.format(project=bq_source_project, 
                                        #  dataset=source_pc_dataset, 
                                         pc_dataset=source_pc_dataset,
                                         bc_dataset=source_bc_dataset,
                                         config_dataset=config_dataset,
                                         dest_dataset=dq_dataset,
                                         partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                         date="{{ ds }}",
                                         table=dq_tablename)                         
                              
                dq_status = BigQueryCheckOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename),
                                                                     sql=success_sql,
                                                                     gcp_conn_id=bq_gcp_connector,
                                                                     use_legacy_sql=False)
                                     
            create_t_PrimaryRatingLocationBOP >> create_kimberlite_tables >> delete_previous_partition_from_tables >> complete_load >> dq_check >> dq_status >> complete_core_table_dq >> bit_set
        
    for sql_group in script_list_dir:

        sql = sql_group
        table_name = sql.replace('.sql', '')

        with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/Kimberlite_queries/',
                               sql)) as f:
            sql_read_sql = f.readlines()

        sql_sql = "\n".join(sql_read_sql)
        sql_sql = sql_sql.format(project=bq_source_project, 
                                #  dataset=source_pc_dataset, 
                                 pc_dataset=source_pc_dataset,
                                 bc_dataset=source_bc_dataset,
                                 cc_dataset=source_cc_dataset,
                                 cm_dataset=source_cm_dataset,
                                 pe_dbo_dataset=source_pe_dbo_dataset,
                                 pe_cc_dataset=source_pe_cc_dataset,
                                 ad_bief_dataset = source_ad_bief_dataset,
                                 config_dataset=config_dataset,
                                 reference_dataset=destination_dataset,
                                 dest_dataset=destination_core_dataset,
                                 partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                 date="{{ ds }}",
								 rbracket='}',
                                 lbracket='{')
        
        if table_name == 't_Date':
            delete_sql = "Delete  from `{project}.{dataset}.{table}` where 1 = 1".format( project = bq_source_project,
                                                                            dataset = destination_core_dataset,
                                                                            table = table_name,
                                                                            delete_date = delete_partition_date)
        else:
            delete_sql = "Delete  from `{project}.{dataset}.{table}` where bq_load_date <= '{delete_date}'".format(
                                                                                                project = bq_source_project,
                                                                                                dataset = destination_core_dataset,
                                                                                                table = table_name,
                                                                                                 delete_date = delete_partition_date)



        # Creating Kimberlite tables
        create_kimberlite_tables = runbqscriptoperator(task_id='{query}_script'.format(query=table_name),
                                                       sql=sql_sql,
                                                       bigquery_conn_id=bq_gcp_connector)

        delete_previous_partition_from_tables = BigQueryOperator(task_id='Delete_partition_older_than_2days_from_{query}'.format(query=table_name),
                                                                 sql=delete_sql,
                                                                 gcp_conn_id=bq_gcp_connector,
                                                                 use_legacy_sql=False)        
                  
        complete_core_table_dq = EmptyOperator(task_id='complete_load_{query}'.format(query=table_name))                    
        for dq_check in DQ_list:
            dq_filename = sql.replace('.sql', '_' + dq_check + '.sql')
            dq_tablename = dq_filename.replace('.sql', '')

                
            try:
                with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/kimberlite/DQ/', dq_filename)) as f:
                    sql_read_sql = f.readlines()

                    sql_sql = "\n".join(sql_read_sql)
                    sql_sql = sql_sql.format(project=bq_source_project, 
                                            #  dataset=source_pc_dataset, 
                                             pc_dataset=source_pc_dataset,
                                             bc_dataset=source_bc_dataset,
                                             cc_dataset=source_cc_dataset,
                                             cm_dataset=source_cm_dataset,
                                             pe_dbo_dataset=source_pe_dbo_dataset,
                                             pe_cc_dataset=source_pe_cc_dataset,
                                             config_dataset=config_dataset,
                                             ad_bief_dataset = source_ad_bief_dataset,
                                             reference_dataset=destination_dataset,
                                             dest_dataset=destination_core_dataset,
                                             partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                             date="{{ ds }}")
            except:
                sql_sql = ''
                               
            if (len(sql_sql) == 0) or (dq_tablename not in ENABLED_CHECKS):
                dq_check = EmptyOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename))
                dq_status = EmptyOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename))
            else:
                dq_check = BigQueryOperator(task_id='{dq_check}_check'.format(dq_check=dq_filename),
                                            sql=sql_sql,
                                            destination_dataset_table='{project}.{dataset}.{table}'.format(
                                                                project=bq_source_project,
                                                                dataset=dq_dataset,
                                                                table=dq_tablename
                                                                ),
                                            write_disposition='WRITE_TRUNCATE',
                                            gcp_conn_id=bq_gcp_connector,
                                            use_legacy_sql=False,
                                                            #schema_update_options=['ALLOW_FIELD_ADDITION',
                                                                                   #'ALLOW_FIELD_RELAXATION'],
                                                            #time_partitioning={'type': 'DAY', 'field': 'bq_load_date'}
                                                            )

                success_sql = '''SELECT
                                    CASE WHEN EXISTS 
                                    (
                                        SELECT * FROM `{project}.{dest_dataset}.{table}` WHERE DATE(bq_load_date)=DATE({partition_date})
                                    )
                                    THEN FALSE
                                    ELSE TRUE
                                END
                              '''
                success_sql = success_sql.format(project=bq_source_project, 
                                        #  dataset=source_pc_dataset, 
                                         pc_dataset=source_pc_dataset,
                                         bc_dataset=source_bc_dataset,
                                         config_dataset=config_dataset,
                                         dest_dataset=dq_dataset,
                                         partition_date='TIMESTAMP(DATE("{{ ds }}"))',
                                         date="{{ ds }}",
                                         table=dq_tablename)                         
                              
                dq_status = BigQueryCheckOperator(task_id='{dq_check}_success'.format(dq_check=dq_filename),
                                                                     sql=success_sql,
                                                                     gcp_conn_id=bq_gcp_connector,
                                                                     use_legacy_sql=False)     
                                                                     
            create_t_PrimaryRatingLocationBOP >> create_kimberlite_tables >> delete_previous_partition_from_tables  >> complete_load >> dq_check >> dq_status >> complete_core_table_dq >> bit_set                                                       