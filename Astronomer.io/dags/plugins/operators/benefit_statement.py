"""Script to pull all Jeweler Member Benefits Statement data and produce report"""

################################################################################
# Base and third-party libraries
################################################################################

from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from common.df_to_bq import generate_bq_schema
import os
from airflow import configuration
import json
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from plugins.hooks.jm_mssql import MsSqlHook


def mbs_cl_attributes(**kwargs):
    ############################################################################
    # Read DW script and execute query
    ############################################################################
    sql_connection = kwargs['sql_connection']
    destination_project = kwargs['destination_project']
    destination_dataset = kwargs['destination_dataset']
    gcp_connection_id = kwargs['gcp_connection']



    hook = MsSqlHook(mssql_conn_id=sql_connection)
        
    #cl_sql = 'plugins/data_products_t2_sales/benefit_statement/mbs_cl_attributes.sql'
    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'semi-managed/data_products_t2_sales/benefit_statement/mbs_cl_attributes.sql'), 'r') as sql_file:
    #with open(cl_sql, 'r') as sql_file:
        sql = sql_file.read()
        df = hook.get_pandas_df(sql)    


    ############################################################################
    # Cleanup Jeweler names for more accurate matching
    ############################################################################
    
                                                        # replace variations of Jewelers
    df['Jeweler_Key'] = df['Jeweler'].str.replace(r'Jewe?l[l]?e?r?[s]?[^y]', 'Jewelers'
                                                        # replace variations of Jewelry
                                                        ).str.replace(r'Jewel[l]?e?ry\b', 'Jewelry'
                                                        # Clean-up Inc and other business-related suffixes
                                                        ).str.replace(r',?\sInc\.?.*', ''
                                                        ).str.replace(r'\sL[\.]?L[\.]?C[\.]?', ''
                                                        ).str.replace(r'\'', '')

    ############################################################################
    # Generage two schemas: BigQuery Client API version and Pandas-GBQ version
    ############################################################################
    
    client_schema, pandas_schema = generate_bq_schema(df)
    
    
    ############################################################################
    # Big Query Parameters
    ############################################################################

    dest_project = destination_project
    #dest_dataset = 'data_products_t2_sales'
    dest_dataset = destination_dataset
    dest_table = 'nonpromoted_mbs_cl_attributes'


    ############################################################################
    # Upload to Data Lake 
    ############################################################################

    gcp_hook = GoogleBaseHook(gcp_conn_id= gcp_connection_id)
    keyfile_dict = gcp_hook._get_field('keyfile_dict')

    info = json.loads(keyfile_dict)


    credentials = service_account.Credentials.from_service_account_info( info)
    client = bigquery.Client(project=dest_project, credentials=credentials)

    dataset_ref = client.dataset(dest_dataset)
    table_ref = dataset_ref.table(dest_table)
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.autodetect = True
    job_config.schema = client_schema




    df.to_gbq(f'{dest_dataset}.{dest_table}',
              project_id=dest_project,
              if_exists='replace',
              credentials=credentials,
              table_schema=pandas_schema)
    logging.info('CL Attributes uploaded via pandas-gbq library.')


def mbs_claims(**kwargs):
    ############################################################################
    # Read DW script and execute query
    ############################################################################

    sql_connection = kwargs['sql_connection']
    destination_project = kwargs['destination_project']
    destination_dataset = kwargs['destination_dataset']
    gcp_connection_id = kwargs['gcp_connection']


    hook = MsSqlHook(mssql_conn_id=sql_connection)
    


    
    #claims_sql = 'plugins/data_products_t2_sales/benefit_statement/mbs_claims.sql'
    with open(os.path.join(configuration.get('core', 'dags_folder'),
                           r'semi-managed/data_products_t2_sales/benefit_statement/mbs_claims.sql'), 'r') as sql_file:
    #with open(claims_sql, 'r') as sql_file:
        sql = sql_file.read()
        df = hook.get_pandas_df(sql)
    
    
    ############################################################################
    # Cleanup Jeweler names for more accurate matching
    ############################################################################
    
                                                        # replace variations of Jewelers
    df['Jeweler_Key'] = df['AccountJewelerName'].str.replace(r'Jewe?l[l]?e?r?[s]?[^y]', 'Jewelers'
                                                        # replace variations of Jewelry
                                                        ).str.replace(r'Jewel[l]?e?ry\b', 'Jewelry'
                                                        # Clean-up Inc and other business-related suffixes
                                                        ).str.replace(r',?\sInc\.?.*', ''
                                                        ).str.replace(r'\sL[\.]?L[\.]?C[\.]?', ''
                                                        ).str.replace(r'\'', '')




    ############################################################################
    # Generage two schemas: BigQuery Client API version and Pandas-GBQ version
    ############################################################################
    
    client_schema, pandas_schema = generate_bq_schema(df)
    
    
    ################################################################################
    # Big Query Parameters
    ################################################################################
    
    dest_project = destination_project
    #dest_dataset = 'data_products_t2_sales'
    dest_dataset = destination_dataset
    dest_table = 'nonpromoted_mbs_claims'


    ############################################################################
    # Upload to Data Lake 
    ############################################################################
    gcp_hook = GoogleBaseHook(gcp_conn_id= gcp_connection_id)
    keyfile_dict = gcp_hook._get_field('keyfile_dict')


    info = json.loads(keyfile_dict)


    credentials = service_account.Credentials.from_service_account_info( info)
    client = bigquery.Client(project=dest_project, credentials=credentials)
    dataset_ref = client.dataset(dest_dataset)
    table_ref = dataset_ref.table(dest_table)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.autodetect = True
    job_config.schema = client_schema



    df.to_gbq(f'{dest_dataset}.{dest_table}',
              project_id=dest_project,
              if_exists='replace',
              credentials=credentials,
              table_schema=pandas_schema)
    logging.info('Claims uploaded via pandas-gbq library.')
    

if __name__ == "__main__":
    print('This file cannot be executed from the command line. This was repurposed for airflow only.')
