import msal
import requests
import logging
from plugins.operators.jm_gcs import GCSHook
import openpyxl
from plugins.hooks.jm_mssql import MsSqlHook
from airflow import configuration
import os
import ast
import base64
import json
from datetime import date
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.io.json import json_normalize
import pandas_gbq
from airflow.hooks.base import BaseHook


# --------------------------------------------
# Big Query Parameters
# --------------------------------------------

dest_project = 'semi-managed-reporting'
dest_dataset = 'data_products_t2_quotes_offers'
dest_table = 'nonpromoted_cl_model_usage'

temp_project = 'dev-edl'
temp_dataset = 'cl_decile_test'
temp_table = 'cl_model_test2'

bq_connection = BaseHook.get_connection(
    'semi-managed-reporting')
print(bq_connection)
client_secrets = json.loads(
    bq_connection.extra_dejson['keyfile_dict'])
print(client_secrets)
credentials = service_account.Credentials.from_service_account_info(
    client_secrets)
print(credentials)


def build_url(url, columns):
    if columns == 'all':
        return url + 'expand=True'
    elif columns == 'lookup':
        return url + 'expand=fields(select=UW_x0020_Name,PS_x0020_Name)'


def get_data(endpoint, result):
    # Calling graph using the access token
    graph_data = requests.get(  # Use token to call downstream service
        endpoint,
        headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
    # print(graph_data)
    print("Graph API call result: ")
    # print(json.dumps(graph_data, indent=2))
    data = graph_data['value']
    # print(json.dumps(data, indent=4))
    # create a list with column names
    fields_lst = [i['fields'] for i in data]
    df = pd.DataFrame(fields_lst)
    return graph_data, df


def get_sp_data():
    logging.info(' get_sp_data() [[ LOADING SUBMISSION DATA FROM SHAREPOINT ]]')
    # getting connection details from airflow connections 
    connection = BaseHook.get_connection("cl_risk_sp_list")
    extra = json.loads(connection.extra)
    config = {
        'authority': extra['authority'],
        'scope': extra['scope'],
        'client_id': connection.login,
        'secret': connection.password,
        'endpoint': connection.host,
    }

    # start ms graph auth process to get the access token 
    # Create a preferably long-lived app instance which maintains a token cache.
    app = msal.ConfidentialClientApplication(
        config["client_id"], authority=config["authority"],
        client_credential=config["secret"],
    )

    result = None

    result = app.acquire_token_silent(config["scope"], account=None)

    if not result:
        logging.info(
            "No suitable token exists in cache. Let's get a new one from AAD.")
        result = app.acquire_token_for_client(scopes=config["scope"])

    # if the above process successful, 'result' should include the access token 
    if "access_token" in result:
        logging.info('[[ MS GRAPH Auth Success ]]')
        manual_endpoin = build_url(config["endpoint"], 'all')
        lookup_endpoin = build_url(config["endpoint"], 'lookup')
        manual_graph_data, manual_df = get_data(manual_endpoin, result)
        lookup_graph_data, lookup_df = get_data(lookup_endpoin, result)

        num_of_manual_records = len(manual_df)
        num_of_lookup_records = len(lookup_df)

        logging.info(f'[[ Number of manual records loaded = {num_of_manual_records} ]]')
        logging.info(f'[[ Number of lookup records loaded = {num_of_lookup_records} ]]')

        # extract 'next_page' value from response
        manual_next_page = manual_graph_data["@odata.nextLink"]
        lookup_next_page = lookup_graph_data["@odata.nextLink"]

        while manual_next_page and lookup_next_page:
            logging.info('[[ LOADING NEXT PAGE ]]')
            new_manual_df = pd.DataFrame()
            new_lookup_df = pd.DataFrame()

            # send a new requse with next_page as the endpoint
            new_manual_graph_data, new_manual_df = get_data(
                manual_next_page, result)
            logging.info('[[ MANUAL FIELDS LOADED ]]')
            new_lookup_graph_data, new_lookup_df = get_data(
                lookup_next_page, result)
            logging.info('[[ LOOKUP FIELDS LOADED ]]')

            # merging data frames
            manual_df = pd.concat([manual_df, new_manual_df],
                                  ignore_index=True, sort=False)
            lookup_df = pd.concat([lookup_df, new_lookup_df],
                                  ignore_index=True, sort=False)
            logging.info('[[ MERGING DATAFRAMES COMPLETED ]]')

            manual_df = manual_df.reset_index(drop=True)
            lookup_df = lookup_df.reset_index(drop=True)

            num_of_manual_records += len(new_manual_df)
            num_of_lookup_records += len(new_lookup_df)

            print(f'[[ Number of manual records loaded = {num_of_manual_records} ]]')
            print(f'[[ Number of lookup records loaded = {num_of_lookup_records} ]]')

            try:
                logging.info('[[ CHECKING FOR NEXT PAGE ]]')
                manual_next_page = new_manual_graph_data["@odata.nextLink"]
                lookup_next_page = new_lookup_graph_data["@odata.nextLink"]
            except:
                print(f'Data Load Completed - {num_of_manual_records} manual records received')
                print(f'Data Load Completed - {num_of_lookup_records} lookup records received')
                break

        # joining the dataframes
        joined_df = pd.merge(manual_df, lookup_df,
                             on='@odata.etag', how='inner')
        joined_df['Path'] = 'CL/UNDER/Lists/2016 Submissions'
        joined_df['Item Type'] = 'Item'

        # rename and extract columsn
        col_name = {'Prospect_x0020_Name': 'Prospect Name', 'Account_x0020__x0023_': 'Account #', 'Date_x0020_Received': 'App Received', 'Agency_x0020_Code': 'Agency Code', 'Agency_x0020_Name': 'Agency Name', 'ID_x0020_Loc_x0020_State': 'ID Loc State', 'PS_x0020_Name': 'PS Name', 'Initial_x0020_PS_x0020_Prem': 'Initial PS Prem', 'UW_x0020_Name': 'UW Name', 'UW_x0020_Comp': 'UW Quoted', 'Quoted_x0020_Premium': 'Quoted Premium', 'Issued': 'Issued Date', 'Issued_x0020_Premium': 'Issued Premium', 'Notes': 'Notes', 'Risk_x0020_Group_x0020__x0023_': 'Location Group #', 'Risk_x0020_Group_x0020_Letter': 'Location Group Letter',
                    'Line_x0020_Level_x0020_Group_x00': 'Line Level Group Letter', 'Risk_x0020_Model_x0020_Exception': 'Risk Model Exceptions', 'Exception_x0020_Notes': 'Risk Model Notes', 'Item Type': 'Item Type', 'Path': 'Path', 'Prior_x0020_Insurance': 'Prior Insurance', 'Final_x0020_Effective_x0020_Date': 'Final Effective Date', 'Prior_x0020_Carrier': 'Prior Carrier', 'Not_x0020_Bound_x0020_Reason': 'Not Bound Reason', 'Berkley_x0020_WC_x0020__x0023_': 'Berkley WC #', 'WC_x0020_Bound_x003f_': 'WC Quoted Premium', 'WC_x0020_Issued_x0020_Premium': 'WC Issued Premium', 'Carriers_x0020_Quoted_x0020_With': 'Carriers Quoted With'}
        joined_df.rename(columns=col_name, inplace=True)
        final_df = joined_df[['Prospect Name', 'Account #', 'App Received', 'Agency Code', 'Agency Name', 'ID Loc State', 'PS Name', 'Initial PS Prem', 'UW Name', 'UW Quoted', 'Quoted Premium', 'Issued Date', 'Issued Premium', 'Notes', 'Location Group #', 'Location Group Letter',
                              'Line Level Group Letter', 'Risk Model Exceptions', 'Risk Model Notes', 'Item Type', 'Path', 'Prior Insurance', 'Final Effective Date', 'Prior Carrier', 'Not Bound Reason', 'Berkley WC #', 'WC Quoted Premium', 'WC Issued Premium', 'Carriers Quoted With']]

        return final_df

    else:
        print(result.get("error"))
        print(result.get("error_description"))
        # You may need this when reporting a bug
        print(result.get("correlation_id"))

def load_parse_model(sql_creds=None):    
    
    # --------------------------------------------
    # setting BQ Connection
    # --------------------------------------------
    bq_connection = BaseHook.get_connection('semi-managed-reporting')
    client_secrets = json.loads(bq_connection.extra_dejson['keyfile_dict'])
    credentials = service_account.Credentials.from_service_account_info(client_secrets)

    # --------------------------------------------
    # SQL Connection to load data from the data warehouse
    # --------------------------------------------
    with open(os.path.join(configuration.get('core', 'dags_folder'), r'ETL/CLRisk/', 'Account_SQL_with_IRPM.sql')) as f:
        sqlFile = f.read()
    #instDW_PROD
    logging.info('[[ LOADING DATA FROM DATA WAREHOUSE ]]')
    mssql = MsSqlHook(mssql_conn_id='instDW_PROD')
    conn = mssql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sqlFile)
    result = cursor.fetchall()
    logging.info('[[ DATA WAREHOUSE LOADING COMPLATED ]]')

    df_irpm = pd.DataFrame(result)
    df_irpm.columns = [i[0] for i in cursor.description]
    logging.info('[[ DATA WAREHOUSE RECOREDS CONVERTED TO DF ]]')
    logging.info(f'[[ {len(df_irpm)} RECORDS LOADED FROM DATA WAREHOUSE ]]')
    df_irpm['AccountNumber'] = pd.to_numeric(df_irpm['AccountNumber'])
    print('----------------------------------------------------------')

    # --------------------------------------------
    # Load data from data lake
    # --------------------------------------------

    sqla = 'SELECT * FROM `cl-decile-001.jm_webapp_output.model_output`'
    sqlb = 'SELECT * FROM `cl-decile-001.jm_webapp_output.model_output_v3`'
    logging.info('[[ LOADING RISK MODEL DATA FROM DATE LAKE ]]')

    dfa = pd.read_gbq(sqla, project_id=dest_project,
                      dialect='standard', credentials=credentials)
    dfb = pd.read_gbq(sqlb, project_id=dest_project,
                      dialect='standard', credentials=credentials)
    logging.info('[[ DATA LAKE LOAD COMPLETED ]]')
    df = dfa.append(dfb)
    logging.info(f'[[ {len(df)} RECORDS LOADED FROM DATA LAKE ]]')

    del dfa
    del dfb

    df1 = df[df['model_input'].str.startswith("{\'")].copy()
    df2 = df[df['model_input'].str.startswith("{\"")].copy()

    logging.info('[[ PARSING RISK MODEL DATA ... ]]')
    dict_columns = ['model_input', 'raw_input', 'location_input', 'web_values']

    for col in dict_columns:
        df1[col + '_dict'] = df1[col].apply(ast.literal_eval)

    for col in dict_columns:
        df2[col + '_dict'] = df2[col].apply(json.loads)

    df = df1.append(df2).reset_index()
    df = df[df.raw_input_dict.map(type) == dict]

    del df1
    del df2

    def create_id_location(row):
        try:
            val = ''
            if 'additional_fields' in row['raw_input_dict']:
                if 'line_level_characteristics' in row['raw_input_dict']['additional_fields']:
                    val = row['raw_input_dict']['additional_fields']['line_level_characteristics']['idLocationAddress']

            # print(val)
            return val
        except:
            # print(row['raw_input'])
            raise

    # add idLocation column
    df['idLocationAddress'] = df.apply(create_id_location, axis=1)

    for idx, row in df.iterrows():
        # del df.at[idx, 'Raw Input_dict']['additional_fields']
        df.at[idx, 'raw_input_dict'].pop('additional_fields')
        df.at[idx, 'raw_input_dict'].pop('coverages_limits')
    for col in dict_columns:
        a = json_normalize(df[col + '_dict']).add_prefix(col + '_')
        df = df.join([a])

    df_loc = df[df['idLocationAddress'] == '']
    df_ll = df[df['idLocationAddress'] != '']
    df_ll = df_ll.add_prefix('LL_')

    df_all = pd.merge(df_loc, df_ll, how='left', left_on=['quote_id'],
                      right_on=['LL_quote_id'])

    df_all['location_address'] = df_all['location_address'].astype(str)
    df_all['LL_idLocationAddress'] = df_all['LL_idLocationAddress'].astype(str)

    df_all = df_all[df_all.apply(lambda x: x.LL_idLocationAddress in x.location_address,
                                 axis=1) | df_all['LL_idLocationAddress'].isnull()]
    del df_all['index']
    del df_all['LL_index']

    print('merging data...')
    df_final = pd.merge(df_all, df_irpm, how='left', left_on=['raw_input_account_number'], right_on=['AccountNumber'])
    df_final['location_address'] = df_final['location_address'].astype(str)
    df_final['LocationAddress1'] = df_final['LocationAddress1'].astype(str)

    df_final = df_final[df_final.apply(
        lambda x: x.LocationAddress1 in x.location_address, axis=1) | df_final['LocationAddress1'].isnull()]
    df_final = df_final[df_final['location_address'].str.contains(
        "|".join(df_final['LocationAddress1']))]

    return df_final


def merge_submissions(df_model):
    df_model['raw_input_account_number'] = df_model['raw_input_account_number'].astype(str)

    logging.info('[[ LOADING NB SUBMISSION DATA FROM SHAREPOINT LIST ]]')
    df_submissions = get_sp_data()

    df_submissions['UW Quoted'] = pd.to_datetime(df_submissions['UW Quoted']).dt.date
    df_submissions = df_submissions[df_submissions['UW Quoted'] >= date(2019, 1, 1)]
    df_submissions.columns = df_submissions.columns.str.replace('[?]', '')

    logging.info('[[ MERGING DATA ]]')
    df = pd.merge(df_model,
                  df_submissions,
                  how='outer',
                  left_on='raw_input_account_number',
                  right_on='Account #')
    return df


def calculated_fields(df):
    logging.info('[[ Creating calculated fields... ]]')
    # most recent inforce date filter, make sure we are using the most current policy
    df['MostRecentInforceDate'] = df.groupby('AccountNumber').PolicyInforceFromDate.transform('max')
    df['MostRecentInforceDate'] = (df['MostRecentInforceDate'] == df['PolicyInforceFromDate']) | (df['MostRecentInforceDate'].isnull())
    df['MostRecentInforceDate'] = df['MostRecentInforceDate'].astype(str)
    df = df[(df['MostRecentInforceDate'] == 'True')].copy()

    df['MostRecentQuote'] = df.groupby('raw_input_account_number').created_at.transform('max')
    df['MostRecentQuote'] = (pd.to_datetime(df['MostRecentQuote'], utc=True) == pd.to_datetime(df['created_at'])) | (df['created_at'].isnull())
    df['MostRecentQuote'] = df['MostRecentQuote'].astype(str)
    df = df[(df['MostRecentQuote'] == 'True')].copy()

    # policy status
    pol_status_order = ['Draft', 'Quoted', 'Withdrawn', 'Declined', 'Expired', 'NotTaken', 'Bound', 'Canceling','Rescinded', 'Renewing']
    df.PolicyStatus = pd.Categorical(df.PolicyStatus,
                                     categories=pol_status_order,
                                     ordered=True)

    def last_category(data, col):
        data = data.sort_values(col)
        return data[col].iloc[-1]

    group_list_cat = ['Account #']
    group_list = ['Account #', 'MostRecentInforceDate', 'MostRecentQuote']

    df_temp = df[group_list + ['PolicyStatus']].groupby(group_list).apply(last_category, 'PolicyStatus').reset_index()
    df_temp = df_temp.rename(columns={0: 'CurrentPolicyStatus'})

    df = df.merge(df_temp, how='left', on=group_list)
    df = df.drop_duplicates(subset='Account #')

    # convert null premiums to zero
    prem_cols = ['init_premium', 'LL_init_premium',
                 'premium_recommendation_uncap', 'LL_premium_recommendation_uncap',
                 'premium_recommendation_cap', 'LL_premium_recommendation_cap']

    df[prem_cols] = df[prem_cols].fillna(0)

    # create IRPM
    df['IRPM'] = np.where((df[['JBIRPM', 'JSIRPM']].max(axis=1) == 0) | (df['JSIRPM'].isnull()),1,df[['JBIRPM', 'JSIRPM']].max(axis=1))

    # IRPM Smart Average
    # excludes IRPMs of 1.0 from the average unless only IRPMs are 1.0
    group_list = ['UW Name', 'Account #', 'MostRecentInforceDate',
                  'location_address', 'MostRecentQuote', 'CurrentPolicyStatus']

    def smart_irpm_func(data, col):
        if np.isnan(data[data[col] != 1][col].mean()):
            return data[col].mean()
        else:
            return data[data[col] != 1][col].mean()

    df_temp = df[group_list + ['IRPM']].groupby(group_list).apply(smart_irpm_func, 'IRPM').reset_index()
    df_temp = df_temp.rename(columns={0: 'Smart IRPM'})

    df = df.merge(df_temp, how='left', on=group_list)

    return df


def rated_as(df):
    print("Adding 'LocationRatedAs'")

    df['ConformedPolicyNumber'] = df['ConformedPolicyNumber'].astype(
        str).replace('Unassigned', 'Null')
    df['LocationRatedAs'] = df['LocationRatedAs'].astype(
        str).replace('?', 'Null')
    df['LocationRatedAs'] = df['LocationRatedAs'].fillna('Null')

    df = df.sort_values(by=['AccountNumber', 'ConformedPolicyNumber', 'LocationNumber', 'UW Quoted',
                        'LocationRatedAs', 'PolicyStatus'], na_position='first')  # sorts the df (order by)
    df = df.reset_index(drop=True)

    df['RA_Count'] = df.groupby(['AccountNumber', 'ConformedPolicyNumber', 'UW Quoted', 'LocationNumber', 'PolicyStatus'])[
        'LocationRatedAs'].transform('nunique')  # counts the number of unique, distinct LocationRatedAs labels
    df['RA_Count'] = df['RA_Count'].fillna('0')
    df['RA_Count'] = df['RA_Count'].apply(np.int64)
    # Moving Location information so it is grouped together for easier management
    cols = list(df)
    # Moving Location information so it is grouped together for easier management
    cols.insert(3, cols.pop(cols.index('LocationNumber')))
    cols.insert(4, cols.pop(cols.index('LocationRatedAs')))
    cols.insert(5, cols.pop(cols.index('UW Quoted')))
    cols.insert(6, cols.pop(cols.index('RA_Count')))
    df = df[cols]

    df.loc[df['RA_Count'] <= 2, 'DropNulls'] = df.groupby(['AccountNumber', 'ConformedPolicyNumber', 'UW Quoted', 'LocationNumber', 'PolicyStatus'], observed=True)[
        'LocationRatedAs'].transform('max')  # if LocationRatedAs is less than two then carry the one non null label
    # creates a new column 'DropNulls' for the LocationRatedAs null dropping. If RA_Count is 3 LocationRatedAs stays the same.
    df.loc[df['RA_Count'] >= 3, 'DropNulls'] = df['LocationRatedAs']

    cols = list(df)
    cols.insert(7, cols.pop(cols.index('DropNulls')))
    df = df[cols]
    # df = df.rename(columns={'UW Quoted': 'UW_Comp'})

    del df['LocationRatedAs']
    del df['RA_Count']
    df.rename(columns={'DropNulls': 'LocationRatedAs'}, inplace=True)
    return df


def upload(base):
    # --------------------------------------------
    # Upload to data lake
    # --------------------------------------------
    '''
    UPLOAD DATA TO DATA LAKE FROM PANDAS
    api reference: https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
    '''
    try:
        client = bigquery.Client(project=temp_project)
        dataset_ref = client.dataset(temp_dataset)
        table_ref = dataset_ref.table(temp_table)

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        # job_config.schema_update_options = ['ALLOW_FIELD_RELAXATION']
        job_config.autodetect = True

        client.load_table_from_dataframe(
            base, table_ref, job_config=job_config).result()
    except Exception as e:
        print(f'--------------------------- error in the try block {e}')
        pandas_gbq.to_gbq(base,
                          '{}.{}'.format(dest_dataset, temp_table),
                          project_id=dest_project,
                          credentials=credentials,
                          if_exists='replace'
                          )

def _upload_to_gcs(df, file):
    google_cloud_storage_conn_id = 'jm_landing_dev'
    delegate_to = None
    target_gcs_bucket = 'jm-edl-landing-wip'
    file_name = f'DEV_l1/cl_decile_risk_model/{file}'

    hook = GCSHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to)

    hook.upload(bucket=target_gcs_bucket, object=file_name,
                data=df.to_csv(index=False))


def risk_model_report(sql_creds=None):
    df_model = load_parse_model(sql_creds=sql_creds)
    df_merged = merge_submissions(df_model)
    del df_model
    df = calculated_fields(df_merged).copy()
    del df_merged
    df = rated_as(df)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace('(', '')
    df.columns = df.columns.str.replace(')', '')
    df.columns = df.columns.str.replace('>', '')
    df.columns = df.columns.str.replace('<', '')
    df.columns = df.columns.str.replace('+', '')
    df.columns = df.columns.str.replace('-', '')
    df.columns = df.columns.str.replace('#', 'ID')
    logging.info('[[ DATA PROCESSING COMPLETED - UPLOADING TO BQ ]]')
    # _upload_to_gcs(df, 'final_df.csv')
    upload(df)



