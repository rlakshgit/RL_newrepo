"""Script to create agency clearance data"""

# import libs
from google.oauth2 import service_account
from google.cloud import bigquery
#from google.cloud import bigquery_storage
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import pandas as pd
import numpy as np
from time import sleep
import sys
import json
from datetime import timedelta
from datetime import datetime as dt
from plugins.operators.agency_clearance_experian_hook import ExperianBin
from plugins.hooks.jm_bq_hook import BigQueryHook
import logging


def agency_clearance(**kwargs):
    ############################################################################
    # Read DW script and execute query
    ############################################################################

    destination_project = kwargs['destination_project']  #semi-managed-reporting
    destination_dataset = kwargs['destination_dataset'] #data_products_t2_sales
    gcp_connection_id = kwargs['gcp_connection']

    gcp_hook = GoogleBaseHook(gcp_conn_id=gcp_connection_id)
    keyfile_dict = gcp_hook._get_field('keyfile_dict')
    info = json.loads(keyfile_dict)
    credentials = service_account.Credentials.from_service_account_info(info)

    # Make clients.
    gcp_bq_hook = BigQueryHook(gcp_conn_id=gcp_connection_id)
    bqclient = bigquery.Client(credentials=credentials, project=destination_project,)
   # bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

    # sql
    jmis_sf_sql = """
    WITH
         opp_current_stage as (
                SELECT * EXCEPT(stage_rank) FROM(
                          SELECT Id as OppId, AccountId, StageName as CurrentStage, LastModifiedDate, Account_Email__c, Master_Agency_Email__c
                            , DENSE_RANK() OVER (PARTITION BY Id ORDER BY LastModifiedDate  DESC) AS stage_rank
                            FROM `prod-edl.ref_salesforce.sf_opportunity` 
                            WHERE lower(trim(TYPE)) IN ('commercial lines','cl')
                            AND StageName NOT IN('Policy Bound', 'Did Not Bind')
                            AND Name LIKE '%Agent%'
                            )
                WHERE stage_rank = 1
                )
    
          ,
          opp_proposal as (
                SELECT * EXCEPT(opp_rank) FROM(
                            SELECT Id as OppId, LastModifiedDate as InitialProposalDate, StageName as CurrentStage
                            , DENSE_RANK() OVER (PARTITION BY Id, AccountId ORDER BY LastModifiedDate  ASC) AS opp_rank
    
                            FROM `prod-edl.ref_salesforce.sf_opportunity` 
                            WHERE lower(trim(TYPE)) IN ('commercial lines','cl')
                            AND StageName = 'Proposal'
                            )
                WHERE opp_rank = 1
                )
    
    
    
         ,
         acct as (
                  SELECT * EXCEPT(acct_rank) FROM(
                      SELECT Id as AccountId, Name, Phone, Type, ParentId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingStateCode, BillingCountryCode, Owner_Full_Name__c, Jeweler_Long_Name__c, Policy_Number__c
                      , DENSE_RANK() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC) AS acct_rank
                      FROM `prod-edl.ref_salesforce.sf_account`
                      )
                  WHERE acct_rank = 1
                 )
    
          ,
          base as (
                  SELECT c.*, a.* EXCEPT(AccountId), b.InitialProposalDate, DENSE_RANK() OVER (PARTITION BY a.AccountId ORDER BY LastModifiedDate DESC) AS opp_rank
                  FROM opp_current_stage a
                  LEFT JOIN opp_proposal b USING(OppId, CurrentStage)
                  INNER JOIN acct c ON a.AccountId = c.AccountId 
                  --AND DATE(LastModifiedDate) > DATE_SUB(current_date(), INTERVAL 180 DAY) 
                  AND CurrentStage NOT IN('Policy Bound', 'Did Not Bind')
                  --AND c.AccountId = '0010z00001SfMTMAA3'
                  )
    
    SELECT * EXCEPT(opp_rank) FROM base Where opp_rank = 1
    """

    jmic_sql = '''SELECT DISTINCT * FROM `semi-managed-reporting.data_products_t2_insurance_cl.nonpromoted_cl_jmic_clearance_source`
    WHERE AgencyMasterCode NOT IN('W10', 'B50', '200', 'R01', 'M50', 'R02', 'T01', 'T05')'''

    # Lunar Active policies
  #  usecols = ['Customer Name', 'Customer Number', 'Customer Type', 'Active Customer', 'Policy Status',
  #             'Policy Expiration Date', 'Policy Executive', 'DBA', 'Customer Address 1', 'Customer Address 2',
  #             'Customer City', 'Customer State', 'Customer Zip Code']
  #  dfla = pd.read_excel(lunar_active_path, usecols=usecols)

    la_sql = '''SELECT DISTINCT customer_name
                                , customer_number
                                , customer_type
                                , active_customer
                                , policy_status
                                , policy_expiration_date
                                , policy_executive
                                , policy_number
                                , dba
                                , customer_address_1
                                , customer_address_2
                                , customer_city
                                , customer_state
                                , customer_zip_code
                                , file_last_modified_time
                FROM `prod-edl.ref_agency_information.t_lunar_active_clients`
                WHERE lower(trim(department)) in ('commercial lines','jewelers') '''	

    wx_sql = '''SELECT DISTINCT customer_name
                                , customer_number
                                , customer_type
                                , active_customer
                                , policy_status
                                , policy_expiration_date
                                , policy_executive
                                , policy_number
                                , dba
                                , customer_address_1
                                , customer_address_2
                                , customer_city
                                , customer_state
                                , customer_zip_code
                                , file_last_modified_time
                FROM `prod-edl.ref_agency_information.t_wexler_active_clients`
                WHERE lower(trim(department)) in  ('commercial','commercial lines') '''

    ws_sql = '''SELECT DISTINCT customer_firm_name
                                , date_time
                                , action
                FROM `prod-edl.ref_agency_information.t_wexler_report_submission` '''

    jmis_active_sql = '''SELECT DISTINCT customer_name
                                , customer_number
                                , customer_type
                                , active_customer
                                , policy_status
                                , policy_expiration_date
                                , policy_executive
                                , department
                                , dba
                                , customer_address_1
                                , customer_address_2
                                , customer_city
                                , customer_state
                                , customer_zip_code
                                , policy_number
                                , file_last_modified_time
                FROM `prod-edl.ref_agency_information.t_jmis_active_clients`
                WHERE lower(trim(department)) in ('commercial','service center', 'commercial lines') '''


    dfla = gcp_bq_hook.get_pandas_df(la_sql)
    # print(dfla.shape)
    dfla = dfla.drop_duplicates().reset_index(drop=True)
    dfla = dfla.rename(columns={'customer_name': 'Lunar_CustomerName',
                                'customer_number': 'Lunar_CustomerNumber',
                                'customer_type': 'Lunar_CustomerType',
                                'active_customer': 'Lunar_ActiveCustomer',
                                'policy_status': 'Lunar_PolicyStatus',
                                'policy_expiration_date': 'Lunar_PolicyExpirationDate',
                                'policy_executive': 'Lunar_PolicyExecutive',
                                'policy_number': 'Lunar_PolicyNumber',
                                'dba': 'Lunar_DBA',
                                'customer_address_1': 'Lunar_Customer_Address1',
                                'customer_address_2': 'Lunar_Customer_Address2',
                                'customer_city': 'Lunar_Customer_City',
                                'customer_state': 'Lunar_Customer_State',
                                'customer_zip_code': 'Lunar_Customer_ZipCode'
                                })

    dfla = dfla[dfla.Lunar_ActiveCustomer == 'Active']
    dfla['Lunar_PolicyExpirationDate'] = np.where(dfla.Lunar_PolicyExpirationDate.astype(str) > '9999-01-01', np.nan,
                                                  dfla['Lunar_PolicyExpirationDate'])
    lunar_update_time = max(pd.to_datetime(dfla['file_last_modified_time'], utc=True)).strftime('%Y-%m-%d %H:%M')

    dfla['Lunar_Agency'] = 'Lunar'
    dfla['Lunar_Customer_Address1'] = dfla['Lunar_Customer_Address1'].fillna('None')

# Ramesh 06/29 change - 'protected' status must have valid policy_number, else mark it available.
    dfla['Lunar_Protected_Status'] = np.where(
        (dfla.Lunar_CustomerType == 'Customer') 
        & (dfla.Lunar_PolicyStatus.isin(['Active', 'Renewed', 'Rewritten'])) 
        & (pd.to_datetime(dfla.Lunar_PolicyExpirationDate) >= pd.to_datetime('today') - pd.to_timedelta("30day")) 
        & (dfla.Lunar_PolicyNumber != 'None')
        , 'Protected', 'Available')
    # print(dfla.Lunar_PolicyStatus.value_counts(dropna=False))

    dfla_distinct_cust_list = dfla["Lunar_CustomerNumber"][dfla.Lunar_Protected_Status == 'Protected'].unique().tolist()

    dfla["Lunar_Protected_Status"] = np.where((dfla.Lunar_CustomerNumber.isin(dfla_distinct_cust_list)), "Protected", "Available")

    # Wexler Active policies
  #  if 'csv' in wex_active_path:
  #      dfwa = pd.read_csv(wex_active_path)
  #  else:
  #      dfwa = pd.read_excel(wex_active_path)

    dfwa = gcp_bq_hook.get_pandas_df(wx_sql)
    # print(dfwa.shape)
    # dfwa = dfwa.drop(columns=['Policy Status'])
    dfwa = dfwa.drop_duplicates().reset_index(drop=True)

    dfwa = dfwa.rename(columns={'customer_name': 'Wex_CustomerName',
                                'customer_number': 'Wex_CustomerNumber',
                                'customer_type': 'Wex_CustomerType',
                                'active_customer': 'Wex_ActiveCustomer',
                                'policy_status': 'Wex_PolicyStatus',
                                'policy_expiration_date': 'Wex_PolicyExpirationDate',
                                'policy_executive': 'Wex_PolicyExecutive',
                                'policy_number': 'Wex_PolicyNumber',
                                'dba': 'Wex_DBA',
                                'customer_address_1': 'Wex_Customer_Address1',
                                'customer_address_2': 'Wex_Customer_Address2',
                                'customer_city': 'Wex_Customer_City',
                                'customer_state': 'Wex_Customer_State',
                                'customer_zip_code': 'Wex_Customer_ZipCode'
                                })

    dfwa['Wex_PolicyExpirationDate'] = pd.to_datetime(
        dfwa['Wex_PolicyExpirationDate'].astype(str).replace('9999-01-01 00:00:00', np.nan).replace('None', np.nan))

    wex_update_time = max(pd.to_datetime(dfwa['file_last_modified_time'], utc=True)).strftime('%Y-%m-%d %H:%M')

    dfwa = dfwa[dfwa.Wex_ActiveCustomer == 'Active']
    dfwa['Wex_Agency'] = 'Wexler'
    dfwa['Wex_Customer_Address1'] = dfwa['Wex_Customer_Address1'].fillna('None')
    # print(dfwa.shape)
    # print(dfwa.Wex_CustomerType.value_counts())

 #  REMOVED SUBMISSIONS SINCE DATA WAS INCONSISTENT. NOTIFIED KELLY.
 #
 #    # Wexler Submissions
 # #   usecols = ['Customer - Firm Name', 'Date/Time', 'Action']
 # #   dfwb = pd.read_csv(wex_submission_path, skiprows=3, usecols=usecols, engine='python')
 #    dfwb = gcp_bq_hook.get_pandas_df(ws_sql)
 #    # print(dfwb.shape)
 #    dfwb = dfwb.rename(columns={'customer_firm_name': 'Wex_CustomerName_Submissions',
 #                                'date_time': 'Date/Time',
 #                                'action': 'Action'})
 #
 #    # drop rows without a Firm Name and remove duplicates
 #    dfwb = dfwb[~dfwb['Wex_CustomerName_Submissions'].isnull()]
 #    dfwb = dfwb.drop_duplicates().reset_index(drop=True)
 #
 #    # drop actions != Submission, Application, by business rules, only Submission and Application actions can be considered for first submission
 #    dfwb = dfwb[dfwb.Action.isin(['Submission', 'Application'])].sort_values(
 #        ['Wex_CustomerName_Submissions', 'Date/Time'], ascending=True)
 #
 #    # keep only the first row which gives us the first submission date
 #    dfwb = dfwb.drop_duplicates('Wex_CustomerName_Submissions')
 #    print(dfwb.shape)
 #
 #    # split combined columns
 #    def split_submissions(x):
 #        if x == 'None':
 #            return pd.Series([None, None])
 #        else:
 #            try:
 #                test_x = str(x).split(' DBA:', 1)
 #                if len(test_x) == 2:
 #                    p_customer = test_x[0]
 #                    p_dba = test_x[1]
 #                elif len(test_x) == 1:
 #                    p_customer = test_x[0]
 #                    p_dba = None
 #            except:
 #                p_customer = None
 #                p_dba = None
 #
 #            return pd.Series([p_customer, p_dba])
 #
 #  #  dfwb[['Wex_CustomerName_Submissions', 'DBA_Submissions']] = dfwb['Wex_CustomerName_Submissions'].str.split(' DBA:',2,expand=True)
 #  #  dfwb[['Wex_CustomerName_Submissions', 'DBA_Submissions']] = dfwb['Wex_CustomerName_Submissions'].str.split(' DBA:',1,expand=True)
 #    dfwb[['Wex_CustomerName_Submissions', 'DBA_Submissions']] = dfwb['Wex_CustomerName_Submissions'].apply(split_submissions)
 #    dfwb['Wex_CustomerName_Submissions'] = dfwb['Wex_CustomerName_Submissions'].str.strip()
 #    print(dfwb)
 #
 #    # rename Date/Time to date
 #    dfwb = dfwb.rename(columns={'Date/Time': 'Wex_SubmissionDate'})
 #
 #    # calculate 90 day expiration date when new business protections lapse
 #    dfwb['Wex_SubmissionDate'] = pd.to_datetime(dfwb['Wex_SubmissionDate'])
 #    dfwb['Wex_Submission_Expiration_Date'] = dfwb['Wex_SubmissionDate'] + timedelta(days=90)
 #    print(dfwb.shape)
 #    print(dfwb[~dfwb.Wex_CustomerName_Submissions.isin(dfwa.Wex_CustomerName)])
 #
 #    # merge submission data to active
 #    dfwc = pd.merge(dfwa, dfwb, how='left', left_on='Wex_CustomerName', right_on='Wex_CustomerName_Submissions')
 #    dfwc = dfwc.drop(columns='Wex_CustomerName_Submissions')

    # dfwc['Wex_DBA'] = dfwc['Wex_DBA'].fillna(dfwc['DBA_Submissions'])

    dfwc = dfwa.copy()
    # print(dfwc.shape)

# Ramesh 06/29 change - 'protected' status must have valid policy_number, else mark it available.
    dfwc['Wex_Protected_Status'] = np.where(
           ((dfwc.Wex_CustomerType == 'Customer')
             & (dfwc.Wex_PolicyStatus.isin(['Active', 'Renewed', 'Rewritten']))
             & (pd.to_datetime(dfwc.Wex_PolicyExpirationDate) >= pd.to_datetime('today') - pd.to_timedelta("30day"))
             & (dfwc.Wex_PolicyNumber != 'None')  
           ) |
           ((dfwc.Wex_CustomerType == 'Customer')  & (dfwc.Wex_ActiveCustomer == 'Active') & (dfwc.Wex_PolicyNumber != 'None') )  , 'Protected', 'Available')

    logging.info(dfwc.Wex_Protected_Status.value_counts())

    dfwc_distinct_cust_list = dfwc["Wex_CustomerNumber"][dfwc.Wex_Protected_Status == 'Protected'].unique().tolist()

    dfwc["Wex_Protected_Status"] = np.where((dfwc.Wex_CustomerNumber.isin(dfwc_distinct_cust_list)), "Protected", "Available")

    # JMIS Active policies
  #  dfja = pd.read_excel(jmis_active_path)
    dfja = gcp_bq_hook.get_pandas_df(jmis_active_sql)
    dfja = dfja.drop(columns=['active_customer'])
    dfja = dfja.drop_duplicates().reset_index(drop=True)

    # rename columns
    dfja = dfja.rename(columns={'customer_name': 'JMIS_CustomerName',
                                'customer_number': 'JMIS_CustomerNumber',
                                'customer_type': 'JMIS_CustomerType',
                                'active_customer': 'JMIS_ActiveCustomer',
                                'policy_status': 'JMIS_Policy_Status',
                                'policy_expiration_date': 'JMIS_PolicyExpirationDate',
                                'policy_executive': 'JMIS_PolicyExecutive',
                                'policy_number': 'JMIS_PolicyNumber',
                                'dba': 'JMIS_DBA',
                                'customer_address_1': 'JMIS_Customer_Address1',
                                'customer_address_2': 'JMIS_Customer_Address2',
                                'customer_city': 'JMIS_Customer_City',
                                'customer_state': 'JMIS_Customer_State',
                                'customer_zip_code': 'JMIS_Customer_ZipCode',
                                'policy_number': 'JMIS_PolicyNumber'
                                })

    # subset to active only
    dfja = dfja[
        (dfja.JMIS_CustomerType == 'Customer') & (dfja.JMIS_Policy_Status.isin(['Active', 'Renewed', 'Rewritten'])) & (dfja.department == 'Commercial')]
    dfja['JMIS_Agency'] = 'JMIS'
    dfja['JMIS_Customer_Address1'] = dfja['JMIS_Customer_Address1'].fillna('None')

    jmis_update_time = max(pd.to_datetime(dfja['file_last_modified_time'], utc=True)).strftime('%Y-%m-%d %H:%M')

    # remove line of business from policy number
    def hyphen_split(a):
        if a.count("-") == 1:
            return a.split("-")[0]
        else:
            return "-".join(a.split("-", 2)[:2])

    dfja['JMIS_PolicyNumber'] = dfja['JMIS_PolicyNumber'].astype(str).apply(hyphen_split)
    dfja['JMIS_PolicyNumber'] = dfja['JMIS_PolicyNumber'].replace('nan', np.nan)
    dfja['JMIS_PolicyExpirationDate'] = pd.to_datetime(dfja['JMIS_PolicyExpirationDate'].astype(str).replace('9999-01-01 00:00:00', np.nan))


    # subset to used columns only
    dfja = dfja[[x for x in list(dfja) if 'JMIS' in x]]
    dfja = dfja.drop_duplicates()
    logging.info("JMIS Active: " + str(dfja.shape))

    dfjb = gcp_bq_hook.get_pandas_df(jmis_sf_sql)

    # split address 1 and 2
    try:
        dfjb[['JMIS_Customer_Address1', 'JMIS_Customer_Address2']] = dfjb['BillingStreet'].str.split('(?<!\d),', n=1, expand=True)
    except ValueError:
        dfjb['JMIS_Customer_Address1'] = dfjb['BillingStreet']
        dfjb['JMIS_Customer_Address2'] = ''

    # create expiration date
    dfjb['InitialProposalDate'] = pd.to_datetime(dfjb['InitialProposalDate'])
    dfjb['InitialProposalDate'] = dfjb['InitialProposalDate'].dt.tz_localize(None)
    dfjb['JMIS_Proposal_Expiration_Date'] = dfjb['InitialProposalDate'] + timedelta(days=90)

    # create active and policy status columns
    dfjb['JMIS_Policy_Status'] = 'Proposal'
    dfjb['JMIS_PolicyExpirationDate'] = None

    # rename columns
    dfjb = dfjb.rename(columns={'AccountId': 'JMIS_CustomerNumber',
                                'Name': 'JMIS_CustomerName',
                                'Type': 'JMIS_CustomerType',
                                'Master_Agency_Email__c': 'JMIS_PolicyExecutive',
                                'Jeweler_Long_Name__c': 'JMIS_DBA',
                                'BillingCity': 'JMIS_Customer_City',
                                'BillingStateCode': 'JMIS_Customer_State',
                                'BillingPostalCode': 'JMIS_Customer_ZipCode',
                                'BillingCountryCode': 'JMIS_Customer_CountryCode',
                                'InitialProposalDate': 'JMIS_InitialProposalDate',
                                'CurrentStage': 'JMIS_CurrentStage',
                                'CreatedDate': 'JMIS_CreatedDate',
                                'Account_Email__c': 'JMIS_Customer_Email',
                                'Policy_Number__c': 'JMIS_PolicyNumber'
                                })
    dfjb['JMIS_Agency'] = 'JMIS'
    dfjb['JMIS_Customer_Address1'] = dfjb['JMIS_Customer_Address1'].fillna('None')

    # reorder and subset columns
    dfjb = dfjb[list(dfja) + [x for x in list(dfjb) if (x not in list(dfja)) & ('JMIS' in x)]]

    # remove rows that are present in the active list
    dfjb = dfjb[~dfjb.JMIS_PolicyNumber.isin(dfja.JMIS_PolicyNumber.tolist())]
    # print(dfjb.shape)
    dfjc = pd.concat([dfja, dfjb], axis=0, ignore_index=True)
    dfjc = dfjc.drop_duplicates()
    dfjc['JMIS_CustomerNumber'] = dfjc['JMIS_CustomerNumber'].astype(str)
    # print(dfjc.shape)
# Ramesh 06/29 change - 'protected' status must have valid policy_number, else mark it available.
    dfjc['JMIS_Protected_Status'] = np.where(
        ((dfjc.JMIS_CustomerType == 'Customer') &(dfjc.JMIS_Policy_Status.isin(['Active', 'Renewed', 'Rewritten'])) 
                                                &(pd.to_datetime(dfjc.JMIS_PolicyExpirationDate) >= pd.to_datetime('today') - pd.to_timedelta("30day"))
                                                &(dfjc.JMIS_PolicyNumber != 'None')
        )  |  ( (dfjc.JMIS_CustomerType != 'Customer')  & (dfjc.JMIS_Proposal_Expiration_Date > pd.to_datetime('today')) & (dfjc.JMIS_PolicyNumber != 'None' )
              )
        , 'Protected', 'Available')

    dfjc_distinct_cust_list = dfjc["JMIS_CustomerNumber"][dfjc.JMIS_Protected_Status == 'Protected'].unique().tolist()

    dfjc["JMIS_Protected_Status"] = np.where((dfjc.JMIS_CustomerNumber.isin(dfjc_distinct_cust_list)), "Protected", "Available")

    # for index, row in dfjc.iterrows():
    #     try:
    #         if (((row['JMIS_CustomerType'] == 'Customer') & (row['JMIS_Policy_Status'] in ['Active', 'Renewed', 'Rewritten'])
    #         & (pd.to_datetime(row['JMIS_PolicyExpirationDate']) >= pd.to_datetime('today') - pd.to_timedelta("30day"))) or
    #             ((row['JMIS_CustomerType'] != 'Customer') & (row['JMIS_Proposal_Expiration_Date'] > pd.to_datetime('today')))):
    #             dfjc.at[index,'JMIS_Protected_Status'] = 'Protected'
    #         else:
    #             dfjc.at[index, 'JMIS_Protected_Status'] = 'Available'
    #
    #     except:
    #         raise
    #         dfjc.at[index, 'JMIS_Protected_Status'] = 'Undefined'



    # print(dfjc.shape)

    dfia = gcp_bq_hook.get_pandas_df(jmic_sql)
    jmic_update_time = min(pd.to_datetime(dfia.Last_Research_Update)).strftime('%Y-%m-%d %H:%M')
    # rename columns
    dfia = dfia.rename(columns={'AccountNumber': 'JMIC_CustomerNumber',
                                'PolicyInsuredContactFullName': 'JMIC_CustomerName',
                                'AgencyName': 'JMIC_Agency',
                                'LocationAddress1': 'JMIC_Customer_Address1',
                                'LocationAddress2': 'JMIC_Customer_Address2',
                                'LocationCity': 'JMIC_Customer_City',
                                'LocationStateCode': 'JMIC_Customer_State',
                                'LocationPostalCode': 'JMIC_Customer_ZipCode',
                                'LocationCountryCode': 'JMIC_Customer_CountryCode'
                                })
    dfia['JMIC_Protected_Status'] = 'Protected'
    dfia['JMIC_DBA'] = np.nan
    print(dfia[dfia.experian_business_id.isnull()].shape)

    def subset_new_existing(df, sql, agency, jdp_src=False):
        c_num = f'{agency}_CustomerNumber'
        c_addr = f'{agency}_Customer_Address1'

        df_old = gcp_bq_hook.get_pandas_df(sql)
        df_old = df_old.drop_duplicates(subset=[c_num, c_addr]).copy()

        try:
            df_old[c_num] = df_old[c_num].astype('int64')
            df[c_num] = df[c_num].astype('int64')
        except ValueError:
            df_old[c_num] = df_old[c_num].astype(str)
            df[c_num] = df[c_num].astype(str)

        # subset records that already exist and have experian
        df_existing = df[(df[c_num].isin(df_previous_records[c_num].tolist())) & (
            df[c_addr].isin(df_previous_records[c_addr].tolist()))]
        if jdp_src:
            exp_cols = ['experian_business_id', 'experian_update_time', 'experian_name', 'experian_street',
                        'experian_city', 'experian_state', 'experian_zip']
            df_existing = pd.merge(df_existing.drop(columns=exp_cols), df_old, how='left', on=[c_num, c_addr])
        else:
            df_existing = pd.merge(df_existing, df_old, how='left', on=[c_num, c_addr])

        # subset new records and those that need to check for experian numbers
        df_new = df[(~df[c_num].isin(df_existing[c_num].tolist())) | ~(df[c_addr].isin(df_existing[c_addr].tolist()))]

        logging.info(f'existing: {df_existing.shape}')
        logging.info(f'new     : {df_new.shape}')

        return df_existing, df_new

    def exp_data_merge(exp_records, df_new, df_existing, agency, df_current_records, ams_src=True):
        df_expa = pd.DataFrame(exp_records)

        if len(df_expa) > 0:
            # remove duplicates, ignore update time since it's not relevant
            df_expa = df_expa[~df_expa.duplicated(subset=[x for x in list(df_expa) if x is not 'experian_update_time'])]

            # remove payload duplicates since this would create a many to many relationship when joining back to main dataset
            df_expa = df_expa.drop_duplicates(subset='payload')

            # normalize payload json column. I know it's ugly
            df_expb = pd.concat(
                [df_expa, df_expa.payload.str.replace('NaN', '""').map(lambda x: dict(eval(x))).apply(pd.Series)],
                axis=1)
            df_expb = df_expb.drop(columns=['payload', 'subcode'])

            # join 	 output to dataset
            left_cols = [f'{agency}_CustomerName', f'{agency}_Customer_Address1', f'{agency}_Customer_City',
                         f'{agency}_Customer_State', f'{agency}_Customer_ZipCode']
            right_cols = ['name', 'street', 'city', 'state', 'zip']
            dfd = pd.merge(df_new, df_expb, how='left', left_on=left_cols, right_on=right_cols).drop(columns=right_cols)
            dfd = dfd.rename(columns={'experian_name': 'Experian_CustomerName',
                                      'experian_street': 'Experian_Customer_Address1',
                                      'experian_city': 'Experian_Customer_City',
                                      'experian_state': 'Experian_Customer_State',
                                      'experian_zip': 'Experian_Customer_ZipCode',
                                      'experian_update_time': f'{agency}_experian_update_time'
                                      })
        else:
            dfd = pd.DataFrame(columns=[f'{agency}_experian_update_time'])

        # union new with existing
        dfd = pd.concat([dfd, df_existing], axis=0, ignore_index=True)
        dfd[f'{agency}_experian_update_time'] = dfd[f'{agency}_experian_update_time'].fillna(str(dt.utcnow()))

        # add unique records to current records list
        df_current_records = df_current_records.append(dfd[['experian_business_id', f'{agency}_CustomerNumber',
                                                            f'{agency}_Customer_Address1',
                                                            f'{agency}_experian_update_time']].drop_duplicates(
            subset=[f'{agency}_CustomerNumber', f'{agency}_Customer_Address1']))

        # create subset with bin matches, remove rows with duplicate bin values, keep customer type based on numerical value
        if ams_src:
            # map customer type names to numerical ordering, this is needed b/c business can appear more than once under different customer types
            dfd[f'{agency}_CustomerType'] = dfd[f'{agency}_CustomerType'].replace({'Customer': '1. Customer',
                                                                                   'Suspect': '2. Suspect',
                                                                                   'Prospect': '3. Prospect',
                                                                                   'Former Customer': '4. Former Customer'
                                                                                   })
            dfd_bin = dfd[dfd.experian_business_id > 0].sort_values(f'{agency}_CustomerType')
        else:
            dfd_bin = dfd[dfd.experian_business_id > 0]

        # drop duplicates based on bin and address
        dfd_bin = dfd_bin.drop_duplicates(subset=['experian_business_id', f'{agency}_Customer_Address1'])

        # create subset without bin matches
        dfd_nobin = dfd[(dfd.experian_business_id <= 0) | dfd.experian_business_id.isnull()]

        print(f'nobin: {dfd_nobin.shape}')
        print(f'bin  : {dfd_bin.shape}')

        return dfd_nobin, dfd_bin, df_current_records

    # create empty data frame for tracking all records
    df_current_records = pd.DataFrame()

    # loads this list of previous records to help filter the new from the old

    sql = '''
    SELECT *
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_current_records`
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)
    df_previous_records = gcp_bq_hook.get_pandas_df(sql)
    # print(df_previous_records.shape)


    # After the initial load, we can compare with existing based on customer number.
    # Only new records will be run through experian
    # Records that failed an experian match will be rerun after 7 days
    exp = ExperianBin()

    sql = '''
    SELECT DISTINCT Lunar_CustomerNumber, Lunar_Customer_Address1, experian_business_id, Experian_CustomerName, Experian_Customer_Address1, Experian_Customer_City, Experian_Customer_State, Experian_Customer_ZipCode
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_matches`
    WHERE (Lunar_CustomerNumber IS NOT NULL AND (experian_business_id > 0) OR (Lunar_CustomerNumber IS NOT NULL AND DATE(CAST(Lunar_experian_update_time as DATETIME)) > DATE_SUB(current_date(), INTERVAL 30 DAY) ))
    AND NOT (experian_business_id IS NOT NULL AND Experian_CustomerName IS NULL)
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)

    dflc_existing, dflc_new = subset_new_existing(dfla, sql, 'Lunar')
    cols = ['Lunar_CustomerName', 'Lunar_Customer_Address1', 'Lunar_Customer_City', 'Lunar_Customer_State',
            'Lunar_Customer_ZipCode']
    payloads = dflc_new[cols].rename(columns={'Lunar_CustomerName': 'name',
                                              'Lunar_Customer_Address1': 'street',
                                              'Lunar_Customer_City': 'city',
                                              'Lunar_Customer_State': 'state',
                                              'Lunar_Customer_ZipCode': 'zip'}).to_dict(orient='records')

    exp_records = []
    i = 0
    print(f'Total records to process: {len(payloads)}')

    # Process through experian
    for i in range(i, len(payloads), 1):
        try:
            print("Lunar data experial calls ",payloads[i])
            r = exp.execute(payloads[i])
        except:
            sleep(5)
            r = exp.execute(payloads[i])
        exp_records.append(r)
        if i % 30 == 0:
            print("i =", i)
            print("r =", r)
            sleep(60)
    print('Done.')

    dfld_nobin, dfld_bin, df_current_records = exp_data_merge(exp_records, dflc_new, dflc_existing, 'Lunar', df_current_records)
    print(dfld_bin[~(dfld_bin.experian_business_id.isnull()) & (dfld_bin.Experian_CustomerName.isnull())])


    # After the initial load, we can compare with existing based on customer number.
    # Only new records will be run through experian
    # Records that failed an experian match will be rerun after 7 days

    sql = '''
    SELECT DISTINCT Wex_CustomerNumber, Wex_Customer_Address1, experian_business_id, Experian_CustomerName, Experian_Customer_Address1, Experian_Customer_City, Experian_Customer_State, Experian_Customer_ZipCode--, CAST(Wex_experian_update_time as DATETIME) as Wex_experian_update_time
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_matches`
    WHERE Wex_CustomerNumber IS NOT NULL 
    AND (experian_business_id > 0 OR DATE(CAST(Wex_experian_update_time as DATETIME)) > DATE_SUB(current_date(), INTERVAL 30 DAY) )
    AND NOT (experian_business_id IS NOT NULL AND Experian_CustomerName IS NULL)
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)

    dfwc_existing, dfwc_new = subset_new_existing(dfwc, sql, 'Wex')
    cols = ['Wex_CustomerName', 'Wex_Customer_Address1', 'Wex_Customer_City', 'Wex_Customer_State',
            'Wex_Customer_ZipCode']
    payloads = dfwc_new[cols].rename(columns={'Wex_CustomerName': 'name',
                                              'Wex_Customer_Address1': 'street',
                                              'Wex_Customer_City': 'city',
                                              'Wex_Customer_State': 'state',
                                              'Wex_Customer_ZipCode': 'zip'}).to_dict(orient='records')

    exp_records = []
    i = 0
    print(f'Total records to process: {len(payloads)}')

    for i in range(i, len(payloads), 1):
        print("Wexler data experian calls ",payloads[i])
        r = exp.execute(payloads[i])
        exp_records.append(r)
        if i % 30 == 0:
            print("i =", i)
            print("r =", r)
            sleep(60)
    print('Done.')

    dfw_expa = pd.DataFrame(exp_records)
    print(dfw_expa.shape)

    dfwd_nobin, dfwd_bin, df_current_records = exp_data_merge(exp_records, dfwc_new, dfwc_existing, 'Wex', df_current_records)

    # After the initial load, we can compare with existing based on customer number.
    # Only new records will be run through experian
    # Records that failed an experian match will be rerun after 7 days

    sql = '''
    SELECT DISTINCT JMIS_CustomerNumber, JMIS_Customer_Address1, experian_business_id, Experian_CustomerName, Experian_Customer_Address1, Experian_Customer_City, Experian_Customer_State, Experian_Customer_ZipCode, CAST(JMIS_experian_update_time as DATETIME) as JMIS_experian_update_time
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_matches`
    WHERE JMIS_CustomerNumber != 'nan' AND (experian_business_id > 0 OR DATE(CAST(JMIS_experian_update_time as DATETIME)) > DATE_SUB(current_date(), INTERVAL 30 DAY) )
    AND NOT (experian_business_id IS NOT NULL AND Experian_CustomerName IS NULL)
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)

    dfjc_existing, dfjc_new = subset_new_existing(dfjc, sql, 'JMIS')

    # After the initial load, we can compare with existing based on customer number.
    # Only new records will be run through experian
    # Records that failed an experian match will be rerun after 7 days

    sql = '''
    SELECT DISTINCT JMIS_CustomerNumber, JMIS_Customer_Address1, experian_business_id, Experian_CustomerName, Experian_Customer_Address1, Experian_Customer_City, Experian_Customer_State, Experian_Customer_ZipCode, CAST(JMIS_experian_update_time as DATETIME) as JMIS_experian_update_time
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_matches`
    WHERE JMIS_CustomerNumber != 'nan' AND (experian_business_id > 0 OR DATE(CAST(JMIS_experian_update_time as DATETIME)) > DATE_SUB(current_date(), INTERVAL 30 DAY) )
    AND NOT (experian_business_id IS NOT NULL AND Experian_CustomerName IS NULL)
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)
    dfj_old = gcp_bq_hook.get_pandas_df(sql)
    dfj_old['JMIS_CustomerNumber'] = dfj_old['JMIS_CustomerNumber'].astype(str)
    dfjc['JMIS_CustomerNumber'] = dfjc['JMIS_CustomerNumber'].astype(str)
    # subset records that already exist and have experian
    dfjc_existing = dfjc[dfjc.JMIS_CustomerNumber.isin(
        df_previous_records.JMIS_CustomerNumber.tolist()) & dfjc.JMIS_Customer_Address1.isin(
        df_previous_records.JMIS_Customer_Address1.tolist())]
    dfjc_existing = pd.merge(dfjc_existing, dfj_old, how='left',
                             on=['JMIS_CustomerNumber', 'JMIS_Customer_Address1'])

    # subset new records and those that don't have experian numbers
    dfjc_new = dfjc[~(dfjc.JMIS_CustomerNumber.isin(dfjc_existing.JMIS_CustomerNumber.tolist())) | ~(
        dfjc.JMIS_Customer_Address1.isin(dfjc_existing.JMIS_Customer_Address1.tolist()))]

    # print(dfjc_new.shape)
    # print(dfjc_existing.shape)

    cols = ['JMIS_CustomerName', 'JMIS_Customer_Address1', 'JMIS_Customer_City', 'JMIS_Customer_State',
            'JMIS_Customer_ZipCode']
    payloads = dfjc_new[cols].rename(columns={'JMIS_CustomerName': 'name',
                                              'JMIS_Customer_Address1': 'street',
                                              'JMIS_Customer_City': 'city',
                                              'JMIS_Customer_State': 'state',
                                              'JMIS_Customer_ZipCode': 'zip'}).to_dict(orient='records')

    exp_records = []
    i = 0
    print(f'Total records to process: {len(payloads)}')

    for i in range(i, len(payloads), 1):
        try:
            print("JMIS data experian calls ",payloads[i])
            r = exp.execute(payloads[i])
        except:
            print(r)
            sleep(2)
            r = exp.execute(payloads[i])
        exp_records.append(r)
        if i % 30 == 0:
            print("i =", i)
            print("r =", r)
            sleep(60)
    print('Done.')

    # print(df_current_records.shape)

    dfjd_nobin, dfjd_bin, df_current_records = exp_data_merge(exp_records, dfjc_new, dfjc_existing, 'JMIS', df_current_records)
    # print(df_current_records.shape)

    # After the initial load, we can compare with existing based on customer number.
    # Only new records will be run through experian
    # Records that failed an experian match will be rerun after 7 days

    sql = '''
    SELECT DISTINCT JMIC_CustomerNumber, JMIC_Customer_Address1, experian_business_id, Experian_CustomerName, Experian_Customer_Address1, Experian_Customer_City, Experian_Customer_State, Experian_Customer_ZipCode, CAST(JMIC_experian_update_time as DATETIME) as JMIC_experian_update_time
    FROM `{dest_project}.{dest_dataset}.nonpromoted_agency_clearance_matches`
    WHERE JMIC_CustomerNumber IS NOT NULL AND (experian_business_id > 0 OR DATE(CAST(JMIC_experian_update_time as DATETIME)) > DATE_SUB(current_date(), INTERVAL 30 DAY) )
    AND NOT (experian_business_id IS NOT NULL AND Experian_CustomerName IS NULL)
    '''.format(dest_project=destination_project, dest_dataset=destination_dataset)
    dfic_existing, dfic_new = subset_new_existing(dfia.copy(), sql, 'JMIC', jdp_src=True)

    dfic_new_bin = dfic_new[~dfic_new.experian_business_id.isnull()]
    dfic_new_nobin = dfic_new[dfic_new.experian_business_id.isnull()]

    print(dfic_new_bin.shape)
    print(dfic_new_nobin.shape)

    cols = ['JMIC_CustomerName', 'JMIC_Customer_Address1', 'JMIC_Customer_City', 'JMIC_Customer_State',
            'JMIC_Customer_ZipCode']
    payloads = dfic_new_nobin[cols].rename(columns={'JMIC_CustomerName': 'name',
                                                    'JMIC_Customer_Address1': 'street',
                                                    'JMIC_Customer_City': 'city',
                                                    'JMIC_Customer_State': 'state',
                                                    'JMIC_Customer_ZipCode': 'zip'}).to_dict(orient='records')

    exp_records = []
    i = 0
    print(f'Total records to process: {len(payloads)}')

    for i in range(i, len(payloads), 1):
        print("JMIC data experian calls ",payloads[i])
        r = exp.execute(payloads[i])
        exp_records.append(r)
        if i % 30 == 0:
            print("i =", i)
            print("r =", r)
            sleep(60)
    print('Done.')

    # required since jdp sources already have experian columns
    drop_cols = ['experian_business_id', 'experian_update_time', 'experian_name', 'experian_street', 'experian_city',
                 'experian_state', 'experian_zip']

    dfid_nobin, dfid_bin, df_current_records = exp_data_merge(exp_records, dfic_new_nobin.drop(columns=drop_cols),
                                                              dfic_existing, 'JMIC', df_current_records, ams_src=False)

    # since this is a jdp source, new records coming in with pre-filled BIN#s need to be added back into bin dataframe
    dfic_new_bin = dfic_new_bin.rename(columns={'experian_name': 'Experian_CustomerName',
                                                'experian_street': 'Experian_Customer_Address1',
                                                'experian_city': 'Experian_Customer_City',
                                                'experian_state': 'Experian_Customer_State',
                                                'experian_zip': 'Experian_Customer_ZipCode',
                                                'experian_update_time': 'JMIC_experian_update_time'
                                                })

    dfid_bin = dfid_bin.append(dfic_new_bin)

    agency = 'JMIC'
    df_current_records = df_current_records.append(dfic_new_bin[['experian_business_id', f'{agency}_CustomerNumber',
                                                                 f'{agency}_Customer_Address1',
                                                                 f'{agency}_experian_update_time']].drop_duplicates(
        subset=[f'{agency}_CustomerNumber', f'{agency}_Customer_Address1']))

    print(dfid_bin.shape)
    print(df_current_records.shape)

    def upload_to_bq(dataframe, table):
        # upload matches to bigquery
        # schema = [bigquery.SchemaField(x, 'STRING') for x in
        #           list(dataframe.select_dtypes(include='object'))]
        # schema.extend([bigquery.SchemaField(x, 'FLOAT') for x in
        #                list(dataframe.select_dtypes(include='number'))])

        types = {
            "i": "INTEGER",
            "b": "BOOLEAN",
            "f": "FLOAT",
            "O": "STRING",
            "S": "STRING",
            "U": "STRING",
            "M": "TIMESTAMP",
        }

        schema = [{'name': x, 'type': types.get(dtype.kind, 'STRING')} for x, dtype in dataframe.dtypes.iteritems()]

        # job_config = bigquery.LoadJobConfig(
        #     # Specify a (partial) schema. All columns are always written to the
        #     # table. The schema is used to assist in data type definitions.
        #     schema=schema,
        #     write_disposition="WRITE_TRUNCATE"
        # )
        #
        table_id = table

     #   job = bqclient.load_table_from_dataframe(
     #       dataframe, table_id, job_config=job_config
     #   )  # Make an API request.
     #   job.result()  # Wait for the job to complete.

     #   table = bqclient.get_table(table_id)  # Make an API request.

        print(dataframe.dtypes)
        print(schema)
        dataframe.to_gbq(table,
                         project_id=destination_project,
                         if_exists='replace',
                         credentials=credentials,
                         table_schema=schema)
        print(
            "Loaded {} rows and {} columns to {}".format(
                len(dataframe), len(schema), table_id
            )
        )

    # join wex and jmis rows w/ bin numbers
    exp_cols = ['experian_business_id', 'Experian_CustomerName', 'Experian_Customer_Address1', 'Experian_Customer_City',
                'Experian_Customer_State', 'Experian_Customer_ZipCode']

    lunar_cols = [x for x in list(dfld_bin) if 'Lunar' in x]
    wex_cols = [x for x in list(dfwd_bin) if 'Wex' in x]
    jmis_cols = [x for x in list(dfjd_bin) if 'JMIS' in x]
    jmic_cols = [x for x in list(dfid_bin) if 'JMIC' in x]

    dfma = pd.merge(dfjd_bin[exp_cols + jmis_cols], dfwd_bin[exp_cols + wex_cols], how='outer', on=exp_cols)
    dfma = pd.merge(dfma, dfld_bin[exp_cols + lunar_cols], how='outer', on=exp_cols)
    dfma = pd.merge(dfma, dfid_bin[exp_cols + jmic_cols], how='outer', on=exp_cols)

    # union wex & jmis rows w/o bin numbers
    dfmb = pd.concat(
        [dfjd_nobin[exp_cols + jmis_cols], dfwd_nobin[exp_cols + wex_cols], dfld_nobin[exp_cols + lunar_cols],
         dfid_nobin[exp_cols + jmic_cols]], axis=0, ignore_index=True)
    dfmb['experian_business_id'] = np.nan

    # union bin and non-bin rows
    dfm = pd.concat([dfma, dfmb], axis=0, ignore_index=True)

    # conflict logic, count # of times "Protected" shows up in same row
    dfm['PotentialConflict'] = np.where(dfm.JMIS_Protected_Status == 'Undefined','Undefined',
       np.where(dfm[['JMIS_Protected_Status', 'Wex_Protected_Status', 'Lunar_Protected_Status', 'JMIC_Protected_Status']].apply(
            lambda row: '|'.join(row.values.astype(str)), axis=1).str.count('Protected') > 1, 'Conflict', 'Okay'))

    # TODO  warning if JMIS prospect is in the sales pipeline
    dfm['PotentialConflict'] = np.where(
        (dfm.JMIS_CurrentStage.isin(['Vision and Alignment', 'Discovery'])) & (dfm.Wex_Protected_Status == 'Protected'),
        'Warning', dfm['PotentialConflict'])

    # dtype conversions
    dfm['JMIS_CustomerNumber'] = dfm['JMIS_CustomerNumber'].astype(str)
    dfm['JMIS_experian_update_time'] = pd.to_datetime(dfm['JMIS_experian_update_time'], utc=True, cache=False)
    dfm['Wex_experian_update_time'] = pd.to_datetime(dfm['Wex_experian_update_time'], utc=True, cache=False)
    dfm['Lunar_experian_update_time'] = pd.to_datetime(dfm['Lunar_experian_update_time'], utc=True, cache=False)
    dfm['JMIC_experian_update_time'] = pd.to_datetime(dfm['JMIC_experian_update_time'], utc=True, cache=False)

    dfm['JMIS_PolicyExpirationDate'] = dfm['JMIS_PolicyExpirationDate'].fillna(np.nan)
    dfm['JMIS_PolicyExpirationDate'] = pd.to_datetime(dfm['JMIS_PolicyExpirationDate'], utc=True, cache=False)
    dfm['Wex_PolicyExpirationDate'] = pd.to_datetime(dfm['Wex_PolicyExpirationDate'], utc=True, cache=False)
    dfm['Lunar_PolicyExpirationDate'] = pd.to_datetime(dfm['Lunar_PolicyExpirationDate'], utc=True, cache=False)
    dfm['JMIC_experian_update_time'] = pd.to_datetime(dfm['JMIC_experian_update_time'], utc=True, cache=False)

    # print(dfm.shape)

    assert len(dfm[~(dfm.experian_business_id.isnull()) & (
        dfm.Experian_CustomerName.isnull())]) == 0, 'Should not have experian_business_id without Experian_CustomerName'

  #  dfm.to_csv('data/complete_table_001_20210107.csv', index=False)


    # upload matches to bigquery
    upload_to_bq(dfm, "{dest_dataset}.nonpromoted_agency_clearance_matches".format(dest_dataset=destination_dataset))

   # dfm[dfm.PotentialConflict != 'Okay'].to_csv('data/agency_conflicts20210101.csv', index=False)

    print(dfm[dfm.PotentialConflict != 'Okay'].shape)
    print(dfm.shape)

    def clearance_unpivot(df, agency):
        ecols = ['experian_business_id', 'Experian_CustomerName', 'Experian_DBA', 'Experian_Customer_Address1',
                 'Experian_Customer_City', 'Experian_Customer_State', 'Experian_Customer_ZipCode', f'{agency}_Agency']
        col_list = ['CustomerName', 'DBA', 'Customer_Address1', 'Customer_City', 'Customer_State', 'Customer_ZipCode',
                    'Agency', 'Protected_Status']
        cols = ['experian_business_id'] + [f'{agency}_' + x for x in col_list]
        col_list2 = ['experian_business_id'] + col_list

        df1 = df[~df[f'{agency}_CustomerName'].isnull()][cols]
        df1.columns = col_list2
        dfe = df[(~df[f'{agency}_CustomerName'].isnull()) & (~df.experian_business_id.isnull())].copy()
        dfe['Experian_DBA'] = np.nan
        dfe = dfe[ecols]
        dfe = dfe.rename(columns={f'{agency}_Agency': 'Experian_Agency'})
        dfe = pd.merge(dfe, df1[['experian_business_id', 'Protected_Status']], how='left', on='experian_business_id')
        dfe.columns = col_list2
        df1 = df1.append(dfe)

        return df1

    # unpivot
    dfj1 = clearance_unpivot(dfm, 'JMIS')
    dfw1 = clearance_unpivot(dfm, 'Wex')
    dfl1 = clearance_unpivot(dfm, 'Lunar')
    dfi1 = clearance_unpivot(dfm, 'JMIC')

    # combined
    dfu = pd.concat([dfj1, dfw1, dfl1, dfi1])
    dfu = dfu.sort_values('Protected_Status', ascending=False)
    dfu = dfu[~dfu.duplicated([x for x in list(dfu) if x != 'Protected_Status'])]

    # add update times
    dfu['Last_Refresh'] = pd.to_datetime(dt.now().strftime('%Y-%m-%d %H:%M'))
    dfu['Lunar_Update_Time'] = lunar_update_time
    dfu['Wex_Update_Time'] = wex_update_time
    dfu['JMIS_Update_Time'] = jmis_update_time
    dfu['JMIC_Update_Time'] = jmic_update_time

    assert len(dfu[dfu.CustomerName.isnull()]) == 0, 'Null customer name should not exist'

    print(dfu.drop_duplicates().shape)

    # upload clearance
    upload_to_bq(dfu, "{dest_dataset}.nonpromoted_agency_clearance".format(dest_dataset=destination_dataset))

    df_current_records['JMIS_CustomerNumber'] = df_current_records['JMIS_CustomerNumber'].astype(str)
    df_current_records['JMIS_experian_update_time'] = pd.to_datetime(df_current_records['JMIS_experian_update_time'], utc=True)
    df_current_records['Wex_experian_update_time'] = pd.to_datetime(df_current_records['Wex_experian_update_time'], utc=True)
    df_current_records['Lunar_experian_update_time'] = pd.to_datetime(df_current_records['Lunar_experian_update_time'], utc=True)
    df_current_records['JMIC_experian_update_time'] = pd.to_datetime(df_current_records['JMIC_experian_update_time'], utc=True)

    # upload current records
    if len(df_current_records) > 0:
        upload_to_bq(df_current_records, "{dest_dataset}.nonpromoted_agency_clearance_current_records".format(dest_dataset=destination_dataset))

if __name__ == "__main__":
    print('This file cannot be executed from the command line. This was repurposed for airflow only.')