"""
Author: Gayathri Narayanasamy
This operator download trade association files from GCS and does data processing and write the result back to Sharepoint location
"""

import os
import sys
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
import tempfile
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import logging
import json
from plugins.hooks.jm_mssql import MsSqlHook
import io
import datetime as dt
from plugins.hooks.jm_mssql import MsSqlHook
#import pyodbc
from tempfile import NamedTemporaryFile
import numpy as np
#from fuzzywuzzy import process
#from fuzzywuzzy import fuzz
from thefuzz import fuzz
from thefuzz import process
import re



address = "123-127 Main St"
s = r"^\d+|\S+ +\S+$"
re.findall(s, address)


class TradeAssociationOperator(BaseOperator):
    template_fields = ('gcs_destination_path',)
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 gcs_destination_path,
                 google_storage_connection_id,
                 sharepoint_connection = 'NA',
                 bq_gcp_connection_id = 'NA',
                 semi_managed_connection = 'NA',
                 destination_file_url = 'NA',
                 source = 'NA',
                 project = 'NA',
                 dataset = 'NA',
                 table = 'NA',
                 mime_type="text/plain",
                 destination_bucket='jm-edl-landing-wip',
                 delegate_to=None,
                 mssql_connection='',
                 *args,
                 **kwargs):


        super(TradeAssociationOperator, self).__init__(*args, **kwargs)
        self.sharepoint_site = sharepoint_connection
        self.google_storage_connection = google_storage_connection_id
        self.bq_gcp_connection_id =bq_gcp_connection_id
        self.destination_file_url = destination_file_url
        self.gcs_destination_path = gcs_destination_path
        self.mime_type = mime_type
        self.destination_bucket = destination_bucket
        self.delegate_to = delegate_to
        self.semi_managed_connection_id = semi_managed_connection
        self.hook = GCSHook(
            google_cloud_storage_conn_id=self.google_storage_connection,
            delegate_to=self.delegate_to)
        self.bq_hook =  BigQueryHook(gcp_conn_id= self.bq_gcp_connection_id)
        self.new_pull = True
        self.project = project
        self.dataset = dataset
        self.table = table
        self.mssql_connection=mssql_connection



    def clean(self,item,rstring):
        if isinstance(item, list) == True:
            newlist = []
            for word in item:
                for s in rstring:
                    word = str(word)
                    word = word.lower()
                    new = word.replace(s, "")
                    word = new
                newlist.append(word)
            return newlist
        else:
            for s in rstring:
                item = str(item)
                item = item.lower()
                new = item.replace(s, "")
                item = new
            return item

    def execute(self, context):

        logging.info("start proessing ..")
        sql = '''
        --MP_Clearance (V2)
        SELECT   experian_business_id,
                 Experian_CustomerName,
                 Experian_Customer_Address1,
                 Experian_Customer_City,
                 Experian_Customer_State,
                 Experian_Customer_ZipCode,
                 JMIS_CustomerName,
                 JMIS_CustomerNumber, 
                 JMIS_CustomerType,
                 JMIS_PolicyNumber,
                 JMIS_PolicyExpirationDate,
                 JMIS_Policy_Status,
                 JMIS_PolicyExecutive,
                 JMIS_DBA,
                 JMIS_Customer_Address1,
                 JMIS_Customer_City,
                 JMIS_Customer_State,
                 JMIS_Customer_ZipCode,
                 JMIS_Customer_Address2,
                 JMIS_Agency,
                 JMIS_Customer_CountryCode,
                 JMIS_CurrentStage,
                 JMIS_Customer_Email,
                 JMIS_InitialProposalDate,
                 JMIS_Proposal_Expiration_Date,
                 JMIS_Protected_Status,
                 JMIS_experian_update_time,
                 Wex_CustomerName,
                 Wex_CustomerNumber,
                 Wex_CustomerType,
                 Wex_ActiveCustomer,
                 Wex_PolicyExpirationDate,
                 Wex_PolicyStatus,
                 Wex_PolicyExecutive,
                 Wex_DBA,
                 Wex_Customer_Address1,
                 Wex_Customer_City,
                 Wex_Customer_State,
                 Wex_Customer_ZipCode,
                 Wex_Customer_Address2,
                 Wex_Agency,
                 --Wex_SubmissionDate,
                 --Wex_Submission_Expiration_Date,
                 Wex_Protected_Status,
                 Wex_experian_update_time,
                 Lunar_CustomerName,
                 Lunar_CustomerNumber,
                 Lunar_CustomerType,
                 Lunar_ActiveCustomer,
                 Lunar_PolicyExpirationDate,
                 Lunar_PolicyStatus,
                 Lunar_PolicyExecutive,
                 Lunar_DBA,
                 Lunar_Customer_Address1,
                 Lunar_Customer_City,
                 Lunar_Customer_State,
                 Lunar_Customer_ZipCode,
                 Lunar_Customer_Address2,
                 Lunar_Agency,
                 Lunar_Protected_Status,
                 Lunar_experian_update_time,
                 JMIC_CustomerNumber,
                 JMIC_CustomerName,
                 JMIC_Customer_City,
                 JMIC_Customer_State,
                 JMIC_Customer_CountryCode,
                 JMIC_Agency,
                 JMIC_Customer_Address1,
                 JMIC_Customer_Address2,
                 JMIC_Customer_ZipCode,
                 JMIC_Protected_Status,
                 JMIC_DBA,
                 JMIC_experian_update_time,
                 PotentialConflict,

                 --(CASE WHEN JMIS_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN Wex_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN JMIC_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN Lunar_CustomerName IS NULL THEN 1 ELSE 0 END) AS NullCount        
        FROM        semi-managed-reporting.data_products_t2_sales.nonpromoted_agency_clearance_matches nacm
        -- FULL JOIN   semi-managed-reporting.data_products_t2_sales.nonpromoted_agency_clearance nac
        -- ON          nacm.experian_business_id = nac.experian_business_id
        --WHERE          (CASE WHEN JMIS_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN Wex_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN JMIC_CustomerName IS NULL THEN 1 ELSE 0 END) + 
                 --(CASE WHEN Lunar_CustomerName IS NULL THEN 1 ELSE 0 END) <> 3
        '''
        temp1 = self.bq_hook.get_pandas_df(sql=sql)
        agents = ['exp', 'jmic', 'jmis', 'lunar', 'wex']

        col_dict = {}

        col_dict['exp'] = ['agency_combo', 'experian_business_id', 'Experian_Customer_Address1',
                           'Experian_Customer_State',
                           'PotentialConflict', 'Experian_CustomerName', 'Experian_Customer_City',
                           'Experian_Customer_ZipCode']

        col_dict['jmic'] = ['JMIC_Agency', 'JMIC_Customer_Address1', 'JMIC_Customer_CountryCode', 'JMIC_DBA',
                            'JMIC_CustomerName',
                            'JMIC_Customer_Address2', 'JMIC_Customer_State', 'JMIC_Protected_Status',
                            'JMIC_CustomerNumber',
                            'JMIC_Customer_City', 'JMIC_Customer_ZipCode', 'JMIC_experian_update_time']

        col_dict['jmis'] = ['JMIS_Agency', 'JMIS_CurrentStage', 'JMIS_CustomerName',
                            'JMIS_CustomerNumber', 'JMIS_CustomerType', 'JMIS_Customer_Address1',
                            'JMIS_Customer_Address2', 'JMIS_Customer_City', 'JMIS_Customer_CountryCode',
                            'JMIS_Customer_Email', 'JMIS_Customer_State', 'JMIS_Customer_ZipCode',
                            'JMIS_DBA', 'JMIS_InitialProposalDate', 'JMIS_PolicyExecutive',
                            'JMIS_PolicyExpirationDate', 'JMIS_PolicyNumber', 'JMIS_Policy_Status',
                            'JMIS_Proposal_Expiration_Date', 'JMIS_Protected_Status', 'JMIS_experian_update_time']

        col_dict['lunar'] = ['Lunar_ActiveCustomer', 'Lunar_Agency', 'Lunar_CustomerName',
                             'Lunar_CustomerNumber', 'Lunar_CustomerType', 'Lunar_Customer_Address1',
                             'Lunar_Customer_Address2', 'Lunar_Customer_City', 'Lunar_Customer_State',
                             'Lunar_Customer_ZipCode', 'Lunar_DBA', 'Lunar_PolicyExecutive',
                             'Lunar_PolicyExpirationDate', 'Lunar_PolicyStatus', 'Lunar_Protected_Status',
                             'Lunar_experian_update_time']

        col_dict['wex'] = ['Wex_ActiveCustomer', 'Wex_Agency', 'Wex_CustomerName', 'Wex_CustomerNumber',
                           'Wex_CustomerType', 'Wex_Customer_Address1', 'Wex_Customer_Address2',
                           'Wex_Customer_City', 'Wex_Customer_State', 'Wex_Customer_ZipCode',
                           'Wex_DBA', 'Wex_PolicyExecutive', 'Wex_PolicyExpirationDate',
                           'Wex_PolicyStatus', 'Wex_Protected_Status', 'Wex_experian_update_time']

        ##Start index at 1
        temp1.index = temp1.index + 1

        ##Create uid from index
        temp1['uid'] = temp1.index

        ##Move uid to front of dataframe
        cols = temp1.columns
        cols = cols[-1:].append(cols[:-1])
        temp1 = temp1[cols]

        temp1['agency1'] = np.where(temp1['JMIC_CustomerName'].isnull(), np.nan, "c")
        temp1['agency2'] = np.where(temp1['JMIS_CustomerName'].isnull(), np.nan, "s")
        temp1['agency3'] = np.where(temp1['Lunar_CustomerName'].isnull(), np.nan, "r")
        temp1['agency4'] = np.where(temp1['Wex_CustomerName'].isnull(), np.nan, "x")

        ##Create agency Combo Column
        print(temp1.head(5))
        print(temp1.columns)
        cols = temp1.columns[-4:]
        print(cols)

        temp1['agency_combo'] = temp1[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        print(temp1['agency_combo'])
        temp1['agency_combo'] = temp1['agency_combo'].str.replace('nan_', '')
        temp1['agency_combo'] = temp1['agency_combo'].str.replace('_nan', '')

        temp1[['agency_combo', 'JMIC_CustomerName', 'JMIS_CustomerName', 'Lunar_CustomerName', 'Wex_CustomerName',
               'agency1', 'agency2', 'agency3', 'agency4']]

        #creating Dataframe for each agents
        temp_dict = {}
        for agent in agents:
            col_list = col_dict[agent]
            col_list.append('uid')
            col_list = col_list[-1:] + col_list[:-1]
            temp_dict[agent] = temp1[col_list]

        temp2 = {}
        for agent in agents:
            df = temp_dict[agent]

            ##Deal with missing Values
            df = df.fillna(np.nan)
            df = df.astype(object).replace("nan", np.nan)
            df = df.astype(object).replace(pd.NaT, np.nan)

            ##Drop rows where there is not at least 2 not null columns
            df = df.dropna(thresh=2)

            if agent == 'jmic':
                df['JMIC_CustomerType'] = '1. Customer'
            else:
                pass

            temp2[agent] = df

        temp2['wex'][temp2['wex']['Wex_CustomerName'] == 'ADFA, INC']
        print("---------------------------------------------------------------")
        print(temp2['wex'][temp2['wex']['Wex_CustomerName'] == 'ADFA, INC'])

        ##Drop Duplicate Lists for each agent

        jmic_d_cols = ['Agency', 'CustomerType', 'CustomerName', 'CustomerNumber', 'Customer_Address1',
                       'Customer_Address2',
                       'Customer_City', 'Customer_CountryCode', 'Customer_State', 'Customer_ZipCode',
                       'DBA', 'Protected_Status', 'experian_update_time']

        jmis_d_cols = ['Agency', 'CurrentStage', 'CustomerName', 'CustomerNumber', 'Customer_Address2',
                       'Customer_Email',
                       'DBA', 'PolicyExpirationDate', 'Proposal_Expiration_Date', 'PolicyNumber', 'Protected_Status',
                       'Customer_City',
                       'experian_update_time', 'CustomerType', 'Customer_State', 'Policy_Status',
                       'Customer_CountryCode',
                       'Customer_Address1', 'Customer_ZipCode', 'InitialProposalDate', 'PolicyExecutive']

        lunar_d_cols = ['ActiveCustomer', 'CustomerNumber', 'Customer_Address2', 'Customer_ZipCode',
                        'PolicyExpirationDate', 'PolicyStatus', 'experian_update_time', 'Agency',
                        'Customer_City', 'DBA', 'Protected_Status', 'CustomerType', 'Customer_State',
                        'CustomerName', 'PolicyExecutive', 'Customer_Address1']

        wex_d_columns = ['ActiveCustomer', 'CustomerType', 'Customer_City', 'DBA', 'PolicyStatus',
                          'Agency', 'CustomerName', 'CustomerNumber', 'Customer_Address1',
                         'Customer_State', 'Protected_Status', 'Customer_Address2', 'PolicyExecutive',
                          'Customer_ZipCode', 'PolicyExpirationDate']

        ##Rename Columns to match between dataframes
        temp3 = {}
        for agent in agents:
            df = temp2[agent]

            ##Different capitalization rules based on agent
            if agent == 'exp':
                continue
            elif agent == 'jmis':
                agentu = agent.upper()
            elif agent == 'jmic':
                agentu = agent.upper()
            else:
                agentu = agent.capitalize()

            ##column names that neet to change
            rlist = [name for name in df.columns if name.startswith(agentu)]
            print(rlist)
            ##New Values of column names
            rlist2 = []
            for i in rlist:
                i = i.replace(agentu + '_', '')
                rlist2.append(i)

            ##Create dictionary for column names
            renames = {rlist[i]: rlist2[i] for i in range(len(rlist))}

            ##Rename columns
            df = df.rename(columns=renames)
            df = df.rename(columns={'Agency': 'Subagency'})

            ##Specify new columns for long version
            df['Agency'] = agent.upper()
            df['PolicyExecutive'] = ''

            #print(df[['Agency','PolicyExecutive']])

            df = df.sort_values(['Customer_Address1', "CustomerName"], ascending=(True, True))

            ##Define Drop List based on Agent (Different Columns per agency)

            if agent == 'jmis':
                drop_list = jmis_d_cols
            elif agent == 'jmic':
                drop_list = jmic_d_cols
            elif agent == 'lunar':
                drop_list = lunar_d_cols
            elif agent == 'wex':
                drop_list = wex_d_columns

            df = df.drop_duplicates(drop_list)
            df = df[df['CustomerType'] == '1. Customer']
            df['CustomerNumber'] = df['CustomerNumber'].astype(str)
            temp3[agent] = df
        sums = 0
        for agent in agents:
            if agent == 'exp':
                continue
            df = temp3[agent]
            print(agent)
            print(df.shape)
            sums = sums + len(df)

            print(f'total rows is {sums}')

        logging.info("Creating stacked dataframe...")
        stacked_dat = pd.DataFrame()
        for agent in agents:
            if agent == 'exp':
                continue
            df = temp3[agent]
            stacked_dat = stacked_dat.append(df)
            #print(stacked_dat)

        ## Merge Stacked batch with Exp Data
        experian = temp2['exp']

        stacked_dat = stacked_dat.merge(experian,
                                        left_on=['uid'],
                                        right_on=['uid'],
                                        how="left")



        ##Create unique counters and address cleaning
        counterfields = ['counter1field', 'counter2field', 'counter3field', 'counter4field', 'counter5field']

        counterfields2 = [i + 'unique' for i in counterfields]

        counterfieldfinal = ['uniq_counter1', 'uniq_counter2', 'uniq_counter3', 'uniq_counter4', 'uniq_counter5']

        ##Ultimate rename counters
        renames = {counterfields2[i]: counterfieldfinal[i] for i in range(len(counterfields2))}

        ##Change to blank so we can concatenate
        stacked_dat = stacked_dat.replace(np.nan, '', regex=True)
        stacked_dat['counter1field'] = stacked_dat['Agency'] + '_' + stacked_dat['CustomerNumber']
        stacked_dat['counter2field'] = stacked_dat['Agency'] + '_' + stacked_dat['CustomerNumber'] + '_' + stacked_dat['Customer_State']
        stacked_dat['counter3field'] = stacked_dat['Agency'] + '_' + stacked_dat['CustomerNumber'] + '_' + stacked_dat['Customer_State'] + '_' + stacked_dat['Customer_City']
        stacked_dat['counter4field'] = stacked_dat['Agency'] + '_' + stacked_dat['CustomerNumber'] + '_' + stacked_dat[
            'Customer_State'] + '_' + stacked_dat['Customer_City'] + '_' + stacked_dat['Customer_Address1']

        stacked_dat['counter5field'] = stacked_dat['Agency'] + '_' + stacked_dat['CustomerNumber'] + '_' + stacked_dat[
            'Customer_State'] + '_' + stacked_dat['Customer_City'] + '_' + stacked_dat['Customer_Address1'] + '_' + \
                                       stacked_dat['Customer_Address2']

        for col in counterfields:
            stacked_dat = stacked_dat.sort_values([col], ascending=(True))
            stacked_dat[col + 'unique'] = stacked_dat[col].shift() != stacked_dat[col]

            stacked_dat[col + 'unique'] = stacked_dat[col + 'unique'].astype('uint8')

        for col in counterfields:
            stacked_dat = stacked_dat.drop(columns=[col])
            stacked_dat = stacked_dat.rename(columns=renames)

        finalvars = ['uid', 'uniq_counter1', 'uniq_counter2', 'uniq_counter3', 'uniq_counter4', 'uniq_counter5',
                     'PotentialConflict','Agency', 'Subagency', 'agency_combo', 'CustomerType', 'ActiveCustomer', 'CurrentStage',
                     'CustomerNumber', 'CustomerName','Customer_Address1', 'Customer_Address2', 'Customer_City', 'Customer_State', 'Customer_Zip',
                     'Customer_Country','experian_business_id', 'Experian_CustomerName', 'Experian_Customer_Address1',
                     'Experian_Customer_City','Experian_Customer_State', 'Experian_Customer_ZipCode', 'PolicyExecutive', 'PolicyExpirationDate',
                     'PolicyNumber','PolicyStatus', 'Policy_Status', 'Proposal_Expiration_Date', 'DBA', 'InitialProposalDate',
                     'Protected_Status', 'experian_update_time']

        ##Address Cleaning
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "4533 MacArthur Blvd. Suite D") & (
                    stacked_dat.Customer_City == "Newport Beach")), "Customer_State"] = "CA"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "601 W 1st Ave") & (
                    stacked_dat.Customer_City == "Parkesburg")), "Customer_State"] = "PA"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "679 3rd Street South") & (
                    stacked_dat.Customer_City == "Jacksonville Beach")), "Customer_State"] = "FL"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "11944 Streamside Dr") & (
                    stacked_dat.Customer_City == "Loveland")), "Customer_State"] = "OH"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "6387 THISTLEWOOD LANE") & (
                    stacked_dat.Customer_City == "Burlington")), "Customer_State"] = "KY"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "3412 S OAK ST") & (
                    stacked_dat.Customer_City == "TEMPE")), "Customer_State"] = "AZ"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "236 N Santa Cruz Ave") & (
                    stacked_dat.Customer_City == "Los Gatos")), "Customer_State"] = "CA"

        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2710 Turri Rd") & (
                    stacked_dat.Customer_State == "CA")), "Customer_City"] = "San Luis Obispo"

        ##Address Cleaning Continued
        stacked_dat.loc[(stacked_dat.Customer_Address1 == "171 Moyer Dr"), "Customer_State"] = "PA"
        stacked_dat.loc[(stacked_dat.Customer_Address1 == "171 Moyer Dr"), "Customer_City"] = "Dublin"

        stacked_dat.loc[(stacked_dat.Customer_City == "Edmonton Alberta"), "Customer_City"] = "Edmonton"
        stacked_dat.loc[(stacked_dat.Customer_City == "Edmonton Alberta"), 'Customer_State'] = "AB"
        stacked_dat.loc[(stacked_dat.Customer_City == "Edmonton Alberta"), "Customer_Zip"] = "T6H 4M6"

        stacked_dat.loc[(stacked_dat.Customer_City == "Ottawa"), "Customer_State"] = "QN"
        stacked_dat.loc[(stacked_dat.Customer_City == "Montreal"), "Customer_State"] = "QC"
        stacked_dat.loc[(stacked_dat.Customer_City == "Los Angeles"), "Customer_State"] = "CA"
        stacked_dat.loc[(stacked_dat.Customer_City == "Hesperia"), "Customer_State"] = "CA"
        stacked_dat.loc[(stacked_dat.Customer_City == "Philadelphia"), "Customer_State"] = "PA"
        stacked_dat.loc[(stacked_dat.Customer_City == "Trenton"), "Customer_State"] = "NJ"
        stacked_dat.loc[(stacked_dat.Customer_City == "Brooklyn"), "Customer_State"] = "NY"
        stacked_dat.loc[(stacked_dat.Customer_City == "Miami"), "Customer_State"] = "FL"
        stacked_dat.loc[(stacked_dat.Customer_City == "Lake Worth"), "Customer_State"] = "TX"
        stacked_dat.loc[(stacked_dat.Customer_City == "Fort Lauderdale"), "Customer_State"] = "FL"

        ##Missing Zips
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "6517 N. May Ave, Ste A") & (
                    stacked_dat.Customer_City == "Oklahoma City") & (
                                     stacked_dat.Customer_State == "OK")), "Customer_ZipCode"] = "73116"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "580 5TH AVENUE STE 915") & (
                    stacked_dat.Customer_City == "NEW YORK") & (
                                     stacked_dat.Customer_State == "NY")), "Customer_ZipCode"] = "10036"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "580 Fifth Avenue") & (
                    stacked_dat.Customer_City == "NEW YORK") & (
                                     stacked_dat.Customer_State == "NY")), "Customer_ZipCode"] = "10036"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2933 N HAYDEN RD #3") & (
                    stacked_dat.Customer_City == "SCOTTSDALE") & (
                                     stacked_dat.Customer_State == "AZ")), "Customer_ZipCode"] = "85251"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "529 Fifth Avenue") & (
                    stacked_dat.Customer_City == "NEW YORK") & (
                                     stacked_dat.Customer_State == "NY")), "Customer_ZipCode"] = "10017"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "3307 Colby Avenue") & (
                    stacked_dat.Customer_City == "Los Angeles") & (
                                     stacked_dat.Customer_State == "CA")), "Customer_ZipCode"] = "90066"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "8710 Belfry Court") & (
                    stacked_dat.Customer_City == "Duluth") & (
                                     stacked_dat.Customer_State == "GA")), "Customer_ZipCode"] = "30097"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2061 Swamp Pike Road") & (
                    stacked_dat.Customer_City == "Gilbertsville") & (
                                     stacked_dat.Customer_State == "PA")), "Customer_ZipCode"] = "19525"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "1307 Zachary Road") & (
                    stacked_dat.Customer_City == "Abington") & (
                                     stacked_dat.Customer_State == "PA")), "Customer_ZipCode"] = "19001"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "1 Woodbridge Center, 1st Floor") & (
                    stacked_dat.Customer_City == "Woodbridge") & (
                                     stacked_dat.Customer_State == "NJ")), "Customer_ZipCode"] = "07095"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2014 S Craycroft Road") & (
                    stacked_dat.Customer_City == "Tucson") & (
                                     stacked_dat.Customer_State == "AZ")), "Customer_ZipCode"] = "85711"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2015 E. Capital Avenue") & (
                    stacked_dat.Customer_City == "Pierce") & (
                                     stacked_dat.Customer_State == "SD")), "Customer_ZipCode"] = "57501"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "6031 Hemby Road") & (
                    stacked_dat.Customer_City == "Weddington") & (
                                     stacked_dat.Customer_State == "NC")), "Customer_ZipCode"] = "28104"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "6901 S Lyncrest Pl #105") & (
                    stacked_dat.Customer_City == "Sioux Falls") & (
                                     stacked_dat.Customer_State == "SD")), "Customer_ZipCode"] = "57108"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "401 Reid Avenue") & (
                    stacked_dat.Customer_City == "Port St. Joe") & (
                                     stacked_dat.Customer_State == "FL")), "Customer_ZipCode"] = "32456"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2717 Waverly Drive") & (
                    stacked_dat.Customer_City == "Los Angeles") & (
                                     stacked_dat.Customer_State == "CA")), "Customer_ZipCode"] = "90039"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "25 OSPREY DR.") & (
                    stacked_dat.Customer_City == "LOWMAN") & (
                                     stacked_dat.Customer_State == "ID")), "Customer_ZipCode"] = "83637"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "5 South Wabash Ave") & (
                    stacked_dat.Customer_City == "Chicago") & (
                                     stacked_dat.Customer_State == "IL")), "Customer_ZipCode"] = "60603"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "5163 Merrick Road") & (
                    stacked_dat.Customer_City == "Massapequa Park") & (
                                     stacked_dat.Customer_State == "NY")), "Customer_ZipCode"] = "11762"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "50 Baptist Corner Road") & (
                    stacked_dat.Customer_City == "Ashfield") & (
                                     stacked_dat.Customer_State == "MA")), "Customer_ZipCode"] = "01330"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "2050 N Stemmons Freeway") & (
                    stacked_dat.Customer_City == "Dallas") & (
                                     stacked_dat.Customer_State == "TX")), "Customer_ZipCode"] = "75207"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "1112 Montana Ave") & (
                    stacked_dat.Customer_City == "Santa Monica") & (
                                     stacked_dat.Customer_State == "CA")), "Customer_ZipCode"] = "90403"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "1455 Sherbrooke St West #605") & (
                    stacked_dat.Customer_City == "Montreal") & (
                                     stacked_dat.Customer_State == "QC")), "Customer_ZipCode"] = "H3G 2S8"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "114 Roxborough Drive") & (
                    stacked_dat.Customer_City == "Toronto") & (
                                     stacked_dat.Customer_State == "ON")), "Customer_ZipCode"] = "M4W 1X4"
        stacked_dat.loc[((stacked_dat.Customer_Address1 == "10 Muirhead Road Ste 108") & (
                    stacked_dat.Customer_City == "Toronto") & (
                                     stacked_dat.Customer_State == "ON")), "Customer_ZipCode"] = "M2J 4P9"

        # Fix Zips
        stacked_dat.loc[(stacked_dat.Customer_ZipCode == "K2P1W-7"), "Customer_ZipCode"] = "K2P 1W7"
        stacked_dat.loc[(stacked_dat.Customer_ZipCode == "T6H4M-6"), "Customer_ZipCode"] = "T6H 4M6"
        stacked_dat.loc[(stacked_dat.Customer_ZipCode == "H3G2C-1"), "Customer_ZipCode"] = "H3G 2C1"

        CanStates = ["AB", "BC", "MB", "NB", "NT", "NL", "NS", "NU", "ON", "PE", "QC", "SK", "YT"]
        us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA',
                     'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM',
                     'NY', 'NC','ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI',
                     'WY']

        ##Country Code not fully populated
        stacked_dat.loc[stacked_dat.Customer_State.isin(CanStates), 'Customer_Country'] = "CAN"
        stacked_dat.loc[stacked_dat.Customer_State.isin(us_states), 'Customer_Country'] = "USA"

        ##Final Fields
        stacked_dat = stacked_dat[finalvars]

        #Clean Date Fields
        datefields = [name for name in stacked_dat.columns if 'Date' in name]
        datefields.append('experian_update_time')

        for col in datefields:
            stacked_dat[col] = pd.to_datetime(stacked_dat[col], utc=True).dt.date

        ##Clean up Zip Code Column
        changelist = ['POOR_MATCH', 'NOT_FOUND', 'REQUEST_ERROR']
        for i in changelist:
            stacked_dat['Experian_Customer_ZipCode'] = stacked_dat['Experian_Customer_ZipCode'].str.replace(i, '')

        ##Sub Agency is blank for JMIS, Lunar, and Wexler
        stacked_dat['Subagency'] = np.where(
            (stacked_dat['Agency'] == 'JMIS') | (stacked_dat['Agency'] == 'WEX') | (stacked_dat['Agency'] == 'LUNAR'),
            '', stacked_dat['Subagency'])

        stacked_dataframe = stacked_dat[['Agency', 'CustomerType', 'ActiveCustomer', 'CustomerNumber', 'CustomerName', 'Customer_Address1',
                     'Customer_Address2', 'Customer_City', 'Customer_State', 'Customer_Zip',
                     'Customer_Country']]

        self.bq_hook.gcp_bq_write_table(
            df_in=stacked_dataframe,
            project=self.project,
            dataset=self.dataset,
            table_name=self.table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')

        logging.info("Successfully written teh stacked data to BigQuery")

        us_state_to_abbrev = {
            "Newfoundland and Labrador": "NL",
            "Prince Edward Island": "PE",
            "Nova Scotia": "NS",
            "New Brunswick": "NB",
            "Quebec": "QC",
            "Ontario": "ON",
            "Manitoba": "MB",
            "Saskatchewan": "SK",
            "Alberta": "AB",
            "British Columbia": "BC",
            "Yukon": "YT",
            "Northwest Territories": "NT",
            "Nunavut": "NU",
            "Alabama": "AL",
            "Alaska": "AK",
            "Arizona": "AZ",
            "Arkansas": "AR",
            "California": "CA",
            "Colorado": "CO",
            "Connecticut": "CT",
            "Delaware": "DE",
            "Florida": "FL",
            "Georgia": "GA",
            "Hawaii": "HI",
            "Idaho": "ID",
            "Illinois": "IL",
            "Indiana": "IN",
            "Iowa": "IA",
            "Kansas": "KS",
            "Kentucky": "KY",
            "Louisiana": "LA",
            "Maine": "ME",
            "Maryland": "MD",
            "Massachusetts": "MA",
            "Michigan": "MI",
            "Minnesota": "MN",
            "Mississippi": "MS",
            "Missouri": "MO",
            "Montana": "MT",
            "Nebraska": "NE",
            "Nevada": "NV",
            "New Hampshire": "NH",
            "New Jersey": "NJ",
            "New Mexico": "NM",
            "New York": "NY",
            "North Carolina": "NC",
            "North Dakota": "ND",
            "Ohio": "OH",
            "Oklahoma": "OK",
            "Oregon": "OR",
            "Pennsylvania": "PA",
            "Rhode Island": "RI",
            "South Carolina": "SC",
            "South Dakota": "SD",
            "Tennessee": "TN",
            "Texas": "TX",
            "Utah": "UT",
            "Vermont": "VT",
            "Virginia": "VA",
            "Washington": "WA",
            "West Virginia": "WV",
            "Wisconsin": "WI",
            "Wyoming": "WY",
            "District Of Columbia": "DC",
            "American Samoa": "AS",
            "Guam": "GU",
            "Northern Mariana Islands": "MP",
            "Puerto Rico": "PR",
            "United States Minor Outlying Islands": "UM",
            "U.S. Virgin Islands": "VI",
            "Québec": "QC",
        }

        abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))
        gwpc_sql =  """SELECT * FROM `semi-managed-reporting.core_sales_cl.promoted_recent_active_gwpc_locations`"""
        gwpc = self.bq_hook.get_pandas_df(sql=gwpc_sql)
        logging.info("GWPC Dataframe")
        print(gwpc.head(5))

        gwloc_sql = """with loc AS (
        SELECT DISTINCT cl.AccountNumber
        , cl.PolicyNumber
        , cl.JobNumber
        , cl.risk_location_key
        , cl.LocationNumber
        , TO_BASE64(SHA256(ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationAddress2
        , cl.LocationCity
        , cl.LocationStateDesc
        , cl.LocationPostalCode
        , cl.LocationCountryDesc], ' '))) as GWPC_LocationKey
        , cl.PolicyInsuredContactFullName as input_name
        , ARRAY_TO_STRING( [
        cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], ' ') as input_address
        , ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], ' ') as PlaceID_Lookup
        , ARRAY_TO_STRING( [
        cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationCity
        , cl.LocationStateDesc
        , SPLIT(cl.LocationPostalCode, '-')[OFFSET(0)]
        , cl.LocationCountryDesc], '||') as ExperianBIN_Lookup
        , cl.PolicyInsuredContactFullName
        , cl.LocationAddress1
        , cl.LocationAddress2
        , cl.LocationCity
        , cl.LocationStateDesc
        , cl.LocationPostalCode
        , cl.LocationCountryDesc
        FROM `semi-managed-reporting.core_insurance_cl.promoted_pc_cl_locations_core` cl)
        select input_address , LocationAddress1, LocationCity, LocationStateDesc, LocationPostalCode from loc
        """

        gwloc = self.bq_hook.get_pandas_df(sql=gwloc_sql)
        logging.info("GWLoc Dataframe")


        ###Bring in zip to gwpc
        gwpc = gwpc.merge(
            gwloc[['input_address', 'LocationAddress1', 'LocationPostalCode', 'LocationStateDesc']].drop_duplicates(
                'input_address'), on='input_address', how='left')
        gwpc = gwpc[gwpc["Insured_Status"] == "Current_Insured"]
        print(gwpc["Insured_Status"])

        cp = stacked_dataframe[(stacked_dataframe["Agency"] != "JMIC")]


        cp = cp[["Agency", "CustomerNumber", "CustomerName", "Customer_Address1", "Customer_State", ]]
        gwpc["Agency"] = "JMIC"
        gwpc = gwpc[["Agency", "AccountNumber", "input_name", "LocationAddress1", "LocationStateDesc", ]].rename(
                                                                            columns={
                                                                                "AccountNumber": "CustomerNumber",
                                                                                "input_name": "CustomerName",
                                                                                "LocationAddress1": "Customer_Address1",
                                                                                "LocationStateDesc": "Customer_State",
                                                                            })
        cp = cp.append(gwpc)
        cp = cp[cp["Agency"].notna()]
        rstrings = [ " ","_","inc",".com","co.",".","llc","-","'","jewelery","jewelry","jewelers","jewellers","jeweler","jewels","jewel","(", ")","+","\\","/","#","ltd","company","corporation","corp","ltd","&",",",]
        cp["clean_name"] = self.clean(list(cp.CustomerName),rstrings)
        cp["letter"] = cp.clean_name.str[0]


        #List of Trade Associations
       ## New Yaswanth -- Added new associations MJSA and CJG ##
        tas = ["AGS", "AGTA", "CJA", "IJO", "JA", "RJO","MJSA", "CJG"]

        ## Column Dictionaries for Consistency
        ags_dict = {
            "Organization": "Company",
            "Address": "Address1",
            "Address Line 2": "Address2",
            "City": "City",
            "State": "State",
            "Postal Code": "Zip",}
        agta_dict = {
            "Company": "Company",
            "Address": "Address1",
            "Address2": "Address2",
            "City Name": "City",
            "State": "State",
            "Zip": "Zip",
        }
        cja_dict = {
            "Organization": "Company",
            "Address": "Address1",
            "City": "City",
            "Province": "State",
            "Postal Code": "Zip",
        }
        ijo_dict = {
            "Storename": "Company",
            "Address1": "Address1",
            "Address2": "Address2",
            "City": "City",
            "State": "State",
            "Zip": "Zip",
        }
        ja_dict = {
            "Company": "Company",
            "Address 1": "Address1",
            "Address 2": "Address2",
            "City": "City",
            "State Province": "State",
            "Zip": "Zip",
        }
        ## New Yaswanth ##
        mjsa_dict = {
            "Company": "Company",
            "Address 1": "Address1",
            "State Province": "State",
        }
        ## End Yaswanth
        rjo_dict = {
            "Customer Name": "Company",
            "Address 1": "Address1",
            "Address 2": "Address2",
            "City": "City",
            "State": "State",
            "Zip": "Zip",
        }
        ## New Yaswanth ##

        cjg_dict = {
            "Member": "Company",
            "Mailing Address": "Address1",
            "City": "City",
            "Prov": "State",
        }
        ## End Yaswanth ##
        dicts = {
            "AGS": ags_dict,
            "AGTA": agta_dict,
            "CJA": cja_dict,
            "IJO": ijo_dict,
            "JA": ja_dict,
            "MJSA": mjsa_dict,
            "CJG": cjg_dict,
            "RJO": rjo_dict,
        }

        dfs = {}
        for ta in tas:
            #df = pd.read_excel(ta_path + ta + ".xlsx", engine="openpyxl")
            path = self.gcs_destination_path + ta+".xlsx"
            file_data = self.hook.download(self.destination_bucket, path)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_excel(file_stream, engine='openpyxl')
            df["Association"] = ta
            dfs[ta] = df


        dfs2 = {}
        for ta in tas:
            df = dfs[ta]
            cols = list(dicts[ta].keys())
            df = df[cols]
            df["Association"] = ta
            df = df.rename(columns=dicts[ta])
            df["clean_name"] = self.clean(list(df["Company"]),rstrings)
            dfs2[ta] = df

        records = 0
        for ta in tas:
            records = records + len(dfs2[ta])

        for ta in tas:
            states = []
            df = dfs2[ta]
            for i in list(df["State"]):
                i = str(i).upper()
                try:
                    states.append(us_state_to_abbrev[i.lower().title()])
                except KeyError:
                    states.append(i)

            df["State"] = states
            dfs2[ta] = df

        states = []
        for i in list(cp["Customer_State"]):
            try:
                states.append(us_state_to_abbrev[str(i).lower().title()])
            except KeyError:
                states.append(i)

        cp["Customer_State"] = states
        cp["Customer_Address1"] = (cp["Customer_Address1"].str.replace("Street", "st").replace("street", "st"))

        records = 0
        for ta in tas:
            records = records + len(dfs2[ta])
        ### New Yaswanth ###
        sql = """
         -- A query to pull registration records
         select distinct --tgu.Family_ID
        		ci.CompanyName as [Name]
                ,tgu.clientID as ID
        		,ci.AltEmail as Email
        		,ci.Address1 as Address1
        		,ci.address2 as Address2
        		,ci.state as State
        		,ci.City
        		,ci.Zip
        		,ci.Phone as Phone
        		,'JMSS' as [system]
        		,Last_Shipment_Date

        from        dbo.tgUsers as tgu
        inner join [KPI].[dbo].[ClientInfo] as ci

        left join (select s.clientid AS s_ClientID,
        	    max(s.transaction_time) AS Last_Shipment_Date

          FROM			[dbo].[shipments] AS s
          LEFT JOIN		dbo.tgUsers AS tg
          ON			s.clientid = tg.clientid
          LEFT JOIN		dbo.ClientInfo ci
          ON			ci.clientid = tg.Family_ID
          WHERE			transaction_time >= '2022-01-01'
        				and s.clientID > 0
        				and s.voided_on is null and s.void_id = 0
        				and tg.isdemo = 0

        				group by s.clientid) last_s on ci.clientid = last_s.s_ClientID

        on            ci.ClientId = tgu.clientID
        where        --RegistrationDate >= '01-01-2018'

                     Last_Shipment_Date>='2022-01-01'
                     and tgu.IsDemo = 0
                     and Address1 is not null
        """

      #  conn = pyodbc.connect(
       #     "Driver={SQL Server};"
        #    "Server=instjmshipping_PROD;"
        #    "Database=KPI;"
         #   "Trusted_Connection=yes;"
     #   )
        #mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        #conn = mssql.get_conn(database=database)
        print("Printing COnnection",self.mssql_connection)
        db_database='KPI'
        mssql = MsSqlHook(self.mssql_connection)
        conn = mssql.get_conn(database=db_database)
        cursor = conn.cursor()

        ############NEW##################
        sql_query = pd.read_sql_query(sql, conn)

        jmss = pd.DataFrame(sql_query)

        # In[198]:

        ############NEW##################
        jmss = jmss.rename(
            columns={
                "ID": "CustomerNumber",
                "Name": "CustomerName",
                "Address1": "Customer_Address1",
                "State": "Customer_State",
            }
        )[["CustomerName", "CustomerNumber", "Customer_Address1", "Customer_State"]]

        # In[199]:
        ############NEW##################

        jmss["clean_name"] = self.clean(list(jmss["CustomerName"]),rstrings)
        jmss["letter"] = jmss["clean_name"].str[0]

        merged = {}

        thresh = 80
        count = 0
        print(f"Records to Process: {records}")
        for ta in tas:
            df = dfs2[ta]
            # df = df[df["clean_name"].str.contains("scham")]
            customer_status = []
            agency = []
            customernumber = []
            probabilities = []

            customer_status_jmss = []
            customernumber_jmss = []
            probabilities_jmss = []

            for index, row in df.iterrows():
                count += 1
                if count % 500 == 0:
                    print(f"{round((count / records) * 100, 0)}% Complete.")

                clean_name = row["clean_name"]
                state = row["State"]
                Address1 = str(row["Address1"]).replace("street", "st").replace("Street", "st")
                name_address = clean_name + "_" + Address1

                lookup = cp[cp["letter"].str.lower() == clean_name[0]]
                #print("ramesh check ", clean_name[0], " letter ", jmss["letter"].str.lower())
                #print("ramesh check ", clean_name)
                lookup_jmss = jmss[jmss["letter"].str.lower() == clean_name[0]]
                
                matches = pd.DataFrame(
                    process.extract(
                        clean_name,
                        list(lookup.clean_name),
                        scorer=fuzz.token_set_ratio,
                        limit=10,
                    ),
                    columns=["clean_name", "name_probability"],
                )
                ##JMSS
                #print("ramesh count ", count) 
                #print("ramesh before process.extract ", list(lookup_jmss.clean_name)) 
                matches_jmss = pd.DataFrame(
                    process.extract(
                        clean_name,
                        list(lookup_jmss.clean_name),
                        scorer=fuzz.token_set_ratio,
                        limit=10,
                    ),
                    columns=["clean_name", "name_probability"],
                )
                matches = matches.merge(
                    lookup[
                        [
                            "clean_name",
                            "Customer_Address1",
                            "Customer_State",
                            "Agency",
                            "CustomerNumber",
                        ]
                    ],
                    on="clean_name",
                    how="left",
                )
                matches_jmss = matches_jmss.merge(
                    lookup_jmss[
                        [
                            "clean_name",
                            "Customer_Address1",
                            "Customer_State",
                            #                     "Agency",
                            "CustomerNumber",
                        ]
                    ],
                    on="clean_name",
                    how="left",
                )
                #####End Yaswanth ###
                #         print(clean_name)
                #         print(matches)

                matches["name_probability2"] = matches.apply(
                    lambda x: fuzz.partial_ratio(x["clean_name"], clean_name), axis=1
                )

                matches["name_probability"] = (
                        matches["name_probability2"] * 0.5 + matches["name_probability"] * 0.5
                )

                matches["address_probability"] = matches.apply(
                    lambda x: fuzz.token_set_ratio(
                        str(str(x["Customer_Address1"])).lower(), str(str(Address1)).lower()
                    ),
                    axis=1,
                )

                matches["state_probability"] = matches.apply(
                    lambda x: fuzz.ratio(
                        str(str(x["Customer_State"])).lower(), str(str(state)).lower()
                    ),
                    axis=1,
                )

                matches["final_probability"] = (
                        matches["name_probability"] * 0.4
                        + matches["address_probability"] * 0.25
                        + matches["state_probability"] * 0.35
                )
                ### New Yaswanth ###
                if len(matches_jmss) > 0:
                    matches_jmss = matches_jmss
                else:
                    matches_jmss = pd.DataFrame(
                        {
                            "name_probability": [0],
                            "clean_name": ["0"],
                            "Customer_Address1": ["0"],
                            "Customer_State": ["0"],
                            "CustomerNumber": ["0"],
                        }
                    )

                matches_jmss["name_probability2"] = matches_jmss.apply(
                    lambda x: fuzz.partial_ratio(x["clean_name"], clean_name), axis=1
                )

                matches_jmss["name_probability"] = (
                        matches_jmss["name_probability2"] * 0.5
                        + matches_jmss["name_probability"] * 0.5
                )

                matches_jmss["address_probability"] = matches_jmss.apply(
                    lambda x: fuzz.token_set_ratio(
                        str(str(x["Customer_Address1"])).lower(), str(str(Address1)).lower()
                    ),
                    axis=1,
                )

                matches_jmss["state_probability"] = matches_jmss.apply(
                    lambda x: fuzz.ratio(
                        str(str(x["Customer_State"])).lower(), str(str(state)).lower()
                    ),
                    axis=1,
                )

                matches_jmss["final_probability"] = (
                        matches_jmss["name_probability"] * 0.4
                        + matches_jmss["address_probability"] * 0.25
                        + matches_jmss["state_probability"] * 0.35
                )
                ############End yaswanth #################
                ##Matches 2 will use partial ratio
                matches2 = pd.DataFrame(
                    process.extract(
                        clean_name,
                        list(lookup.clean_name),
                        scorer=fuzz.partial_ratio,
                        limit=10,
                    ),
                    columns=["clean_name", "name_probability"],
                )
                #         print("matches2")
                #         print(matches2)

                matches2 = matches2.merge(
                    lookup[
                        [
                            "clean_name",
                            "Customer_Address1",
                            "Customer_State",
                            "Agency",
                            "CustomerNumber",
                        ]
                    ],
                    on="clean_name",
                    how="left",
                )
                #         print("matches2")

                matches2["name_probability2"] = matches2.apply(
                    lambda x: fuzz.token_set_ratio(x["clean_name"], clean_name), axis=1
                )

                matches2["name_probability"] = (
                        matches2["name_probability2"] * 0.5 + matches2["name_probability"] * 0.5
                )

                matches2["address_probability"] = matches2.apply(
                    lambda x: fuzz.token_set_ratio(
                        str(str(x["Customer_Address1"])).lower(), str(str(Address1)).lower()
                    ),
                    axis=1,
                )

                matches2["state_probability"] = matches2.apply(
                    lambda x: fuzz.ratio(
                        str(str(x["Customer_State"])).lower(), str(str(state)).lower()
                    ),
                    axis=1,
                )

                matches2["final_probability"] = (
                        matches2["name_probability"] * 0.4
                        + matches2["address_probability"] * 0.25
                        + matches2["state_probability"] * 0.35
                )
                ### New Yaswanth ###        print(matches2)
                matches2_jmss = pd.DataFrame(
                    process.extract(
                        clean_name,
                        list(lookup_jmss.clean_name),
                        scorer=fuzz.partial_ratio,
                        limit=10,
                    ),
                    columns=["clean_name", "name_probability"],
                )
                #         print("matches2")
                #         print(matches2)

                matches2_jmss = matches2_jmss.merge(
                    lookup_jmss[
                        [
                            "clean_name",
                            "Customer_Address1",
                            "Customer_State",
                            #                     "Agency",
                            "CustomerNumber",
                        ]
                    ],
                    on="clean_name",
                    how="left",
                )
                #         print("matches2")
                if len(matches2_jmss) > 0:
                    matches2_jmss = matches2_jmss
                else:
                    matches2_jmss = pd.DataFrame(
                        {
                            "name_probability": [0],
                            "clean_name": ["0"],
                            "Customer_Address1": ["0"],
                            "Customer_State": ["0"],
                            "CustomerNumber": ["0"],
                        }
                    )

                matches2_jmss["name_probability2"] = matches2_jmss.apply(
                    lambda x: fuzz.token_set_ratio(x["clean_name"], clean_name), axis=1
                )

                matches2_jmss["name_probability"] = (
                        matches2_jmss["name_probability2"] * 0.5
                        + matches2_jmss["name_probability"] * 0.5
                )

                matches2_jmss["address_probability"] = matches2_jmss.apply(
                    lambda x: fuzz.token_set_ratio(
                        str(str(x["Customer_Address1"])).lower(), str(str(Address1)).lower()
                    ),
                    axis=1,
                )

                matches2_jmss["state_probability"] = matches2_jmss.apply(
                    lambda x: fuzz.ratio(
                        str(str(x["Customer_State"])).lower(), str(str(state)).lower()
                    ),
                    axis=1,
                )

                matches2_jmss["final_probability"] = (
                        matches2_jmss["name_probability"] * 0.4
                        + matches2_jmss["address_probability"] * 0.25
                        + matches2_jmss["state_probability"] * 0.35
                )
                matches = matches.append(matches2)
                matches_jmss = matches_jmss.append(matches2_jmss)
                matches = matches.sort_values(by="final_probability", ascending=False).head(1)
                matches_jmss = matches_jmss.sort_values( by="final_probability", ascending=False).head(1)

                if list(matches.final_probability)[0] > thresh:
                    customer_status.append(True)
                    agency.append(list(matches.Agency)[0])
                    customernumber.append(list(matches.CustomerNumber)[0])
                    probabilities.append(list(matches.final_probability)[0])

                else:
                    customer_status.append(False)
                    agency.append("")
                    customernumber.append("")
                    probabilities.append("")

                if list(matches_jmss.final_probability)[0] > thresh:
                    customer_status_jmss.append(True)
                    #             agency.append(list(matches.Agency)[0])
                    customernumber_jmss.append(list(matches_jmss.CustomerNumber)[0])
                    probabilities_jmss.append(list(matches_jmss.final_probability)[0])

                else:
                    customer_status_jmss.append(False)
                    #             agency.append("")
                    customernumber_jmss.append("")
                    probabilities_jmss.append("")
            df["Customer_Status"] = customer_status
            df["Agency"] = agency
            df["CustomerNumber"] = customernumber
            df["Probability"] = probabilities
            df["Customer_Status_JMSS"] = customer_status_jmss
            #     df["Agency"] = agency
            df["CustomerNumber_JMSS"] = customernumber_jmss
            df["Probability_JMSS"] = probabilities_jmss
            merged[ta] = df
        ### End Yaswanth End ###
        print("Done.")

        def check(n, street='blank', andor='or'):
            # print(type(n));
            if type(n) == int:
                df = cp
                n = str(n)
                df = df[df['CustomerNumber'] == n]
            elif street:
                if andor == 'and':
                    # print('and')
                    df = cp
                    df = df[(df['clean_name'].str.contains(n, na=False)) & (
                        df['Customer_Address1'].str.contains(street, na=False))]
                else:
                    df = cp
                    df = df[(df['clean_name'].str.contains(n, na=False)) | (
                        df['Customer_Address1'].str.contains(street, na=False))]
            else:
                df = cp
                df = df[df['clean_name'].str.contains(n, na=False)]

            return (df)

        check("master", "Goodman", "and")
        len(merged["AGS"][merged["AGS"].Customer_Status == True]) / len(merged["AGS"])
        summary = pd.DataFrame()
        for ta in tas:
            details = {}
            df = merged[ta]
            customercount = len(df[df["Customer_Status"] == True])
            customercount_jmss = len(df[df["Customer_Status_JMSS"] == True]) ## User story Yaswanth ##
            total = len(df)
            penetration = customercount / total
            penetration_jmss = customercount_jmss / total
            newrow = pd.DataFrame(
                data=[
                    {
                        "Trade Association": ta,
                        "CustomerCount": customercount,
                        "Total_Members": total,
                        "Penetration": penetration,
                        "Penetration_JMSS": penetration_jmss   ##User story YAswanth ##
                    }
                ]
            )
            summary = summary.append(newrow)
        summary.reset_index(drop=True, inplace=True)

        path = self.gcs_destination_path + "Master-List-Association-Membership.xlsx"
        file_data = self.hook.download(self.destination_bucket, path)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        master = pd.read_excel(file_stream, engine='openpyxl')

        for i in ["Suite", "Ste", "ste", "Unit", "unit", "#", ","]:
            master["Business Street"] = master["Business Street"].str.split(i, expand=True)[0]

        print(master["Business Street"])

        print(master[master["Account Name"].str.contains("leiman")])

        master["ta"] = [i.split("-")[0].replace(" ", "") for i in list(master["Trade Association: Account Name"])]

        master["clean_name"] = self.clean(list(master["Account Name"]),rstrings)

        merged2 = {}

        thresh = 60
        count = 0
        print(f"Records to Process: {records}")
        for ta in tas:
            df = merged[ta]
            sf_status = []

            for index, row in df.iterrows():
                count += 1
                if count % 500 == 0:
                    print(f"{round((count / records) * 100, 0)}% Complete.")

                clean_name = row["clean_name"]

                Address1 = str(row["Address1"])

                lookup = master[master["ta"] == ta]

                matches = pd.DataFrame(
                    process.extract(
                        clean_name,
                        list(lookup.clean_name),
                        scorer=fuzz.token_sort_ratio,
                        limit=5,
                    ),
                    columns=["clean_name", "name_probability"],
                )

                matches = matches.merge(
                    lookup[["clean_name", "Business Street"]], on="clean_name", how="left",
                )

                matches["address_probability"] = matches.apply(
                    lambda x: fuzz.ratio(
                        str(str(x["Business Street"])).lower(), str(str(Address1)).lower()
                    ),
                    axis=1,
                )

                matches["final_probability"] = (
                        matches["name_probability"] * 0.6 + matches["address_probability"] * 0.4
                )

                matches = matches.sort_values(by="final_probability", ascending=False).head(1)

                if list(matches.final_probability)[0] > thresh:
                    sf_status.append(True)

                else:
                    sf_status.append(False)

            df["Salesforce_Status"] = sf_status

            merged2[ta] = df

        print("Done.")

        #print(cp[cp["CustomerNumber"] == "3000409763"])

        #site_url = 'https://jminsure.sharepoint.com/sites/TeamHermes'
        #client_secret = ''
        #client_username = ''
        logging.info("Writing the result to {location}".format(location =  self.destination_file_url ))
        connection = BaseHook.get_connection(self.sharepoint_site)

        site_url = connection.host
        client_secret = connection.password
        client_username = connection.login
        #logging.info("Site _url :{url}".format(url=self.file_url))
        credentials = ClientCredential(client_username, client_secret)
        ctx = ClientContext(site_url).with_credentials(credentials)
        temp = tempfile.NamedTemporaryFile(suffix=".xlsx")

        with pd.ExcelWriter(temp.name) as writer:
            for name in tas:
                data = merged2[name]
                #print(data.head(5))
                data.to_excel(writer, sheet_name=name, index=False)
                writer.save()
            summary.to_excel(writer, sheet_name="Summary", index=False)
            writer.save()

        with open(temp.name, 'rb') as content_file:
            file_content = content_file.read()

        #remotepath = '/Shared Documents/TradeAssociation/Output/TradeAssociationReport.xlsx'
        dir, name = os.path.split( self.destination_file_url)
        file = ctx.web.get_folder_by_server_relative_url(dir).upload_file(name, file_content).execute_query()
        logging.info("Files written succesfully..")























