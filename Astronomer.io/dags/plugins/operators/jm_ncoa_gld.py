# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import logging
import sys

import datetime as dt
import os
import warnings
import pandas as pd
import json
import re
import calendar
import time
import urllib
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import configuration
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook


# Global Settings.
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

class NCOAAddressMatchOperator(BaseOperator):
    @apply_defaults
    def __init__(self,

                 project,
                 google_cloud_storage_conn_id,
                 source_dataset,
                 source_table,
                 destination_dataset,
                 matched_tablename,
                 unmatched_tablename,
                 adhoc_dataset,
                 pc_table,
                 ncoa_ref_to_gld,
                 threshold=60,
                 *args,
                 **kwargs):
        super(NCOAAddressMatchOperator, self).__init__(*args, **kwargs)

        self.project = project
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.gcs_hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_storage_conn_id)
        self.source_dataset = source_dataset
        self.source_table = source_table
        self.destination_dataset = destination_dataset
        self.matched_tablename = matched_tablename
        self.unmatched_tablename = unmatched_tablename
        self.adhoc_dataset = adhoc_dataset
        self.pc_table = pc_table
        self.ncoa_ref_to_gld = ncoa_ref_to_gld
        self.threshold = threshold



    def execute(self, context):
        if self.ncoa_ref_to_gld.upper() == 'FALSE':
            print('No Data to process....')
            return
        partition_date = context['ds']
        match_date = pd.to_datetime(context['ds']).strftime('%Y-%m-%d')


        # partition_table = str(my_table_name) + '$' + str(start_date.replace('-', ''))


        ncoa_sql = '''SELECT  Account,AltAccount,File
                          ,MoveDate,MoveType,NCOACode,OriginalAddress1
                          ,OriginalAddress2,OriginalAddressee,OriginalCity,OriginalState,OriginalZip
                          ,Reason1,Reason2,Reason3,Reason4,Updated
                          ,UpdatedAddress1,UpdatedAddress2,UpdatedAddressee,UpdatedCity
                          ,UpdatedCounty,UpdatedState,UpdatedZip,Verified 
                          FROM `{dataset}.{table}` '''.format(project=self.project, dataset=self.source_dataset,
                                                                        table=self.source_table)

        df_ncoa = self.gcp_bq_hook.get_pandas_df(ncoa_sql)
        logging.info('gathered ncoa file info')

        pc_sql = '''SELECT * FROM `{dataset}.{table}` '''.format(project=self.project,
                                                                           dataset=self.adhoc_dataset,
                                                                           table=self.pc_table)
        df_pc = self.gcp_bq_hook.get_pandas_df(pc_sql)
        logging.info('gathered PC info')

        df_merged = df_ncoa.merge(df_pc, how='left'
                                  , left_on=['Account']
                                  , right_on=['AccountNumber'])
        logging.info('merging done')

        for col in df_merged.columns:
            if df_merged[col].dtype.kind != 'm':
                df_merged[col] = df_merged[col].fillna('')


        # The next line is essential to get accurate matching due to one data source having 'None' and the other having null
        df_merged = df_merged.replace('None', '').replace('nan', '').replace(np.nan, '', regex=True)


        # Create time_central and time_utc for consistency
        extract_date = dt.datetime.strptime(context['ds'], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        df_merged['run_date'] = context['next_ds_nodash']
        df_merged['extract_date'] = extract_date
        df_merged['run_date'] = pd.to_datetime(df_merged['run_date'])
        df_merged['extract_date'] = pd.to_datetime(df_merged['extract_date'])
        df_merged['time_utc'] = pd.to_datetime(df_merged['extract_date'])
        df_merged['time_central'] = df_merged.time_utc.dt.tz_localize('UTC', ambiguous=True).dt.tz_convert(
            'US/Central').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        df_merged['time_central'] = pd.to_datetime(df_merged['time_central'], errors='coerce')


        try:
            df_merged['time_utc'] = pd.to_datetime(df_merged['extract_date'])
            df_merged['time_central'] = df_merged.time_utc.dt.tz_localize('UTC', ambiguous=True).dt.tz_convert(
                'US/Central').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            df_merged['time_central'] = pd.to_datetime(df_merged['time_central'], errors='coerce')
        except:
            logging.error('Could not find extract_date')



        df_merged['OriginalAddressLine'] = df_merged['OriginalAddress1'] + ' ' + df_merged['OriginalAddress2']
        df_merged['UpdatedAddressLine'] = df_merged['UpdatedAddress1'] + ' ' + df_merged['UpdatedAddress2']

        df_merged['FirstMailingAddressLine'] = df_merged['FirstMailingAddressLine1'] + ' ' + df_merged[
            'FirstMailingAddressLine2'] + ' ' + df_merged['FirstMailingAddressLine3']
        df_merged['SecondMailingAddressLine'] = df_merged['SecondMailingAddressLine1'] + ' ' + df_merged[
            'SecondMailingAddressLine2'] + ' ' + df_merged['SecondMailingAddressLine3']
        df_merged['ThirdMailingAddressLine'] = df_merged['ThirdMailingAddressLine1'] + ' ' + df_merged[
            'ThirdMailingAddressLine2'] + ' ' + df_merged['ThirdMailingAddressLine3']
        df_merged['PrimaryMailingAddressLine'] = df_merged['PrimaryMailingAddressLine1'] + ' ' + df_merged[
            'PrimaryMailingAddressLine2'] + ' ' + df_merged['PrimaryMailingAddressLine3']

        # strip leading and trailing whitespace from the columns
        for col in df_merged.columns:
            if df_merged[col].dtype.kind == 'O' and col != 'MoveDate':
                df_merged[col] = df_merged[col].str.strip()


        x_address_list = ['FirstMailingAddress', 'SecondMailingAddress', 'ThirdMailingAddress', 'PrimaryMailingAddress']
        y_address_list = ['OriginalAddress', 'UpdatedAddress']
        z_list = ['State', 'City', 'PostalCode', 'Line']

        logging.info('renaming column names for consistency')
        df_merged = df_merged.rename(
            columns={'OriginalCity': 'OriginalAddressCity', 'OriginalState': 'OriginalAddressState',
                     'OriginalZip': 'OriginalAddressPostalCode',
                     'UpdatedCity': 'UpdatedAddressCity', 'UpdatedState': 'UpdatedAddressState',
                     'UpdatedZip': 'UpdatedAddressPostalCode', 'UpdatedCounty': 'UpdatedAddressCounty'})
        # print(df_merged.columns)


        for y in y_address_list:
            for x in x_address_list:
                for z in z_list:
                    # print(x,y,z)
                    df_merged[x + '_' + y + '_' + z + '_score'] = df_merged.apply(lambda row: self.match_score(row, x, y, z),
                                                                                  axis=1)

                    df_merged[x + '_' + y + '_' + z + '_match'] = df_merged.apply(
                        lambda row: self.string_match(row, x, y, z, self.threshold), axis=1)

                df_merged[x + '_' + y + '_AddressMatch'] = df_merged.apply(lambda row: self.address_match(row, x, y), axis=1)

        df_merged['AddressMatch'] = df_merged.apply(lambda row: self.final_address_match(row), axis=1)

        logging.info('renaming back column names to match source')
        df_merged = df_merged.rename(
            columns={'OriginalAddressCity': 'OriginalCity', 'OriginalAddressState': 'OriginalState',
                     'OriginalAddressPostalCode': 'OriginalZip',
                     'UpdatedCity': 'UpdatedAddressCity', 'UpdatedAddressState': 'UpdatedState',
                     'UpdatedAddressPostalCode': 'UpdatedZip', 'UpdatedAddressCounty': 'UpdatedCounty'})


        # matched and unmatched records separation
        logging.info('Filtering records based on address match')
        df_merged_matched = df_merged[df_merged['AddressMatch'] == 1]
        df_merged_matched['date_of_match'] = match_date
        df_merged_unmatched = df_merged[df_merged['AddressMatch'] == 0]



        logging.info('Writing into BigQuery...... Append')

        try:
            result = self.gcp_bq_hook.gcp_bq_write_table(df_merged_matched, self.project, self.destination_dataset, self.matched_tablename, 'APPEND', schema_enable = True)
        except:
            logging.info('no matched records')

        try:
            result = self.gcp_bq_hook.gcp_bq_write_table(df_merged_unmatched, self.project, self.destination_dataset, self.unmatched_tablename,
                                            'APPEND', schema_enable = True)
        except:
            logging.info('no unmatched records')

        return


    def urlencode(self,x):
        return urllib.quote(str(x))


    def urldecode(self,x):
        return urllib.unquote(str(x))


    def address_match(self,row, x, y):
        if (row[x + '_' + y + '_Line_match'] == 1) and (row[x + '_' + y + '_State_match'] == 1) and (
                row[x + '_' + y + '_City_match'] == 1) and (row[x + '_' + y + '_PostalCode_match'] == 1):
            return 1
        else:
            return 0


    def match_score(self,row, x, y, z):
        return fuzz.token_sort_ratio(row[x + z].lower(), row[y + z].lower())


    def string_match(self,row, x, y, z, threshold):
        if (row[x + '_' + y + '_' + z + '_score'] >= int(threshold)):
            return 1
        else:
            return 0


    def final_address_match(self,row):
        if (row['FirstMailingAddress_UpdatedAddress_AddressMatch'] == 1) or (
                row['SecondMailingAddress_UpdatedAddress_AddressMatch'] == 1) or (
                row['ThirdMailingAddress_UpdatedAddress_AddressMatch'] == 1) or (
                row['PrimaryMailingAddress_UpdatedAddress_AddressMatch'] == 1):
            return 1
        else:
            return 0