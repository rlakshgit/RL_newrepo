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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


import sys
import json
import logging
import pandas as pd
import numpy as np
import math
pd.options.mode.chained_assignment = None
import datetime as dt

from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook as BQH2
# from plugins.hooks.jm_bq import BigQueryHook
from airflow import configuration
import os




class MixpanelDigitalDashboardUsageCountSummary(BaseOperator):
    @apply_defaults
    def __init__(self,
                 source_dataset,
                 source_table,
                 destination_dataset,
                 destination_table,
                 project,
                 file_name,
                 target_gcs_bucket,
                 google_cloud_storage_conn_id,
                 schema_filename,
                 schema_data=None,
                 *args,
                 **kwargs):
        super(MixpanelDigitalDashboardUsageCountSummary, self).__init__(*args, **kwargs)

        self.project = project
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_storage_conn_id)
        self.gcp_bq_hook_v2 = BQH2(bigquery_conn_id=self.google_cloud_storage_conn_id)
        self.source_dataset = source_dataset
        self.file_name = str(file_name)
        self.source_table = source_table
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table
        self.target_gcs_bucket = target_gcs_bucket
        self.schema_filename = schema_filename
        self.schema_data = schema_data


    def execute(self, context):
        partition_date = context['ds_nodash']
        partition_table_name = str(self.destination_table) + "$" + str(partition_date)
        logging.info('processing from : ' + partition_table_name)
        print(configuration.get('core', 'dags_folder'))
        if self.schema_data == None:
            file_path = os.path.join(configuration.get('core', 'dags_folder'),
                                     r'ini/mixpanel_digital_dashboard_schema.json')
            file_path = file_path.replace('dags/', '')

            with open(file_path, 'rb') as inputJSON:
                schemaList = json.load(inputJSON)
        else:
            schemaList = self.schema_data

        ssql = """select event,distinct_id,time_central,item_types,item_values,technology_source, acquisition_channel, 
                   acquisition_source, acquisition_source_detail, search_engine, browser, state_province, zip_postal_code 
               from `{project}.{dataset}.{table}` where time_central >= '{starttime}' and time_central <= '{endtime}'""".format(project=self.project,dataset=self.source_dataset,
                   table=self.source_table,
                   starttime=str(context['ds'] + ' 00:00:00'),
                   endtime=str(context['ds'] + ' 23:59:59'))

        logging.info('sql: ' + ssql)
        logging.info('Pull data from BQ..MASTER_TABLE..START')
        # df_bqt = self.gcp_bq_hook.get_pandas_df(ssql)
        df_bqt = self.gcp_bq_hook_v2.get_data(ssql)
        if len(df_bqt) == 0:
            print('No datat to pull....')
            return
        print(df_bqt)
        logging.info('Pull data from BQ..MASTER_TABLE..COMPLETE')

        df_bqt['time_central'] = pd.to_datetime(df_bqt['time_central'])
        logging.info('Adjust Item Types....')
        df_bqt = self._tf_support_string_to_list(df_bqt, 'item_types')

        logging.info('Complete transformation....')
        df_transformed = self._mixpanel_digital_dashboard(df_bqt)
        file_name = self.file_name + str(context['ds_nodash']) + '/gld_data_mixpanel_UsageCountSummary_data.json'

        #print(df_transformed)

        new_cols = []
        for col in df_transformed.columns:
             new_col = col.replace(' ', '_')
             new_cols.append(new_col)

        df_transformed.columns = new_cols
        final_column_list = []
        for val in schemaList:
            print(val)
            if val['name'] in df_transformed.columns:
                final_column_list.append(val['name'])
                if val['type'].upper() == "NUMERIC":
                    print(val['name'])
                    df_transformed[val['name']] = df_transformed[val['name']].apply(lambda x: self._round_me(x))

        df_transformed = df_transformed[final_column_list]



        json_data = df_transformed.to_json(orient='records', lines='\n', date_format='iso',double_precision=2)
        self.gcp_storage_hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)
        logging.info('Generating UsageCount Summary Data ....COMPLETE')

        return


    def _tf_support_to_list(self,x):
        '''
        This is support function for an Pandas apply function. This function will convert a comma delimited string
        to a python list for processing.
        :param x: This in the value from the apply function that the transformation must occur on
        :return: List of values
        '''
        if x == None:
            return 'NA'
        return x.split(',')
    def _tf_support_string_to_list(self,df_in,target_col):
        '''
        Function for targetting a specific col, converting to a list, and dropping all non-populated fields
        :param df_in: Source dataframe input
        :param target_col: Targeted column for parsing
        :return: Returns dataframe based on targetted with no Nulls.
        '''

        #df_in[target_col] = df_in[target_col].str.strip('"')
        df_in[target_col] = df_in[target_col].apply(self._tf_support_to_list)
        df_in = df_in[(df_in[target_col] != 'NA')]
        return df_in
    def _tf_support_parse_string_list(self, col, func, **kwargs):
        '''
        Support function for the parsing and count support for classification.
        :param col: Target Col of the parsing
        :param func: Type of numpy function to by applied
        :param kwargs: Additional key word arguements for function support
        :return: Returns values based on the function type.
        '''
        # Cleans string formatted lists from Mixpanel
        # Example: the string "['Ring', 'Ring', 'Earring']" will return a Python list with those values still in single quotes
        # Example: The string "[100, 200, 500]" will return a list with numeric values.
        my_list = pd.to_numeric(col.strip('"').strip('-')
                                .replace('None', '')
                                .replace('{}', '')
                                .replace(' ', '')
                                .split(','))
        return (func(my_list, **kwargs))

    def _tf_support_time_test(self, x):
        '''
        Apply function support
        :param x: This in the value from the apply function that the transformation must occur on
        :return: 1 if valid time contruct, 0 if not.
        '''

        if '-' in str(x):
            return 1
        else:
            return 0

    def _mixpanel_digital_dashboard(self,df_in):
        '''
        This is the digital dashboard tranformation code for the preperation of data for the output to a GCP BQT
        :param df_in: Dataframe input
        :return: Output is the transformed dataframe for the posting to a table.
        '''
        #Filter on QnA events only.....
        df = df_in[df_in['technology_source'] == 'QnA']
        df['datetime'] = df['time_central']#.apply(datetime.fromtimestamp, local_timezone)  ## CENTRAL TIME
        df['date'] = df['time_central'].dt.date
        # Create a composite key - imperfect solution to sessionization problem
        df['date_distinct_id'] = df['distinct_id'] + df['date'].astype(str)

        page_changes = (df[(df['event'] == 'Page Changed')]
                        .sort_values('time_central')
                        .groupby('date_distinct_id')
                        .last())

        order_features = pd.DataFrame(index=page_changes.index)
        order_features.loc[:, 'datetime'] = page_changes.loc[:, 'datetime']
        order_features.loc[:, 'browser'] = page_changes.loc[:, 'browser']
        order_features.loc[:, 'acquisition_channel'] = page_changes.loc[:, 'acquisition_channel']
        order_features.loc[:, 'acquisition_source'] = page_changes.loc[:, 'acquisition_source']
        order_features.loc[:, 'acquisition_source_detail'] = page_changes.loc[:, 'acquisition_source_detail']
        order_features.loc[:, 'search_engine'] = page_changes.loc[:, 'search_engine']
        order_features.loc[:, 'state_province'] = page_changes.loc[:, 'state_province']
        order_features.loc[:, 'zip_postal_code'] = page_changes.loc[:, 'zip_postal_code']
        # Item Types
        order_features.loc[:, 'num_items'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count(",") + 1).fillna(
            value=0).astype(int)
        order_features.loc[:, 'num_ring'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Ring")).fillna(
            value=0).astype(int)
        order_features.loc[:, 'num_watch'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Watch")).fillna(
            value=0).astype(int)
        order_features.loc[:, 'num_earrings'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Earrings")).fillna(value=0).astype(int)
        order_features.loc[:, 'num_bracelet'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Bracelet")).fillna(value=0).astype(int)
        order_features.loc[:, 'num_necklace'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Necklace")).fillna(value=0).astype(int)
        order_features.loc[:, 'num_pendant'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Pendant")).fillna(value=0).astype(int)
        order_features.loc[:, 'num_loosestone'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Loose stone")).fillna(value=0).astype(int)
        order_features.loc[:, 'num_other'] = page_changes.loc[:, 'item_types'].map(
            lambda x: x.count("Other")).fillna(
            value=0).astype(int)


        order_features['num_items'] = order_features['num_other'] + order_features['num_loosestone']+order_features['num_pendant']+order_features['num_necklace']
        order_features['num_items'] = order_features['num_items']+order_features['num_bracelet']+order_features['num_earrings']+order_features['num_watch']+order_features['num_ring']
        #Item Values
        order_features.loc[:, 'max_item'] = page_changes.loc[:, 'item_values'].apply(self._tf_support_parse_string_list,args=[max]).fillna(value=0).astype(int)
        order_features.loc[:, 'min_item'] = page_changes.loc[:, 'item_values'].apply(self._tf_support_parse_string_list,args=[min]).fillna(value=0).astype(int)
        order_features.loc[:, 'avg_item_val'] = page_changes.loc[:, 'item_values'].apply(self._tf_support_parse_string_list,args=[np.mean]).fillna(value=0)
        order_features.loc[:, 'total_item_val'] = page_changes.loc[:, 'item_values'].apply(self._tf_support_parse_string_list,args=[np.sum]).fillna(value=0)
        order_features.loc[:, 'stdev_item_val'] = page_changes.loc[:, 'item_values'].apply(self._tf_support_parse_string_list,args=[np.std],ddof=1).fillna(value=0)

        df_counts = (df[df['event'] != 'Page Changed']
                     .groupby(['date_distinct_id', 'event'])
                     .size()
                     .unstack(fill_value=0)
                     )

        #Filter through all columns and generate counts.
        count_list = ['Application Finished','Application Started','Quote Finished','Quote Started']
        for target_count in count_list:
            try:
                df_counts['Unique ' + target_count] = np.where(df_counts[target_count] > 0, 1, 0)
            except:
                df_counts['Unique ' + target_count] = 0



        df_counts = df_counts.reset_index(level=['date_distinct_id'])
        order_features_types = dict(num_items=np.int32,
                                    num_ring=np.int32,
                                    num_watch=np.int32,
                                    num_earrings=np.int32,
                                    num_bracelet=np.int32,
                                    num_necklace=np.int32,
                                    num_pendant=np.int32,
                                    num_loosestone=np.int32,
                                    num_other=np.int32,
                                    max_item=np.int32,
                                    min_item=np.int32,
                                    avg_item_val=np.float64,
                                    stdev_item_val=np.float64)

        df_final = pd.merge(df_counts, order_features, how='left', left_on='date_distinct_id',
                            right_index=True).fillna(
            value=0)
        df_final = df_final.astype(order_features_types)
        df_final = df_final.sort_index()

        df_final['time_value'] = df_final['datetime'].apply(self._tf_support_time_test)
        df_final=df_final[(df_final['time_value'] == 1)]
        df_final.reset_index(drop=True,inplace=True)
        del df_final['time_value']
        df_final.sort_values(by=['datetime'],ascending=[True],inplace=True)
        return df_final

    def _round_me(self,x):
        frac, whole = math.modf(x)
        #print(frac, len(str(frac)), np.round(x, decimals=2))
        if len(str(frac)) != 5:
            return np.round(x, decimals=2)
        frac_test = np.round(frac, decimals=3)
        test_field = int(str(frac_test)[-1:])
        if (test_field > 5) | (test_field < 5):
            return np.round(x, decimals=2)
        if test_field == 5:
            new_x = np.round(x, decimals=2)
            return new_x