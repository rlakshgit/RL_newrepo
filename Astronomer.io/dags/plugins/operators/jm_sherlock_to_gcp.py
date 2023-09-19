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
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.models import Variable

import os
import pandas as pd
import warnings
import numpy as np
import logging
import xmltodict
import sys
import io
import argparse
import datetime as dt
import urllib
import json


class SherlockToGoogleCloudStorageOperator(BaseOperator):

  #  template_fields = ('partition_date', )
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 source_project,
                 target_project,
                 source_file_list,
                 target_file_base,
                 target_schema_base,
                 target_metadata_base,
                 target_archive_base,
                 source_gcs_conn_id,
                 target_gcs_conn_id,
                 source_bucket,
                 target_bucket,
                 target_dataset,
                 target_table,
                 airflow_files_var_set,
                 airflow_present_files_var_set,
                 reprocess_history,
                 archive_project,
                 *args,
                 **kwargs):
        super(SherlockToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_project = source_project
        self.target_project = target_project
        self.source_file_list = source_file_list
        self.target_file_base = target_file_base
        self.target_schema_base = target_schema_base
        self.target_metadata_base = target_metadata_base
        self.target_archive_base = target_archive_base
        self.source_gcs_conn_id = source_gcs_conn_id
        self.target_gcs_conn_id = target_gcs_conn_id
        self.source_gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.source_gcs_conn_id)
        self.target_gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.target_gcs_conn_id)
        self.target_gcp_bq_hook = BigQueryHook(gcp_conn_id=self.target_gcs_conn_id)
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.target_dataset = target_dataset
        self.target_table = target_table
        self.airflow_files_var_set = airflow_files_var_set
        self.airflow_present_files_var_set = airflow_present_files_var_set
        self.overwrite_enable = False
        self.source = 'sherlock'
        self.reprocess_history = reprocess_history
        self.archive_project=archive_project

    def execute(self, context):

        if self.airflow_files_var_set != 'NA':
            # Get a list of files that are staged..
            file_list = self.source_gcp_storage_hook.list(self.source_bucket)
            Variable.set(self.airflow_files_var_set, ','.join(x for x in file_list))
            return


        ##Put in file filter and then move forward.....
        #Three potential checks
        # 1. ensure it is an xml
        # 2. Ensure not in archive folder or l1
        # 3. Ensure not a duplicate
        # 4. Ensure DS matches archive folder (only valid if self.reprocess_history = TRUE

        filtered_files = []
        source_file_list = self.source_gcp_storage_hook.list(self.source_bucket)
        for f in source_file_list:
            if '.xml' not in f:
                continue

            if self.reprocess_history:
                if (context['ds_nodash'] in f) and ('archive' in f) and ('l1' not in f):
                    print('Staging file for re-processing....',f)
                    filtered_files.append(f)
                    continue

            if ('archive' not in f) and ('l1' not in f):
                found_count = 0
                for dup_f in source_file_list:
                    if f in dup_f:
                        found_count += 1
                if found_count > 1:
                    print('Duplicate file found....', f)
                    print('Skipping file...')
                    continue
                filtered_files.append(f)
                continue



        print(filtered_files)

        schema_list = []
        for source_file in filtered_files:
            print('*' * 20)
            print('Processing file....',source_file)
            print('*' * 20)
            f_temp = source_file.replace('.xml', '').replace('::', '')
            f_split = f_temp.split('_')
            if len(f_split) > 3:
                transaction_id = '_'.join(f_split[0:len(f_split) - 3])
            else:
                transaction_id = '_'.join(f_split[0:len(f_split) - 1])

            if len(f_split) > 4:
                load_date = ':'.join(f_split[len(f_split) - 3:len(f_split)])
            else:
                load_date = f_split[len(f_split) - 1]

            try:
                load_date = pd.to_datetime(load_date)
            except:
                load_date = pd.to_datetime(context['ds'])

            results_df = self._sherlock_xml_file_to_df(source_file, True, self.source_bucket)

            results_df['filename'] = source_file
            results_df['transaction_ssid'] = transaction_id
            results_df['request_timestamp'] = load_date
            results_df['data_land_date'] = load_date
            results_df['time_central'] = load_date
            results_df['data_run_date'] = pd.to_datetime(context['ds'])

            def time_test(x):
                # print(x)
                if str(x) == '1900-01-01 00:00:00.000':
                    return pd.to_datetime('1970-01-01')
                if (':' not in str(x)) and ('-' not in str(x)) and ('/' not in str(x)):
                    return pd.to_datetime('1970-01-01')

                if 'T' in str(x):
                    return pd.to_datetime(x, errors='coerce')
                try:
                    x_temp = pd.to_datetime(x, unit='ms')
                except:
                    x_temp = pd.to_datetime(x, errors='ignore')
                return x_temp

            if 'RecordDate' in results_df.columns:
                results_df['RecordDate'] = results_df['RecordDate'].apply(time_test)


            # if self.target_gcp_bq_hook.table_exists(project_id=self.target_project,
            #                                         dataset_id=self.target_dataset,
            #                                         table_id=self.target_table):
            #     logging.info('Destination table {} already exists!'.format(self.target_table))
            #
            #     table_schema = self.target_gcp_bq_hook.get_schema(self.target_dataset, self.target_table, self.target_project)['fields']
            #     table_columns = []
            #     for row in table_schema:
            #         table_columns.append(row['name'])
            #         if row['name'] not in results_df.columns:
            #             results_df[row['name']] = None
            #         if row['type'] == 'TIMESTAMP':
            #             results_df[row['name']] = pd.to_datetime(results_df[row['name']])
            #             continue
            #         elif row['type'] == 'FLOAT':
            #             results_df[row['name']] = results_df[row['name']].astype(float)
            #             continue
            #         elif row['type'] == 'INT':
            #             results_df[row['name']] = results_df[row['name']].astype(int)
            #             continue
            #         elif row['type'] == 'STRING':
            #             results_df[row['name']] = results_df[row['name']].astype(str)
            #             continue
            #         else:
            #             results_df[row['name']] = results_df[row['name']].astype(str)
            #
            #     for col in results_df.columns:
            #         if col not in table_columns:
            #             try:
            #                 self.target_gcp_bq_hook.gcp_bq_table_schema_update(self.target_project, self.target_dataset, self.target_table, col, 'STRING', 'NULLABLE')
            #             except:
            #                 print('Failed to update schema')


            self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                                object='{}.json'.format(self.target_file_base.format(context['ds_nodash'], f_temp)),
                                                data=results_df.to_json(orient='records', lines='\n',date_format='iso'))

            # Generating schema from the dataframe
            fields = self._generate_schema(results_df)
            [schema_list.append(x) for x in fields if x not in schema_list]


            ##Archive the file:
            if 'archive/' not in source_file:
                if self.archive_project == 'prod-edl':
                    self.source_gcp_storage_hook.copy(self.source_bucket, source_file,
                                                      self.source_bucket, self.target_archive_base.format(context['ds_nodash'],source_file))


        ####
        print('Uploading schema file to the storage bucket...')
        self._upload_schema(schema_list, directory_date=context['ds_nodash'])

        print('Uploading metadata.....')
        ###Upload of metadata a.k.a file_list
        df_test = pd.DataFrame(filtered_files,columns=['files_list'])
        self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                            object=self.target_metadata_base.format(context['ds_nodash']),
                                            data=df_test.to_json(orient='records', lines='\n'))


        return

    def _generate_schema(self, data, default_type='STRING'):
        field = []
        type_mapping = {
                'i': 'INTEGER',
                'b': 'BOOLEAN',
                'f': 'FLOAT',
                'O': 'STRING',
                'S': 'STRING',
                'U': 'STRING',
                'M': 'TIMESTAMP'
            }

        data_columns = []
        for column_name, column_dtype in data.dtypes.iteritems():
            data_columns.append(column_name)
            field.append({'name': column_name,
                          'type': type_mapping.get(column_dtype.kind, default_type),
                          'mode': 'NULLABLE'})

        try:
            table_schema = self.target_gcp_bq_hook.get_schema(self.target_dataset, self.target_table, self.target_project)['fields']
        except:
            table_schema = {}

        table_columns = []
        for row in table_schema:
            table_columns.append(row['name'])
            if row['name'] not in data_columns:
                field.append({'name': row['name'],
                              'type': row['type'],
                              'mode': 'NULLABLE'})

        return field

    def _upload_schema(self, file_schema, directory_date):
        schema = json.dumps(file_schema, sort_keys=True)
        schema = schema.encode('utf-8')

        self.target_gcp_storage_hook.upload(
            bucket=self.target_bucket,
            object=self.target_schema_base.format(directory_date),
            data=schema
        )
        return

    def _sherlock_xml_file_to_df(self, f, gcp_file=True, input_bucket='NA'):
        def parse_to_df(data, col_name):
            if (type(data).__name__ == 'list'):
                df = pd.DataFrame()
                for l in data:
                    df_sub = pd.DataFrame.from_dict(l, orient='index')
                    if len(df_sub) > 2:
                        df_sub.reset_index(drop=False, inplace=True)
                        df_sub.set_index(['index'], inplace=True, drop=True)
                        df_sub = df_sub.transpose()
                        df = pd.concat([df, df_sub])
                        continue
                return df, 0

            try:
                df = pd.DataFrame.from_dict([data])
                df = df.transpose()
                df.reset_index(drop=False, inplace=True)
                if len(df.columns) == 3:
                    df_sub1 = df[[df.columns[0], df.columns[1]]]
                    df_sub1['index'] = df_sub1['index'] + '_' + df.columns[1]
                    df_sub1.columns = ['data_identifier', 'value']
                    df_sub2 = df[[df.columns[0], df.columns[2]]]
                    df_sub2['index'] = df_sub2['index'] + '_' + df.columns[2]
                    df_sub2.columns = ['data_identifier', 'value']
                    df = pd.concat([df_sub1, df_sub2])
                    df['data_identifier'] = col_name + '_' + df['data_identifier']
                    return df
                df.columns = ['data_identifier', 'value']
                df['data_identifier'] = col_name + '_' + df['data_identifier']

            except:
                try:
                    df = pd.DataFrame.from_dict(data, orient='index')
                    df.reset_index(drop=False, inplace=True)
                    df.columns = ['data_identifier', 'value']
                    df['data_identifier'] = col_name + '_' + df['data_identifier']
                except:
                    return pd.DataFrame(), -1
            return df, 0

        if gcp_file:
            downloaded_file = self.source_gcp_storage_hook.download(input_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(downloaded_file))
            lines = file_stream.readlines()

        else:
            with open(f, 'r') as one_file:
                lines = one_file.readlines()

        line_string = ''
        for line in lines:
            line = line.decode('utf-8').strip()  # Python 3 has bytes type so this is needed to treat as string
            if '<?xml version="1.0" encoding="utf-8"?>' in line:
                line = line.replace('<?xml version="1.0" encoding="utf-8"?>', '')

            if '<?xml version' in line:
                continue
            line_string = line_string + line

        result = xmltodict.parse(line_string)
        result_df = pd.DataFrame.from_dict(result)
        result_df.reset_index(drop=False, inplace=True)
        result_df.columns = ['data_identifier', 'value']
        combined_result_df = pd.DataFrame()
        for i in range(len(result_df)):
            result, result_status = parse_to_df(result_df.loc[i, 'value'], result_df.loc[i, 'data_identifier'])
            combined_result_df = pd.concat([combined_result_df, result])
        combined_result_df.reset_index(drop=True, inplace=True)

        run_me = True
        while run_me:
            run_me = False
            for j in range(len(combined_result_df)):
                result, result_status = parse_to_df(combined_result_df.loc[j, 'value'], combined_result_df.loc[j, 'data_identifier'])
                if result_status == 'value':
                    continue
                if result_status == -1:
                    continue
                if result_status == 0:
                    combined_result_df = combined_result_df.drop([j])
                    run_me = True

                combined_result_df = pd.concat([combined_result_df, result])
                combined_result_df.reset_index(drop=True, inplace=True)



        try:
            del combined_result_df['data_identifier']
            del combined_result_df['value']
        except:
            pass

        combined_result_df.dropna(inplace=True, how='all')
        combined_result_df.reset_index(drop=True, inplace=True)
        for i in range(len(result_df)):
            if 'DriverDeterminationResults' == result_df.loc[i, 'data_identifier']:
                continue

            if len(combined_result_df) == 0:
                combined_result_df.loc[0,result_df.loc[i, 'data_identifier']] = result_df.loc[i, 'value']
            else:
                combined_result_df[result_df.loc[i, 'data_identifier']] = result_df.loc[i, 'value']

        new_col = []
        for col in combined_result_df.columns:
            new_col.append(col.replace('@', ''))
        combined_result_df.columns = new_col

        return combined_result_df
