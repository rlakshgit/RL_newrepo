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
from plugins.operators.jm_gcs import GCSHook
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
import re


class RatabaseToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('target_file_base',
                       'target_schema_base',
                       'target_metadata_base',
                       'target_archive_base',
                       'source_staging_location',)
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 project,
                 source_file_list,
                 target_file_base,
                 target_schema_base,
                 target_metadata_base,
                 target_archive_base,
                 source_staging_location,
                 gcs_conn_id,
                 source_bucket,
                 target_bucket,
                 target_dataset,
                 target_table,
                 airflow_files_var_set,
                 reprocess_history,
                 *args,
                 **kwargs):
        super(RatabaseToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.project = project
        self.source_file_list = source_file_list
        self.target_file_base = target_file_base
        self.target_schema_base = target_schema_base
        self.target_metadata_base = target_metadata_base
        self.target_archive_base = target_archive_base
        self.source_staging_location = source_staging_location
        self.gcs_conn_id = gcs_conn_id
        self.gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcs_conn_id)
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.gcs_conn_id)
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.target_dataset = target_dataset
        self.target_table = target_table
        self.airflow_files_var_set = airflow_files_var_set
        self.overwrite_enable = False
        self.source = 'ratabase'
        self.reprocess_history = reprocess_history

    def execute(self, context):

        if self.airflow_files_var_set != 'NA':
            # Get a list of files that are staged..
            file_list = self.gcp_storage_hook.list(self.source_bucket, prefix=self.source_staging_location)

            print(len(file_list))
            if len(file_list) > 5000:
                file_list = file_list[:5000]
            Variable.set(self.airflow_files_var_set, file_list)
            return

        schema_list = []
        metadata_list = []
        num_writes = 0

        # See if the BQ table exists.
        table_exists = self.gcp_bq_hook.table_exists(project_id = self.project, dataset_id = self.target_dataset,
                                                     taable_id = 't_{table}'.format(table=self.target_table))
        print(table_exists)

        # Get the string to identify files belonging to this refined table type
        match_dict = {'all': 'final-all:before_',
                      'request_pa': '-getrating-request',
                      'response_pa': '-getrating-response',
                      'request_cl': '-requestobject-',
                      'response_cl': '-responseobject-',
                      'soaprequest_cl': 'soaprequest',
                      'soapresponse_cl': 'soapresponse'}

        matches = match_dict[self.target_table].split(':')

        # Pull a list of all archived files.
        # If reprocessing history, pull all archived files that match the execution date and add them to the list for reprocessing.
        archive_list = self.gcp_storage_hook.list(self.target_bucket,
                                                  prefix=self.target_archive_base)  # This is all archive files across dates
        filtered_files = self.source_file_list.strip('][').split(', ')
        if self.reprocess_history:
            for f in archive_list:
                if (context['ds_nodash'] in f) and ('archive' in f):
                    print('Staging files for re-processing...', f)
                    filtered_files.append(f)

        # Now go through each file in the staging bucket.
        # Make sure the file text matches the type of file being processed currently.
        # If the file meets those criteria, add it to the filtered_files list for processing.
        for source_file in filtered_files:
            source_file = source_file.strip("'")

            matched = False
            for match_string in matches:
                if match_string.lower() in source_file.lower():
                    matched = True

            if not matched:
                continue

            print(source_file)
            if 'archive' in source_file:
                use_bucket = self.target_bucket
            else:
                use_bucket = self.source_bucket

            # Parse the data into a dataframe, flattening as needed.
            try:
                results_df = self._ratabase_xml_file_to_df(source_file, True, use_bucket, self.target_table)
            except:
                results_df = pd.DataFrame()

            if results_df.empty:
                continue
            print('we have results!')
            # If duplicate column names resulting from flattening, rename with _x appended.
            unique_list = []
            i = 0
            # traverse for all elements
            for x in results_df.columns:
                if x not in unique_list:
                    unique_list.append(x)
                else:
                    i = i + 1
                    unique_list.append(x + '_' + str(i))

            results_df.columns = unique_list

            # Clear up redundant or lengthy column names resulting from flattening with acceptable replacement values.
            results_df.columns = results_df.columns.str.replace('@', '')
            results_df.columns = results_df.columns.str.replace('#', '')
            results_df.columns = results_df.columns.str.replace(':', '_')
            results_df.columns = results_df.columns.str.replace('_s_', '_')
            results_df.columns = results_df.columns.str.replace('_tns_', '_')
            results_df.columns = results_df.columns.str.replace('s_Header', 'Header')
            results_df.columns = results_df.columns.str.replace('xmlns_s', 'xmlns')
            results_df.columns = results_df.columns.str.replace('_tns_', '_')
            results_df.columns = results_df.columns.str.replace('_tns_', '_')
            results_df.columns = results_df.columns.str.replace('Policies_Policy', 'Policy')
            results_df.columns = results_df.columns.str.replace('LOBS_LOB', 'LOB')
            results_df.columns = results_df.columns.str.replace('Regions_Region', 'Region')
            results_df.columns = results_df.columns.str.replace('Coverages_Coverage', 'Coverage')
            results_df.columns = results_df.columns.str.replace('Calculators_Calculator', 'Calculator')
            results_df.columns = results_df.columns.str.replace('ExtensionData', 'ExtData')
            results_df.columns = results_df.columns.str.replace('Items_Item', 'Item')
            results_df.columns = results_df.columns.str.replace('__', '_')
            results_df.columns = results_df.columns.str.replace('POLICIES_POLICY', 'POLICY')
            results_df.columns = results_df.columns.str.replace('POLEXT_FLD_', '')
            results_df.columns = results_df.columns.str.replace('Policies_CalcPolicy', 'Policy')
            results_df.columns = results_df.columns.str.replace('Extension_CalcRatabaseField_', '')
            results_df.columns = results_df.columns.str.replace('Policies_tns_Policy', 'Policy')
            results_df.columns = results_df.columns.str.replace('Policies_Policy', 'Policy')
            results_df.columns = results_df.columns.str.replace(
                'GetRateRatabaseObjects_tns_RatingServiceObjectsRequest', 'RatingServiceRequest')
            results_df.columns = results_df.columns.str.replace('GetRateRatabaseObjects_RatingServiceObjectsRequest',
                                                                'RatingServiceRequest')
            results_df.columns = results_df.columns.str.replace(
                'GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult', 'RatabaseObjectsResult')
            results_df.columns = results_df.columns.str.replace('tns_Extension_tns_RatabaseField_', '')
            results_df.columns = results_df.columns.str.replace('Extension_RatabaseField_', '')
            results_df.columns = results_df.columns.str.replace('_string_', '_')

            # Add some timestamp fields to the data
            t_now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            results_df['data_land_date'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
            results_df['data_run_date'] = pd.to_datetime(context['ds'], format='%Y-%m-%d %H:%M:%S')

            element_parts = source_file.split('/')
            filename_parts = element_parts[-1].split('.')
            UUID_filename_parts = filename_parts[0].split('-')

            # Parse the QuoteServiceID from the filename (field name requested by Shannon Schmiege)
            results_df['QuoteServiceID'] = '_'.join(UUID_filename_parts[:-2])
            # Parse the file name from the full path
            results_df['Full_File_Name'] = element_parts[-1]

            # # Capture the type of XML file as a field
            # def response_request(x):
            #     if 'soaprequest' in x.lower():
            #         return 'soaprequest'
            #     elif 'soapresponse' in x.lower():
            #         return 'soapresponse'
            #     elif 'request' in x.lower():
            #         return 'request'
            #     elif 'response' in x.lower():
            #         return 'response'
            #     elif ('final-all' in x.lower()) | ('before_' in x.lower()):
            #         return 'all'
            #
            # results_df['Type'] = results_df['Full_File_Name'].apply(response_request)

            # Check for column names exceeding length limit
            new_col = []
            for col in results_df.columns:
                if len(col) > 127:
                    new_col.append(col.replace('_', ''))
                else:
                    new_col.append(col)
            results_df.columns = new_col

            # Make sure the fields are written as STRING or TIMESTAMP.
            reorder_columns = ['QuoteServiceID', 'Full_File_Name', 'data_land_date', 'data_run_date']
            for col in results_df.columns:
                if col not in reorder_columns:
                    reorder_columns.append(col)
                if results_df[col].dtypes != 'datetime64[ns]':
                    results_df[col] = results_df[col].astype(str)

            results_df = results_df[reorder_columns]

            # Write the dataframe to json and upload to l1_norm directory.
            json_data = results_df.to_json(orient='records', lines='\n', date_format='iso')

            self.gcp_storage_hook.upload(bucket=self.target_bucket,
                                         object='{}.json'.format(self.target_file_base.format(filename_parts[0])),
                                         data=json_data)

            # Add the file name to the metadata list.
            metadata_list.append(source_file)

            # Generating schema from the dataframe
            fields = self._generate_schema(results_df)
            [schema_list.append(x) for x in fields if x not in schema_list]

            num_writes += 1
            print(num_writes)

            # Archive the file:
            if 'archive/' not in source_file and 'PROD' in self.project.upper():
                self.gcp_storage_hook.copy(self.source_bucket, source_file,
                                           self.target_bucket,
                                           source_file.replace(self.source_staging_location,
                                                               '{base}{date}/'.format(base=self.target_archive_base,
                                                                                      date=context['ds_nodash'])))
                self.gcp_storage_hook.delete(self.source_bucket, source_file)

            if num_writes >= 3000:
                break

        # Write the metadata file list
        if len(metadata_list) > 0:
            metadata = '\n'.join(x for x in metadata_list).encode('utf-8')
        else:
            metadata = 'No matching files found in staging'

        self.gcp_storage_hook.upload(bucket=self.target_bucket,
                                     object=self.target_metadata_base,
                                     data=metadata)

        # Upload the schema for the refined table from all of the individual files combined
        # Don't overwrite with blank schema in case this is a rerun of a date already processed.
        if (not table_exists) or (len(schema_list) > 0):
            self._upload_schema(schema_list)
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
            table_schema = \
            self.gcp_bq_hook.get_schema(self.target_dataset, 't_{table}'.format(table=self.target_table), self.project)[
                'fields']
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

    def _upload_schema(self, file_schema):
        schema = json.dumps(file_schema, sort_keys=True)
        schema = schema.encode('utf-8')

        self.gcp_storage_hook.upload(
            bucket=self.target_bucket,
            object=self.target_schema_base,
            data=schema
        )

    def _ratabase_xml_file_to_df(self, f, gcp_file=True, input_bucket='NA', parse_type='all'):
        def strip_special(x):
            return x.replace('@', '').replace(':', '_').replace('#', '')

        def strip_special_soap(x):
            return x.replace('@s:', '').replace('tns:', '').replace('@xmlns:tns', 'tns').replace('s:Body',
                                                                                                 'Body').replace(
                's_Body', 'Body').replace('s_Header', 'Header').replace('@', '').replace(':', '_').replace('#', '')

        def strip_special_value(x):
            return json.loads(
                json.dumps(x).replace('@s:', '').replace('tns:', '').replace('@xmlns:tns', 'tns').replace('s:Body',
                                                                                                          'Body').replace(
                    's_Body', 'Body').replace('s_Header', 'Header').replace('@', '').replace('#', ''))

        def unorder_dictionary(x):
            return json.loads(json.dumps(x))

        def parse_all(df_sub):
            if len(df_sub) == 8:
                df_sub.reset_index(drop=False, inplace=True)

            if len(df_sub) == 5:
                if len(df_sub.columns) == 1:
                    df_sub.reset_index(inplace=True)
                    df_sub.reset_index(drop=True, inplace=True)
                    if len(df_sub.columns) == 1:
                        df_sub = df_sub.transpose()
                else:
                    df_temp = df_sub.copy()
                    df_sub = pd.DataFrame()
                    for r in range(len(df_temp)):
                        df_one = df_temp.iloc[r]
                        df_one.transpose()
                        df_sub = pd.concat([df_sub, df_one])

            if len(df_sub) > 2:
                if len(df_sub.columns) == 1:
                    df_sub.reset_index(inplace=True)
                    df_sub.reset_index(drop=True, inplace=True)
                    if len(df_sub.columns) == 1:
                        df_sub = df_sub.transpose()

            if len(df_sub) == 2:
                if len(df_sub.columns) == 1:
                    df_sub = df_sub.transpose()
                    if len(df_sub.columns) == 1:
                        if ('#text' not in df_sub.columns) & ('ARRAYVALUE' not in df_sub.columns):
                            df_sub['#text'] = 'NA'

            if len(df_sub) == 1:
                if ('#text' not in df_sub.columns) & ('ARRAYVALUE' not in df_sub.columns):
                    df_sub['#text'] = 'NA'

            return df_sub

        def parse_cl_request_response(df_sub):
            if len(df_sub) == 5:
                df_sub.reset_index(drop=False, inplace=True)

            if len(df_sub) == 4:
                df_sub = df_sub.transpose()
                df_sub.reset_index(drop=True, inplace=True)

            if len(df_sub) == 3:
                df_sub = df_sub.transpose()

            if len(df_sub) == 2:
                if len(df_sub.columns) == 1:
                    df_sub = df_sub.transpose()
                    if 'Value' not in df_sub.columns:
                        df_sub['Value'] = 'NA'

                else:
                    if 'Value' not in df_sub.columns:
                        df_sub['Value'] = 'NA'
                    df_sub = df_sub.transpose()

                    if 'string' in df_sub.columns:
                        df_sub['string'] = df_sub['string'].fillna('NA')
                        df_sub['string'] = np.where(df_sub['string'].astype(str) == 'NA',
                                                    df_sub['RatabaseField'], df_sub['string'])
                        df_sub.reset_index(drop=False, inplace=True)
                        del df_sub['RatabaseField']

            if len(df_sub) == 1:
                if 'Value' not in df_sub.columns:
                    df_sub['Value'] = 'NA'
                    df_sub.reset_index(drop=True, inplace=True)

            return df_sub

        def parse_cl_soaprequest(df_sub):
            if len(df_sub) == 5:
                df_sub.reset_index(drop=False, inplace=True)

            if len(df_sub) == 4:
                df_sub = df_sub.transpose()
                df_sub.reset_index(drop=True, inplace=True)

            if len(df_sub) == 3:
                df_sub = df_sub.transpose()

            if len(df_sub) == 2:

                if len(df_sub.columns) == 1:
                    df_sub = df_sub.transpose()
                    if 'tns:Value' not in df_sub.columns:
                        df_sub['tns:Value'] = 'NA'

                else:
                    if 'tns:Value' not in df_sub.columns:
                        df_sub['tns:Value'] = 'NA'
                    df_sub = df_sub.transpose()

                    if 'string' in df_sub.columns:
                        df_sub['string'] = df_sub['string'].fillna('NA')
                        df_sub['string'] = np.where(df_sub['string'].astype(str) == 'NA',
                                                    df_sub['RatabaseField'], df_sub['string'])
                        df_sub.reset_index(drop=False, inplace=True)
                        del df_sub['RatabaseField']

            if len(df_sub) == 1:
                if 'tns:Value' not in df_sub.columns:
                    df_sub['tns:Value'] = 'NA'
                    df_sub.reset_index(drop=True, inplace=True)

            return df_sub

        def parse_pa_request(df_sub):
            if len(df_sub) == 5:
                df_sub.reset_index(drop=False, inplace=True)

            if len(df_sub) == 4:
                df_sub = df_sub.transpose()
                df_sub.reset_index(drop=True, inplace=True)

            if len(df_sub) == 3:
                df_sub = df_sub.transpose()

            if len(df_sub) == 2:

                if len(df_sub.columns) == 1:
                    df_sub = df_sub.transpose()
                    if 'Value' not in df_sub.columns:
                        df_sub['Value'] = 'NA'

                else:
                    if 'Value' not in df_sub.columns:
                        df_sub['Value'] = 'NA'
                    df_sub = df_sub.transpose()

                    if 'string' in df_sub.columns:
                        df_sub['string'] = df_sub['string'].fillna('NA')
                        df_sub['string'] = np.where(df_sub['string'].astype(str) == 'NA',
                                                    df_sub['RatabaseField'], df_sub['string'])
                        df_sub.reset_index(drop=False, inplace=True)
                        del df_sub['RatabaseField']

            if len(df_sub) == 1:
                if 'Value' not in df_sub.columns:
                    df_sub['Value'] = 'NA'
                    df_sub.reset_index(drop=True, inplace=True)

            return df_sub

        def parse_pa_response(df_sub):
            col_extra = 'NA'

            if len(df_sub) == 3:
                df_sub = df_sub.transpose()

            # if len(df_sub) == 2:
            #
            #     if len(df_sub.columns) == 1:
            #         df_sub = df_sub.transpose()
            #         if 'Value' not in df_sub.columns:
            #             df_sub['Value'] = 'NA'
            #
            #     else:
            #         if 'Value' not in df_sub.columns:
            #             df_sub['Value'] = 'NA'
            #         df_sub = df_sub.transpose()
            #
            #         if 'string' in df_sub.columns:
            #             df_sub['string'] = df_sub['string'].fillna('NA')
            #             df_sub['string'] = np.where(df_sub['string'].astype(str) == 'NA',
            #                                         df_sub['RatabaseField'], df_sub['string'])
            #             df_sub.reset_index(drop=False, inplace=True)
            #             del df_sub['RatabaseField']

            for col in df_sub.columns:
                if col != 'Name' and col != 'Value':
                    col_extra = col

            if col_extra != 'NA':
                if len(df_sub.columns) == 1:
                    return pd.DataFrame()

                df_sub.loc[0, 'Name'] = str(col_extra) + '_' + str(df_sub.loc[0, 'Name'])

                del df_sub[col_extra]

            if 'Value' not in df_sub.columns:
                df_sub['Value'] = None
            if 'Name' not in df_sub.columns:
                df_sub['Name'] = 'Unknown'

                if len(df_sub) > 4:
                    df_sub.reset_index(drop=False, inplace=True)

            return df_sub

        def parse_to_df(data, col_name, parse_type):
            if type(data).__name__ == 'list':
                df = pd.DataFrame()

                for l in data:
                    if type(l).__name__ == 'unicode':
                        print('Unicode -- You are in trouble!')
                        df.loc[0, 'data_identifier'] = 'Reserve'
                        df.loc[0, 'value'] = str(l)
                        continue

                    if l is None:
                        continue

                    if type(l).__name__ == 'list':
                        for k, v in l.items():
                            if v is None:
                                l[k] = {}

                    try:
                        df_sub = pd.DataFrame.from_dict(l, orient='index')
                    except:
                        if type(l).__name__ == 'str':
                            #     l = {'Value': l}
                            print(l)
                            df_sub = pd.DataFrame()
                            df_sub.loc[0, 'data_identifier'] = 'Reserve'
                            df_sub.loc[0, 'value'] = str(l)
                            df_sub['data_identifier'] = col_name + '_' + df_sub['data_identifier']
                            df = pd.concat([df, df_sub])
                            continue

                    # CL rating XML files output many individual pieces.  The files marked 'final-all' are the ones
                    # wanted by Shannon.  Files marked 'before_' are identical in structure so included as well;
                    # They may be duplicates or unneeded.
                    if parse_type == 'all':
                        df_sub = parse_all(df_sub)

                    # CL request, response, and soap response files all follow this format.  At this time, soap response
                    # files are not being ingested, but the code is prepped if that should change.
                    elif parse_type in ('request_cl', 'response_cl', 'soapresponse_cl'):
                        df_sub = parse_cl_request_response(df_sub)

                    # CL soap request files
                    elif parse_type == 'soaprequest_cl':
                        df_sub = parse_cl_soaprequest(df_sub)

                    # PA request files (FULL FLATTENING)
                    elif parse_type == 'request_pa':
                        df_sub = parse_pa_request(df_sub)

                    # PA response files (FULL FLATTENING)
                    elif parse_type == 'response_pa':
                        df_sub = parse_pa_response(df_sub)

                    # For all types of files, name the columns and concat to the main df.
                    if not df_sub.empty:
                        df_sub.columns = ['data_identifier', 'value']
                        df_sub['data_identifier'] = col_name + '_' + df_sub['data_identifier']
                        df = pd.concat([df, df_sub])

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

        # Download the file and read the lines
        if gcp_file:
            downloaded_file = self.gcp_storage_hook.download(input_bucket, f)
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
            if '<?xml version="1.0" encoding="UTF-8"?>' in line:
                line = line.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
            if '<?xml version' in line:
                continue
            if '< s:Envelope xmlns:s' in line:
                continue

            line_string = line_string + line

        # Parse the lines to a dictionary
        result = xmltodict.parse(line_string)
        result_unordered = json.loads(json.dumps(result))

        # Get the data in a dataframe with named columns
        results_df = pd.DataFrame.from_dict(result)
        results_df.reset_index(drop=False, inplace=True)
        results_df.columns = ['data_identifier', 'value']

        # Strip special characters from the data identifiers
        results_df['data_identifier'] = results_df['data_identifier'].apply(strip_special)

        # Get the data in a dataframe with named columns
        results_dfu = pd.DataFrame.from_dict(result_unordered)
        results_dfu.reset_index(drop=False, inplace=True)
        results_dfu.columns = ['data_identifier', 'value']
        results_dfu['data_identifier'] = results_dfu['data_identifier'].apply(strip_special_soap)
        results_dfu['value'] = results_dfu['value'].apply(strip_special_value)

        # Strip special characters from the data identifiers
        results_dfu['data_identifier'] = results_dfu['data_identifier'].apply(strip_special)

        combined_results_df = pd.DataFrame()

        # Go through each column in the first level of XML tags and flatten.  Results are appended in place as columns.
        for i in range(len(results_df)):
            result, result_status = parse_to_df(results_df.loc[i, 'value'], results_df.loc[i, 'data_identifier'],
                                                parse_type)
            combined_results_df = pd.concat([combined_results_df, result])
        combined_results_df.reset_index(drop=True, inplace=True)

        # Recursively flatten each column.  For CL, too many columns output to fully flatten.  Targeted flattening
        # occurs to be able to isolate the Policy # in its own field for searching.  For each type of CL file,
        # targeted columns are specified in a list and all others will remain unflattened.
        run_me = True
        while run_me:
            run_me = False
            for j in range(len(combined_results_df)):
                if parse_type == 'all':  # ('final-all' in f.lower()) | ('before_' in f.lower()):
                    field_list = ['RATEREQUEST_POLICIES',
                                  'RATEREQUEST_POLICIES_POLICY_POLEXT',
                                  'RATEREQUEST_POLICIES_POLICY_POLEXT_FLD',
                                  'RATEREQUEST',
                                  'RATEREQUEST_POLICIES_POLICY']
                    if str(combined_results_df.loc[j, 'data_identifier']) not in field_list:
                        continue

                elif parse_type in ('request_cl', 'response_cl', 'soapresponse_cl'):  # 'responseobject' in f.lower():
                    field_list = ['RatingStatus_Policies_CalcPolicy_Extension_CalcRatabaseField',
                                  'RatingStatus_Policies_CalcPolicy_Extension',
                                  'RatingStatus_Policies_CalcPolicy',
                                  'RatingStatus_Policies',
                                  'RatingStatus',
                                  'RatabaseProperties_Policies_CalcPolicy_Extension_CalcRatabaseField',
                                  'RatabaseProperties_Policies_CalcPolicy_Extension',
                                  'RatabaseProperties_Policies_CalcPolicy',
                                  'RatabaseProperties_Policies',
                                  'RatabaseProperties',
                                  's_Body_GetRateRatabaseObjectsResponse',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy_Extension',
                                  's_Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy_Extension_RatabaseField',
                                  's_Body_GetRateRatabaseObjectsResponse',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy_Extension',
                                  'Body_GetRateRatabaseObjectsResponse_GetRateRatabaseObjectsResult_RatingStatus_Policies_Policy_Extension_RatabaseField'
                                  ]
                    if str(combined_results_df.loc[j, 'data_identifier']) not in field_list:
                        continue

                elif parse_type == 'soaprequest_cl':  # 'soaprequest' in f.lower():
                    field_list = ['s_Body_tns:GetRateRatabaseObjects',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest_tns:RatabaseProperties',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest_tns:RatabaseProperties_tns:Policies',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest_tns:RatabaseProperties_tns:Policies_tns:Policy',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest_tns:RatabaseProperties_tns:Policies_tns:Policy_tns:Extension',
                                  's_Body_tns:GetRateRatabaseObjects_tns:RatingServiceObjectsRequest_tns:RatabaseProperties_tns:Policies_tns:Policy_tns:Extension_tns:RatabaseField',
                                  'Body_GetRateRatabaseObjects',
                                  'Body_GetRateRatabaseObjects_tns',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest_RatabaseProperties',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest_RatabaseProperties_Policies',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest_RatabaseProperties_Policies_Policy',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest_RatabaseProperties_Policies_Policy_Extension',
                                  'Body_GetRateRatabaseObjects_RatingServiceObjectsRequest_RatabaseProperties_Policies_Policy_Extension_RatabaseField'
                                  ]
                    if str(combined_results_df.loc[j, 'data_identifier']) not in field_list:
                        continue
                elif parse_type == 'response_pa':
                    field_list = ['RatingStatus',
                                  'RatingStatus_Policies',
                                  'RatingStatus_Policies_Policy',
                                  'RatingStatus_Policies_Policy_Extension',
                                  'RatingStatus_Policies_Policy_Extension_RatabaseField',
                                  'RatingStatus_RatabaseSteps',
                                  'RatingStatus_RatabaseSteps_Output']
                    if str(combined_results_df.loc[j, 'data_identifier']) not in field_list:
                        continue
                elif parse_type == 'request_pa':
                    field_list = ['RatabaseProperties',
                                  'RatabaseProperties_Policies',
                                  'RatabaseProperties_Policies_Policy',
                                  'RatabaseProperties_Policies_Policy_Inputs',
                                  'RatabaseProperties_Policies_Policy_Inputs_RatabaseField',
                                  'RatabaseProperties_Policies_Policy_Outputs',
                                  'RatabaseProperties_Policies_Policy_Outputs_RatabaseField',
                                  'RatabaseProperties_Policies_Policy_LOBS',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB_Inputs',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions_Region',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions_Region_Coverages',
                                  # 'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions_Region_Coverages_Coverage',
                                  'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions_Region_Items',
                                  # 'RatabaseProperties_Policies_Policy_LOBS_LOB_Regions_Region_Items_Item'
                                  ]
                    if str(combined_results_df.loc[j, 'data_identifier']) not in field_list:
                        continue

                result, result_status = parse_to_df(combined_results_df.loc[j, 'value'],
                                                    combined_results_df.loc[j, 'data_identifier'],
                                                    parse_type)

                print(combined_results_df.loc[j, 'data_identifier'])
                print('result status is ' + str(result_status))

                if result_status == 'value':
                    continue
                if result_status == -1:
                    continue
                if result_status == 0:
                    combined_results_df = combined_results_df.drop([j])
                    run_me = True

                if len(result) == 0:
                    continue

                if 'soap' in f.lower():
                    result['data_identifier'] = result['data_identifier'].apply(strip_special_soap)
                else:
                    result['data_identifier'] = result['data_identifier'].apply(strip_special)

                # Remove ordered dictionary here and replace with normal dictionary
                try:
                    result['value'] = result['value'].apply(unorder_dictionary)
                except:
                    print('Could not convert to non-ordered dict')
                    print(result['value'])
                combined_results_df = pd.concat([combined_results_df, result])
                combined_results_df.reset_index(drop=True, inplace=True)

        try:
            combined_results_df['value'] = combined_results_df['value'].apply(strip_special_value)
        except:
            print('Could not strip special characters from the value column.')

        results_df = pd.concat([results_dfu, combined_results_df])
        results_df.reset_index(drop=True, inplace=True)
        results_df.set_index(['data_identifier'], inplace=True)
        results_df = results_df.transpose()
        results_df.reset_index(drop=True, inplace=True)

        return results_df