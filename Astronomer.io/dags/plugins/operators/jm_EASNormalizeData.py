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

import json
import decimal

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.contrib.hooks.gcs_hook import GCSHook
from plugins.hooks.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.base import BaseHook
from airflow.operators.bash_operator import BashOperator
import logging
import io
import pandas as pd
import datetime as dt
#import gcsfs

class EASNormalizeData(BaseOperator):
    """
    Write Audit data for api from landing metadata and bigquery

    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str

    """


    ui_color = '#c6fc03' #'#aa19323'

    @apply_defaults
    def __init__(self,
                 project,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 source_target_gcs_bucket,
                 google_cloud_storage_conn_id='google_cloud_default',
                 source_google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 prefix_for_filename_search=None,
                 schema_filename = 'NA',
                 folder_prefix='',
                 metadata_filename='',
                 subject_area='',
                 environment='',
                 *args,
                 **kwargs):

        super(EASNormalizeData, self).__init__(*args, **kwargs)

        self.project = project
        self.source  = source
        self.target_gcs_bucket = target_gcs_bucket
        self.source_target_gcs_bucket = source_target_gcs_bucket
        self.source_abbr = source_abbr
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.source_google_cloud_storage_conn_id = source_google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.prefix_for_filename_search = prefix_for_filename_search
        self.schema_filename = schema_filename
        self.folder_prefix = folder_prefix
        self.metadata_filename = metadata_filename + '/eas_metadata.json'
        self.subject_area = subject_area
        self.environment = environment


    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        source_hook = GCSHook(
            google_cloud_storage_conn_id=self.source_google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        if "{{ ds_nodash }}" in self.prefix_for_filename_search:
            self.prefix_for_filename_search = self.prefix_for_filename_search.replace("{{ ds_nodash }}",context['ds_nodash'])



        if "{{ ds_nodash }}" in self.metadata_filename:
            self.metadata_filename = self.metadata_filename.replace("{{ ds_nodash }}",context['ds_nodash'])

        if "{{ ds_nodash }}" in self.schema_filename:
            self.schema_filename = self.schema_filename.replace("{{ ds_nodash }}",context['ds_nodash'])

        print(self.prefix_for_filename_search)
        print(self.schema_filename)
        base_file_list = hook.list(self.target_gcs_bucket,maxResults=1000,prefix=self.prefix_for_filename_search) #self.prefix_for_filename_search[:-2])
        file_list = source_hook.list(self.source_target_gcs_bucket,maxResults=1000,prefix=self.prefix_for_filename_search) #self.prefix_for_filename_search[:-2])
        print(base_file_list)

        print(file_list)
        print(self.source_target_gcs_bucket)
        print (self.prefix_for_filename_search)



        file_counter = 0
        total_row_count = 0
        if 'Vendor_Information' in self.prefix_for_filename_search:
            Variable.set('eas_vendor_gcs_to_bq', 'False')
        if 'Vendor_Data' in self.prefix_for_filename_search:
            Variable.set('eas_transaction_gcs_to_bq', 'False')

        if len(file_list) != 0:
            # print('No files for processing schema file....')
            # return

            for f in file_list:
                if ('.txt' not in f) and ('.TXT' not in f):
                    print('Skipping file....', f)
                    continue
                # if context['ds_nodash'] not in f:
                #     continue
                print('Processing file....',f)
                row_count = self._process_file_from_landing(f,source_hook,context,hook)
                if row_count > 0:
                    total_row_count += row_count
                    file_counter += 1

            if total_row_count > 0:
                if 'Vendor_Information' in self.prefix_for_filename_search:
                    Variable.set('eas_vendor_gcs_to_bq', 'True')
                if 'Vendor_Data' in self.prefix_for_filename_search:
                    Variable.set('eas_transaction_gcs_to_bq', 'True')

        if self.metadata_filename != '/eas_metadata.json':
            print(self.metadata_filename)
            self._metadata_upload(context,total_row_count,file_counter)
        print("deleted_files")


        for source_file in file_list:
            if (self.environment):
                print(self.source_target_gcs_bucket, "/", source_file)
                source_hook.delete(self.source_target_gcs_bucket, source_file)
        return


    def _process_file_data(self,file_stream):
        line_to_df = []
        header_count = 0
        item_trigger_list = ['SYSTEM', 'BATCH', 'SCHEDULE']
        for line in file_stream.readlines():
            line_string = str(line).replace(r"\r\n'", '').replace(r"b'", '').replace(r'\r\n"', '').replace(r'b"', '')
            line_string_split = line_string.split('|')
            if header_count == 0:
                header_count = len(line_string_split)


            if (len(line_string_split) > header_count) or (line_string_split[12] in item_trigger_list):
                print('*' * 20)
                print(object)
                print(line_string)
                print(line_string_split)
                print(len(line_string_split), header_count)
                print('*' * 20)

                item_counter = 0
                for item in line_string_split:
                    if item in item_trigger_list:
                        break
                    else:
                        item_counter += 1

                print('Item Counter Value = ', item_counter)
                line_string_split[item_counter - 2] = line_string_split[item_counter - 2] + line_string_split[
                    item_counter - 1]
                del line_string_split[item_counter - 1]
                print(line_string_split)
                print(len(line_string_split), header_count)
                print('#' * 20)

            delta_split = header_count - len(line_string_split)
            for n in range(delta_split):
                line_string_split.append(None)
            line_to_df.append(line_string_split)

        counter = 0
        col_names = []
        found = False
        dup_columns = ['STATE', 'INV AMT']

        print('Head Count = ', header_count)
        print('Line count...', len(line_to_df))
        if len(line_to_df) == 0:
            return pd.DataFrame()

        if len(line_to_df) < 6:
            print(line_to_df)

        for col in line_to_df[0]:
            if col in dup_columns:
                if found:
                    col_names.append(col + '_' + str(counter))
                    counter += 1
                    continue
                else:
                    found = True
                    col_names.append(col)
                    counter += 1
                    continue
            else:
                col_names.append(col)

        #target_date_df = pd.DataFrame(line_to_df[1:], columns=col_names)
        # #drop columns div DIV, PROV, PURP, SPTYP
        if(self.subject_area == 'Transactions'):

            del_columns = ['DIV', 'PROV', 'PURP', 'SPTYP' , 'Susp Stat', 'Match ID','Susp Proc Date','Susp Match']
            landing_raw_df=pd.DataFrame(line_to_df[1:], columns=col_names)
            acct_list = ['10510', '10515', '10530', '10610','12502', '29200', '29380','29320', '29340', '29370', '29375','17000', '17320', '17600', '17700', '17400', '17800', '17500','26000', '26250', '26900', '26700', '26910','23002', '23002', '12502']
            landing_raw_acct_ftr_df = landing_raw_df[~landing_raw_df['ACCT'].isin(acct_list)]
            target_date_df = landing_raw_acct_ftr_df.drop(del_columns,axis=1,errors='ignore')


        else:
            target_date_df  = pd.DataFrame(line_to_df[1:], columns=col_names)
            print(target_date_df.columns.tolist())
            target_date_df = target_date_df[['Vendor ID', 'Vendor Name', 'User Def Category']]
            # target_date_df = pd.DataFrame(line_to_df[1:], columns=col_names)

        #This is to deal with rogue UTF data
        try:
            col_cleanup = ['DESCR','CITY']
            for col in col_cleanup:
                target_date_df[col] = target_date_df[col].str.replace(r'\x96', '-').str.replace(r'\xe9','e')
        except:
            pass
        return target_date_df

    def _process_column_names(self,target_date_df):
        new_col = []
        for col in target_date_df.columns:
            new_col.append(col.replace(' ', '_').replace('/', '_').replace('.', '_').replace('&', '_'))

        target_date_df.columns = new_col

        return target_date_df

    def _process_df_dataypes(self,target_date_df):
        to_floats = ['CNV_AMT', 'Disc_Pct']
        for col in to_floats:
            try:
                target_date_df[col] = target_date_df[col].astype(float)
            except:
                pass

        to_ints = ['Disc_Days', 'Net_Due_Days']
        for col in to_ints:
            try:
                target_date_df[col] = target_date_df[col].astype(int)
            except:
                pass
        return target_date_df

    def _upload_normalized_data(self,object,target_date_df,gcs_hook):
        object_split = object.split('/')
        target_date = object_split[len(object_split) - 2]
        new_file_name = object.replace('.TXT', '.json').replace('.txt', '.json').replace('l1/', 'l1_norm/').replace(
                                                                            target_date + '/', target_date + '/l1_norm_')

        gcs_hook.upload(self.target_gcs_bucket, new_file_name, target_date_df.to_json(orient='records', lines='\n'))
        return

    def _process_file_from_landing(self,object,gcs_hook,context,gcs_target_hook):
        try:
            file_data=gcs_hook.download(self.target_gcs_bucket,object)
        except:
            file_data = gcs_hook.download(self.source_target_gcs_bucket, object)

        file_stream = io.BufferedReader(io.BytesIO(file_data))

        target_date_df = self._process_file_data(file_stream)

        if len(target_date_df) > 0:
            target_date_df = self._process_column_names(target_date_df)
            target_date_df = self._process_df_dataypes(target_date_df)
            self._upload_normalized_data(object,target_date_df,gcs_target_hook)
            schema = self._generate_schema(target_date_df)
            print(schema)
            self._upload_schema(schema)
            return len(target_date_df)
        else:
            return 0

    def _generate_schema(self,df, default_type='STRING'):
        """ Given a passed df, generate the associated Google BigQuery schema.
        Parameters
        ----------
        df : DataFrame
        default_type : string
            The default big query type in case the type of the column
            does not exist in the schema.
        """

        type_mapping = {
            'i': 'INTEGER',
            #'b': 'BOOLEAN',
            'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})

        return fields

    def _upload_schema(self,json_data):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        try:
            gcs_hook.upload(self.target_gcs_bucket,
                            self.schema_filename,
                            str(json_data).replace("'",'"'))
        except:
            gcs_hook.upload(self.target_gcs_bucket,
                            self.schema_filename,
                            str(json_data).replace("'", '"'))
        return

    def _metadata_upload(self,context,row_count,file_count):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        target_prefix = self.prefix_for_filename_search.replace('l1/','l1_norm/')
        if target_prefix.startswith(self.folder_prefix):
           pass
        else:
            target_prefix = self.folder_prefix + target_prefix

        print('Metadata Targeted Prefix: ',target_prefix)
        file_list = []
        base_file_list = gcs_hook.list(self.target_gcs_bucket, versions=True, maxResults=1000, prefix=target_prefix, delimiter=',')
        for f in base_file_list:
            if ('.json' not in f):
                print('Skipping file....', f)
                continue
            # if context['ds_nodash'] not in f:
            #     continue
            file_list.append(f)

        json_metadata = {
                         'file_count' : file_count,
                         'l1_row_count': row_count,
                         'dag_execution_date' : context['ds']
                         }


        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)

        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename.format(ds_nodash=context['ds_nodash']),
                        df.to_json(orient='records', lines='\n'))
        return