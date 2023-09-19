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

class ExperianStagingToL1(BaseOperator):
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


    ui_color = '#4b9ce3' #'#aa19323'

    @apply_defaults
    def __init__(self,
                 project,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 get_file_list=False,
                 base_gcs_folder=None,
                 target_file = 'NA',
                 approx_max_file_size_bytes=1900000000,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 prefix_for_filename_search=None,
                 audit_location = 'NA',
                 *args,
                 **kwargs):

        super(ExperianStagingToL1, self).__init__(*args, **kwargs)

        self.project = project
        self.source  = source
        self.base_gcs_folder = base_gcs_folder
        self.target_gcs_bucket = target_gcs_bucket
        self.source_abbr = source_abbr
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.prefix_for_filename_search = prefix_for_filename_search
        self.get_file_list = get_file_list
        self.target_file = target_file
        self.audit_location = audit_location


    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        if "{{ ds_nodash }}" in self.audit_location:
            self.audit_location = self.audit_location.replace("{{ ds_nodash }}",context['ds_nodash'])

        if "{{ ds_nodash }}" in self.prefix_for_filename_search:
            self.prefix_for_filename_search = self.prefix_for_filename_search.replace("{{ ds_nodash }}",context['ds_nodash'])
        print('Looking at the following location for files...',self.prefix_for_filename_search)

        if 'test_' in self.target_file:
            return


        file_list = hook.list(self.target_gcs_bucket,maxResults=100,prefix=self.prefix_for_filename_search) #[:-2]) #self.prefix_for_filename_search[:-2])

        file_list_filtered = []
        for f in file_list:
            print(f)
            if '.ZIP' not in f:
                continue
            file_list_filtered.append(f)

        if self.get_file_list:
            print('Number of files found and reported to variable....',len(file_list_filtered))
            if len(file_list_filtered) != 0:
                Variable.set('experian_file_list', ','.join(file_list_filtered))
            else:
                Variable.set('experian_file_list', 'test_1,test_2')
            return

        if len(file_list) == 0:
            print('No files for processing...')
            Variable.set('experian_brick_present', 'False')
            total_row_count = 0
            self._upload_audit_results(total_row_count, context)
            return

        Variable.set('experian_brick_present', 'True')
        total_row_count = 0
        file_counter = 0
        for f in file_list:
            if f != self.target_file:
                continue
            print('Processing file start.....',f)
            row_count_return = self._process_file_from_staging(f,hook)
            print('Processing file end.....', f,row_count_return)
            total_row_count += row_count_return
            # if file_counter > 2:
            #     break
            file_counter += 1
            #break


        self._upload_audit_results(total_row_count,context)

        return

    def _get_gcs_file_audit(self,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_list = gcs_hook.list(self.target_gcs_bucket, prefix=self.audit_location)
        print('*' * 20)
        print(file_list)
        print(self.audit_location)
        print('*' * 20)

        for f in file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, f)
            json_data = json.loads(file_data)
            break
        return json_data

    def _upload_audit_results(self,row_count,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        file_list = gcs_hook.list(self.target_gcs_bucket,maxResults=10000,prefix=self.prefix_for_filename_search.replace('staging','l1'))
        total_row_count = 0
        for f in file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, object=f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df_temp = pd.read_json(file_stream, orient='records', lines='\n')
            total_row_count += len(df_temp)
            del df_temp


        metadata_json = self._get_gcs_file_audit(context)
        print(metadata_json)

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'file_count':metadata_json['file_count'],
                     'landing_file_count': metadata_json['landing_file_count'],
                     'landing_row_count' : total_row_count,
                     'dag_execution_date': context['ds'],
                     'audit_execution_date': audit_dt,
                     }

        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.transpose()
        if self.audit_location != 'NA':
            gcs_hook.upload(self.target_gcs_bucket,
                            self.audit_location + '/experian_audit.json',
                            df.to_json(orient='records', lines='\n'))
            #audit file for platform data
            ##gcs_hook.upload(self.target_gcs_bucket,
            ##                self.audit_location.replace('experian','experian_platform') + '/experian_audit.json',
            ##                df.to_json(orient='records', lines='\n'))
        return

    def _process_file_from_staging(self,object,gcs_hook):
        object_filename_base = object.replace('staging', 'l1').replace('.CSV.ZIP', '')
        file_data=gcs_hook.download(self.target_gcs_bucket,object)
        zipbytes = io.BytesIO(file_data)
        chunk_size = 25000 #50000 #50000 #500000
        i = 0
        counter = 0
        row_count = 0
        for data in pd.read_csv(zipbytes,encoding='ISO-8859-1',chunksize=chunk_size, sep='","', compression='zip', engine='python'):
            new_cols = []
            for col in data.columns:
                if col not in new_cols:
                    new_cols.append(
                        col.replace('.', '_').replace('-', '_').replace(',', '_').replace('(', '_').replace(')',
                                                                                                            '').replace(
                            '/', '_').replace(' ', '_').replace('"', ''))
                else:
                    i = i + 1
                    new_cols.append(
                        (col + str(i)).replace('.', '_').replace('-', '_').replace(',', '_').replace('(', '_').replace(
                            ')',
                            '').replace(
                            '/', '_').replace(' ', '_').replace('"', ''))
                data[col] = data[col].astype('str').replace('\.0', '', regex=True)
                data[col] = data[col].str.replace('"', '')

            data.columns = new_cols

            # del data['TAX_ID']
            # logging.info('uploading full file with tax_id')
            # full_object_filename = object_filename_base + '_{counter}.json'.format(counter=counter)
            # full_object_filename = full_object_filename.replace('experian','experian_full')
            # print('Object File name :',full_object_filename,len(data))
            # gcs_hook.upload(self.target_gcs_bucket, full_object_filename,data=data.to_json(orient='records',lines='\n'),filename=None,
            #        mime_type='application/octet-stream', gzip=False,
            #        multipart=None, num_retries=None)

            ##logging.info('uploading required columns with tax_id into platform project')

            #data_platform = pd.DataFrame()

            #required columns for platform

            ##try:
            ##    required_cols = Variable.get('platform_experian_cols').split(',')
            ##except:
            ##    Variable.set('platform_experian_cols', 'EXPERIAN_BUSINESS_ID,BUSINESS_NAME,ADDRESS,CITY,STATE,ZIP_CODE,ZIP_PLUS_4,COUNTY_NAME,PHONE_NUMBER,TAX_ID')
            ##    required_cols = Variable.get('platform_experian_cols').split(',')

            # required_cols = ['EXPERIAN_BUSINESS_ID'
            # ,'BUSINESS_NAME'
            # ,'ADDRESS'
            # ,'CITY'
            # ,'STATE'
            # ,'ZIP_CODE'
            # ,'ZIP_PLUS_4'
            # ,'COUNTY_NAME'
            # ,'PHONE_NUMBER'
            # ,'TAX_ID']

            #subset using list
            # for col in required_cols:
            #     data_platform[col] = data[col]
            ##object_filename_platform = object_filename_base + '_{counter}.json'.format(counter=counter)
            ##object_filename_platform = object_filename_platform.replace('experian','experian_platform')
            ##print('Object File name :', object_filename_platform, len(data))
            ##gcs_hook.upload(self.target_gcs_bucket, object_filename_platform, data=data[required_cols].to_json(orient='records', lines='\n'),
            ##                filename=None,
            ##                mime_type='application/octet-stream', gzip=False,
            ##                multipart=None, num_retries=None)

            #have list of columns to subset while uploading
            logging.info('uploading full files except tax_id')
            del data['TAX_ID']
            object_filename = object_filename_base + '_{counter}.json'.format(counter=counter)
            print('Object File name :', object_filename, len(data))
            gcs_hook.upload(self.target_gcs_bucket, object_filename, data=data.to_json(orient='records', lines='\n'),
                            filename=None,
                            mime_type='application/octet-stream', gzip=False,
                            multipart=None, num_retries=None)

            row_count += len(data)
            counter += 1
            del data
            #delete after testing
            #if counter >=1:
            ##   return row_count
            ##############
        return row_count


    #     new_audit_df = pd.DataFrame()
    #     audit_df = pd.DataFrame()
    #
    #     if '{ds_nodash}' in self.metadata_filename:
    #         self.metadata_filename = self.metadata_filename.format(ds_nodash=context['ds_nodash'])
    #
    #     if '{ds_nodash}' in self.audit_filename:
    #         self.audit_filename = self.audit_filename.format(ds_nodash=context['ds_nodash'])
    #
    #     metadata_file_data = hook.download(self.target_gcs_bucket, object=self.metadata_filename)
    #     metada_file_stream = io.BufferedReader(io.BytesIO(metadata_file_data))
    #     metadata_df = pd.read_json(metada_file_stream, orient='records', lines='\n')
    #     metadata_df = metadata_df.iloc[[-1]]
    #     metadata_df.reset_index(drop=True,inplace=True)
    #
    #     if self.check_landing_only:
    #         audit_result = 'PASSED'
    #         audit_reason = ''
    #
    #         if metadata_df.loc[0, self.source_count_label] != metadata_df.loc[0, self.l1_count_label]:
    #             audit_result = 'FAILED'
    #             audit_reason += 'Source Count did not match Landing Count;'
    #
    #         audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    #         json_data = {'source_count': metadata_df.loc[0, self.source_count_label],
    #                      'l1_count': metadata_df.loc[0, self.l1_count_label],
    #                      'dag_execution_date': context['ds'],
    #                      'audit_execution_date': audit_dt,
    #                      'audit_result': audit_result,
    #                      'audit_reason': audit_reason
    #                      }
    #         print(json_data)
    #         self._upload_audit_results(json_data)
    #
    #         if audit_result != 'PASSED':
    #             raise
    #
    #         return
    #
    #     file_exists = hook.exists(self.target_gcs_bucket, self.audit_filename)
    #     logging.info('file_exists value is ' + str(file_exists))
    #
    #     if file_exists:
    #         file_data = hook.download(self.target_gcs_bucket, object=self.audit_filename)
    #         file_stream = io.BufferedReader(io.BytesIO(file_data))
    #         audit_df = pd.read_json(file_stream, orient='records', lines='\n')
    #     else:
    #         print('Landing Audit did not post the file to the proper location.')
    #         print('Location of audit posting was: ',self.audit_filename)
    #         raise
    #
    #     bq_result = self._get_bq_audit_data(context)
    #     audit_result = 'PASSED'
    #     audit_reason = ''
    #
    #     if bq_result.loc[0,'count'] != audit_df.loc[0, self.l1_count_label]:
    #         audit_result = 'FAILED'
    #         audit_reason += 'BQ Count did not match Landing Count;'
    #
    #     audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    #     json_data = {'source_count': audit_df.loc[0, self.source_count_label],
    #                  'l1_count': audit_df.loc[0, self.l1_count_label],
    #                  'bq_count' : bq_result.loc[0,'count'],
    #                  'dag_execution_date': context['ds'],
    #                  'audit_execution_date': audit_dt,
    #                  'audit_result': audit_result,
    #                  'audit_reason': audit_reason
    #                  }
    #     print(json_data)
    #
    #     self._upload_audit_results(json_data)
    #
    #     if audit_result != 'PASSED':
    #         raise
    #
    #     return
    #
    #
    # def _upload_audit_results(self,json_data):
    #     gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
    #     df = pd.DataFrame.from_dict(json_data, orient='index')
    #     df = df.transpose()
    #     gcs_hook.upload(self.bucket,
    #                     self.audit_filename,
    #                     df.to_json(orient='records', lines='\n'))
    #     return
    #
    # def _get_bq_audit_data(self, context):
    #
    #     sql = 'SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
    #             project=self.project,
    #             dataset=self.dataset,
    #             table=self.table,
    #             date=context["ds"])
    #
    #     logging.info('audit bq sql: ' + sql)
    #     bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
    #     table_audit = bq_hook.get_pandas_df(sql=sql)
    #     return table_audit
    #