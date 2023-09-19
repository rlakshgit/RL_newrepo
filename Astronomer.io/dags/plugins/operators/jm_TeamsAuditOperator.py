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
from plugins.operators.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
#from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook
import logging
import io
import pandas as pd
import datetime as dt

class TeamsAuditOperator(BaseOperator):
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


    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 project,
                 dataset,
                 table,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 schema_object=None,
                 base_gcs_folder=None,
                 bucket=None,
                 approx_max_file_size_bytes=1900000000,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 airflow_schema_var_set='NA',
                 landed_filename=None,
                 check_landing_only = False,
                 metadata_filename='NA',
                 audit_filename='NA',
                 source_count_label = 'source_count',
                 l1_count_label = 'l1_count',
                 *args,
                 **kwargs):

        super(TeamsAuditOperator, self).__init__(*args, **kwargs)

        self.bucket = bucket
        self.project = project
        self.dataset = dataset
        self.table = table
        self.source  = source
        self.base_gcs_folder = base_gcs_folder
        self.target_gcs_bucket = target_gcs_bucket
        self.source_abbr = source_abbr
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_schema_var_set = airflow_schema_var_set
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.table_present = False
        self.landed_filename = landed_filename
        self.check_landing_only = check_landing_only
        self.metadata_filename = metadata_filename
        self.audit_filename = audit_filename
        self.source_count_label = source_count_label
        self.l1_count_label = l1_count_label
        self.schema_object = schema_object




    def execute(self, context):

        if self.schema_object != None:
            gcs_hook = GCSHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            try:
                schema_fields = json.loads(gcs_hook.download(
                    self.bucket,
                    self.schema_object).decode("utf-8"))
                print(len(schema_fields))
                if len(schema_fields) == 0:
                    print('No schema fields detected for this run')
                    print('Not posting data to the table')
                    return
            except:
                print('No schema fields detected for this run')
                print('Not posting data to the table')
                return
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        new_audit_df = pd.DataFrame()
        audit_df = pd.DataFrame()

        if self.schema_object != None:
            gcs_hook = GCSHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            try:
                schema_fields = json.loads(gcs_hook.download(
                    self.bucket,
                    self.schema_object).decode("utf-8"))
                print(len(schema_fields))
                if len(schema_fields) == 0:
                    print('No schema fields detected for this run')
                    print('Not posting data to the table')
                    return
            except:
                print('No schema fields detected for this run')
                print('Not posting data to the table')
                return


        if '{ds_nodash}' in self.metadata_filename:
            self.metadata_filename = self.metadata_filename.format(ds_nodash=context['ds_nodash'])

        if '{ds_nodash}' in self.audit_filename:
            self.audit_filename = self.audit_filename.format(ds_nodash=context['ds_nodash'])

        metadata_file_data = hook.download(self.target_gcs_bucket, object=self.metadata_filename)
        metada_file_stream = io.BufferedReader(io.BytesIO(metadata_file_data))
        metadata_df = pd.read_json(metada_file_stream, orient='records', lines='\n')
        metadata_df = metadata_df.iloc[[-1]]
        metadata_df.reset_index(drop=True,inplace=True)

        if self.check_landing_only:
            audit_result = 'PASSED'
            audit_reason = ''

            if metadata_df.loc[0, self.source_count_label] != metadata_df.loc[0, self.l1_count_label]:
                audit_result = 'FAILED'
                audit_reason += 'Source Count did not match Landing Count;'

            audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
            json_data = {'source_count': metadata_df.loc[0, self.source_count_label],
                         'l1_count': metadata_df.loc[0, self.l1_count_label],
                         'dag_execution_date': context['ds'],
                         'audit_execution_date': audit_dt,
                         'audit_result': audit_result,
                         'audit_reason': audit_reason
                         }
            print(json_data)
            self._upload_audit_results(json_data)

            if audit_result != 'PASSED':
                raise

            return

        file_exists = hook.exists(self.target_gcs_bucket, self.audit_filename)
        logging.info('file_exists value is ' + str(file_exists))

        if file_exists:
            file_data = hook.download(self.target_gcs_bucket, object=self.audit_filename)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            audit_df = pd.read_json(file_stream, orient='records', lines='\n')
        else:
            print('Landing Audit did not post the file to the proper location.')
            print('Location of audit posting was: ',self.audit_filename)
            raise

        bq_result = self._get_bq_audit_data(context)

        if bq_result.empty:
            print('No Data Table present....',self.audit_filename)
            return

        audit_result = 'PASSED'
        audit_reason = ''

        if bq_result.loc[0,'count'] != audit_df.loc[0, self.l1_count_label]:
            audit_result = 'FAILED'
            audit_reason += 'BQ Count did not match Landing Count;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'source_count': audit_df.loc[0, self.source_count_label],
                     'l1_count': audit_df.loc[0, self.l1_count_label],
                     'bq_count' : bq_result.loc[0,'count'],
                     'dag_execution_date': context['ds'],
                     'audit_execution_date': audit_dt,
                     'audit_result': audit_result,
                     'audit_reason': audit_reason
                     }
        print(json_data)

        self._upload_audit_results(json_data)

        if audit_result != 'PASSED':
            raise

        return


    def _upload_audit_results(self,json_data):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.bucket,
                        self.audit_filename,
                        df.to_json(orient='records', lines='\n'))
        return

    def _get_bq_audit_data(self, context):

        try:
            sql = '''SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` '''.format(
                    project=self.project,
                    dataset=self.dataset,
                    table=self.table,
                    date=context["ds"])

            logging.info('audit bq sql: ' + sql)
            bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
            table_list = bq_hook.get_dataset_tables(self.dataset,self.project,max_results=1000)
            print(table_list)
            table_audit = bq_hook.get_pandas_df(sql=sql)


            return table_audit
        except:
            return pd.DataFrame()
