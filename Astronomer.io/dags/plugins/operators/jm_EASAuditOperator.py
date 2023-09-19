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
#from airflow.hooks.mssql_hook import MsSqlHook
from plugins.hooks.jm_mssql import MsSqlHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
#from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook
import io
import pandas as pd
import datetime as dt

class EASAuditOperator(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
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
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 sql=None,
                 bucket=None,
                 filename=None,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 gzip=False,
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 airflow_schema_var_set='NA',
                 change_tracking_check=True,
                 primary_key_check=True,
                 google_cloud_bq_config={'project': 'NA'},
                 metadata_filename=None,
                 landing_file_location=None,
                 check_landing_only = False,
                 *args,
                 **kwargs):

        super(EASAuditOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.gzip = gzip
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_schema_var_set = airflow_schema_var_set
        self.change_tracking_check = change_tracking_check
        self.primary_key_check = primary_key_check
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.table_present = False
        self.metadata_filename = metadata_filename
        self.landing_file_location = landing_file_location
        self.check_landing_only = check_landing_only



    def execute(self, context):

        if "{{ ds_nodash }}" in self.metadata_filename:
            self.metadata_filename = self.metadata_filename.replace("{{ ds_nodash }}", context['ds_nodash'])

        if "{{ ds_nodash }}" in self.landing_file_location:
            self.landing_file_location = self.landing_file_location.replace("{{ ds_nodash }}", context['ds_nodash'])

        if self.check_landing_only:
            self._landing_check(context)
            return

        metadata_json = self._get_gcs_file_audit(context)
        print(metadata_json)

        audit_result = 'PASSED'
        audit_reason = ''
        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        table_audit_results = self._get_bq_audit_data(context)
        if table_audit_results.loc[0, 'count'] != metadata_json['l1_row_count']:
            audit_result = 'FAILED'
            audit_reason += 'BQ count did not match Landing Count;'

        json_data = {'file_count': metadata_json['file_count'],
                     'l1_row_count': metadata_json['l1_row_count'],
                     'audit_l1_file_count': metadata_json['audit_l1_file_count'],
                     'audit_l1_row_count': metadata_json['audit_l1_row_count'],
                     'audit_bq_row_count': table_audit_results.loc[0, 'count'],
                     'dag_execution_date': context['ds'],
                     'audit_execution_date': audit_dt,
                     'audit_result': audit_result,
                     'audit_reason': audit_reason
                     }


        print(json_data)

        # raise
        self._upload_audit_results(json_data)

        if audit_result != 'PASSED':
            raise
        return

    def _landing_audit_result(self):

        return

    def _upload_audit_results(self,json_data):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.bucket,
                        self.metadata_filename + '/eas_metadata.json',
                        df.to_json(orient='records', lines='\n'))
        return

    def _get_gcs_file_audit(self,context):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        file_list = gcs_hook.list(self.bucket, prefix=self.metadata_filename)
        if len(file_list) == 0:
            return None
        for f in file_list:
            file_data = gcs_hook.download(self.bucket, f)
            json_data = json.loads(file_data)
            break
        return json_data

    def _get_bq_audit_data(self,context):


        # print('SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}`'.format(
        #         project=self.google_cloud_bq_config['project'],
        #         sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
        #         dataset=self.google_cloud_bq_config['dataset'],
        #         table=self.google_cloud_bq_config['table'],
        #         date=context["ds"]))

        bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
        # sql = 'SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}`'.format(
        #     project=self.google_cloud_bq_config['project'],
        #     sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
        #     dataset=self.google_cloud_bq_config['dataset'],
        #     table=self.google_cloud_bq_config['table']))



        table_audit = bq_hook.get_pandas_df(
            sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                project=self.google_cloud_bq_config['project'],
                sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'],
                date=context["ds"]))
        return table_audit

    def _landing_check(self,context):

        metadata_json = self._get_gcs_file_audit(context)
        print(metadata_json)
        if metadata_json == None:
            print('No Metadata to process')
            raise

        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)


        file_list = []
        base_file_list = gcs_hook.list(self.bucket, versions=True, maxResults=1000, prefix=self.landing_file_location,
                                       delimiter=',')
        for f in base_file_list:
            if ('.json' not in f):
                print('Skipping file....', f)
                continue
            file_list.append(f)

        row_count = 0
        for f in file_list:
            file_data = gcs_hook.download(self.bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream,orient="records",lines='\n')
            row_count += len(df)

        audit_result = 'PASSED'
        audit_reason = ''
        if int(metadata_json['file_count']) != int(len(file_list)):
            audit_result = 'FAILED'
            audit_reason += 'Landing file count did not match L1 Norm Count;'

        if int(metadata_json['l1_row_count']) != int(row_count):
            audit_result = 'FAILED'
            audit_reason += 'Landing row count did not match L1 Norm Count;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'file_count' : metadata_json['file_count'],
                     'l1_row_count': metadata_json['l1_row_count'],
                     'audit_l1_file_count': int(len(file_list)),
                     'audit_l1_row_count': int(row_count),
                     'dag_execution_date': context['ds'],
                     'audit_execution_date': audit_dt,
                     'audit_result': audit_result,
                     'audit_reason': audit_reason
                     }
        print(json_data)
        if audit_result != 'PASSED':
            raise
        self._upload_audit_results(json_data)


        return
