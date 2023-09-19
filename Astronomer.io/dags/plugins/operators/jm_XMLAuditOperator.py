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
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook
import pandas as pd
import datetime as dt

class XMLAuditOperator(BaseOperator):
    """
    Do comparison check on XML data in  Google Cloud Storage
    to BigQuery.
    :param sql: The SQL to execute on the BigQuery table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param google_cloud_bigquery_conn_id: Reference to a specific Google
        cloud BigQuery hook.
    :type google_cloud_bigquery_conn_id: str
    """

    template_fields = ('bucket', 'filename', 'l1_prefix')
    template_ext = ('.sql',)
    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 bucket=None,
                 filename=None,
                 approx_max_file_size_bytes=1900000000,
                 google_cloud_bigquery_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_cloud_bq_config={'project': 'NA'},
                 bq_count_field='',
                 l1_prefix='NA',
                 *args,
                 **kwargs):

        super(XMLAuditOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.filename = filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.google_cloud_bigquery_conn_id = google_cloud_bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.table_present = False
        self.l1_prefix = l1_prefix
        self.bq_count_field = bq_count_field

    def execute(self, context):

        # Get the row count for the table in Big Query
        table_audit_results = self._get_bq_audit_data(context)
        print(table_audit_results)
        print(self.l1_prefix)

        # Get the file list and the total row count for the files that are loaded into GCS.
        file_list = self._get_gcs_file_audit(context, self.l1_prefix)
        print(file_list)
        file_count = len(file_list)

        # Comparing

        audit_result = 'PASSED'
        audit_reason = ''
        if table_audit_results.loc[0, 'count'] != file_count:
            audit_result = 'FAILED'
            audit_reason += 'BQ count did not match Landing count;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'landing_count': file_count,
                     'bq_loaded_record_count': table_audit_results.loc[0, 'count'],
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

    def _upload_audit_results(self, json_data):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.bucket,
                        self.filename,
                        df.to_json(orient='records', lines='\n'))
        return

    def _get_gcs_file_audit(self, context, prefix):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_list = gcs_hook.list(self.bucket, prefix=prefix)
        return file_list

    def _get_bq_audit_data(self, context):

        print('SELECT COUNT(DISTINCT {target_column}) as count FROM `{project}.{dataset}.{table}` WHERE data_run_date = "{date}"'.format(
                project=self.google_cloud_bq_config['project'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'],
                date=context["ds"],
                target_column=self.bq_count_field))

        bq_hook = BigQueryHook(self.google_cloud_bigquery_conn_id, use_legacy_sql=False)
        try:
            table_audit = bq_hook.get_pandas_df(
                sql='SELECT COUNT(DISTINCT {target_column}) as count FROM `{project}.{dataset}.{table}` WHERE data_run_date = "{date}"'.format(
                project=self.google_cloud_bq_config['project'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'],
                date=context["ds"],
                target_column=self.bq_count_field))
        except:
            table_audit = pd.DataFrame([[0]], columns=['count'])

        return table_audit

