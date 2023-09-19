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
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_postgres import PostgresHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook
import io
import pandas as pd
import datetime as dt
import logging

class PostgresAuditOperator(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the Postgres table.
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
        for the table that was dumped from Postgres.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param sql_conn_id: Reference to a specific Postgres hook.
    :type sql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given Postgres Database and then upload it to the
        'Postgres-export' GCS bucket (along with a schema file). ::
            export_customers = PostgresToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='Postgres-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                sql_conn_id='Postgres_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket=None,
                 filename=None,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 gzip=False,
                 sql_conn_id='postgres_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 airflow_schema_var_set='NA',
                 google_cloud_bq_config={'project': 'NA'},
                 landed_filename=None,
                 check_landing_only=False,
                 *args,
                 **kwargs):

        super(PostgresAuditOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.gzip = gzip
        self.sql_conn_id = sql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_schema_var_set = airflow_schema_var_set
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.table_present = False
        self.landed_filename = landed_filename
        self.check_landing_only = check_landing_only

    def execute(self, context):
        if self.check_landing_only:
            self._landing_check(context)
            return


        # Pull row count for current extraction.
        #pull_source_result = self._query_postgres()

        # Extract the results from the pull to get the data.
        #result = pull_source_result.fetchall()

        # Get the row count for the table in Big Query
        table_audit_results = self._get_bq_audit_data(context)
        print(table_audit_results)

        # Get the file list and the total row count for the files that are loaded into GCS.
        file_list, record_count = self._get_gcs_file_audit(context)
        print(file_list, record_count)

        # Comparing
        # load_type = 'UNKNOWN'
        # if table_audit_results.loc[0, 'count']-result[0][0] == 0:
        #     load_type = 'FULL'

        audit_result = 'PASSED'
        audit_reason = ''
        if table_audit_results.loc[0, 'count'] != record_count:
            audit_result = 'FAILED'
            audit_reason += 'BQ count did not match Landing Count;'

        # if (table_audit_results.loc[0, 'count'] - record_count) != 0:
        #     audit_result = 'FAILED'
        #     audit_reason += 'BQ count did not match Source Count;'
        # if load_type == 'FULL':
        #     if record_count != result[0][0]:
        #         audit_result = 'FAILED'
        #         audit_reason += 'Landing count did not match Source Count;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'load_count': record_count,
                     'load_delta': table_audit_results.loc[0, 'count']-record_count,
                     'bq_loaded_record_count': table_audit_results.loc[0, 'count'],
                     'loaded_record_count': record_count,
                     'loaded_file_count': len(file_list),
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
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.bucket,
                        self.filename,
                        df.to_json(orient='records', lines='\n'))
        return

    def _get_gcs_file_audit(self, context):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        file_list = gcs_hook.list(self.bucket, prefix=self.landed_filename + '{date}/l1_data_{table}'.format(
            date=context['ds_nodash'],
            table=self.google_cloud_bq_config['table']))

        df_full_length = 0
        for f in file_list:
            file_data = gcs_hook.download(self.bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            df_full_length += len(df)
            del df
        return file_list, df_full_length

    def _query_postgres(self):
        """
        Queries Postgres and returns a cursor of results.
        :return: postgres cursor
        """
        postgres = PostgresHook(postgres_conn_id=self.sql_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def _get_bq_audit_data(self, context):

        print('SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                project=self.google_cloud_bq_config['project'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'],
                date=context["ds"]))

        bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
        try:
            table_audit = bq_hook.get_pandas_df(
            sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                project=self.google_cloud_bq_config['project'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'],
                date=context["ds"]))
            return table_audit
        except:
            logging.info('no table')
            df = pd.DataFrame()
            df.loc[0, 'count'] = 0
            return df

    def _landing_check(self, context):

        # Pull row count for an extraction.
        #pull_source_result = self._query_postgres()

        # Extract the results from the pull to get the data.
        #result = pull_source_result.fetchall()

        # Get the file list and the total row count for the files that are loaded into GCS.
        file_list, record_count = self._get_gcs_file_audit(context)

        # Comparing
        # load_type = 'UNKNOWN'
        # if record_count - result[0][0] == 0:
        #     load_type = 'FULL'

        audit_result = 'PASSED'
        audit_reason = ''
        # if load_type == 'FULL':
        #     if record_count != result[0][0]:
        #         audit_result = 'FAILED'
        #         audit_reason += 'Landing count did not match Source Count;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'load_count': record_count,
                     'loaded_record_count': record_count,
                     'loaded_file_count': len(file_list),
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
