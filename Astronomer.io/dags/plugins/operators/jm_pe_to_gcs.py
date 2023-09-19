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
import sys

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.gcs_hook import GCSHook
from plugins.operators.jm_gcs import GCSHook
# from airflow.hooks.mssql_hook import MsSqlHook
from plugins.hooks.jm_mssql import MsSqlHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
import pandas as pd
import numpy as np


class MsSqlToGoogleCloudStorageOperator(BaseOperator):
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

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename','metadata_filename','audit_filename', 'target_date','target_date_nodash')
    template_ext = ('.sql',)
    ui_color = '#008b02'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=500000000,  # 5000,#5000000,
                 gzip=False,
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_cloud_bq_conn_id='google_cloud_deafult',
                 delegate_to=None,
                 airflow_var_set='NA',
                 airflow_schema_var_set='NA',
                 change_tracking_check=False,
                 primary_key_check=False,
                 google_cloud_bq_config={'project': 'NA', 'db': 'NA'},
                 audit_filename=None,
                 metadata_filename=None,
                 column_name=None,
                 target_date=None,
                 target_date_nodash=None,
                 *args,
                 **kwargs):

        super(MsSqlToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.gzip = gzip
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_var_set = airflow_var_set
        self.airflow_schema_var_set = airflow_schema_var_set
        self.change_tracking_check = change_tracking_check
        self.primary_key_check = primary_key_check
        self.google_cloud_bq_conn_id = google_cloud_bq_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.audit_filename = audit_filename
        self.table_present = False
        self.metadata_filename = metadata_filename
        self.column_name = column_name
        self.target_date = target_date
        self.target_date_nodash = target_date_nodash

    def execute(self, context):
        full_pull = True

        if self.google_cloud_bq_config['project'] != 'NA':
            bq_hook = BigQueryHook(self.google_cloud_bq_conn_id)
            self.table_present = bq_hook.table_exists(project_id = self.google_cloud_bq_config['target_project'],
                                                      dataset_id = self.google_cloud_bq_config['dataset'],
                                                      table_id = self.google_cloud_bq_config['table'].replace('$',''))
        msg = 'table presence:' + str(self.table_present)
        print(msg)
        if self.table_present:
            bq_hook = BigQueryHook(self.google_cloud_bq_conn_id)
            schema = bq_hook.get_schema(self.google_cloud_bq_config['dataset'], self.google_cloud_bq_config['table'].replace('$',''))
            schema=schema['fields']
            print('schema:')
            print(schema)
            schema=pd.DataFrame(schema)
            if self.column_name in schema.name.values:
                bq_hook = JMBQHook(self.google_cloud_bq_conn_id)

                sql = 'SELECT max({columnname}) as maxdate FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) <= "{date}"'.format(
                project=self.google_cloud_bq_config['target_project'],
                sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                dataset=self.google_cloud_bq_config['dataset'],
                table=self.google_cloud_bq_config['table'].replace('$', ''),
                schema=self.google_cloud_bq_config['schema'],
                columnname=self.column_name,
                date=context['ds'])
                print(sql)
                start_date = bq_hook.get_data(sql)
                start_date = start_date.iloc[0,0]
                print(start_date)

                #if start_date == np.datetime64('NaT'):
                if str(start_date) == 'NaT':
                    full_pull = True

                else:
                    end_date = context['next_ds'] + ' 23:59:59'

                    sql_temp = self.sql.replace('SELECT * FROM [', '').replace('].[', '.').replace('];', '')
                    sql_temp_split = sql_temp.split('.')
                    ct_sql = ''' Select * from [{center}].[{dataset}].[{table}] 
                          where {columname}  >  cast('{startdate}' as datetime2) and {columname}  <= convert(DATETIME, '{enddate}') '''.format(center=sql_temp_split[0], dataset=sql_temp_split[1],
                                                                 table=sql_temp_split[2],
                                                                 columname=self.column_name,
                                                                 startdate=start_date,
                                                                 enddate=end_date)
                    print(ct_sql)
                    cursor = self._query_mssql(sql=ct_sql, database=sql_temp_split[0])
                    print('Completed data pull based on Change Tracking...')
                    full_pull = False
            else:
                full_pull = True


        else:
            full_pull = True

        print('Full pull option....', full_pull)
        if full_pull:
            print('Completing a full data pull..')
            cursor = self._query_mssql(database=self.google_cloud_bq_config['db'])

        if self.airflow_var_set != 'NA':
            val = cursor.fetchall()
            table_string = ', '.join([x[0] for x in val])
            Variable.set(self.airflow_var_set, table_string)
            return

        if self.metadata_filename != None:
            files_to_upload, row_count = self._write_local_data_files_row_count(cursor)
            self._metadata_upload(context, row_count, full_pull)
            files_to_upload = {}

        else:
            files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            # print(self.sql)
            sql_temp = self.sql.replace('SELECT * FROM [', '').replace('].[', '.').replace('];', '')
            sql_temp_split = sql_temp.split('.')

            schema_sql = '''SELECT COLUMN_NAME, DATA_TYPE 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = '{table}'
                            and TABLE_SCHEMA = '{schema}';'''.format(table=self.google_cloud_bq_config['table'],
                                                                     schema=sql_temp_split[1])
            cursor = self._query_mssql(sql=schema_sql, database=self.google_cloud_bq_config['db'])
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for file_handle in files_to_upload.values():
            print(file_handle)
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles
        for file_handle in files_to_upload.values():
            file_handle.close()

    def _get_primary_key(self):
        sql_temp = self.sql.replace('SELECT * FROM [', '').replace('].[', '.').replace('];', '')
        sql_temp_split = sql_temp.split('.')
        sql = '''SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
                           FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                           INNER JOIN
                               INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                                     ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                                        TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                                        KU.table_name = '{table}'
                           ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;'''.format(table=sql_temp_split[2])

        result = self._query_mssql(sql=sql, database=sql_temp_split[0])
        data_result = result.fetchall()
        pkey = 'NA'
        if len(data_result) == 1:
            print('Primary key found for the table....')
            pkey = data_result[0][1]
        else:
            print('No primary key found and defaulting to full pull')
            pkey = 'None'
        return pkey

    def _metadata_upload(self, context, row_count, full_pull):
        if full_pull:
            load_type = 'FULL_PULL'
        else:
            load_type = 'CT_PULL'
        json_metadata = {
            'row_count': row_count,
            'pull_type': full_pull,
            'load_type': load_type,
            'dag_execution_date': context['ds']
        }

        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.bucket,
                        self.metadata_filename.format(ds_nodash=context['ds_nodash']),
                        df.to_json(orient='records', lines='\n'))
        return

    def _query_mssql(self, sql='NA', database='NA'):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn(database=database)
        cursor = conn.cursor()
        if sql == 'NA':
            cursor.execute(self.sql)
            print(self.sql)
        else:
            cursor.execute(sql)
            # print(sql)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.
        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}
        for row in cursor:
            # Convert if needed
            row = map(self.convert_types, row)
            row_dict = dict(zip(schema, row))

            s = json.dumps(row_dict, sort_keys=True, default=str)
            s = s.encode('utf-8')
            tmp_file_handle.write(s)

            # Append newline to make dumps BQ compatible
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

    def _write_local_data_files_row_count(self, cursor):
        """
        Takes a cursor, and writes results to a local file.
        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}
        row_counter = 0
        tmp_file_row_count = 0

        for row in cursor:
            # Convert if needed
            row_counter += 1
            tmp_file_row_count += 1
            row = map(self.convert_types, row)
            row_dict = dict(zip(schema, row))
            s = json.dumps(row_dict, sort_keys=True, default=str)
            s = s.encode('utf-8')
            tmp_file_handle.write(s)
            # # Append newline to make dumps BQ compatible
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                tmp_file_handle.seek(0)
                self._upload_to_gcs_oneoff(tmp_file_handle, self.filename.format(file_no))
                tmp_file_row_count = 0
                tmp_file_handle.flush()
                tmp_file_handle.close()
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)

        tmp_file_handle.seek(0)
        self._upload_to_gcs_oneoff(tmp_file_handle, self.filename.format(file_no))
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}
        return tmp_file_handles, row_counter

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.
        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = []
        for field in cursor.fetchall():
            # See PEP 249 for details about the description tuple.
            field_name = field[0].replace(' ', '_')  # Clean spaces
            field_type = self.type_map(field[1])
            field_mode = 'NULLABLE'  # pymssql doesn't support field_mode
            # print(field_name, field[1],field_type)

            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': field_mode,
            })

        self.log.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        s = json.dumps(schema, sort_keys=True)
        if self.airflow_schema_var_set != 'NA':
            Variable.set(self.airflow_var_set, s)
        s = s.encode('utf-8')
        tmp_schema_file_handle.write(s)
        return {self.schema_filename: tmp_schema_file_handle}

    def _write_local_audit_file(self, audit_data):
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        s = json.dumps(audit_data, sort_keys=True)
        if self.airflow_schema_var_set != 'NA':
            Variable.set(self.airflow_var_set, s)
        s = s.encode('utf-8')
        tmp_schema_file_handle.write(s)
        return {self.audit_filename: tmp_schema_file_handle}

    def _upload_to_gcs_oneoff(self, tmp_file_handle, object_name, df_enable=False, df=pd.DataFrame()):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        if df_enable:
            hook.upload(self.bucket, object_name, df.to_json(orient='records', lines='\n', date_format='iso'), 'NA',
                        'application/json',
                        (self.gzip if object_name != self.schema_filename else False))

        else:
            hook.upload(self.bucket, object_name, 'NA', tmp_file_handle.name, 'application/json',
                        (self.gzip if object_name != self.schema_filename else False))

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object_name, tmp_file_handle in files_to_upload.items():
            print(object_name, tmp_file_handle.name)
            hook.upload(self.bucket, object_name, 'NA', tmp_file_handle.name, 'application/json',
                        (self.gzip if object_name != self.schema_filename else False))

    @classmethod
    def convert_types(cls, value):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        """
        if isinstance(value, decimal.Decimal):
            return float(value)
        else:
            return value

    @classmethod
    def type_map(cls, mssql_type):
        """
        Helper function that maps from MSSQL fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            'int': 'INTEGER',
            'bit': 'BOOLEAN',
            'bigint': 'INTEGER',
            'smallint': 'INTEGER',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'decimal': 'NUMERIC',
            'money': 'NUMERIC',
            'smallmoney': 'NUMERIC',
        }
        return d[mssql_type] if mssql_type in d else 'STRING'