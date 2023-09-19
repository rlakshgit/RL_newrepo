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
"""
MsSQL to GCS operator.
"""

import decimal
import json
import datetime
import pandas as pd
from io import StringIO
from tempfile import NamedTemporaryFile
from airflow.models import BaseOperator
from plugins.hooks.jm_mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook


class TrianglesSQLServerToGCSOperator(BaseOperator):
    """Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON or CSV format.

    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str

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
    ui_color = '#e0a98c'
    template_ext = ('.sql',)
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename', 'metadata_filename')

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename,
                 metadata_filename,
                 approx_max_file_size_bytes=1900000000,
                 export_format='json',
                 field_delimiter=',',
                 gzip=False,
                 schema=None,
                 parameters=None,
                 gcp_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id=None,
                 delegate_to=None,
                 mssql_conn_id='mssql_default',
                 *args,
                 **kwargs):
        super(TrianglesSQLServerToGCSOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.metadata_filename = metadata_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.export_format = export_format.lower()
        self.field_delimiter = field_delimiter
        self.gzip = gzip
        self.schema = schema
        self.parameters = parameters
        self.google_cloud_storage_conn_id=google_cloud_storage_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters
        self.mssql_conn_id = mssql_conn_id

    type_map = {
        2: 'DATE',
        3: 'INTEGER',
        4: 'TIMESTAMP',
        5: 'NUMERIC'
    }

    def execute(self,context):
        cursor = self.query()
        files_to_upload, source_count = self._write_local_data_files(cursor)

        if self.schema_filename:
            files_to_upload.append(self._write_local_schema_file(cursor))
            
        print('*'*100)
        print("cursor: ", cursor.description)
        print(source_count)
        print("files_to_upload",files_to_upload)
        print("No of files_to_upload",len(files_to_upload))
        print('*'*100)

        # Flush all files before uploading
        for tmp_file in files_to_upload:
            tmp_file['file_handle'].flush()

        self._upload_to_gcs(files_to_upload)

        #get L1's row_count for auditing
        row_count = 0
        for tmp_file in files_to_upload:
            tmp_file_name = tmp_file.get('file_name')
            if tmp_file_name == self.filename:
                tmp_file_handle = tmp_file.get('file_handle')
                tmp_file_handle.seek(0)
                l1_data = tmp_file_handle.readlines()
                row_count += len(l1_data)
            tmp_file['file_handle'].close()
        
        self._metadata_upload(context, source_count, row_count)


    def query(self):
        """
        Queries MSSQL and returns a cursor of results.

        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def _metadata_upload(self, context, source_count, row_count):
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }
      
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()

        gcs_hook.upload(self.bucket,
                        self.metadata_filename,
                        df.to_json(orient='records', lines='\n'))         
        return


    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)
        for tmp_file in files_to_upload:
            hook.upload(self.bucket, tmp_file.get('file_name'), 'NA',
                        tmp_file.get('file_handle').name,
                        mime_type=tmp_file.get('file_mime_type'),
                        gzip=self.gzip if tmp_file.get('file_name') == self.schema_filename else False)

    def _write_local_data_files(self,cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        col_type_dict = self._get_col_type_dict()
        tmp_file_handle = NamedTemporaryFile(delete=True)
  
        files_to_upload = [{
            'file_name': self.filename,
            'file_handle': tmp_file_handle,
            'file_mime_type': 'application/json'
        }]
        
        row_counter = 0
        for row in cursor:
            # Convert datetime objects to utc seconds, and decimals to floats.
            # Convert binary type object to string encoded with base64.
            row = map(self.convert_type, row)
            row_counter += 1
            def myconverter(o):
                if isinstance(o, datetime.datetime):
                    return datetime.datetime.strftime(o,'%Y-%m-%d')
                    
            # Reverse order of columns as importing data to BQ 
            row_dict = dict(zip(schema, row))
            row_dict = dict(reversed(list(row_dict.items())))

            # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
            tmp_file_handle.write(json.dumps(row_dict,default=myconverter).encode('utf-8'))

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                tmp_file_handle = NamedTemporaryFile(delete=True)
                files_to_upload.append({
                    'file_name': self.filename,
                    'file_handle': tmp_file_handle,
                    'file_mime_type': 'application/json'
                })

        return files_to_upload, row_counter
    
    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = [self.field_to_bigquery(field) for field in cursor.description]

        self.log.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        tmp_schema_file_handle.write(json.dumps(schema).encode('utf-8'))
        schema_file_to_upload = {
            'file_name': self.schema_filename,
            'file_handle': tmp_schema_file_handle,
            'file_mime_type': 'application/json',
        }
        return schema_file_to_upload
    
    def field_to_bigquery(self, field):
        _field = {
            'name': field[0].replace(" ", "_"),
            'type': self.type_map.get(field[1], "STRING"),
            'mode': "NULLABLE",
        }
        return _field

    @classmethod
    def convert_type(cls, value):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        """
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value
        


    def _get_col_type_dict(self):
        """
        Return a dict of column name and column type based on self.schema if not None.
        """
        schema = []
        if isinstance(self.schema, str):
            schema = json.loads(self.schema)
        elif isinstance(self.schema, list):
            schema = self.schema
        elif self.schema is not None:
            self.log.warning('Using default schema due to unexpected type.'
                             'Should be a string or list.')

        col_type_dict = {}
        try:
            col_type_dict = {col['name']: col['type'] for col in schema}
        except KeyError:
            self.log.warning('Using default schema due to missing name or type. Please '
                             'refer to: https://cloud.google.com/bigquery/docs/schemas'
                             '#specifying_a_json_schema_file')
        return col_type_dict

    