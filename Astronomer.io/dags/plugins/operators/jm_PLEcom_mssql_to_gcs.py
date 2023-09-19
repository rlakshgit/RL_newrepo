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
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.gcs_hook import GCSHook
from plugins.operators.jm_gcs import GCSHook
# from airflow.hooks.mssql_hook import MsSqlHook
from plugins.hooks.jm_mssql import MsSqlHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.base import BaseHook
import pandas as pd




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

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
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
                 delegate_to=None,
                 airflow_var_set='NA',
                 airflow_schema_var_set='NA',
                 change_tracking_check=False,
                 primary_key_check=False,
                 google_cloud_bq_config={'project': 'NA', 'db': 'NA'},
                 audit_filename=None,
                 metadata_filename=None,
                 table=None,
                 get_primary_key=False,
                 *args,
                 **kwargs):

        super(MsSqlToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
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
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.audit_filename = audit_filename
        self.table_present = False
        self.get_primary_key = get_primary_key
        self.metadata_filename = metadata_filename

    def execute(self, context):

        if self.get_primary_key:
            primary_key = self._get_primary_key()
            return primary_key

        full_pull = True
        print('change tracking option....', self.change_tracking_check)

        if self.google_cloud_bq_config['project'] != 'NA':
            bq_hook = BigQueryHook(self.google_cloud_bq_conn_id)
            print('*' * 20)
            print(self.google_cloud_bq_config['project'], self.google_cloud_bq_config['dataset'],
                  self.google_cloud_bq_config['table'])
            print('*' * 20)
            self.table_present = bq_hook.table_exists(project_id = self.google_cloud_bq_config['project'],
                                                      dataset_id = self.google_cloud_bq_config['dataset'],
                                                      table_id = self.google_cloud_bq_config['table'])

        print('Table present check....', self.table_present)
        if self.table_present:
            if self.primary_key_check:
                primary_key = self._get_primary_key()
                print('Primary key......', primary_key)
                if primary_key == 'None':
                    full_pull = True
                elif primary_key == 'NA':
                    print('Failed to find primary key')
                    raise
                else:
                    full_pull = False

            if self.change_tracking_check:
                sql_temp = self.sql.replace('''SELECT *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [''',
                                            '').replace('].[', '.').replace('];', '')
                sql_temp_split = sql_temp.split('.')                
                prev_ds = str(context['prev_start_date_success'])
                prev_ds = prev_ds.split(' ')
                print(prev_ds)
                center1 = sql_temp_split[0]

                ct_sql = ''' USE {center}
                select B.*, D.SYS_CHANGE_OPERATION as SYS_CHANGE_OPERATION, D.CT_ID as CT_ID from [{center}].[{dataset}].[{table}] B
                                                        RIGHT OUTER JOIN (
                                                        select  distinct {pkey} as CT_ID, SYS_CHANGE_OPERATION
                                                        FROM CHANGETABLE(CHANGES [{center}].[{dataset}].[{table}], 0) AS CT
                                                        INNER JOIN sys.dm_tran_commit_table
                                                        ON CT.sys_change_version = commit_ts
                                                        WHERE cast(commit_time as date) >= '{date}' and cast(commit_time as date) <= '{next_date}'  )D
                                                        on B.{pkey} = D.CT_ID'''.format(
                    center=sql_temp_split[0], dataset=sql_temp_split[1],
                    table=sql_temp_split[2],
                    pkey=primary_key,
                    date=prev_ds[0],
                    next_date=context['next_ds'])

                print('ct_sql')
                print(ct_sql)

                if (self.table == 't_SavedQuote'):
                    sqlquery1 = '''SELECT top 1 SavedQuote_ID,GoogleAnalyticsClientID,PostalCode,County,State,CountryID,cast(Timestamp as datetime2(7)) as Timestamp,DistributionSourceId,DistributionSourceSubTypeId,AgencyExpressPartner_ID,UserId,ApplicationSourceId,GuidewireProducerCode,ReferringJewelerId,ReferralCode,ExternalApplicationKey,Address1,Address2,City,FirstName,LastName,PhoneNumber,EmailAddress,SessionId,JewelerProgramName,OrderNumber,Obfuscated, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [{center}].[{dataset}].[{table}]'''.format(
                    center=center1[-6:], dataset=sql_temp_split[1],
                    table=sql_temp_split[2])
                    
                    sqlquery2 = '''SELECT top 1  *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [{center}].[{dataset}].[{table}]'''.format(
                    center=center1[-6:], dataset=sql_temp_split[1],
                    table=sql_temp_split[2])
                    result1 = self._query_mssql(sql=sqlquery1, database=center1[-6:])
                    data_result1 = result1.fetchall()
                    data_result11 = [item for t in data_result1 for item in t]
                    print(data_result11)
                    print(len(data_result11))
                    
                    result2 = self._query_mssql(sql=sqlquery2, database=center1[-6:])
                    data_result2 = result2.fetchall()
                    data_result22 = [item for t in data_result2 for item in t]
                    print(data_result22)
                    print(len(data_result22))
                    

                    if len(data_result11) != len(data_result22):
                        Variable.set('send_alert', 'True')
                    else:
                        Variable.set('send_alert', 'False')
                    

                try:
                    cursor = self._query_mssql(sql=ct_sql, database=sql_temp_split[0])
                    print('Completed data pull based on Change Tracking...')
                    full_pull = False
                except:
                    full_pull = True
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
            sql_temp = self.sql.replace('''SELECT *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [''', '').replace(
                '].[', '.').replace('];', '')
            sql_temp_split = sql_temp.split('.')

            schema_sql = '''SELECT COLUMN_NAME, DATA_TYPE 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = '{table}'
                            and TABLE_SCHEMA = '{schema}'
                            union 
                            select 'SYS_CHANGE_OPERATION' as COLUMN_NAME, 'varchar' as DATA_TYPE
                            union
                            select 'CT_ID' as COLUMN_NAME, 'bigint' as DATA_TYPE;''' \
                .format(table=self.google_cloud_bq_config['table'],
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
        sql_temp = self.sql.replace('''SELECT *, 'I' as SYS_CHANGE_OPERATION, '' as CT_ID FROM [''', '').replace('].[',
                                                                                                                 '.').replace(
            '];', '')
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
            # print(self.sql)
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

            try:
                row = map(self.convert_types, row)
                row_dict = dict(zip(schema, row))
                s = json.dumps(row_dict, sort_keys=True, default=str)
                s = s.encode('utf-8')
            except:
                print(row)
                raise

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