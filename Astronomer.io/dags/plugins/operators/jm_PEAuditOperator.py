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
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
from plugins.hooks.jm_bq_hook import BigQueryHook
import io
import pandas as pd
import datetime as dt
import numpy as np


class SQLAuditOperator(BaseOperator):
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

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename', 'target_date', 'target_date_nodash')
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
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_cloud_bq_conn_id='google_cloud_default',
                 delegate_to=None,
                 airflow_schema_var_set='NA',
                 change_tracking_check=True,
                 primary_key_check=True,
                 google_cloud_bq_config={'project': 'NA'},
                 landed_filename=None,
                 check_landing_only = False,
                 column_name=None,
                 target_date=None,
                 target_date_nodash=None,
                 non_partitioned_tables=[],
                 *args,
                 **kwargs):

        super(SQLAuditOperator, self).__init__(*args, **kwargs)
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
        self.google_cloud_bq_conn_id = google_cloud_bq_conn_id
        self.google_cloud_bq_config = google_cloud_bq_config
        self.table_present = False
        self.landed_filename = landed_filename
        self.check_landing_only = check_landing_only
        self.column_name = column_name
        self.target_date = target_date
        self.target_date_nodash = target_date_nodash
        self.non_partitioned_tables = non_partitioned_tables

    def execute(self, context):
        if self.check_landing_only:
            self._landing_check(context)
            return


        #Get primary key for the target table
        primary_key = self._return_primary_key()
        #Get the row count for a change tracking pull if available for the target table
        ct_source_result = self._return_change_tracking(context)
        if self.change_tracking_check:
            if ct_source_result == 0:
                ct_len = 0
            else:
                ct_len = ct_source_result
        else:
            ct_len = 0

        #Pull row count for a full extraction.
        full_pull_source_result = self._query_mssql(database=self.google_cloud_bq_config['db'])
        #Extract the results from the full pull to get the data.
        result_full = full_pull_source_result.fetchall()

        #Get the row count for the table in Big Query
        table_audit_results = self._get_bq_audit_data(context)
        #Get the file list and the total row count for the files that are loaded into GCS.
        metadata_json = self._get_gcs_file_audit(context)

        audit_result = 'PASSED'
        audit_reason = ''
        if table_audit_results.loc[0,'count'] != metadata_json['row_count']:
            audit_result = 'FAILED'
            audit_reason += 'BQ count did not match Landing Count;'
        if ((table_audit_results.loc[0,'count']-result_full[0][0]) != 0) and ((table_audit_results.loc[0,'count']-ct_len) != 0):
            audit_result = 'FAILED'
            audit_reason += 'BQ count did not match Source Count;'
        if metadata_json['load_type'] == 'FULL_PULL':
            if metadata_json['row_count'] != result_full[0][0]:
                audit_result = 'FAILED'
                audit_reason += 'Landing count did not match Source Count;'
        if metadata_json['load_type'] == 'CT_PULL':
            if metadata_json['row_count'] != ct_len:
                audit_result = 'FAILED'
                audit_reason += 'Landing count did not match Source Count;'
        if metadata_json['load_type'] == 'UNKNOWN':
            audit_result = 'FAILED'
            audit_reason += 'Load Type Unknown;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'full_load_count': result_full[0][0],
                     'ct_load_count': ct_len,
                     'full_load_delta' : table_audit_results.loc[0,'count']-result_full[0][0],
                     'ct_load_delta' : table_audit_results.loc[0,'count']-ct_len,
                     'load_type' : metadata_json['load_type'],
                     'bq_loaded_record_count': table_audit_results.loc[0,'count'],
                     'loaded_record_count':metadata_json['row_count'],
                     'dag_execution_date' : context['ds'],
                     'audit_execution_date' : audit_dt,
                     'audit_result' : audit_result,
                     'audit_reason': audit_reason
                     }

        print(json_data)

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
                        self.filename,
                        df.to_json(orient='records', lines='\n'))
        return

    def _get_gcs_file_audit(self,context):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        print(self.landed_filename + '{date}/metadata_'.format(date=self.target_date_nodash))
        file_list = gcs_hook.list(self.bucket, prefix=self.landed_filename + '{date}/l1_metadata_'.format(
            date=self.target_date_nodash, table=self.google_cloud_bq_config['table'].replace('$','')))
        print(file_list)

        df_full_length = 0
        for f in file_list:
            file_data = gcs_hook.download(self.bucket, f)
            json_data = json.loads(file_data)
            break
        return json_data

    def _get_primary_key(self):
        sql = '''SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
                           FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                           INNER JOIN
                               INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                                     ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                                        TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                                        KU.table_name = '{table}'
                           ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;'''.format(table=self.google_cloud_bq_config['table'])

        result = self._query_mssql(sql=sql, database=self.google_cloud_bq_config['db'])
        data_result = result.fetchall()
        pkey = 'NA'
        if len(data_result) == 1:
            print('Primary key found for the table....')
            pkey = data_result[0][1]
        else:
            print('No primary key found and defaulting to full pull')
            pkey = 'None'
        return pkey

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
        else:
            cursor.execute(sql)
        return cursor

    def _return_primary_key(self):
        primary_key = 'NA'
        if self.primary_key_check:
            primary_key = self._get_primary_key()
        return primary_key

    def _return_change_tracking(self,context):
        bq_hook = BigQueryHook(self.google_cloud_bq_conn_id)
        self.table_present = bq_hook.table_exists(project_id = self.google_cloud_bq_config['target_project'],
                                                  dataset_id = self.google_cloud_bq_config['dataset'],
                                                  table_id = self.google_cloud_bq_config['table'].replace('$', ''))
        if self.change_tracking_check & self.table_present:
            bq_hook = BigQueryHook(self.google_cloud_bq_conn_id)
            schema = bq_hook.get_schema(self.google_cloud_bq_config['dataset'],
                                        self.google_cloud_bq_config['table'].replace('$', ''))
            schema = schema['fields']
            schema = pd.DataFrame(schema)
            if self.column_name in schema.name.values:
                bq_hook = JMBQHook(self.google_cloud_bq_conn_id)

                if self.google_cloud_bq_config['table'].replace('$', '') in self.non_partitioned_tables:
                    print('>>>>>>> this is a non partitioned tabel')
                    sql = 'SELECT max({columnname}) as maxdate FROM `{project}.{dataset}.{table}`'.format(
                        project=self.google_cloud_bq_config['target_project'],
                        sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                        dataset=self.google_cloud_bq_config['dataset'],
                        table=self.google_cloud_bq_config['table'].replace('$', ''),
                        schema=self.google_cloud_bq_config['schema'],
                        columnname=self.column_name)
                else:
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
                start_date = start_date.iloc[0, 0]
                #if start_date == np.datetime64('NaT'):
                if str(start_date) == 'NaT':
                    #start_date = context['ds'] + ' 00:00:00'
                    return 0

                end_date = context['next_ds'] + ' 23:59:59'

                ct_sql = ''' Select count(*) from [{center}].[{dataset}].[{table}] 
                      where {columname}  >  cast('{startdate}' as datetime2) and {columname}  <= convert(DATETIME, '{enddate}') '''.format(center=self.google_cloud_bq_config['db'],dataset=self.google_cloud_bq_config['schema'],
                                                                 table=self.google_cloud_bq_config['table'],columname=self.column_name,
                                                             startdate = start_date, enddate=end_date)

            #select count(B.*) from [{center}].[{dataset}].[{table}] B
                print(ct_sql)
                cursor = self._query_mssql(sql=ct_sql, database=self.google_cloud_bq_config['db'])
                results_ct = cursor.fetchall()
                results_ct_number = results_ct[0][0]
            else:
                results_ct_number = 0

            return results_ct_number

    def _get_bq_audit_data(self,context):

        print(f' >>>  Non-Partitioned Tables: {self.non_partitioned_tables}')

        if self.google_cloud_bq_config['table'].replace('$','') in self.non_partitioned_tables:
            print('this is a non-partitioned table')
            print('SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}`'.format(
                    project=self.google_cloud_bq_config['target_project'],
                    sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                    dataset=self.google_cloud_bq_config['dataset'],
                    table=self.google_cloud_bq_config['table'].replace('$',''),
                    schema=self.google_cloud_bq_config['schema']))

            try:
                bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
                table_audit = bq_hook.get_pandas_df(
                    sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}`'.format(
                        project=self.google_cloud_bq_config['target_project'],
                        sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                        dataset=self.google_cloud_bq_config['dataset'],
                        table=self.google_cloud_bq_config['table'].replace('$',''),
                        schema=self.google_cloud_bq_config['schema']))
            except:
                bq_hook = JMBQHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
                table_audit = bq_hook.get_data(
                    sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}`'.format(
                        project=self.google_cloud_bq_config['target_project'],
                        sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                        dataset=self.google_cloud_bq_config['dataset'],
                        table=self.google_cloud_bq_config['table'].replace('$', ''),
                        schema=self.google_cloud_bq_config['schema']))
        
        else:
            print('SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                    project=self.google_cloud_bq_config['target_project'],
                    sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                    dataset=self.google_cloud_bq_config['dataset'],
                    table=self.google_cloud_bq_config['table'].replace('$',''),
                    schema=self.google_cloud_bq_config['schema'],
                    date=self.target_date))

            try:
                bq_hook = BigQueryHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
                table_audit = bq_hook.get_pandas_df(
                    sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                        project=self.google_cloud_bq_config['target_project'],
                        sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                        dataset=self.google_cloud_bq_config['dataset'],
                        table=self.google_cloud_bq_config['table'].replace('$',''),
                        schema=self.google_cloud_bq_config['schema'],
                        date=self.target_date))
            except:
                bq_hook = JMBQHook(self.google_cloud_bq_conn_id, use_legacy_sql=False)
                table_audit = bq_hook.get_data(
                    sql='SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}"'.format(
                        project=self.google_cloud_bq_config['target_project'],
                        sql_source_abbr=self.google_cloud_bq_config['source_abbr'],
                        dataset=self.google_cloud_bq_config['dataset'],
                        table=self.google_cloud_bq_config['table'].replace('$', ''),
                        schema=self.google_cloud_bq_config['schema'],
                        date=self.target_date))
        return table_audit

    def _landing_check(self,context):
        # Get primary key for the target table
        primary_key = self._return_primary_key()
        print(primary_key)
        # Get the row count for a change tracking pull if available for the target table
        ct_source_result = self._return_change_tracking(context)
        print(ct_source_result)
        if self.change_tracking_check:
            if ct_source_result == 0:
                ct_len = 0
            else:
                ct_len = ct_source_result
        else:
            ct_len = 0

        # Pull row count for a full extraction.
        full_pull_source_result = self._query_mssql(database=self.google_cloud_bq_config['db'])
        # Extract the results from the full pull to get the data.
        result_full = full_pull_source_result.fetchall()
        # Get the file list and the total row count for the files that are loaded into GCS.
        metadata_json = self._get_gcs_file_audit(context)

        audit_result = 'PASSED'
        audit_reason = ''
        if metadata_json['load_type'] == 'FULL_PULL':
            if metadata_json['row_count'] != result_full[0][0]:
                audit_result = 'FAILED'
                audit_reason += 'Landing count did not match Source Count;'
        if metadata_json['load_type'] == 'CT_PULL':
            if metadata_json['row_count'] != ct_len:
                audit_result = 'FAILED'
                audit_reason += 'Landing count did not match Source Count;'
        if metadata_json['load_type'] == 'UNKNOWN':
            audit_result = 'FAILED'
            audit_reason += 'Load Type Unknown;'

        audit_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = {'full_load_count': result_full[0][0],
                     'ct_load_count': ct_len,
                     'load_type': metadata_json['load_type'],
                     'loaded_record_count': metadata_json['row_count'],
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
