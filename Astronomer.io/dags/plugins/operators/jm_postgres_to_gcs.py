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
import datetime as dt
import logging
import pandas as pd
import io
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_postgres import PostgresHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable



class PostgresToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from a Postgres DB to Google Cloud Storage
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
    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
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
        'postgres-export' GCS bucket (along with a schema file). ::
            export_customers = PostgresToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='postgres-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                postgres_conn_id='postgres_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename','prod_file_prefix','file_path')
    template_ext = ('.sql',)
  #  ui_color = '#e0a98c'
    ui_color = '#ff1ce8'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 file_path=None,
                 filename=None,
                 history_check='True',
                 prod_file_prefix=None,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 gzip=False,
                 postgres_conn_id='postgres_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 airflow_var_set='NA',
                 airflow_schema_var_set='NA',

                 *args,
                 **kwargs):

        super(PostgresToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.file_path = file_path
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.gzip = gzip
        self.postgres_conn_id = postgres_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_var_set = airflow_var_set
        self.airflow_schema_var_set = airflow_schema_var_set
        self.history_check = history_check
        self.prod_file_prefix = prod_file_prefix

    def execute(self, context):
        logging.info('start processing')

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        if self.airflow_var_set != 'NA':
            cursor = self._query_postgres()
            val = cursor.fetchall()
            table_string = ','.join([x[1].replace(' ', '') + '.' + x[2] for x in val])
            Variable.set(self.airflow_var_set, table_string)
            return

        if self.history_check.upper() == 'TRUE':
            logging.info('checking landing project for files present')
            history_file_list = self._l1_data_check(context, self.google_cloud_storage_conn_id, self.bucket, self.file_path, 'LAST')
            if history_file_list != 'NONE':
                logging.info('pulling from landing project')
                file_list = self._l1_data_check(context, self.google_cloud_storage_conn_id, self.bucket, self.file_path, 'LAST').split(',')
                for f in file_list:
                    logging.info('Processing the following file: ', f)
                    self._process_from_l1_gcs(context, f)
                return 0
            else:
                logging.info('checking prod-edl project for files present')
                # Attempt to pull from PROD
                history_file_list = self._l1_data_check(context, 'prod_edl', 'jm_prod_edl_lnd', self.prod_file_prefix, 'LAST')
                if  history_file_list != 'NONE':
                    logging.info('pulling from prod-edl project')
                    file_list = self._l1_data_check(context, 'prod_edl', 'jm_prod_edl_lnd', self.prod_file_prefix, 'LAST').split(',')
                    for f in file_list:
                        logging.info('Processing the following file: ', f)
                        self._process_from_prod_l1_gcs(context, f)
                    return 0

        cursor = self._query_postgres()
        column_names = [item[0] for item in cursor.description]
        row_cnt = list(cursor)
        df = pd.DataFrame(row_cnt)
        if df.empty:
            print('DataFrame is empty!')
        else:
            df.columns = column_names
            for column in df.columns:
                if 'date' in column:
                    try:
                        df[column] = pd.to_datetime(df[column],unit='ms')
                    except:
                        df[column] = pd.to_datetime(df[column])
            df = self._standardize_datatypes(df)
        self._upload_df_to_gcs(df,context,file_dt)



        # files_to_upload = self._write_local_data_files(cursor)
        #
        # # If a schema is set, create a BQ schema JSON file.
        # if self.schema_filename:
        #     files_to_upload.update(self._write_local_schema_file(cursor))
        #
        # # Flush all files before uploading
        # for file_handle in files_to_upload.values():
        #     file_handle.flush()
        #
        # self._upload_to_gcs(files_to_upload)
        #
        # # Close all temp file handles
        # for file_handle in files_to_upload.values():
        #     file_handle.close()

    def _query_postgres(self):
        """
        Queries Postgres and returns a cursor of results.
        :return: postgres cursor
        """
        logging.info('connecting to API')
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
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
            # if self.scrub_data:
            #     row = map(self.anonymize, row)
            row_dict = dict(zip(schema, row))
            s = json.dumps(row_dict, sort_keys=True, default=str)
            s = s.encode('utf-8')
            tmp_file_handle.write(s)

            # Append newline to make dumps BQ compatible
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                print('exceeding file size limit')
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.
        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = []
        for field in cursor.description:
            # See PEP 249 for details about the description tuple.
            field_name = field[0].replace(' ', '_')  # Clean spaces
            field_type = self.type_map(field[1])
            field_mode = 'NULLABLE'

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

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object_name, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object_name, tmp_file_handle.name, 'application/json',
                        (self.gzip if object_name != self.schema_filename else False))

    def _process_from_prod_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id='prod_edl',
            delegate_to=self.delegate_to)

        target_bucket = 'jm_prod_edl_lnd'
        prod_file_path = object
        print('Attempting to load object======', object)
        ####
        file_data = hook.download(target_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        for column in df.columns:
            if 'date' in column:
                try:
                    df[column] = pd.to_datetime(df[column], unit='ms')
                except:
                    df[column] = pd.to_datetime(df[column])
        df = self._standardize_datatypes(df)

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        self._upload_df_to_gcs(df, context, file_dt)
        self.source_count = len(df)
        # json_normalized_df = self._upload_normalized_to_gcs(df, context)
        # self._upload_metadata(context, json_normalized_df)
        # del df
        # self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

    def _process_from_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_data = hook.download(self.bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        for column in df.columns:
            if 'date' in column:
                try:
                    df[column] = pd.to_datetime(df[column], unit='ms')
                except:
                    df[column] = pd.to_datetime(df[column])

        df = self._standardize_datatypes(df)

        self.source_count = len(df)

        #json_normalized_df = self._upload_normalized_to_gcs(df, context)
        #self._upload_metadata(context, df)
        #del df
        self._upload_schema_to_gcs(df, context)

        return 0

    def _l1_data_check(self, context, storage_conn_id, target_bucket,object, return_type='LAST'):
        logging.info('history check')
        hook = GCSHook(
            google_cloud_storage_conn_id=storage_conn_id,
            delegate_to=self.delegate_to
        )

        file_prefix = object
        logging.info('looking for files under')
        logging.info(file_prefix)

        # Getting List of files from the folder
        file_list = hook.list(target_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')

        if len(file_list) == 0:
            return 'NONE'
        elif len(file_list) == 1:
            return file_list[0]
        else:
            df = pd.DataFrame()
            for f in file_list:
                df_len = len(df)
                df.loc[df_len, 'filename'] = f
                df.loc[df_len, 'filename_base'] = f.replace('.json', '')

            # df['counter_value'] = df.filename_base.str.split('_').str[-1]
            df['file_dt'] = df.filename_base.str.split('_').str[-1]
            df['file_datetime'] = pd.to_datetime(df['file_dt'], format='%Y%m%d%H%M%S')

            if return_type == 'FIRST':
                df.sort_values(by=['file_datetime'], ascending=[True], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]
            else:
                df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]

            return ','.join(df['filename'].values.tolist())

        return 'NONE'

    def _upload_df_to_gcs(self, df, context, file_dt):

        file_no = 0

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        json_data = df.to_json(orient='records', lines='\n', date_format='iso')

        file_name = self.filename.format(file_no)
        hook.upload(bucket=self.bucket, object=file_name, data=json_data)
        del json_data

        if df.empty:
            print('Dataframe is empty.  Returning to main.....')
            return -1
        else:

            self._upload_schema_to_gcs(df, context)

            return 0

    def _upload_metadata(self, context, df):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        audit_df = pd.DataFrame()

        file_name = '{base_gcs_folder}/{source}/{date_nodash}/l1_metadata_mp_cd_all.json'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)

        file_exists = hook.exists(self.bucket, file_name)
        logging.info('file_exists value is ' + str(file_exists))

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': self.source_count, 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        hook.upload(bucket=self.bucket, object=file_name, data=json_data)

        del json_data
        del audit_df

        return

    def _upload_schema_to_gcs(self, json_normalized_df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df, context)

        # file_name = '{base_gcs_folder}_schema/{source}/{date_nodash}/l1_schema_mp_cd_all.json'.format(
        #     source=self.source_abbr.lower(),
        #     date_nodash=context['ds_nodash'],
        #     base_gcs_folder=self.base_gcs_folder)

        hook.upload(bucket=self.bucket, object=self.schema_filename, data=schema_data)

        return

    def _standardize_datatypes(self, df):
        for column_name, dtype in df.dtypes.iteritems():
            if (dtype.kind != 'M'):
                df[column_name] = df[column_name].astype(str)
        return df

    # This function generate schema by reading the data from file in l1_norm and return schema in a list
    def _schema_generator(self, df, context, default_type='STRING'):
        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            # print(column_name,dtype.kind)
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return_string = "[{fields}]".format(fields=",".join(fields))
        return json.dumps(fields)  # return_string

    @classmethod
    def convert_types(cls, value):
        """
        Takes a value from Postgres, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        """

        if (isinstance(value, pd.Timestamp)) or (isinstance(value, dt.date)):
            return value
        else:
            return str(value)

    @classmethod
    def type_map(cls, postgres_type):
        """
        Helper function that maps from Postgres fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            1114: 'TIMESTAMP',
            1184: 'TIMESTAMP',
            1082: 'DATE',
            1083: 'TIMESTAMP',
            1005: 'STRING',
            1007: 'STRING',
            1016: 'STRING',
            20: 'STRING',
            21: 'STRING',
            23: 'STRING',
            16: 'STRING',
            700: 'STRING',
            701: 'STRING',
            1700: 'STRING',
        }
        return d[postgres_type] if postgres_type in d else 'STRING'
