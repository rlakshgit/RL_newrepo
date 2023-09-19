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
from pandas.io.json import json_normalize
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.dq_common import dq_common
from plugins.operators.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
# from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
from plugins.hooks.jm_bq_hook import BigQueryHook

from airflow.hooks.base import BaseHook
import logging
import pymongo
from pymongo import MongoClient
import io
import pandas as pd
from datetime import datetime
from pandas import DataFrame
import datetime as dt


class Mongodbtogcs(BaseOperator):
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

    template_fields = ('metadata_filename',
                       'audit_filename','base_gcs_folder')

    @apply_defaults
    def __init__(self,
                 project,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 mongo_db_conn_id,
                 entity,
                 database_name,
                 collection,
                 base_gcs_folder=None,
                 base_schema_folder=None,
                 base_norm_folder=None,
                 bucket=None,
                 history_check=True,
                 approx_max_file_size_bytes=1900000000,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 metadata_filename='NA',
                 audit_filename='NA',
                 *args,
                 **kwargs):

        super(Mongodbtogcs, self).__init__(*args, **kwargs)

        self.bucket = bucket
        self.project = project
        self.source = source
        self.base_gcs_folder = base_gcs_folder
        self.entity = entity
        self.mongo_db_conn_id = mongo_db_conn_id
        self.target_gcs_bucket = target_gcs_bucket
        self.source_abbr = source_abbr
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.table_present = False
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.audit_filename = audit_filename
        self.database_name = database_name
        self.collection = collection
        self.base_schema_folder=base_schema_folder
        self.base_norm_folder = base_norm_folder
        self.schema_data_new = []


    def execute(self, context):
        pull_new = True

        mongo_db_connection = BaseHook.get_connection(self.mongo_db_conn_id)

        self.CONNECTION_STRING = mongo_db_connection.password

        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        dq = dq_common()
        if self.history_check.upper() == 'TRUE':
            #process data from GCP
            logging.info("Inside History check")
            if self._check_history(context) is not None:
                logging.info("Files present...")
                file_list = self._check_history(context)
                counter = 0
                length_df = 0
                source_count = 0
                for f in file_list:
                    file_data = gcs_hook.download(self.target_gcs_bucket, f)
                    file_stream = io.BufferedReader(io.BytesIO(file_data))
                    df = pd.read_json(file_stream, orient='records', lines='\n')
                    df = dq.gcp_bq_data_encoding(df)

                    # pd.DataFrame(stream_response['data'])
                    try:
                        df = json_normalize(data=df.to_dict('records'))

                    except:
                        df = pd.DataFrame()
                    source_count = source_count + len(df)
                    #del df

                    df_normalized = df
                    for col in df_normalized.columns:
                        if df_normalized[col].dtypes != 'datetime64[ns]':
                            df_normalized[col] = df_normalized[col].astype(str)

                    # Normalize the data
                    self._upload_normalized_data_gcs(df_normalized, context, counter=counter)
                    self._upload_schema_to_gcs(df_normalized, context)
                    length_df = length_df + len(df_normalized)

                    counter = counter + 1
                    if self.metadata_filename is not None:
                        self._metadata_upload(context, length_df, source_count)

                pull_new = False

        if pull_new:
            #Get data from mongo db
            client = MongoClient(self.CONNECTION_STRING)
            mydatabase = client[self.database_name]
            mycollection = mydatabase[self.collection]
            start_date = datetime.strptime(context['ds'], '%Y-%m-%d')
            end_date = datetime.strptime(context['tomorrow_ds'], '%Y-%m-%d')
            data = mycollection.find({'$and': [{"ModifiedOn": {'$gte': start_date}},{"ModifiedOn": {'$lt': end_date}},{"EntityType":self.entity}]})
            print('data type:')
            print('data')
            print(type(data))
            items_df = DataFrame(data)
            items_df = dq.gcp_bq_data_encoding(items_df)
            #source_count = mycollection.count({'$and': [{"ModifiedOn": {'$gte': start_date}},{"ModifiedOn": {'$lt': end_date}},{"EntityType":self.entity}]})
            self._upload_raw_data_gcs(items_df.to_json(orient='records', lines='\n', date_format='iso'), context)

            # Normalize the data
            df_norm = json_normalize(items_df.to_dict('records'))
            for col in df_norm.columns:
                if df_norm[col].dtypes != 'datetime64[ns]':
                    df_norm[col] = df_norm[col].astype(str)

            self._upload_normalized_data_gcs(df_norm, context)
            self._upload_schema_to_gcs(df_norm, context)
            if self.metadata_filename is not None:
                self._metadata_upload(context, len(df_norm), len(df_norm))

    def _upload_raw_data_gcs(self, data, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{entity}/{date_nodash}/l1_data_{source}_{entity}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data)
        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        file_name = '{base_gcs_folder}{entity}/{date_nodash}/l1_data_'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=file_name)
        logging.info("Files are - {files}".format(files=base_file_list))
        if len(base_file_list) == 0:
            return None
        else:
            return base_file_list

    def _upload_normalized_data_gcs(self, df_in, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_norm_folder}{entity}/{date_nodash}/l1_norm_{source}_{entity}.json'.format(
            base_norm_folder=self.base_norm_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _upload_schema_to_gcs(self, json_normalized_df, context, counter=0):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df)

        schema_data_intermediate = [items for items in schema_data if items not in self.schema_data_new]
        self.schema_data_new.extend(schema_data_intermediate)
        file_name = '{base_schema_folder}{entity}/{date_nodash}/l1_schema_{source}_{entity}.json'.format(
            base_schema_folder=self.base_schema_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json.dumps(self.schema_data_new))

        return

    def _schema_generator(self, df, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',
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
        # return json.dumps(fields)
        return fields

    def _metadata_upload(self, context, row_count, source_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        metadata_filename = '{base_folder}{entity}/{date_nodash}/l1_metadata_{source}_{entity}.json'.format(
            base_folder=self.metadata_filename,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

        print('Metadata File - ', metadata_filename)
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _convert_to_dicts(self, ld):
        if type(ld) == list:
            if str(ld) == '[]':
                return {}
            for item in ld:
                if type(item) == dict:
                    return item
                if type(item[0]) != list:
                    return item[0]
        else:
            return ld




