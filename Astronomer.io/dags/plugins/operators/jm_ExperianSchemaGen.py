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
from plugins.hooks.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.base import BaseHook
from airflow.operators.bash_operator import BashOperator
import logging
import io
import pandas as pd
import datetime as dt

class ExperianSchemaGen(BaseOperator):
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


    ui_color = '#c6fc03' #'#aa19323'

    @apply_defaults
    def __init__(self,
                 project,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 prefix_for_filename_search=None,
                 schema_filename = 'NA',
                 *args,
                 **kwargs):

        super(ExperianSchemaGen, self).__init__(*args, **kwargs)

        self.project = project
        self.source  = source
        self.target_gcs_bucket = target_gcs_bucket
        self.source_abbr = source_abbr
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.prefix_for_filename_search = prefix_for_filename_search
        self.schema_filename = schema_filename


    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        if "{{ ds_nodash }}" in self.prefix_for_filename_search:
            self.prefix_for_filename_search = self.prefix_for_filename_search.replace("{{ ds_nodash }}",context['ds_nodash'])

        if "{{ ds_nodash }}" in self.schema_filename:
            self.schema_filename = self.schema_filename.replace("{{ ds_nodash }}",context['ds_nodash'])

        print(self.prefix_for_filename_search)
        print(self.schema_filename)
        file_list = hook.list(self.target_gcs_bucket,maxResults=100,prefix=self.prefix_for_filename_search) #self.prefix_for_filename_search[:-2])

        if len(file_list) == 0:
            print('No files for processing schema file....')
            return

        print(file_list)

        for f in file_list:
            self._process_file_from_landing(f,hook)
            break



        return



    def _process_file_from_landing(self,object,gcs_hook):
        file_data=gcs_hook.download(self.target_gcs_bucket,object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        file_df = pd.read_json(file_stream, orient='records', lines='\n')
        print(file_df)
        schema = self._generate_schema(file_df)
        self._upload_schema(schema)
        return

    def _generate_schema(self,df, default_type='STRING'):
        """ Given a passed df, generate the associated Google BigQuery schema.
        Parameters
        ----------
        df : DataFrame
        default_type : string
            The default big query type in case the type of the column
            does not exist in the schema.
        """

        type_mapping = {
            #'i': 'INTEGER',
            #'b': 'BOOLEAN',
            #'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})

        return fields


    def _upload_schema(self,json_data):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        # df = pd.DataFrame(json_data)
        gcs_hook.upload(self.target_gcs_bucket,
                        self.schema_filename,
                        str(json_data).replace("'",'"'))
        return