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
from pandas.io.json import json_normalize
from airflow.hooks.base import BaseHook
import logging
import pymongo
from pymongo import MongoClient
import io
import pandas as pd
from datetime import datetime
from pandas import DataFrame
import datetime as dt
from flatten_json import flatten
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype


class BQSchemaGenerationOperator(BaseOperator):


    ui_color = '#e0aFFc'
    #template_fields = ('schema_location', )

    @apply_defaults
    def __init__(self,
                 project,
                 config_file,
                 source,
                 table,
                 target_gcs_bucket,
                 schema_location,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super(BQSchemaGenerationOperator, self).__init__(*args, **kwargs)


        self.project = project
        self.source = source
        self.target_gcs_bucket = target_gcs_bucket
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.api_configuration = config_file
        self.table = table
        self.schema_location = schema_location




    def execute(self, context):
        column_list = self.api_configuration[self.table]["Columns_to_BQ"]
        logging.info("Generating SChema step ..")
        self._upload_schema_to_gcs(column_list, context)

    def _schema_generator(self, col_list):
        fields = []
        for column_name in sorted(col_list):
            if '-' in column_name:
                column_name = column_name.replace('-', '_')
                fields.append({'name': column_name,
                               'type': 'String',
                               'mode': 'NULLABLE'})
            else:
                fields.append({'name': column_name,
                               'type': 'String',
                               'mode': 'NULLABLE'})
        return json.dumps(fields)  # return_string

    # Upload Schema File
    def _upload_schema_to_gcs(self, column_list, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        schema_data = self._schema_generator(column_list)

        file_name = self.schema_location+'{table}/{date_nodash}/l1_schema_{source}_{table}.json'.format(
            source=self.source,
            date_nodash=context['ds_nodash'],
            table=self.table)
        logging.info(" Shema File name - {file}".format(file = file_name))
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return









