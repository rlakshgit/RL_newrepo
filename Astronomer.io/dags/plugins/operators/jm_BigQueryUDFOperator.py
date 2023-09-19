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
from airflow import configuration
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_mssql import MsSqlHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import io
import datetime as dt

class BigQueryUDFOperator(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param udf_sql: The udf_sql to executed to create a BigQuery UDF.
    :type udf_sql: str

    """

    ui_color = '#57d3e6'

    @apply_defaults
    def __init__(self,
                 udf_sql = '',
                 google_cloud_bq_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super(BigQueryUDFOperator, self).__init__(*args, **kwargs)
        self.udf_sql = udf_sql
        self.google_cloud_bq_conn_id = google_cloud_bq_conn_id

    def execute(self, context):
        bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn_id, use_legacy_sql=False)
        result= bq_hook.run_query(sql=self.udf_sql) 
        return

