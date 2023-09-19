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

#imports section
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
import json
import logging
import pandas as pd
import numpy as np
import math
pd.options.mode.chained_assignment = None
import datetime as dt
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_mssql import MsSqlHook
from airflow import configuration
import os


class SQLServertoGCSOperator(BaseOperator):
    """
       Copy data from Microsoft SQL Server to Google Big Query
       :param sql: The SQL to execute on the MSSQL table.
       :type sql: str
       :param mssql_conn_id: Reference to a specific MSSQL hook.
       :type mssql_conn_id: str
       :param bigquery_conn_id: Reference to a specific Google BigQuery hook.
       :type bigquery_conn_id: str
       :param destination_dataset: The name of the dataset to load the data into .
       :type destination_dataset: str
       :param destination_table:  The name of the table to load the data into .
       :type destination_table: str

       """
    template_fields = ('sql','filename')
    @apply_defaults
    def __init__(self,
                 sql,
                 target_gcs_bucket,
                 filename,
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super(SQLServertoGCSOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.google_cloud_storage_conn_id =  google_cloud_storage_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.target_gcs_bucket = target_gcs_bucket
        self.filename = filename
        self.delegate_to = None
        #self.hook = BigQueryHook(gcp_conn_id= self.bigquery_conn_id)
        self.gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        #self.destination_dataset = destination_dataset
        #self.destination_table = destination_table
        self.dialect = 'standard'

    def _query_mssql(self):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor



    def execute(self, context):

        print(self.sql)


        #call to a function that executes sql and returns cursor
        cursor = self._query_mssql()
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]

        for col in df.columns:
            if df[col].dtypes != 'datetime64[ns]':
                df[col] = df[col].astype(str)
            df['bq_load_date'] = pd.to_datetime(df['bq_load_date'],format='%Y-%m-%d')


        if len(result) != len(df):
            raise ValueError('The length of dataframe did not match with sql cursor length. throwing manual exception.. ')


        logging.info("uploading raw data to gcs...")
        result = df.to_json(orient='records', lines='\n',date_format='iso')
        self.gcs_hook.upload(bucket=self.target_gcs_bucket, object=self.filename, data=result)
        return




