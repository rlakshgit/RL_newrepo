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
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
from plugins.hooks.jm_mssql import MsSqlHook
from airflow import configuration
import os


class SQLServertoBQOperator(BaseOperator):
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
    @apply_defaults
    def __init__(self,
                 sql,
                 destination_dataset,
                 destination_table,
                 project,
                 mssql_conn_id='mssql_default',
                 bigquery_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super(SQLServertoBQOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.project = project
        self. bigquery_conn_id =  bigquery_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.hook = BigQueryHook(gcp_conn_id= self.bigquery_conn_id)
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table
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


        #call to a function that executes sql and returns cursor
        cursor = self._query_mssql()
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]
        type_map = {
            3: 'INTEGER',
            4: 'TIMESTAMP',
            5: 'NUMERIC'
        }
        for i in cursor.description:
            if 'AccountingDate' in i[0]:
                print("AccountingDate")
                df[i[0]] = pd.to_datetime(df[i[0]], format='%Y-%m-%d %H:%M:%S.%f')
                continue
            if 'DataQueryTillDate' in i[0]:
                print("DataQueryTillDate")
                df[i[0]] = pd.to_datetime(df[i[0]], format='%Y-%m-%d')
                continue

            if str(i[1]) == '3':
                df[i[0]] = df[i[0]].astype(int)
            elif str(i[1]) == '5':
                df[i[0]] = pd.to_numeric(df[i[0]])
            elif str(i[1]) == '4':
                df[i[0]] = pd.to_datetime(df[i[0]], format='%d%b%Y:%H:%M:%S.%f')



        if len(result) != len(df):
            raise ValueError('The length of dataframe did not match with sql cursor length. throwing manual exception.. ')

        # write the dataframe into a bigquery table
        self.hook.gcp_bq_write_table(
            df_in=df,
            project=self.project,
            dataset=self.destination_dataset,
            table_name=self.destination_table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')