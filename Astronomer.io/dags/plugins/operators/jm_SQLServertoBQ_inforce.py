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
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
from plugins.hooks.jm_mssql import MsSqlHook
from airflow import configuration
import os
from google.cloud import bigquery
from google.oauth2 import service_account



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
    template_fields = ('sql',)
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
        print(self.sql)
        cursor = self._query_mssql()
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]



        if len(result) != len(df):
            raise ValueError('The length of dataframe did not match with sql cursor length. throwing manual exception.. ')

        gcp_hook = GoogleBaseHook(gcp_conn_id=self. bigquery_conn_id)
        keyfile_dict = gcp_hook._get_field('keyfile_dict')
        info = json.loads(keyfile_dict)

        credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(project=self.project, credentials=credentials)
        job_config = bigquery.LoadJobConfig(
            # to append use "WRITE_APPEND" or don't pass job_config at all (appending is default)
            write_disposition="WRITE_APPEND",
        )

        # Include target partition in the table id:
        table_id = "{project}.{dataset}.{table}".format(project = self.project, dataset = self.destination_dataset,table=self.destination_table)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request
        job.result()  # Wait for job to finish

        # write the dataframe into a bigquery table
        """self.hook.gcp_bq_write_table(
            df_in=df,
            project=self.project,
            dataset=self.destination_dataset,
            table_name=self.destination_table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')"""