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
import calendar
import time
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
from datetime import datetime

import os


class SQLFinalAuditOperator(BaseOperator):
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
    template_fields = ('destination_table', )
    @apply_defaults
    def __init__(self,
                 sql,
                 destination_dataset,
                 destination_table,
                 project,
                 table_list,
                 mssql_conn_id='mssql_default',
                 bigquery_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super(SQLFinalAuditOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.project = project
        self.table_list = table_list
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

        final_df = pd.DataFrame()
        i =0
        for table in self.table_list:
            if table.startswith('cctl_') or table.startswith('pctl_') or table.startswith('bctl_') or table in ['pc_history','cc_history']:
                bqsql = '''select count(*) from `{project}.{dataset}.{table}`'''.format(

                    project=self.project,
                    dataset=self.destination_dataset,
                    table=table,
                    date=context["ds"])
            else:

                bqsql = '''select count(*) from `{project}.{dataset}.{table}` where DATE(_PARTITIONTIME) = "{date}"'''.format(

                project=self.project,
                dataset=self.destination_dataset,
                table=table,
                date=context["ds"])

            result_df = self.hook.get_pandas_df(bqsql)

            final_df.loc[i,'table_name'] = table
            final_df.loc[i, 'bq_count'] = str(result_df.iloc[0,0])
            final_df.loc[i,'sql_count'] = str(df.loc[df.TableName == table,'sql_count'].iloc[0])
            if str(result_df.iloc[0,0]) == str(df.loc[df.TableName == table,'sql_count'].iloc[0]):
                final_df.loc[i, 'match_flag'] = '1'
            else:
                final_df.loc[i, 'match_flag'] = '0'
            i=i+1

        schema_list_bq = [
            {"name": "table_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "bq_count", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sql_count", "type": "STRING", "mode": "NULLABLE"},
            {"name": "match_flag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "run_id", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]

        #print(context["job_id"])

        final_df['run_id'] = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        final_df['run_id'] = pd.to_datetime(final_df['run_id'])



        # write the dataframe into a bigquery table

        if self.hook.table_exists(project_id=self.project, dataset_id=self.destination_dataset,
                                         table_id=self.destination_table):
            logging.info('Destination table {} already exists!'.format(self.destination_table))
        else:
            self.hook.create_empty_table(project_id=self.project, dataset_id=self.destination_dataset,
                                                table_id=self.destination_table,
                                                schema_fields=schema_list_bq,
                                                time_partitioning={'type': 'DAY'})
            logging.info('Destination table {} created!'.format(self.destination_table))

        self.hook.gcp_bq_write_table(
            df_in=final_df,
            project=self.project,
            dataset=self.destination_dataset,
            table_name='{table}${date}'.format(table=self.destination_table,date=context["ds_nodash"]),
            control='Append',
            schema_enable=True,
            schema='NA')

        mismatched_df = final_df.loc[final_df['match_flag'] == '0']

        if mismatched_df.shape[0] >= 1:
            print(mismatched_df)
            raise ValueError('Counts mismatched for above tables..... ')

        return 0

