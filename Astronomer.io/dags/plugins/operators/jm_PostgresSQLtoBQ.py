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
"""
Postgres to BQ operator.
"""

import sys
import pandas as pd
import numpy
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
from airflow.utils.decorators import apply_defaults


# PY3 = sys.version_info[0] == 3

class PostgresToBQOperator(BaseOperator):
    """Copy data from Postgres to BQ
    Google cloud storage in JSON or CSV format.

    :param mysql_conn_id: Reference to a specific MySQL hook.
    :type mysql_conn_id: str """

    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_dataset,
                 destination_table,
                 project,
                 postgres_conn_id,
                 bigquery_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super(PostgresToBQOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.project = project
        self.bigquery_conn_id = bigquery_conn_id
        self.hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table
        self.dialect = 'standard'

    def query_postgres(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        self.log.info('Executing: %s', self.sql)
        cursor.execute(self.sql)
        return cursor

    def execute(self, context):

        # call to a function that executes sql and returns cursor
        cursor = self.query_postgres()
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]
        print(df)
        print(cursor.description)

        type_map = {253: "VAR_STRING", 7: "TIMESTAMP"}

        for i in cursor.description:
            if str(i[1]) == '253':
                df[i[0]] = df[i[0]].astype('str')
            elif str(i[1]) == '7':
                df[i[0]] = pd.to_datetime(df[i[0]], format='%d%b%Y:%H:%M:%S.%f')

        if len(result) != len(df):
            raise ValueError(
                'The length of dataframe did not match with sql cursor length. throwing manual exception.. ')

        # write the dataframe into a bigquery table
        self.hook.gcp_bq_write_table(
            df_in=df,
            project=self.project,
            dataset=self.destination_dataset,
            table_name=self.destination_table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')
