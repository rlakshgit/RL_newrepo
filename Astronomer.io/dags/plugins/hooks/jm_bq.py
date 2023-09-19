#
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

import pandas_gbq as pdbq
import pandas as pd
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

class JMBQHook(GoogleBaseHook):

    conn_name_attr = 'bigquery_default'


    def __init__(self,
                 destination_dataset,
                 destination_table,
                 bigquery_conn_id,
                 delegate_to = None,
                 use_legacy_sql = False,
                 location = None,
                 control = 'APPEND',
                 *args, **kwargs):






        super(JMBQHook, self).__init__(*args, **kwargs)


        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.control = control
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table
        self.source_target = self.destination_dataset + '.' + self.destination_table


    def write_data(self,df_in,schema):
        private_key = self._get_field('key_path', None) or self._get_field('keyfile_dict', None)

        pdbq.to_gbq(df_in, destination_table=self.source_target,
                          project_id=self._get_field('project'),
                          if_exists=self.control.lower(), private_key=private_key, verbose=True,
                          table_schema=schema)

        return

    @classmethod
    def _schema_mapping(cls, df_type):
        """
                Helper function that maps from MSSQL fields to BigQuery fields. Used
                when a schema_filename is set.
                """
        d = {
            'int': 'INTEGER',
            'bit': 'BOOLEAN',
            'bigint': 'INTEGER',
            'smallint': 'INTEGER',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'decimal': 'NUMERIC',
            'money': 'NUMERIC',
            'smallmoney': 'NUMERIC',
        }
        return d[df_type] if df_type in d else 'STRING'