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
This module contains Google BigQuery operators.
"""

import json
from typing import Iterable
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
import logging
import pandas as pd
from airflow.models import Variable

BIGQUERY_JOB_DETAILS_LINK_FMT = 'https://console.cloud.google.com/bigquery?j={job_id}'


class GetNiceTables(BaseOperator):
    #template_fields = ('dataset_id')

    @apply_defaults
    def __init__(self,
                 gcp_connection_id,
                 project_id,
                 dataset_id,
                 *args,
                 **kwargs):
        super(GetNiceTables, self).__init__(*args, **kwargs)

        self.gcp_connection_id = gcp_connection_id
        self.project_id = project_id
        self.dataset_id = dataset_id


    def execute(self, context):
        source_hook = BigQueryHook(self.gcp_connection_id)

        # get the list of tables in the source dataset
        list = source_hook.get_dataset_tables_list(project_id=self.project_id, dataset_id=self.dataset_id)
        print("Printing table list", list)
        df = pd.DataFrame(list)
        table_list = df['tableId'].tolist()
        #removing this table as we are not ingesting this table in Nice dag
        table_list.remove('t_contact_state_descriptions_detail')
        table_string = ', '.join( [str(x) for x in table_list])
        Variable.set('Nice_refined_tables_list', table_string)
        return




