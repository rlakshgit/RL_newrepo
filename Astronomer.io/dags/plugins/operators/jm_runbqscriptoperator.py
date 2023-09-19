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


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
import json
import logging
import pandas as pd
import numpy as np
import math
pd.options.mode.chained_assignment = None
from plugins.hooks.jm_bq_hook import BigQueryHook





####### This opearator can be used to run BQ scripts ##############


class runbqscriptoperator(BaseOperator):


    template_fields = ('sql',)
    @apply_defaults
    def __init__(self,
                 sql,
                 bigquery_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super(runbqscriptoperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self. bigquery_conn_id =  bigquery_conn_id
        self.hook = BigQueryHook(gcp_conn_id= self.bigquery_conn_id)
        self.delegate_to = delegate_to
        self.dialect = 'standard'

    def execute(self, context):
        print("***********************************************")
        print(self.sql)
        print("***********************************************")
        self.hook.run_query(
        sql=self.sql,
        use_legacy_sql=False,
)



        

