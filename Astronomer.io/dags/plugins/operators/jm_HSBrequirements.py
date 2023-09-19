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
import datetime
from plugins.hooks.jm_gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
from plugins.hooks.jm_mssql import MsSqlHook
from airflow import configuration
import os
from google.cloud import bigquery
from google.oauth2 import service_account




class hsb_requirements(BaseOperator):

    template_fields = ('hsb_req_sql','water_loss_sql',)
    @apply_defaults
    def __init__(self,
                 hsb_req_sql,
                 #nforce_policy_sql,
                 water_loss_sql,
                 destination_dataset,
                 destination_table,
                 project,
                 mssql_conn_id='mssql_default',
                 bigquery_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super(hsb_requirements, self).__init__(*args, **kwargs)
        self.hsb_req_sql = hsb_req_sql
        #self.inforce_policy_sql = inforce_policy_sql
        self. water_loss_sql =  water_loss_sql
        self.project = project
        self. bigquery_conn_id =  bigquery_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.hook = BigQueryHook(gcp_conn_id= self.bigquery_conn_id)
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table
        self.dialect = 'standard'

    def _query_mssql(self,sql):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor



    def execute(self, context):


        #call to a function that executes sql and returns cursor
        #print(self.sql)
        #HSB
        print(self.hsb_req_sql)
        #sys.exit(1)
        cursor_1 = self._query_mssql(self.hsb_req_sql)
        result_1 = cursor_1.fetchall()
        df_hsb_req = pd.DataFrame(result_1)
        df_hsb_req.columns = [i[0] for i in cursor_1.description]
        print(df_hsb_req.head(5))

        #WaterLoss
        cursor_3 = self._query_mssql(self.water_loss_sql)
        result_3 = cursor_3.fetchall()
        df_waterloss = pd.DataFrame(result_3)
        df_waterloss.columns = [i[0] for i in cursor_3.description]
        print(df_waterloss.head(5))

        ##Water losses contain Water, Flood or Sprinkler
        wl2 = df_waterloss[
            (df_waterloss['COL'].str.contains('Water')) |
            (df_waterloss['COL'].str.contains('Flood')) |
            (df_waterloss['COL'].str.contains('Sprinkler'))
            ]

        wl2['ReportedDate'] = pd.to_datetime(wl2['ReportedDate'])

        ##Get date of 2 years ago
        #import datetime
        twoyears = datetime.datetime.now() - datetime.timedelta(days=1 * 365)
        twoyears = pd.to_datetime(twoyears)

        ##Filter to last 2 years of water losses
        wl3 = wl2[wl2['ReportedDate'] > twoyears]

        ##Since all have water losses in last 2 years, drop dups by policy number
        wl4 = wl3.drop_duplicates(subset='PolNo')

        ##Filter to need field
        wl5 = wl4[['PolNo']]

        ##Fill in Water loss lookup status
        wl5['WaterLoss'] = 1

        df = df_hsb_req.copy()

        ##Current time
        #from datetime import datetime
        year = int(datetime.date.today().strftime('%Y'))
        ##year
        df['PlumbingYr'] = df['PlumbingYr'].fillna(0)

        df['BldgYr'] = df['BldgYr'].fillna(0)

        ##First filter to US only
        df = df[df['LocCountry'] == 'USA']

        ##Now filter to LOB
        ##LOB JBP, BOP, JSP
        df['LOBFlag'] = np.where(
            (df['PolicyType'] == 'JBP') | (df['PolicyType'] == 'JSP') | (df['PolicyType'] == 'BOP'), 1,
            0)

        df = df[df['LOBFlag'] == 1]

        ##If Year build >35 and Building year is over 35, then Required
        df['plumbing_building_over35'] = np.where((year - df['PlumbingYr'] > 35) & (year - df['BldgYr'] > 35), 1, 0)

        ##If Both the plumbing year and the building year are both null then required
        df['plumbing_bldg_null'] = np.where((df['BldgYr'] == 0) & (df['PlumbingYr'] == 0), 1, 0)

        ##Bring in Water loss status
        df = df.merge(wl5, left_on='PolicyNbr', right_on='PolNo', how='left').drop(columns='PolNo')

        # print(f"{df[df['WaterLoss'].notna()].shape[0]} policies have water losses in the last 2 years")

        ##Now actually filter, if both age or both null then required
        df = df[(df['WaterLoss'] == 1)]  ##(df['plumbing_building_over35'] == 1) | (df['plumbing_bldg_null'] == 1)

        # write the dataframe into a bigquery table"""
        self.hook.gcp_bq_write_table(
            df_in=df,
            project=self.project,
            dataset=self.destination_dataset,
            table_name=self.destination_table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')
