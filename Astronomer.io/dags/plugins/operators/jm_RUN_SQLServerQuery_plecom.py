

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
from airflow.models import Variable



class RUNSQLServerQuery(BaseOperator):
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
                 mssql_conn_id='mssql_default',
                 *args,
                 **kwargs):

        super(RUNSQLServerQuery, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mssql_conn_id = mssql_conn_id
        self.dialect = 'standard'
       

    def _query_mssql(self, run_id):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        sql = self.sql.format(scrub_id=run_id)
        print('the sql is ', sql)
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        mssql.run(sql)

        return 

    def _get_ids(self):

        sql = """
                BEGIN TRANSACTION;
                use [DataRetention_Support_12];
                EXEC [dbo].[s_Scrub_Run_Select_Current] @IsDataLake = 1;
                COMMIT TRANSACTION;

                """
       
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor


    def execute(self, context):

        cursor = self._get_ids()
        result = cursor.fetchall()
        scrub_run_id = [item for t in result for item in t] # convet a list of tuples to list 
        print(scrub_run_id)
        for id in scrub_run_id:
            print('Scurb is >>>>', id)
            cursor = self._query_mssql(int(id))
  
        return










