"""Script to pull all Jeweler Member Benefits Statement data and produce report"""

################################################################################
# Base and third-party libraries
################################################################################


import os
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
#from airflow import AirflowFailException
from airflow.models import Variable
import json
from google.oauth2 import service_account
import logging
import pandas as pd


class JDPProductsAuditOperator(BaseOperator):
    template_fields = ('sql',)
    @apply_defaults
    def __init__(self,
                 sql,
                 bigquery_conn_id,
                 delegate_to=None,
                 jdp_products = True, #set true for jdp products and false for core JDP
                 variable_name ='NA',
                 *args,
                 **kwargs
                 ):
        super(JDPProductsAuditOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.products = jdp_products
        self.variable_name = variable_name


    def execute(self, context):
        bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id,use_legacy_sql=False,
                               delegate_to=self.delegate_to)

        df = bq_hook.get_pandas_df(sql = self.sql,dialect='standard')
        print(self.sql)
        print(df.head(5))
        if len(df) > 0:
            if not self.products:
                logging.info(df)
                Variable.set('places_look_up','False')
                logging.info("Records than needs places look up is greater than 500. skipping downstream tasks....")
                return

            logging.info(df)
            raise ValueError("Duplicated ID's found")

        elif not self.products:
            Variable.set('places_look_up', 'True')
            logging.info("Records than needs places look up is lesser than 500. proceeding with downstream tasks....")
            return
        else:
            logging.info("No duplicates found...")
            return






