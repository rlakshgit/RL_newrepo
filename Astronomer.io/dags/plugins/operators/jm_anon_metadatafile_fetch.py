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

# imports section
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import macros
import pandas as pd
pd.options.mode.chained_assignment = None
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_anon_hook import AnonHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.models import Variable
import io



class anon_metaDataFileFetchOperator(BaseOperator):
    """
       This operator pulls data from refined bigquery tables, anonymize the data and save it as JSON in l1_anon bucket
       :param source_google_cloud_conn_id: Reference to a DEV/QA/Prod project Google cloud storage hook.
       :type source_google_cloud_conn_id: str
       :param destination_google_cloud_conn_id: Reference to a jm_dl_landing project Google cloud storage hook.
       :type destination_google_cloud_conn_id: str
       :param anon_google_cloud_conn_id: Reference to a data-anonymization project Google cloud storage hook.
       :type anon_google_cloud_conn_id: str
       :param destination_bucket: The bucket to upload anon data into.
       :type destination_bucket: str
       :param destination_file_name: The name of the file to upload into.
       :type destination_file_name: str
       :param sql: Query to pull data from BQ table
       :type sql: str

    """

    @apply_defaults
    def __init__(self,
                 landing_project_conn_id, # landing
                 anon_mapping_bucket='jm-edl-landing-wip',
                 data_request_file = 'data_request_metadata.csv',
                 delegate_to=None,
                 *args,
                 **kwargs
                 ):

        super(anon_metaDataFileFetchOperator, self).__init__(*args, **kwargs)

        self.landing_project_conn_id = landing_project_conn_id
        self.anon_mapping_bucket = anon_mapping_bucket
        self.data_request_file = data_request_file
        self.delegate_to = delegate_to
        self.dialect = 'standard'


    def execute(self, context):

        landing_hook = GCSHook(
            google_cloud_storage_conn_id= self.landing_project_conn_id,
            delegate_to=self.delegate_to)

        mapping_stream = landing_hook.download(bucket=self.anon_mapping_bucket, object=self.data_request_file)
        logging.info("Data Request File downloaded")
        request_data = io.BufferedReader(io.BytesIO(mapping_stream))
        Execution_date = context['ds']
        print(Execution_date)
        EXEC_DATE = macros.ds_format(Execution_date, "%Y-%m-%d", "%m/%d/%Y")
        print(EXEC_DATE)
        request_df = pd.read_csv(request_data)
        request_df['reqSchedStart'] = pd.to_datetime(request_df['reqSchedStart'], format="%m/%d/%Y")
        request_df['reqSchedEnd'] = pd.to_datetime(request_df['reqSchedEnd'], format="%m/%d/%Y",errors='coerce')
        print(request_df['reqSchedEnd'].head(5))
        filter = (request_df['reqSchedStart'] <= "'"+EXEC_DATE+"'") & ((request_df['reqSchedEnd'] >= "'"+EXEC_DATE+"'") | (request_df['reqSchedEnd'].isnull()))
        request_df = request_df[filter ]
        print(request_df)
        logging.info("Completed Reading the data request into a dataframe")

        request_df.set_index("dataSubject", drop=True, inplace=True)

        #If there is no request skip downstream task else proceed with downstream task
        if len(request_df) == 0:
            logging.info("setting downstream task condition to skip")
            Variable.set('condition', False)
        else:
            dictionary = request_df.to_json(orient="index")
            Variable.set('data requirement', dictionary)
            logging.info("setting downstream task condition to proceed")
            Variable.set('condition', True)






