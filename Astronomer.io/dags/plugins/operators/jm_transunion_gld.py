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
# from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_gcs import GCSHook
# from plugins.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_bq_hook import BigQueryHook

import logging
import pandas as pd
import numpy as np


class TransunionGoldFileHitIndicatorOperator(BaseOperator):

    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 project,
                 source_dataset,
                 source_table,
                 destination_dataset,
                 destination_table,
                 google_cloud_storage_conn_id,
                 *args,
                 **kwargs):
        super(TransunionGoldFileHitIndicatorOperator, self).__init__(*args, **kwargs)
        self.project = project
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_storage_conn_id)
        self.source_dataset = source_dataset
        self.source_table = source_table
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table

    def execute(self, context):

        logging.info('Fetching data from BigQuery...')

        ssql = 'SELECT * FROM `{target_project}.{target_table}`'.format(target_project=self.project,
                                                       target_table=self.source_table)
        logging.info(ssql)
        df = self.gcp_bq_hook.get_pandas_df(ssql)
        print(df)
        logging.info('Fetching data from BigQuery ... COMPLETE')

        logging.info('Beginning transformation ...')
        target_columns = ['addOnProduct_scoreModel_characteristic']

        col_parse = {}
        first_found = True
        for col in df.columns:
            for target in target_columns:
                if col.startswith(target):
                    col_split = col.split('_')
                    count_value = col_split[len(col_split) - 2]
                    if first_found:
                        col_parse[target] = int(count_value)
                        first_found = False
                    else:
                        if int(count_value) > col_parse[target]:
                            col_parse[target] = int(count_value)

        df = df.apply(self._source_model_column, axis=1, args=(target_columns, col_parse,))
        col_list_keep = ['filename', 'transaction_ssid', 'request_timestamp', 'data_land_date', 'Hit_Results']
        for col in df.columns:
            if 'source_model_' in col:
                col_list_keep.append(col)

        df = df[col_list_keep]

        logging.info(' ... transformation complete!')

        try:
            logging.info('Starting dataset creation...')
            self.gcp_bq_hook.create_empty_dataset(dataset_id=self.destination_dataset, project_id=self.project)
            logging.info(' ... dataset creation complete!')
        except:
            logging.info('Dataset already exists!')
            pass

        if not df.empty:
            result = self.gcp_bq_hook.gcp_bq_write_table(df, self.project, self.destination_dataset, self.destination_table, 'REPLACE', False)
        logging.info('Big Query Table Operation ....COMPLETE')

        return

    def _source_model_column(self, row, target_columns, col_parse):
        for target in target_columns:
            for i in range(col_parse[target] + 1):
                get_attribute_value = row[target + '_' + str(i) + '_algorithmID']
                get_id_label = row[target + '_' + str(i) + '_id']
                get_id_value = row[target + '_' + str(i) + '_value']
                try:
                    row['source_model_' + get_id_label + '_algorithmID'] = get_attribute_value
                    row['source_model_' + get_id_label] = get_id_value
                except:
                    pass

        row['Hit_Results'] = np.where(row['product_subject_subjectRecord_fileSummary_fileHitIndicator'] == 'regularHit',
                                      'Hit', 'No Hit')

        return row



