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
from plugins.operators.jm_gcs import GCSHook
#from plugins.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_bq_hook import BigQueryHook

import logging
import pandas as pd
import re


class AlightDigitalDashboardEventCountSummaryOperator(BaseOperator):

    template_fields = ('partition_date', )
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 partitionDate,
                 project,
                 source_dataset,
                 source_table,
                 destination_dataset,
                 destination_table,
                 google_cloud_storage_conn_id,
                 google_bq_conn_id,
                 *args,
                 **kwargs):
        super(AlightDigitalDashboardEventCountSummaryOperator, self).__init__(*args, **kwargs)
        self.partition_date = partitionDate
        self.project = project
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.google_bq_conn_id = google_bq_conn_id
        self.gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_bq_conn_id)
        self.source_dataset = source_dataset
        self.source_table = source_table
        self.destination_dataset = destination_dataset
        self.destination_table = destination_table

    def execute(self, context):

        schema_list = dict(
            campaign='STRING',
            campaign_id='STRING',
            site='STRING',
            date='TIMESTAMP',
            impressions='INTEGER',
            placement='STRING',
            placement_size='STRING',
            placement_other='STRING',
            creative='STRING',
            creative_campaign='STRING',
            creative_size='STRING',
            creative_other='STRING',
            clicks='INTEGER',
            mediacost='FLOAT',
            time_central='TIMESTAMP',
            funnel_event='STRING',
            funnel_event_count='INTEGER',
        )

        schema_list_bq = [
            {"name": "campaign", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "site", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "impressions", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "placement", "type": "STRING", "mode": "NULLABLE"},
            {"name": "placement_size", "type": "STRING", "mode": "NULLABLE"},
            {"name": "placement_other", "type": "STRING", "mode": "NULLABLE"},
            {"name": "creative", "type": "STRING", "mode": "NULLABLE"},
            {"name": "creative_campaign", "type": "STRING", "mode": "NULLABLE"},
            {"name": "creative_size", "type": "STRING", "mode": "NULLABLE"},
            {"name": "creative_other", "type": "STRING", "mode": "NULLABLE"},
            {"name": "clicks", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "mediacost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "time_central", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "funnel_event", "type": "STRING", "mode": "NULLABLE"},
            {"name": "funnel_event_count", "type": "INTEGER", "mode": "NULLABLE"},
        ]

        # Build a sql query to gather the required data for the dashboard

        ssql = "select campaign, campaign_id, site, report_date as date, impressions, placement, creative, clicks, mediacost, insert_date as time_central, " \
               "jmi_full_funnel_jmi_insurance_quote_1_apply_view_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_view_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_9_app_confirm_view_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_1_apply_click_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_click_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_9_app_confirm_click_through_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_1_apply_total_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_total_conversions, " \
               "jmi_full_funnel_jmi_insurance_quote_9_app_confirm_total_conversions " \
               "from {sourcetable} where insert_date >= '{starttime}' and insert_date <= '{endtime}'". \
            format(sourcetable=self.source_table,
                   starttime=str(pd.to_datetime(self.partition_date)),
                   endtime=str(self.partition_date + ' 23:59:59'))
        logging.info(
            'Fetching data from BigQuery for {startdate} to {enddate}'.format(startdate=str(pd.to_datetime(self.partition_date)),
                                                                              enddate=str(self.partition_date + ' 23:59:59')))
        logging.info(ssql)
        df_bqt = self.gcp_bq_hook.get_pandas_df(ssql)

        if len(df_bqt) == 0:
            logging.info('Fetching data from BigQuery ... COMPLETE')
            logging.info('No data found from BigQuery ...')
            return

        logging.info('Fetching data from BigQuery ... COMPLETE')

        df_bqt['time_central'] = pd.to_datetime(df_bqt['time_central'])

        logging.info('Beginning transformation ...')

        # Rename the funnel fields.  'ct' = click through, 'vt' = view through
        logging.info(' ... renaming funnel fields ')
        df_bqt.rename(columns={
            'jmi_full_funnel_jmi_insurance_quote_1_apply_view_through_conversions': 'vt_1_apply_conversions',
            'jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_view_through_conversions': 'vt_4_app_start_applicant_conversions',
            'jmi_full_funnel_jmi_insurance_quote_9_app_confirm_view_through_conversions': 'vt_9_app_confirm_conversions',
            'jmi_full_funnel_jmi_insurance_quote_1_apply_click_through_conversions': 'ct_1_apply_conversions',
            'jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_click_through_conversions': 'ct_4_app_start_applicant_conversions',
            'jmi_full_funnel_jmi_insurance_quote_9_app_confirm_click_through_conversions': 'ct_9_app_confirm_conversions',
            'jmi_full_funnel_jmi_insurance_quote_1_apply_total_conversions': 'tot_1_apply_conversions',
            'jmi_full_funnel_jmi_insurance_quote_4_app_start_start_applicant_total_conversions': 'tot_4_app_start_applicant_conversions',
            'jmi_full_funnel_jmi_insurance_quote_9_app_confirm_total_conversions': 'tot_9_app_confirm_conversions'
        }, inplace=True)

        logging.info(' ... parsing placement field ...')
        df_bqt[['placement_size', 'placement_other']] = df_bqt['placement'].apply(self._dq_support_alight_placement)

        # Parse the creative field into 3 parts: campaign, size, and the rest (other) using dq_support_alight_creative support function
        logging.info(' ... parsing creative field ... ')
        df_bqt[['creative_campaign', 'creative_size', 'creative_other']] = df_bqt['creative'].apply(self._dq_support_alight_creative)

        # Transpose the data by funnel event to make the data long instead of wide
        logging.info(' ... transposing data by funnel event ... ')
        df_bqt_long = pd.melt(df_bqt, id_vars=['campaign', 'campaign_id', 'site', 'date', 'impressions', 'placement',
                                               'placement_size', 'placement_other',
                                               'creative', 'creative_campaign', 'creative_size', 'creative_other',
                                               'clicks',
                                               'mediacost', 'time_central'],
                              var_name='funnel_event', value_name='funnel_event_count',
                              value_vars=['vt_1_apply_conversions', 'vt_4_app_start_applicant_conversions',
                                          'vt_9_app_confirm_conversions', 'ct_1_apply_conversions',
                                          'ct_4_app_start_applicant_conversions',
                                          'ct_9_app_confirm_conversions', 'tot_1_apply_conversions',
                                          'tot_4_app_start_applicant_conversions', 'tot_9_app_confirm_conversions'])

        # Set the schema
        logging.info(' ... setting schema ... ')
        logging.info(schema_list)
        for key, val in schema_list.items():
            if key in df_bqt_long.columns:
                if val.upper() == "INTEGER":
                    df_bqt_long[key] = pd.to_numeric(df_bqt_long[key], errors='coerce').fillna(0).astype('int64')
                elif val.upper() == "FLOAT":
                    df_bqt_long[key] = pd.to_numeric(df_bqt_long[key], errors='coerce').fillna(0.0).astype('float64')
                elif val.upper() == "TIMESTAMP":
                    df_bqt_long[key] = pd.to_datetime(df_bqt_long[key], errors='coerce').astype('datetime64[ns]')
                elif val.upper() == "STRING":
                    df_bqt_long[key] = df_bqt_long[key].astype(str)
            else:
                if val.upper() == "INTEGER":
                    df_bqt_long[key] = 0
                elif val.upper() == "FLOAT":
                    df_bqt_long[key] = 0.0
        logging.info(df_bqt_long.dtypes)

        logging.info(' ... transformation complete!')

        try:
            logging.info('Starting dataset creation...')
            self.gcp_bq_hook.create_empty_dataset(dataset_id=self.destination_dataset, project_id=self.project)
            logging.info(' ... dataset creation complete!')
        except:
            logging.info('Dataset already exists!')
            pass

        if self.gcp_bq_hook.table_exists(project_id=self.project, dataset_id=self.destination_dataset,
                                         table_id=self.destination_table):
            logging.info('Destination table {} already exists!'.format(self.destination_table))
        else:
            self.gcp_bq_hook.create_empty_table(project_id=self.project, dataset_id=self.destination_dataset,
                                                table_id=self.destination_table,
                                                schema_fields=schema_list_bq,
                                                time_partitioning={'type': 'DAY'})
            logging.info('Destination table {} created!'.format(self.destination_table))

        if not df_bqt_long.empty:
            partition_table_name = '{destination}${partition}'.format(destination=str(self.destination_table),
                                                                      partition=str(self.partition_date.replace('-','')))
            logging.info('Partition table name is: {}'.format(partition_table_name))
            result = self.gcp_bq_hook.gcp_bq_write_table(df_bqt_long, self.project, self.destination_dataset, partition_table_name,
                                                         'Append', False)
        logging.info('Big Query Table Operation ....COMPLETE')

        return


    def _dq_support_alight_placement(self, x):
        '''
        This is an apply support function for parsing the placement field into 2 child fields: placement_size, placement_other
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be a series of 2 appropriate parsed transformations.
        '''

        if x == 'None':
            return pd.Series([None, None])
        else:
            try:
                test_x = re.search('\d+x\d+', str(x))
                if test_x:
                    p_size = test_x.group(0)
                else:
                    p_size = None
            except:
                p_size = None

            try:
                test_x = re.split(r'\d+x\d+', str(x))
                p_other = ''.join(test_x).replace(' _', '_').replace('_ ', '_').replace('_', ' ').rstrip('(').rstrip(
                    ' ').lstrip(')').lstrip(' ').replace('()', '')
            except:
                p_other = None

            return pd.Series([p_size, p_other])


    def _dq_support_alight_creative(self, x):
        '''
        This is an apply support function for parsing the creative field into 3 child fields: creative_campaign, creative_size, creative_other
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be a series of 3 appropriate parsed transformations.
        '''

        if x == 'None':
            return pd.Series([None, None, None])
        else:
            try:
                test_x = re.search('\d+x\d+', str(x))
                if test_x:
                    c_size = test_x.group(0)
                else:
                    c_size = None
            except:
                c_size = None

            try:
                test_x = re.split(r'\d+x\d+', str(x))
                test_x[0] = test_x[0].replace('_', ' ').rstrip('(').rstrip(' ').lstrip(')').lstrip(' ')
                if test_x[0]:
                    c_campaign = test_x[0]
                else:
                    c_campaign = None

                if len(test_x) > 1:
                    test_x[1] = test_x[1].replace('_', ' ').rstrip('(').rstrip(' ').lstrip(')').lstrip(' ')
                    if test_x[1]:
                        c_other = test_x[1]
                    elif test_x[0]:
                        c_other = test_x[0]
                    else:
                        c_other = None

                else:
                    if test_x[0]:
                        c_other = test_x[0]
                    else:
                        c_other = None

            except:
                c_campaign = None
                c_other = None

            return pd.Series([c_campaign, c_size, c_other])








