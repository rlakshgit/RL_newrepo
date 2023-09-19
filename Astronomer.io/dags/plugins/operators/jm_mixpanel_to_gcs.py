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
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import sys
import io
import json
import logging
import pandas as pd
import datetime as dt
import requests
import base64
from plugins.operators.jm_gcs import GCSHook
import re
import urllib
import urllib.parse
import numpy as np
from pandas.io.json import json_normalize
from common.dq_common import dq_common
# from flatten_json import flatten
from time import sleep


class MixpanelToGoogleCloudStorageOperator(BaseOperator):
    """
    Grab data from mixpanle and ingest it into GCS
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param project: The project to upload to.
    :type bucket: str
    :param fromDate: The date from which to grab data.
    :type bucket: str
    :param toDate: The date to which to grab data.
    :type bucket: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    """

    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,

                 project,
                 google_cloud_storage_conn_id,
                 target_gcs_bucket,
                 target_env,
                 api_connection_id,
                 history_check,
                 base_gcs_folder,
                 source,
                 source_abbr,
                 source_count=0,
                 approx_max_file_size_bytes=1900000000,
                 delegate_to=None,
                 airflow_var_set='NA',
                 airflow_schema_var_set='NA',
                 *args,
                 **kwargs):

        super(MixpanelToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)

        self.target_env = target_env
        self.project = project
        self.target_gcs_bucket = target_gcs_bucket
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.airflow_var_set = airflow_var_set
        self.history_check = history_check
        self.base_gcs_folder = base_gcs_folder
        self.source_abbr = source_abbr
        self.source = source
        self.source_count = source_count
        self.airflow_schema_var_set = airflow_schema_var_set
        # self.http_hook = HttpHook(method='GET',
        #                           http_conn_id=self.api_conn_id)
        # self.gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)

    def execute(self, context):
        logging.info('start processing')

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        if self.history_check.upper() == 'TRUE':
            logging.info("rlaksh 0823: ", self.history_check.upper())
            logging.info("rlaksh 0823: value ", self.google_cloud_storage_conn_id)
            if self._l1_data_check(context, 'LAST', self.google_cloud_storage_conn_id) != None :
                logging.info("rlaksh 0823: inside then 110 ", self.google_cloud_storage_conn_id )
                file_list = self._l1_data_check(context, 'LAST', self.google_cloud_storage_conn_id).split(',')
                for f in file_list:
                    logging.info('Processing the following file: ', f)
                    self._process_from_l1_gcs(context, f)
                return 0
            else:
                # Attempt to pull from PROD
                if self._l1_data_check(context, 'LAST', 'prod_edl') != 'NONE':
                    logging.info("rlaksh 0823: inside else ", self.google_cloud_storage_conn_id )
                    file_list = self._l1_data_check(context, 'LAST', 'prod_edl').split(',')
                    for f in file_list:
                        logging.info('Processing the following file: ', f)
                        self._process_from_prod_l1_gcs(context, f)
                    return 0

        payload = {'from_date': context['ds'], 'to_date': context['ds']}

        logging.info('getting data for' + str(payload))

        df = self._call_api_return_df('', payload, self.api_connection_id)
        write_return = self._upload_df_to_gcs(df, context, file_dt)

        if write_return == -1:
            logging.info('No data present ending further processing...')
            Variable.set(self.source + '_gcs_to_bq', 'False')
            return -1

    def _dq_support_url_parsing(self, x):
        '''
        This is an apply support function for parsing the current_url field into child fields
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be a series of appropriate parsed transformations.
        '''

        this_dict = {}
        x = re.sub('&amp;', '&', x)
        test_x = x.split('?')
        if len(test_x) > 1:
            p_email_params = test_x[1].split('&')
            if len(p_email_params) > 0:
                for p in p_email_params:
                    if '=' in str(p):
                        p_parts = p.split('=')
                        if len(p_parts) > 1:
                            parsed_p_part = re.sub(r'[^A-Za-z0-9\_]', '', p_parts[0])
                            new_col_name = 'current_url_' + parsed_p_part
                            if len(new_col_name) > 128:
                                new_col_name_map = 'current_url_extended_paramaters'
                                if new_col_name_map.lower() not in list(this_dict.keys()):
                                    this_dict[new_col_name_map.lower()] = new_col_name
                                new_col_name = '_current_url_extended_parameters_value'

                            if new_col_name.lower() not in list(this_dict.keys()):
                                this_dict[new_col_name.lower()] = p_parts[1]

        return pd.Series(this_dict)

    def _urlencode(self, x):
        return urllib.parse.quote(str(x))

    def _convert_to_dicts(self, ld):
        if type(ld) == list:
            if str(ld) == '[]':
                return {}
            for item in ld:
                if type(item) == dict:
                    return item
                if type(item[0]) != list:
                    return item[0]
        else:
            return ld

    def _call_api_return_df(self, url, payload, http_con_id):
        http = HttpHook(method='GET', http_conn_id=http_con_id)

        retry_count = 0
        while True:
            try:
                print('*' * 20)
                print(url, payload)
                print('*' * 20)
                restResponse = http.run(endpoint=url, data=payload)
                break
            except:
                if retry_count > 5:
                    print('URL response failed....Failing the TASK')
                    print(url, payload)
                    print('*' * 20)
                    raise
                retry_count += 1
                sleep(1)

                # restResponse = http.run(url)
        restResponseDecoded = restResponse.iter_lines(decode_unicode=True)
        get_data = []
        counter = -1
        line_retain = ''
        for line in restResponseDecoded:
            counter += 1
            if line.endswith('}'):
                line = line_retain + line
                get_data.append(json.loads(line))
                line_retain = ''
                self.source_count += 1
            else:
                line_retain = str(line)

        df = pd.DataFrame.from_records(get_data)

        return df

    def _normalize_data(self, df, context):
        dq = dq_common()

        # file_date_time = pd.to_datetime(context['ds_nodash'], format='%Y%m%d').strftime('%Y-%m-%d')

        data = json_normalize(df['properties'].apply(self._convert_to_dicts).tolist())
        # only for testing
        # data = data.head(1000)

        logging.info('normalization done')
        data.rename(columns={'City': 'city_2'}, inplace=True)
        # print(df.head(5))

        data['event'] = df['event'].copy(deep=True)
        new_cols = []
        for col in data.columns:
            col = col.lower()
            col = col.replace('.', '_').replace('-', '_').replace(',', '_').replace('#', 'num').replace(' ', '_')
            col = col.replace('$', '').replace('/', '_').replace('?', '')
            col = col.replace('__', ':')
            col = col.replace(':_', '_')
            col = col.replace(':', '_')
            col = re.sub(r"[^a-zA-Z0-9_:$__/.?-]+", '_', col)
            new_cols.append(col)

        data.columns = new_cols
        data.reset_index(drop=True, inplace=True)
        parsed_data = data['current_url'].apply(self._dq_support_url_parsing)
        data = pd.concat([data, parsed_data], axis=1)

        data['batch_id'] = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        data['load_timestamp'] = pd.to_datetime(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), errors='coerce')
        data['time_central'] = pd.to_datetime(data['time'], unit='s', errors='coerce')

        data['time_utc'] = data['time_central']
        data.time_utc = data.time_central.dt.tz_localize('US/Central', ambiguous=True).dt.tz_convert(
            'UTC').dt.strftime('%Y-%m-%d %H:%M:%S')

        logging.info('added time columns')
        data = dq.dq_column_group_cleanup(data)
        logging.info('rlaksh 0823 : line 263 : ', type(data))
        logging.info('clean up done')
        data = dq.gcp_bq_data_encoding(data)
        return data

    def _l1_data_check(self, context, return_type='LAST', base_con='NA'):
        hook = GCSHook(
            google_cloud_storage_conn_id=base_con,
            delegate_to=self.delegate_to)
        
        logging.info("rlaksh 0823: inside _l1_data_check - base_con ", base_con )
        if base_con == 'prod_edl':
            logging.info("rlaksh 0823: inside _l1_data_check - base_con prod-edl ", base_con )
            file_prefix = '{base_gcs_folder}/{source}/{date_nodash}/mp_'.format(
                source=self.source_abbr.lower(),
                date_nodash=context['ds_nodash'],
                base_gcs_folder='l1')
            target_bucket = 'jm_prod_edl_lnd'
        else:
            file_prefix = '{base_gcs_folder}/{source}/{date_nodash}/l1_data_mp_'.format(
                source=self.source_abbr.lower(),
                date_nodash=context['ds_nodash'],
                base_gcs_folder=self.base_gcs_folder)
            target_bucket = self.target_gcs_bucket

        # file_list = hook.list(self.target_gcs_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        file_list = hook.list(target_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        if (len(file_list) == 0) and (target_bucket == 'jm_prod_edl_lnd'):
            print('Failed to find data in l1\mpcoded looking at mixpanel_interval')
            file_prefix = file_prefix.replace('mp_coded', 'mixpanel_interval')
            file_list = hook.list(target_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')

        print('file_list')
        print(file_list)
        if len(file_list) == 0:
            return 'NONE'
        elif len(file_list) == 1:
            return file_list[0]
        else:
            df = pd.DataFrame()
            for f in file_list:
                df_len = len(df)
                df.loc[df_len, 'filename'] = f
                df.loc[df_len, 'filename_base'] = f.replace('.json', '')

            # df['counter_value'] = df.filename_base.str.split('_').str[-1]
            df['file_dt'] = df.filename_base.str.split('_').str[-1]
            df['file_datetime'] = pd.to_datetime(df['file_dt'], format='%Y%m%d%H%M%S')

            if return_type == 'FIRST':
                df.sort_values(by=['file_datetime'], ascending=[True], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]
            else:
                df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]

            return ','.join(df['filename'].values.tolist())

        return 'NONE'

    def _l1_norm_data_check(self, context, return_type='LAST'):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_prefix = '{base_gcs_folder}_norm/{source}/{date_nodash}/l1_data_mp_'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)

        file_list = []
        file_list = hook.list(self.target_gcs_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        if len(file_list) == 0:
            return 'NONE'
        elif len(file_list) == 1:
            return file_list[0]
        else:
            df = pd.DataFrame()
            for f in file_list:
                df_len = len(df)
                df.loc[df_len, 'filename'] = f
                df.loc[df_len, 'filename_base'] = f.replace('.json', '')

            # df['counter_value'] = df.filename_base.str.split('_').str[-1]
            df['file_dt'] = df.filename_base.str.split('/').str[-1]
            df['file_datetime'] = pd.to_datetime(df['file_dt'], format='%Y%m%d')
            if return_type == 'FIRST':
                df.sort_values(by=['file_datetime'], ascending=[True], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]
            else:
                df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]

            return ','.join(df['filename'].values.tolist())

        return 'NONE'

    def _schema_generator(self, df, context, default_type='STRING'):

        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            # print(column_name,dtype.kind)
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return_string = "[{fields}]".format(fields=",".join(fields))
        return json.dumps(fields)  # return_string

    def _upload_df_to_gcs(self, df, context, file_dt):
        if df.empty:
            print('Dataframe is empty.  Returning to main.....')
            return -1

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        json_data = df.to_json(orient='records', lines='\n', date_format='iso')
        file_name = '{base_gcs_folder}/{source_abbr}/{date_nodash}/l1_data_mp_cd_all_{date_time}.json'.format(
            source_abbr=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            date_time=file_dt,  # context['ts_nodash'],
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)
        del json_data

        json_normalized_df = self._upload_normalized_to_gcs(df, context)

        self._upload_metadata(context, json_normalized_df)
        del df
        self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

    def _upload_normalized_to_gcs(self, df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        json_normalized_df = self._normalize_data(df, context)

        exception_columns = ['link_text', 'button_text']
        for col in json_normalized_df.columns:
            if col in exception_columns:
                df_tmp = json_normalized_df[col].copy(deep=True)
                cols = []
                try:
                    for dup in json_normalized_df[col].columns.get_duplicates():
                        cols = [dup + '_' + str(d_idx) if d_idx != 0 else dup for d_idx in
                                range(len(json_normalized_df[col].columns))]

                    df_tmp.columns = cols
                    df_tmp = df_tmp.replace('Nan', np.nan).replace('NaN', np.nan)
                    df_tmp['new_col'] = df_tmp.apply(lambda x: ','.join(x[x.notnull()]), axis=1)
                    del json_normalized_df[col]
                    json_normalized_df[col] = df_tmp['new_col']
                except:
                    pass

        file_name = '{base_gcs_folder}_norm/{source}/{date_nodash}/l1_norm_mp_cd_all.json'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=json_normalized_df.to_json(orient='records', lines='\n', date_format='iso'))
        return json_normalized_df

    def _upload_schema_to_gcs(self, json_normalized_df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df, context)

        file_name = '{base_gcs_folder}_schema/{source}/{date_nodash}/l1_schema_mp_cd_all.json'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return

    def _process_from_prod_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id='prod_edl',
            delegate_to=self.delegate_to)

        target_bucket = 'jm_prod_edl_lnd'
        print('Attempting to load object======', object)
        ####
        file_data = hook.download(target_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        self._upload_df_to_gcs(df, context, file_dt)
        self.source_count = len(df)
        json_normalized_df = self._upload_normalized_to_gcs(df, context)
        self._upload_metadata(context, json_normalized_df)
        del df
        self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

    def _process_from_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_data = hook.download(self.target_gcs_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')

        self.source_count = len(df)

        json_normalized_df = self._upload_normalized_to_gcs(df, context)
        self._upload_metadata(context, json_normalized_df)
        del df
        self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

    def _upload_metadata(self, context, df):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        audit_df = pd.DataFrame()

        file_name = '{base_gcs_folder}/{source}/{date_nodash}/l1_metadata_mp_cd_all.json'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)

        file_exists = hook.exists(self.target_gcs_bucket, file_name)
        logging.info('file_exists value is ' + str(file_exists))

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': self.source_count, 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)

        del json_data
        del audit_df

        return