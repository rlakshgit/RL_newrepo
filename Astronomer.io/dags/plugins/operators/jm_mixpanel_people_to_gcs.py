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
import math
from pandas.io.json import json_normalize
from common.dq_common import dq_common

class MixpanelPeopleToGoogleCloudStorageOperator(BaseOperator):
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
                 google_cloud_storage_connection_id,
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

        super(MixpanelPeopleToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)

        self.target_env = target_env
        self.project = project
        self.target_gcs_bucket = target_gcs_bucket
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_connection_id = google_cloud_storage_connection_id
        self.delegate_to = delegate_to
        self.airflow_var_set = airflow_var_set
        self.history_check = history_check
        self.base_gcs_folder = base_gcs_folder
        self.source_abbr = source_abbr
        self.source = source
        self.source_count = source_count
        self.airflow_schema_var_set = airflow_schema_var_set


    def execute(self, context):

        logging.info('start processing')

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        # set history check default by true
        # if no file exists send request to API
        # if file exists get the latest file and reprocess/normalize it.
        logging.info('History check ..')
        if self.history_check.upper() == 'TRUE':
            if self._l1_data_check(context) != 'NONE':
                logging.info('File present in L1 ...')
                file_list = self._l1_data_check(context).split(',')
                logging.info("Files to be processed are ..")
                for f in file_list:
                   
                    logging.info('Processing the following file: ', f)
                    self._process_from_l1_gcs(context, f)
                return 0

        #Initialize variables for API request - fronmDate,toDate,endpoint for API request

        payload = {'from_date': context['ds'], 'to_date': context['tomorrow_ds']}
        logging.info('getting data for' + str(payload))
        self.fromDate = '"' + context.get("ds") + 'T00:00:00"'
        self.toDate = '"' + context['tomorrow_ds'] + 'T00:00:00"'
        self.endpoint = 'api/2.0/engage?selector=((properties["$last_seen"]>=datetime({0}))and(properties["$last_seen"]<datetime({1})))'.format(
            self.fromDate, self.toDate)
        # calling function to send request to API
        df = self._call_api_return_df(self.endpoint, self.api_connection_id)
        print('printing the data frame')
        print(df)
        #uploading the response to GCS by calling upload_df_to_gcs
        write_return = self._upload_df_to_gcs(df, context, file_dt)

        if write_return == -1:
            logging.info('No data present ending further processing...')
            Variable.set(self.source + '_gcs_to_bq', 'False')
            return -1
    # _call_api_return_df() function send request to API endpoint and convert the response into list
    def _call_api_return_df(self,endpoint, http_con_id):
        http = HttpHook(method='GET', http_conn_id=http_con_id)

        retry_count = 0
        while True:
            try:
                restResponse = http.run(endpoint=endpoint)
                break
            except:
                if retry_count > 5:
                    print('URL response failed....Failing the TASK')
                    raise
                retry_count += 1

        json_data = str(restResponse.text).replace('\n', '')
        print('json_data')
        print(json_data)
        """Each request sends 1000 records. To get all the records we have to pass the session id and page information along with the request iteratively.
        Getting session id and page number from the first request to pass it to the request iteratively"""

        session_id = str(json.loads(json_data)['session_id'])
        total = str(json.loads(json_data)['total'])
        #self.source_count = total
      
        num_iterator = (float)(int(total) / 1000.0)
        num_iterator = math.ceil(num_iterator)
        logging.info("Number of pulls" + str(num_iterator))
        final_list = []
        for i in range(int(num_iterator)):
            page = i
            params = {"session_id": session_id, 'page': page}
            restResponse = http.run(endpoint=self.endpoint, data=params)
            json_data_new = str( restResponse.text )
            json_data_response_new = json.loads(json_data_new)['results']
            print('printing json_data_response_new')
            print(json_data_response_new)
            get_data = [json.dumps(i) for i in json_data_response_new]
            [final_list.append(x) for x in get_data]
        self.source_count = len(final_list)
        return final_list


    # This function upload the response to GCS L1, Normalize the date, clean up the data and upload it to l1_norm folder
    #generate schema based on the l1_norm file and upload it to l1_schema folder
    #generate metadata for landing(count comparison between l1 and l1_norm) and upload metadat to l1 in GCS
    def _upload_df_to_gcs(self,list , context, file_dt):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)
        data_to_write = '\n'.join(list)
        file_name = '{base_gcs_folder}/{source_abbr}/{date_nodash}/l1_data_mp_people_{date_time}.json'.format(
            source_abbr=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            date_time=file_dt,
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data_to_write)
        json_normalized_df = self._upload_normalized_to_gcs(list, context)

        self._upload_metadata(context, json_normalized_df)

        self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

    def _upload_normalized_to_gcs(self, list_df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)

        if isinstance(list_df, list):

            df_new = pd.DataFrame()
            for i in list_df:
                data = json.loads(i)
                df = pd.DataFrame.from_dict([data])
                df_new = df_new.append(df)

            df_new = df_new.reset_index(drop=True)


        else:
            df_new = list_df
        json_normalized_df = self._normalize_data(df_new, context)

        file_name = '{base_gcs_folder}_norm/{source}/{date_nodash}/l1_data_mp_people.json'.format(
                                                                                    source=self.source_abbr.lower(),
                                                                                    date_nodash=context['ds_nodash'],
                                                                                    base_gcs_folder=self.base_gcs_folder
                                                                                        )
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=json_normalized_df.to_json(orient='records', lines='\n', date_format='iso'))

        return json_normalized_df

    def _upload_metadata(self, context, df):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)

        audit_df = pd.DataFrame()

        file_name = '{base_gcs_folder}/{source}/{date_nodash}/l1_metadata_mp_people.json'.format(
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
    #Below function download the data from l1 to reprocess it and upload to l1_norm, update the schema in l1_schema
    def _process_from_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)
        file_data = hook.download(self.target_gcs_bucket, object)

        file_stream = io.BufferedReader(io.BytesIO(file_data))

        df = pd.read_json(file_stream, orient='records', lines='\n')


        self.source_count = len(df)
        #print(self.source_count)

        json_normalized_df = self._upload_normalized_to_gcs(df, context)
        self._upload_metadata(context,json_normalized_df)
        del df
        self._upload_schema_to_gcs(json_normalized_df, context)

        return 0

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
    # Data transformation functions
    def _normalize_data(self,df,context):

        data_new = json_normalize(df['$properties'].apply(self._convert_to_dicts).tolist())
        logging.info("Normalization done")
        logging.info("Printing data new")
        logging.info(data_new)
        data_new.rename(columns={'City': 'city_2'}, inplace=True)
        data_new['$distinct_id'] = df['$distinct_id'].copy(deep=True)
        
        new_cols = []
        """Column name encoding as per big query acceptance criteria"""
        print(data_new.columns)
        for col in data_new.columns:
            print('Col name before:')
            print(col)
            col = col.lower()
            print('Col name after:')
            print(col)
            counter = 1
            col = col.replace('.', '_').replace('-', '_').replace(',', '_').replace('#', 'num').replace(' ', '_')
            col = col.replace('$', '').replace('/', '_').replace('?', '')
            col = col.replace('__', ':')
            col = col.replace(':_', '_')
            col = col.replace(':', '_')               
            print('printing data_new after append')    
            print(data_new)
            new_cols.append(col)
            print('printing new cols')
            print(new_cols)        
        data_new.columns = new_cols
        data_new.reset_index(drop=True, inplace=True)

        """Adding Time columns"""
        print('Printing batch_id')
        data_new['batch_id'] = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        print(data_new['batch_id'])
        data_new['load_timestamp'] = pd.to_datetime(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), errors='coerce')
        print('load_timestamp',data_new['load_timestamp'])
        data_new['time_central'] = pd.to_datetime(data_new['last_seen'])
        print('time_central',data_new['time_central'])
        data_new['time_utc'] = data_new['time_central']
        data_new.time_utc = data_new.time_central.dt.tz_localize('US/Central', ambiguous=True).dt.tz_convert(
            'UTC').dt.strftime('%Y-%m-%d %H:%M:%S')
        data_new['time_utc'] = pd.to_datetime(data_new['time_utc'], errors='coerce')
        data_new = self.column_group_cleanup(data_new)

        data_new = self.gcp_bq_data_encoding(data_new)

        data_new = self.convert_date_columns_to_timestamp(data_new)

        df1 = data_new.select_dtypes(include=[np.datetime64])

        print('returning data_new')
        return data_new

    def column_group_cleanup(self, df_in, target_col=[]):

        '''
        This function is a single instance of going this a dataframe and applying the appropriate functions.  This function
        reduces the number of loops of review for each dataframe.  The less the loops the faster the processing time.
        :param df_in: Souce dataframe in
        :param target_col: Any specific targed columns.  This aspect is not used yet, but should be a dictionary in the future
        :return: Output dataframe with the applied functions.
        '''
        for col in df_in.columns.values:

            try:
                df_in[col] = df_in[col].apply(self.col_data_special_remove)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.strip_leading_quotes)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.none_from_nan)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.strip_newline)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.comma_space_strip)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.strip_single_quotes)
            except:
                pass

            if col == 'state_province':
                try:
                    df_in[col] = df_in[col].apply(self.state_abbreviate_test)
                except:
                    pass

        return df_in
    def gcp_bq_data_encoding(self, df_in):
        '''
        This function is to prepare the data for the uploading to a GCP Big Query Table.  Per documentation the
        GCP BQ Table can only support UTF-8 format
        :param df_in: This is the input dataframe to be converted to UTF-8
        :return: The dataframe with all target fields converted to UTF-8 except for time fields.
        '''

        def string_test(x):
            '''
            This function is to strip the "b '" that is prefixed in all string fields as a part of the converstion
            to UTF-8
            :param x: This is the value of each item for the apply.
            :return: Returns the scrubed data value that is uploaded into the appropriate cell location.
            '''

            if str(x)[-1] == "'":
                x_test = str(x)[:-1]
            else:
                x_test = str(x)
            if x_test[:2] == "b'":
                return x_test[2:]
            elif x_test[:2] == 'b"':
                return x_test[2:]
            else:
                return x_test

        try:
            df_in['time_central'] = pd.to_datetime(df_in['time_central'])
            df_in['time_utc'] = pd.to_datetime(df_in['time_utc'])

        except:
            pass
        for col in df_in.columns:
            try:
                if 'time_' in col or 'last_seen' in col:
                    continue
            except:
                pass

            try:
                df_in[col] = df_in[col].astype(str)
                df_in[col] = df_in[col].str.encode('utf-8')
                df_in[col] = df_in[col].apply(string_test)
            except:
                pass
        return df_in
    #This function identify the columns containing Dates based on Regex pattern and convert it to TIMESTAMP datatype
    def convert_date_columns_to_timestamp(self,df):

        exception_list = ['age_itemssummary_itembrands','age_itemssummary_itempurchaseorappraisaldates',
                          'age_itemssummary_itemserialnumbers']
        check_list=[]
        for i in range(0,len(df.columns)):
            for j in range(0,len(df.columns)):
                if(df.columns[i]==df.columns[j] and i!=j):
                    check_list.append(df.columns[i])
        check_list=list(set(check_list))
        df_dup = pd.DataFrame()
        df_dup = df
        df = df.loc[:,~df.columns.duplicated()].copy()
        for x in df_dup.columns:
            if x in check_list:
                df_1 = pd.DataFrame()
                df_2 = pd.DataFrame()
                df_1_dup = pd.DataFrame()
                df_1 = df_dup[x]
                df_2 = df_dup[x]
                indexer=check_list.index(x)
                check_list[indexer]= '0'
                new_df= pd.DataFrame()
                df_1 = df_1.iloc[:, 0]
                df_2 = df_2.iloc[:, 1]
                df_1_list=[]
                df_2_list=[]
                new_list=[]
                df_2=df_2.to_frame()
                df_2_list=[item for sublist in df_2.values for item in sublist]
                df_1=df_1.to_frame()
                df_1_list=[item for sublist in df_1.values for item in sublist]
                for i in range(0,len(df_1_list)):
                    if(df_2_list[i]=='nan'):
                        new_list.append(df_1_list[i])
                    elif(df_1_list[i]!='nan' and df_2_list[i]!='nan'):
                        new_list.append(df_1_list[i])
                    elif(df_2_list[i]!='nan'  and (df_1_list[i]=='nan' or df_1_list[i]=='' or df_1_list[i]=='None')):
                        new_list.append(df_2_list[i]) 
                se=pd.Series(new_list)
                df_1_dup[x]=se.values              
                df_2.drop_duplicates(subset=x, inplace=True)
                new_df= pd.merge(df_1_dup,df_2,how='left',on=x)
                new_df= new_df.iloc[:,0]
                df[x]=new_df  
            if x.startswith('age_'):
                if x not in exception_list:
                    df[x] = df[x].astype(str)
                    continue
            if df[x].astype(str).str.match(
                    r'\d{4}-\d{2}-\d{2}\w?|(None)').all():
                df[x] = pd.to_datetime(df[x], errors='coerce')

        # data = [pd.to_datetime(df[x], errors='coerce') if df[x].astype(str).str.match( r'\d{4}-\d{2}-\d{2}\w?|(None)').all() else df[x] for x in df.columns]
        # df = pd.concat(data, axis=1, keys=[s.name for s in data])
        return df
    def col_data_special_remove(self, x):
        '''
        This is a an apply support function for removal of special characters
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if 'http' in str(x):
            return x
        else:
            try:
                x = str(x).strip('$').replace('[]', '').replace("['", '').replace("']", '').replace("[", '').replace(
                    "]", '')
                x = x.replace("', '", ',').strip(' ').replace("','", ',').strip('\r').strip('\n')
                return x
            except:
                return x
    def strip_leading_quotes(self,x):
        '''
        This is a an apply support function for removal of double quotes
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replce('"','')
    def none_from_nan(self,x):
        '''
        This is a an apply support function for converting all variations of None to NONE
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if str(x).upper() == 'NONE':
            return 'None'
        elif str(x).upper() == 'NAN':
            return 'None'
        elif str(x).upper() == 'NA':
            return 'None'
        elif str(x) == '-':
            return 'None'
        else:
            return x
    def strip_newline(self,x):
        '''
        This is a an apply support function for removing all newlines from a string
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace(r'\n', ' ')
    def comma_space_strip(self,x):
        '''
        This is a an apply support function for replace and occurances of ", " with only a comma
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace(', ', ',')
    def strip_single_quotes(self,x):
        '''
        This is a an apply support function for removing all single quotes from a string
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace("'", '')
    def state_abbreviate_test(self, x):
        '''
        This is a an apply support function for converting all spelled out states to the abbrievated values.
        :param x: This in the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if str(x) == 'nan':
            return x

        us_state_abbrev = {
            'Alabama': 'AL',
            'Alaska': 'AK',
            'Arizona': 'AZ',
            'Arkansas': 'AR',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'Delaware': 'DE',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Hawaii': 'HI',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Iowa': 'IA',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Maine': 'ME',
            'Maryland': 'MD',
            'Massachusetts': 'MA',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Mississippi': 'MS',
            'Missouri': 'MO',
            'Montana': 'MT',
            'Nebraska': 'NE',
            'Nevada': 'NV',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'New York': 'NY',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Vermont': 'VT',
            'Virginia': 'VA',
            'Washington': 'WA',
            'West Virginia': 'WV',
            'Wisconsin': 'WI',
            'Wyoming': 'WY',
            'British Columbia': 'BC',
            'Ontario': 'ON',
        }
        mapping_df = pd.DataFrame.from_dict(us_state_abbrev, orient='index')
        mapping_df.reset_index(drop=False, inplace=True)
        mapping_df.columns = ['fullname', 'shorthand']
        if x in mapping_df['shorthand'].values.tolist(): return x
        try:
            if len(x) == 2: return x
        except:
            return x
        if x not in mapping_df['fullname'].values.tolist(): return x

        target_df = mapping_df[(mapping_df['fullname'] == x)]
        try:
            target_df.reset_index(drop=True, inplace=True)
            return target_df.loc[0, 'shorthand']
        except:
            return x

    # function to check if the data is already present in l1. if present this function will return the latest file based on timestamp in the file name
    def _l1_data_check(self, context, return_type='LAST'):
        hook = GCSHook(
                                        google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
                                        delegate_to=self.delegate_to
                                     )

        file_prefix = '{base_gcs_folder}/{source}/{date_nodash}/l1_data_mp_'.format(
                                                 source=self.source_abbr.lower(),
                                                 date_nodash=context['ds_nodash'],
                                                 base_gcs_folder=self.base_gcs_folder
                                                                                 )



        #Getting List of files from the folder
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
    def _upload_schema_to_gcs(self, json_normalized_df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df, context)

        file_name = '{base_gcs_folder}_schema/{source}/{date_nodash}/l1_schema_mp_people.json'.format(
            source=self.source_abbr.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return
    #This function generate schema by reading the data from file in l1_norm and return schema in a list
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
