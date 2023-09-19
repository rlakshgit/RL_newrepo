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

import json
from bson.json_util import dumps
import decimal
from pandas.io.json import json_normalize
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.dq_common import dq_common
from plugins.operators.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
# from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from pandas.io.json import json_normalize
from airflow.hooks.base import BaseHook
import logging
import pymongo
from pymongo import MongoClient
import io
import pandas as pd
from datetime import datetime
from pandas import DataFrame
import datetime as dt
from flatten_json import flatten
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype




class Mongodbtogcs(BaseOperator):
    """
    Write Audit data for api from landing metadata and bigquery
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """

    ui_color = '#e0aFFc'

    template_fields = ('metadata_filename',
                       'audit_filename','base_gcs_folder')

    @apply_defaults
    def __init__(self,
                 project,
                 source,
                 source_abbr,
                 target_gcs_bucket,
                 mongo_db_conn_id,
                 entity,
                 database_name,
                 collection,
                 base_gcs_folder=None,
                 base_schema_folder=None,
                 base_norm_folder=None,
                 bucket=None,
                 history_check=True,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 metadata_filename='NA',
                 audit_filename='NA',
                 confJSON = None,
                 *args,
                 **kwargs):

        super(Mongodbtogcs, self).__init__(*args, **kwargs)

        self.bucket = bucket
        self.project = project
        self.source = source
        self.base_gcs_folder = base_gcs_folder
        self.entity = entity
        self.mongo_db_conn_id = mongo_db_conn_id
        self.target_gcs_bucket = target_gcs_bucket
        self.source_abbr = source_abbr
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.table_present = False
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.audit_filename = audit_filename
        self.database_name = database_name
        self.collection = collection
        self.base_schema_folder=base_schema_folder
        self.base_norm_folder = base_norm_folder
        self.schema_data_new = []
        self.pull_new = True
        self.confJSON = confJSON


    def execute(self, context):


        mongo_db_connection = BaseHook.get_connection(self.mongo_db_conn_id)
        self.CONNECTION_STRING = mongo_db_connection.password

        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        
        client = MongoClient(self.CONNECTION_STRING, uuidRepresentation="csharpLegacy")
        mydatabase = client[self.database_name]
        mycollection = mydatabase[self.collection]

        start_ds = context['ds_nodash']
        start_year = int(start_ds[:4])
        start_month = int(start_ds[4:6])
        start_day = int(start_ds[6:8])
        start_date = datetime(start_year,start_month,start_day)

        end_ds = context['tomorrow_ds_nodash']
        end_year = int(end_ds[:4])
        end_month = int(end_ds[4:6])
        end_day = int(end_ds[6:8])
        end_date = datetime(end_year,end_month,end_day)
        
        logging.info(f'LANDING - {self.entity} - DATA FOR {start_date} TO {end_date}')

        response = mycollection.find({'$and': [{"ModifiedOn": {'$gte': start_date}},{"ModifiedOn": {'$lt': end_date}},{"EntityType": 'orderrequest'}]})
        #response = mycollection.find({'$and': [{"ModifiedOn": {'$lte': start_date}},{"EntityType": 'orderrequest'}]})
        #response = mycollection.find({'$or': [{"RequestId":"O9LXT04"}, {"RequestId":"OX0W5C6"}]})
        logging.info('REQUEST SENT')    

        # convert the response to list 
        list_data = list(response)
        
        # get list of nested arrays from conf file        
        arrays_list = self.confJSON[self.entity]["arrays_list"]

        # get parent table pk
        parent_pk = self.confJSON[self.entity]["parent_pk"]
        # Extract, Flatten and Upload Nested Arrays 
        for array in arrays_list:
            if len(array) == 2:
                array_name = '_' + array[0] + '_' + array[1]
                logging.info(f'EXTRACING {array_name} FROM OBJECT')
                array_df, source_count = self._flatten_array_2(list_data, array, parent_pk)
            else:
                array_name = '_' + array
                logging.info(f'EXTRACING {array_name} FROM OBJECT')
                array_df, source_count = self._flatten_array(list_data, array, parent_pk)

            logging.info(f'UPLOADING {array_name} FILE TO GCS')
            write_return = self._upload_normalized_data_gcs(array_df, context, array_name=array_name)
            row_count = len(array_df)
            logging.info(f'UPLOADING {array_name} METADATA TO GCS')
            self._metadata_upload_child(context, row_count, source_count, check_landing=True, array_name=array_name)

            items_df = pd.DataFrame(list_data)
            source_count = len(items_df)
            logging.info("Uploading Raw data to GCS...")
            self._upload_raw_data_gcs(items_df.to_json(orient='records', lines='\n', date_format='iso',default_handler=str), context)
       
        logging.info("Flattening the dataset..")
        df_norm = DataFrame()
        #list_of_dicts = items_df.to_dict('records')
        for dict in list_data:
            flat_dict = flatten(dict)
            flat_df = json_normalize(flat_dict)
            df_norm = df_norm.append(flat_df, ignore_index=True)

        logging.info("Uploading Normalized data to GCS...")
        self._upload_normalized_data_gcs(df_norm, context)

        logging.info("Uploading Metadata to GCS....")
        if self.metadata_filename is not None:
            self._metadata_upload(context, source_count, len(df_norm))



    def _flatten_array(self, response, array, parent_pk):
        combined_df = pd.DataFrame()

        source_count = 0
        for obj in response:
            object_df = pd.DataFrame()
            try:
                array_name = obj[array]
                for i in array_name:
                    source_count+=1
                    flat_data = flatten(i)
                    df = pd.json_normalize(flat_data)
                    object_df = pd.concat([df, object_df])
                    object_df[parent_pk] = obj[parent_pk]
     
                combined_df = pd.concat([object_df, combined_df])
                combined_df.reset_index(inplace=True, drop=True)

            except:
                logging.warning(f'{array} Object NOT Available in Document') 
                source_count = 0

        logging.info(f'RETURNING ARRAY DF WITH {len(combined_df)} RECORDS')        
        return combined_df, source_count

    def _flatten_array_2(self, response, array, parent_pk):
        combined_df = pd.DataFrame()

        source_count = 0
        for obj in response:
            object_df = pd.DataFrame()
            print('Object inside loop flatten_2', obj, '  array', array[0],' and ', array[1])
            array_name = obj[array[0]][array[1]]
            print('Array_name', array_name)
            for i in array_name:
                source_count+=1
                flat_data = flatten(i)
                df = pd.json_normalize(flat_data)
                object_df = pd.concat([df, object_df])
                object_df[parent_pk] = obj[parent_pk]

            combined_df = pd.concat([object_df, combined_df])
            combined_df.reset_index(inplace=True, drop=True)


        print('--- checking for datetime fields')
        for col in combined_df:
            print(col)
            if 'loggedinon' in col.lower():
                print(f'{col}: add new fields')
                new_date_field = col+'_date'
                new_time_field = col+'_time'
                combined_df[new_date_field] = pd.to_datetime(combined_df[col].astype(str))
                combined_df[new_date_field] = combined_df[new_date_field].dt.strftime('%Y-%m-%d')
                combined_df[new_time_field] = pd.to_datetime(combined_df[col].astype(str))
                combined_df[new_time_field] = combined_df[new_time_field].dt.strftime('%H:%M:%S.%f')
        
        return combined_df, source_count


    def _upload_raw_data_gcs(self, data, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{collection}/{date_nodash}/l1_data_{source}_{collection}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            collection=self.collection)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data)
        print('json file name', file_name)
        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        file_name = '{base_gcs_folder}{entity}/{date_nodash}/l1_data_'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=file_name)
        logging.info("Files are - {files}".format(files=base_file_list))
        if len(base_file_list) == 0:
            return  pd.DataFrame(),True,0
        else:

            logging.info("Inside History check")

            for f in base_file_list:
                file_data = gcs_hook.download(self.target_gcs_bucket, f)
                file_stream = io.BufferedReader(io.BytesIO(file_data))
                df = pd.read_json(file_stream, orient='records', lines='\n')
                print(df.head(5))

            return df,False,len(df)

    def _upload_normalized_data_gcs(self, df_in, context, counter=0, array_name=''):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_norm_folder}{collection}{array_name}/{date_nodash}/l1_norm_{source}_{collection}.json'.format(
            base_norm_folder=self.base_norm_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            collection=self.collection,
            array_name = array_name)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    #data=df_in.to_csv(index= False))
        data = df_in.to_json(orient='records', lines='\n', date_format='iso',default_handler=str))
        print('Flat data file', file_name)
        return


    def _metadata_upload(self, context, row_count, source_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        metadata_filename = '{base_folder}{collection}/{date_nodash}/l1_metadata_{source}_{collection}.json'.format(
            base_folder=self.metadata_filename,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            collection=self.collection)

        print('Metadata File - ', metadata_filename)
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return


    def _metadata_upload_child(self, context, row_count, source_count, check_landing=True,array_name=''):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
                          
        metadata_filename = '{base_folder}{entity}{array_name}/{date_nodash}/l1_metadata_{source}_{entity}{array_name}.json'.format(
            base_folder=self.metadata_filename,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity,
            array_name=array_name)

        print('Metadata File - ', metadata_filename)
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return



