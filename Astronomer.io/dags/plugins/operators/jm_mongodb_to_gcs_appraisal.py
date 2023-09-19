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
from lib2to3.refactor import get_all_fix_names
from unittest import expectedFailure
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
from functools import reduce



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

        #Get data from mongo db
        client = MongoClient(self.CONNECTION_STRING, uuidRepresentation="csharpLegacy")
        # client = MongoClient(self.CONNECTION_STRING)
        mydatabase = client[self.database_name]
        mycollection = mydatabase[self.collection]

        start_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        end_date = datetime.strptime(context['tomorrow_ds'], '%Y-%m-%d')
        logging.info(f'Incremental Data Load from {start_date} to {end_date}')

        response = mycollection.find({'$or':
                                            [	 
                                                {'$and': 
                                                    [
                                                        {'ModifiedOn': {'$gte': start_date}},
                                                        {'ModifiedOn': {'$lt': end_date}},      
                                                        {'EntityType':self.entity}
                                                    ]
                                                },
                                                {'$and': 
                                                    [
                                                        {'CreatedOn': {'$gte': start_date}},
                                                        {'CreatedOn': {'$lt': end_date}},      
                                                        {'EntityType':self.entity}
                                                    ]
                                                }
                                            
                                            ]
                                        })

        # get list of nested arrays from conf file        
        arrays_list = self.confJSON[self.entity]["arrays_list"]

        # convert the response to list 
        list_data = list(response)
        logging.info(f'RESPONSE RECEIVED WITH {len(list_data)} DOCUMENTS')

        # get parent table pk
        parent_pk = self.confJSON[self.entity]["parent_pk"]

        # Extract, Flatten and Upload Nested Arrays 
        for array in arrays_list:
            logging.info(f'[FLATTENING ARRAY: {array}]')
            array_df, source_count = self._flatten_array(list_data, array, parent_pk, context)
            array_name = '_' + array
            write_return = self._upload_normalized_data_gcs(array_df, context, array_name=array_name)
            logging.info(f'[FLAT ARRAY: {array} UPLOADED TO GCS]')
            row_count = len(array_df)
            self._metadata_upload_child(context, row_count, source_count, check_landing=True, array_name=array_name)
            logging.info(f'[METADATA FOR: {array} UPLOADED TO GCS]')

            # Extracting and loading arrays nested withing other arrays 
            nested_arrays_list = self.confJSON[self.entity+'_'+array]["nested_arrays"]
            logging.info(nested_arrays_list)
            for nested_array in nested_arrays_list:
                logging.info(f'[FLATTENING NESTED ARRAY: {nested_array}]')
                nested_array_name = '_' + array + '_' + nested_array
                flat_nested_array_df, nested_array_source_count = self._flatten_nested_array(list_data, array, parent_pk,nested_array,context)
                write_return = self._upload_normalized_data_gcs(flat_nested_array_df, context, array_name=nested_array_name)
                logging.info(f'[FLAT NESTED ARRAY: {nested_array} UPLOADED TO GCS]')
                nested_array_row_count = len(flat_nested_array_df)
                self._metadata_upload_child(context, nested_array_row_count, nested_array_source_count, check_landing=True, array_name=nested_array_name)
                logging.info(f'[METADATA FOR: {nested_array} UPLOADED TO GCS]')

        items_df = pd.DataFrame(list_data)
        
        source_count = len(items_df)
        logging.info('[UPLOADING RAW FILE TO GCS]')
        self._upload_raw_data_gcs(items_df.to_json(orient='records', lines='\n', date_format='iso',default_handler=str), context)
        
        df_norm = DataFrame()

        logging.info('[FLATTENING RESPONSE]')
        for dict in list_data:
            flat_dict = flatten(dict)
            flat_df = json_normalize(flat_dict)
            df_norm = df_norm.append(flat_df, ignore_index=True)
            df_norm['bq_load_date'] = context['ds_nodash']

        logging.info("[UPLOADING FLAT RESPONSE TO GCS]")
        self._upload_normalized_data_gcs(df_norm, context)

        logging.info("[UPLOADING METADATA TO GCS]")
        if self.metadata_filename is not None:
            self._metadata_upload(context, source_count, len(df_norm))
        logging.info(f' NUMBER OF ROWS IN APPRAISAL: {len(df_norm)}')

    def _flatten_nested_array(self, response, array, parent_pk,nested_array,context):
        combined_df = pd.DataFrame()
        source_count = 0 

        if nested_array == 'MetalGroupPrices':
            for obj in response:
                object_df = pd.DataFrame()
                array_name = obj[array]
                for item in array_name:
                    items_df = pd.DataFrame()
                    try:
                        for detail in item['MarketDetail'][nested_array]:
                            source_count+=1
                            flat_data = flatten(detail)
                            df = pd.json_normalize(flat_data)
                            items_df = pd.concat([df, items_df])
                    except Exception as e:
                        logging.info(f'>>>> {nested_array} is empty <<<<')
                        logging.info(e)                        
                    
                    object_df = pd.concat([object_df, items_df])
                    # appending parent ID 
                    parent_id_col_name = 'item' + '_' + parent_pk
                    object_df[parent_id_col_name] = item[parent_pk]
                    object_df['bq_load_date'] = context['ds_nodash']

                combined_df = pd.concat([object_df, combined_df])
                combined_df.reset_index(inplace=True, drop=True)
            
        else: 
            for obj in response:
                object_df = pd.DataFrame()
                array_name = obj[array]
                for item in array_name:
                    items_df = pd.DataFrame()
                    try:
                        for detail in item['StoneDetail'][nested_array]:
                            source_count+=1
                            flat_data = flatten(detail)
                            df = pd.json_normalize(flat_data)
                            items_df = pd.concat([df, items_df])
                    except Exception as e:
                        logging.info(f'>>>> {nested_array} is empty <<<<')
                        logging.info(e)                        
                    
                    object_df = pd.concat([object_df, items_df])
                    # appending parent ID 
                    parent_id_col_name = 'item' + '_' + parent_pk
                    object_df[parent_id_col_name] = item[parent_pk]
                    object_df['bq_load_date'] = context['ds_nodash']

                combined_df = pd.concat([object_df, combined_df])
                combined_df.reset_index(inplace=True, drop=True) 

        logging.info(f'NUMBER OF ROWS IN {array}: {source_count}')
        return combined_df, source_count


    def get_list(self, dictionary, keys, default=None):
        return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)

    def _flatten_array(self, response, array, parent_pk, context):
        logging.info('flattening arrays')
        combined_df = pd.DataFrame()

        source_count = 0
        for obj in response:
            object_df = pd.DataFrame()
            df = pd.DataFrame()
            # print('object inside loop', obj)
            if '-' in array:
                ar_nest = array.split('-')
                elm_list = obj[ar_nest[0]][ar_nest[1]]
                source_count+=len(elm_list)
                key_col_name = array.split('-')
                key_col_name = key_col_name[1]
                df = pd.DataFrame(elm_list, columns=[key_col_name[:-1]])
                parent_id_col_name = self.entity + '_' + parent_pk
                df[parent_id_col_name] = obj[parent_pk]
                df['bq_load_date'] = context['ds_nodash']
                combined_df=pd.concat([df,combined_df])
                combined_df.reset_index(inplace=True, drop=True)
            
            elif array == 'Users':
                array_name = obj['AppraisalInTrialRestrictionUsage'][array]
                for i in array_name:
                    source_count+=1
                    flat_data = flatten(i)
                    df = pd.json_normalize(flat_data)
                    object_df = pd.concat([df, object_df])
                    parent_id_col_name = self.entity + '_' + parent_pk
                    object_df[parent_id_col_name] = obj[parent_pk]
                    object_df['bq_load_date'] = context['ds_nodash']

                combined_df = pd.concat([object_df, combined_df])
                combined_df.reset_index(inplace=True, drop=True)                


            else:
                array_name = obj[array]
                for i in array_name:
                    source_count+=1
                    flat_data = flatten(i)
                    df = pd.json_normalize(flat_data)
                    object_df = pd.concat([df, object_df])
                    parent_id_col_name = self.entity + '_' + parent_pk
                    object_df[parent_id_col_name] = obj[parent_pk]
                    object_df['bq_load_date'] = context['ds_nodash']

                combined_df = pd.concat([object_df, combined_df])
                combined_df.reset_index(inplace=True, drop=True)

        logging.info(f'NUMBER OF ROWS IN {array}: {source_count}')
        logging.info('returning flat df')
        return combined_df, source_count



    def _upload_raw_data_gcs(self, data, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{entity}/{date_nodash}/l1_data_{source}_{entity}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data)
        logging.info(f'[RAW FILE NAME: {file_name}]')
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
                # print(df.head(5))

            return df,False,len(df)

    def _upload_normalized_data_gcs(self, df_in, context, counter=0, array_name=''):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_norm_folder}{entity}{array_name}/{date_nodash}/l1_norm_{source}_{entity}.json'.format(
            base_norm_folder=self.base_norm_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity,
            array_name = array_name)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    #data=df_in.to_csv(index= False))
        data = df_in.to_json(orient='records', lines='\n', date_format='iso',default_handler=str))
        logging.info(f'[NORMALIZED FILE UPLOADED: {file_name}]')
        return

    def _metadata_upload(self, context, row_count, source_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        metadata_filename = '{base_folder}{entity}/{date_nodash}/l1_metadata_{source}_{entity}.json'.format(
            base_folder=self.metadata_filename,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            entity=self.entity)

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




