import json
import string
from bson.json_util import dumps
import decimal
from pandas.io.json import json_normalize
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.dq_common import dq_common
from plugins.operators.jm_gcs import GCSHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from pandas.io.json import json_normalize
from airflow.hooks.base import BaseHook
import logging
from pymongo import MongoClient
import io
import pandas as pd
from datetime import datetime
import datetime as dt


class Mongodbtogcs(BaseOperator):

    ui_color = '#e0aFFc'

    template_fields = ('metadata_filename',
                       'audit_filename', 'base_gcs_folder')

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
                 confJSON=None,
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
        self.base_schema_folder = base_schema_folder
        self.base_norm_folder = base_norm_folder
        self.schema_data_new = []
        self.pull_new = True
        self.confJSON = confJSON

    def execute(self, context):

        logging.info("Making Connections to CosmosDB....")

        mongo_db_connection = BaseHook.get_connection(self.mongo_db_conn_id)
        self.CONNECTION_STRING = mongo_db_connection.password

        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        client = MongoClient(self.CONNECTION_STRING,
                             uuidRepresentation="csharpLegacy")
        mydatabase = client[self.database_name]
        mycollection = mydatabase[self.collection]

        start_ds = context['ds_nodash']
        start_year = int(start_ds[:4])
        start_month = int(start_ds[4:6])
        start_day = int(start_ds[6:8])
        start_date = datetime(start_year, start_month, start_day)

        end_ds = context['tomorrow_ds_nodash']
        end_year = int(end_ds[:4])
        end_month = int(end_ds[4:6])
        end_day = int(end_ds[6:8])
        end_date = datetime(end_year, end_month, end_day)

        filter = {'$or':
                  [
                      {'$and':
                       [
                           {'ModifiedOn': {'$gte': start_date}},
                           {'ModifiedOn': {'$lt': end_date}},
                           {'EntityType': self.entity}
                       ]
                       },
                      {'$and':
                       [
                           {'CreatedOn': {'$gte': start_date}},
                           {'CreatedOn': {'$lt': end_date}},
                           {'EntityType': self.entity}
                       ]
                       }
                  ]
                  }

        logging.info(
            f'LANDING - {self.entity} - DATA FOR {start_date} TO {end_date}')

        response = client['platform-shipping-v2']['shippingdocuments'].find(
            filter=filter)
        logging.info('Request sent...')
        # convert response to list
        list_data = []
        for i in response:
            list_data.append(i)
        source_count = len(list_data)
        # convert list to pandas DataFrame
        items_df = pd.DataFrame(list_data)
        source_count = len(items_df)
        logging.info("Uploading Raw data to GCS")
        self._upload_raw_data_gcs(
            items_df.to_json(orient='records', lines='\n', date_format='iso', default_handler=str), context)

        logging.info("Flattening Response...")
        df_norm = json_normalize(list_data)
        new_columns = {}
        for i in df_norm.columns:
            new_columns[i] = i.replace('.', '_')
        df_norm.rename(columns=new_columns, inplace=True)
        logging.info("Uploading Normalized files to GCS...")
        df_norm['bq_load_date'] = context['ds_nodash']
        self._upload_normalized_data_gcs(df_norm, context)

        logging.info("Uploading MetaData file to GCS...")
        if self.metadata_filename is not None:
            self._metadata_upload(context, source_count, len(df_norm))

        # get list of nested arrays from conf file
        arrays_list = self.confJSON[self.entity]["arrays_list"]
        nested_list = self.confJSON[self.entity]["nested_arrays"]
        arrays_in_dict = self.confJSON[self.entity]["arrays_in_dict"]

        # Extract, Flatten and Upload Nested Arrays
        for array in arrays_list:
            logging.info(f'EXTRACING {array} FROM OBJECT')
            source_count = self._source_array_count(
                list_data, parent_object=array)
            df_sub = json_normalize(list_data, record_path=[array], meta=[
                                    '_id'], meta_prefix=self.entity)
            rename_column = {0: array}
            df_sub = df_sub.rename(columns=rename_column)
            logging.info(f'UPLOADING{self.entity}_{array} FILE TO GCS')
            array_name = '_' + array
            logging.info(f"Uploading {array} File to GCS ...")
            df_sub['bq_load_date'] = context['ds_nodash']
            write_return = self._upload_normalized_data_gcs(
                df_sub, context, array_name=array_name)
            row_count = len(df_sub)
            logging.info(f'Uploading {array} Metadata to GCS')
            self._metadata_upload_child(
                context, row_count, source_count, check_landing=True, array_name=array_name)

        for array in nested_list:
            parent_object = array.split('.')[0]
            child_object = array.split('.')[1]
            logging.info(
                f'EXTRACING {self.entity}_{parent_object}_{child_object} FROM OBJECT')
            source_count = self._source_array_count(
                list_data, parent_object=parent_object, child_object=child_object)
            df_nested_sub = json_normalize(list_data, record_path=[parent_object, child_object], meta=[
                                           parent_object, '_id'], meta_prefix=self.entity)
            logging.info(
                f'UPLOADING {self.entity}_{parent_object}_{child_object} FILE TO GCS')
            array_name = '_' + parent_object+'_'+child_object
            df_nested_sub['bq_load_date'] = context['ds_nodash']
            write_return = self._upload_normalized_data_gcs(
                df_nested_sub, context, array_name=array_name)
            row_count = len(df_nested_sub)
            logging.info(
                f'UPLOADING {self.entity}_{parent_object}_{child_object} METADATA TO GCS')
            self._metadata_upload_child(
                context, row_count, source_count, check_landing=True, array_name=array_name)

        for i in arrays_in_dict:
            logging.info("Extracting {i} from object".format(i=i))
            parent_key = i.split('-')[0]+'_id'
            parent_object = i.split('-')[1]
            child_object = i.split('-')[2]
            df_norm = pd.DataFrame()
            source_count = 0
            try:
                for element in list_data:
                    try:
                        for i in element[parent_object][child_object]:
                            source_count = source_count + 1
                        df = json_normalize(
                            element[parent_object], child_object)
                        df[parent_key] = element['_id']
                        df_norm = df_norm.append(df)
                    except Exception as e:
                        logging.warning(
                            f"{parent_object}-{child_object} Object NOT Available in Document with Etag as {element['ETag']}")
                        print('inner exception')
                        print(e)
                print("********************************")
                print(list(df_norm.columns.values))
                print("********************************")
                rename_column = {0: child_object}
                df_norm = df_norm.rename(columns=rename_column)
                new_columns = {}
                for i in df_norm.columns:
                    new_columns[i] = i.replace('.', '_')
                df_norm.rename(columns=new_columns, inplace=True)

            except Exception as e:
                print(e)
                logging.warning(
                    f"{i} Object NOT Available in Document with Etag as {element['ETag']}")
            logging.info(
                f'UPLOADING {self.entity}_{parent_object}_{child_object} FILE TO GCS')
            print(list(df_norm.columns.values))

            array_name = '_' + parent_object + '-' + child_object
            df_norm['bq_load_date'] = context['ds_nodash']
            write_return = self._upload_normalized_data_gcs(
                df_norm, context, array_name=array_name)
            row_count = len(df_norm)
            logging.info(
                f'UPLOADING {self.entity}_{parent_object}_{child_object} METADATA TO GCS')
            self._metadata_upload_child(
                context, row_count, source_count, check_landing=True, array_name=array_name)

    def _source_array_count(self, response, parent_object, child_object=''):
        source_count = 0
        for obj in response:
            if child_object == '':
                try:
                    array_name = obj[parent_object]
                    for i in array_name:
                        source_count += 1
                except:
                    pass

            else:
                try:
                    array_name = obj[parent_object]
                    for i in array_name:
                        child_array_name = i[child_object]
                        for j in child_array_name:
                            source_count = source_count+1
                except:
                    pass

        return source_count

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

    def _upload_normalized_data_gcs(self, df_in, context, counter=0, array_name=''):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_norm_folder}{collection}{array_name}/{date_nodash}/l1_norm_{source}_{collection}{array_name}.json'.format(
            base_norm_folder=self.base_norm_folder,
            source=self.source_abbr,
            date_nodash=context['ds_nodash'],
            collection=self.collection,
            array_name=array_name)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    # data=df_in.to_csv(index= False))
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso', default_handler=str))
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

    def _metadata_upload_child(self, context, row_count, source_count, check_landing=True, array_name=''):
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
