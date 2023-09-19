import json
import decimal
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime as dt
import io
from flatten_json import flatten
from time import sleep
from tempfile import NamedTemporaryFile
# from airflow.models import Variable
from airflow.hooks.base import BaseHook
# from airflow.hooks.http_hook import HttpHook
from plugins.hooks.jm_http import HttpHook
from airflow.models import Variable
from plugins.operators.jm_gcs import GCSHook
import base64
import numpy as np
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class HubspotStagingOperator(BaseOperator):
    template_fields = ('api_name', 'api_configuration', 'target_gcs_bucket', 'target_env')
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 api_connection_id='HubspotAPI',
                 api_name='NA',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encryped=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=200,
                 check_limit=True,
                 delegate_to=None,
                 base_gcs_folder='',
                 l2_storage_location='',
                 staging_file_location='',
                 metadata_filename=None,
                 file_prefix='',
                 last_modified = '',
                 after = '',
                 *args,
                 **kwargs):

        super(HubspotStagingOperator, self).__init__(*args, **kwargs)
        self.api_key_encrypted = api_key_encryped
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.api_name = api_name
        self.api_configuration = api_configuration
        self.target_gcs_bucket = target_gcs_bucket
        self.target_env = target_env
        self.max_loop_count = max_loop_count
        self.delegate_to = delegate_to
        self.offset_enable = False
        self.check_limit = check_limit
        self.offset_df = pd.DataFrame()
        self.base_gcs_folder = base_gcs_folder
        self.l2_storage_location = l2_storage_location
        self.staging_file_location = staging_file_location
        self.metadata_filename = metadata_filename
        self.file_prefix = file_prefix
        self.last_modified = last_modified
        self.after = after

    def execute(self, context):
        print('Date_NO_DASH', context['ds_nodash'])
        file_dt = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
        # load the cols to be included in bq - from the conf file 
        column_list = self.api_configuration[self.api_name]["Columns_to_BQ"]

        # get a list of the files to be loaded 
        file_list = self._get_files_list(context)
        print('file list is: ',file_list)

        # if no data - create empty file 
        if file_list == []:
            df = pd.DataFrame()
            write_return = self._upload_csv_to_gcs(df, context)

        else: 
            # loop over the list 
            counter = 0
            for file in file_list:
                #load file
                file_df = self._load_file(context, file)
                df_columns = list(file_df.columns)
                all_columns = list(set(df_columns + column_list))
                file_df = file_df.reindex(columns=all_columns, fill_value='') 
                extracted_columns_df = file_df.loc[:, column_list]
                extracted_columns_df = extracted_columns_df.reindex(sorted(extracted_columns_df.columns), axis=1)
                #store file
                write_return = self._upload_csv_to_gcs(extracted_columns_df, context, counter=counter) 
                counter += 1
            print('[TOTAL NUMBER OF STAGING FILES]:', counter)


    def _upload_csv_to_gcs(self, df, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{staging_file_location}{api_name}/{date_nodash}/staging_hubspot_{api_name}_{counter_suffix}.csv'.format(
            date_nodash=context['ds_nodash'],
            staging_file_location=self.staging_file_location,
            api_name=self.api_name,
            counter_suffix=counter)
        
        csv_data =  df.to_csv(encoding='utf-8', index=False)
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= csv_data)
        print('upload completed')
        return 'upload completed'


    def _load_file(self, context, object_name):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_name = str(object_name)
        file_data = hook.download(self.target_gcs_bucket, object=file_name)
        if file_data == b'\n':
            df = pd.DataFrame()
            print('[File is Empty - No Data to Stag]')
            print('[An empty file will be created')
        else: 
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_csv(file_stream)

        return df


    def _get_files_list(self, context):
        hook = GCSHook(
        google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
        delegate_to=self.delegate_to)

        folder_path = '{l2_storage_location}{api_name}/{date_nodash}/'.format(
            date_nodash=context['ds_nodash'],
            l2_storage_location=self.l2_storage_location,
            api_name=self.api_name)

        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)


             
    