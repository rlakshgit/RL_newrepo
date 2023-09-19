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
import logging



class MembershipStagingOperator(BaseOperator):
    #template_fields = ('target_gcs_bucket',)
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 source,
                 table,
                 config_file,
                 google_cloud_storage_conn_id,
                 target_gcs_bucket,
                 l2_storage_location,
                 staging_file_location,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(MembershipStagingOperator, self).__init__(*args, **kwargs)

        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.table = table
        self.source = source
        self.target_gcs_bucket = target_gcs_bucket
        self.delegate_to = delegate_to
        self.api_configuration =config_file
        self.l2_storage_location = l2_storage_location
        self.staging_file_location = staging_file_location


    def execute(self, context):

        column_list = self.api_configuration[self.table]["Columns_to_BQ"]

        # get a list of the files to be loaded
        file_list = self._get_files_list(context)
        logging.info("The list of files are ..")

        # loop over the list
        counter = 0
        for file in file_list:
            # load file
            logging.info(file)
            logging.info("Loading the file into DataFrame...")
            file_df = self._load_file(context, file)
            logging.info(print(file_df.head(5)))
            df_columns = list(file_df.columns)
            all_columns = list(set(df_columns + column_list))
            print(file_df.head(5))
            file_df = file_df.reindex(columns=all_columns, fill_value='')
            print(file_df.head(5))
            extracted_columns_df = file_df.loc[:, column_list]
            print(extracted_columns_df)
            extracted_columns_df = extracted_columns_df.reindex(sorted(extracted_columns_df.columns), axis=1)
            print(extracted_columns_df)
		    # remove newline char
            print('removing new line char')
            extracted_columns_df = extracted_columns_df.replace(r'\r+|\n+|\t+',' ', regex=True)
            # store file
            logging.info("Writing staging file...")
            write_return = self._upload_csv_to_gcs(extracted_columns_df, counter, context)
            counter += 1
            
    def _upload_csv_to_gcs(self, df, counter, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{staging_file_location}{api_name}/{date_nodash}/staging_{source}_{api_name}_{counter_suffix}.csv'.format(
            date_nodash=context['ds_nodash'],
            #date_time=file_dt,  # context['ts_nodash'],
            staging_file_location=self.staging_file_location,
            source= self.source,
            api_name=self.table,
            counter_suffix=counter)

        csv_data = df.to_csv(encoding='utf-8', index=False)
        # json_data = df.to_json(orient='records', lines='\n', date_format='iso')
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        print('>>>>>>>>>> file converted to csv')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=csv_data)
        print('upload completed')
        return 'upload completed'

    def _load_file(self, context, object_name):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_name = str(object_name)
        file_data = hook.download(self.target_gcs_bucket, object=file_name)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream,orient='records', lines='\n', dtype=False, convert_dates=False)

        return df

    def _get_files_list(self, context):
        logging.info ("Get list of files from storage for processing...")
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        folder_path = '{l2_storage_location}{api_name}/{date_nodash}/'.format(
            date_nodash=context['ds_nodash'],
            l2_storage_location=self.l2_storage_location,
            api_name=self.table)

        logging.info("Folder Path : {folder_path}".format(folder_path = folder_path))
        print(hook.list(bucket=self.target_gcs_bucket, prefix=folder_path))

        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)



