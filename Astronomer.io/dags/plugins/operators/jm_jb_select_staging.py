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
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_http import HttpHook
from airflow.models import Variable
from plugins.operators.jm_gcs import GCSHook
import base64
import numpy as np
import logging


class JBSelectStagingOperator(BaseOperator):
    template_fields = ('metadata_filename',
                       'audit_filename')
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 source,
                 table,
                 config_file,
                 google_cloud_storage_conn_id,
                 target_gcs_bucket,
                 base_file_location,
                 staging_file_location,
                 delegate_to=None,
                 metadata_filename='NA',
                 audit_filename='NA',                 
                 *args,
                 **kwargs):
        super(JBSelectStagingOperator, self).__init__(*args, **kwargs)

        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.table = table
        self.source = source
        self.target_gcs_bucket = target_gcs_bucket
        self.delegate_to = delegate_to
        self.api_configuration = config_file
        self.base_file_location = base_file_location
        self.staging_file_location = staging_file_location
        self.metadata_filename = metadata_filename
        self.audit_filename = audit_filename
        

    def execute(self, context):

        column_list = self.api_configuration[self.table]["Columns_to_BQ"]

        # get a list of the files to be loaded
        file_list = self._get_files_list(context)
        logging.info("The list of files are ..")

        # loop over the list
        counter = 0

        for file in file_list:
            file1 = file.split('.')[0]
            file2 = file1.replace(" ", "")
            file3 = file2.split('/')[3]

            # load file
            if (file3 == '{table}'.format(table=self.table)):
                logging.info(file)
                logging.info("Loading the file into DataFrame...")
                file_df, file_source_count = self._load_file(context, file)
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
                # store file
                logging.info("Writing staging file...")
                write_return = self._upload_csv_to_gcs(extracted_columns_df, context)
                logging.info("Uploading MetaData file to GCS...")
                print('metadata filename')
                print(self.metadata_filename)
                if self.metadata_filename is not None:
                    self._metadata_upload(context, file_source_count)

        # print('[TOTAL NUMBER OF STAGING FILES]:', counter)

    def _upload_csv_to_gcs(self, df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{staging_file_location}{api_name}/{date_nodash}/staging_{source}_{api_name}.csv'.format(
            date_nodash=context['ds_nodash'],
            # date_time=file_dt,  # context['ts_nodash'],
            staging_file_location=self.staging_file_location,
            source=self.source,
            api_name=self.table)

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
        # file_stream = io.BufferedReader(io.BytesIO(file_data))
        read_file = pd.read_excel(file_data, engine='openpyxl')
        df = pd.DataFrame(pd.read_excel(file_data, engine='openpyxl'))
        source_count = len(df)

        print(df)
        # df = pd.read_json(file_stream,orient='records', lines='\n', dtype=False, convert_dates=False)

        return df, source_count

    def _get_files_list(self, context):
        logging.info("Get list of files from storage for processing...")
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        folder_path = '{base_file_location}{date_nodash}/'.format(
            date_nodash=context['ds_nodash'],
            base_file_location=self.base_file_location
        )

        logging.info("Folder Path : {folder_path}".format(folder_path=folder_path))
        print(hook.list(bucket=self.target_gcs_bucket, prefix=folder_path))

        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)

    def _metadata_upload(self, context, row_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        metadata_filename = '{base_gcs_folder}{table}/{date_nodash}/l1_metadata_{source}_{table}.json'.format(
            base_gcs_folder=self.metadata_filename,
            source=self.source,
            date_nodash=context['ds_nodash'],
            table=self.table)

        print('Metadata File - ', metadata_filename)
        json_metadata = {
            'source_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return


