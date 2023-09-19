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



class EASDataAvailabilityCheckOperator(BaseOperator):
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 source,
                 table,
                 config_file,
                 google_cloud_storage_conn_id,
                 target_gcs_bucket,
                 l2_storage_location,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(EASDataAvailabilityCheckOperator, self).__init__(*args, **kwargs)

        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.table = table
        self.source = source
        self.target_gcs_bucket = target_gcs_bucket
        self.delegate_to = delegate_to
        self.api_configuration =config_file
        self.l2_storage_location = l2_storage_location

    def execute(self, context):

        column_list = self.api_configuration[self.table]["Columns_to_BQ"]

        # get a list of the files to be loaded
        file_list = self._get_files_list(context)
        logging.info(" ramesh list of files ", len(file_list), " type of files ", type(file_list))
        eas_files_availability='eas_files_availability_{source}'.format(source=self.source)
        Variable.delete(eas_files_availability)
        if len(file_list) == 0:
            Variable.set(eas_files_availability, 0)
            logging.info("No files to process ")
        else:
            Variable.set(eas_files_availability, 1)
            logging.info("The list of files are ..")
            logging.info(file_list)
    
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
        print("Ramesh add", hook.list(bucket=self.target_gcs_bucket, prefix=folder_path), type(hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)))

        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)



