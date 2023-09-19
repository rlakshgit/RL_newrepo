import os
import json
import decimal
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime as dt
import io
from flatten_json import flatten
from time import sleep
import tempfile
from tempfile import NamedTemporaryFile
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_http import HttpHook
from airflow.models import Variable
from plugins.operators.jm_gcs import GCSHook
import base64
import numpy as np
import logging
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext


class JBSelectGcstoSharepointOperator(BaseOperator):
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 source,
                 table,
                 google_cloud_storage_conn_id,
                 target_gcs_bucket,
                 base_file_location,
                 delegate_to=None,
                 sharepoint_connection='NA',
                 destination_file_url='NA',
                 *args,
                 **kwargs):

        super(JBSelectGcstoSharepointOperator, self).__init__(*args, **kwargs)

        self.source = source
        self.table = table
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.target_gcs_bucket = target_gcs_bucket
        self.base_file_location = base_file_location
        self.delegate_to = delegate_to
        self.sharepoint_connection = sharepoint_connection
        self.destination_file_url = destination_file_url
        self.sharepoint_site = sharepoint_connection

    def execute(self, context):

        # get a list of the files to be loaded
        file_list = self._get_files_list(context)
        logging.info("The list of files are ..")

        # loop over the list
        counter = 0

        for file in file_list:
            file1 = file.split('.')[0]
            file2 = file1.replace(" ", "")
            file3 = file2.split('/')[3]

            filename = file.split('/')[3]

            # load file
            if (file3 == '{table}'.format(table=self.table)):
                logging.info(file)
                df = self._load_file(context, file)

                logging.info("Writing the result to {location}".format(location=self.destination_file_url))

                connection = BaseHook.get_connection(self.sharepoint_site)
                site_url = connection.host
                client_secret = connection.password
                client_username = connection.login
                credentials = ClientCredential(client_username, client_secret)
                ctx = ClientContext(site_url).with_credentials(credentials)
                temp = tempfile.NamedTemporaryFile(suffix=".xlsx")

                with pd.ExcelWriter(temp.name) as writer:
                    data = df
                    data.to_excel(writer, sheet_name="Database", index=False)
                    writer.save()


                with open(temp.name, 'rb') as content_file:
                    file_content = content_file.read()

                dir = self.destination_file_url
                name = '{date_nodash}_{filename}'.format(filename=filename,date_nodash=context['ds_nodash'])
                file = ctx.web.get_folder_by_server_relative_url(dir).upload_file(name, file_content).execute_query()
                logging.info("Files written succesfully..")


    def _load_file(self, context, object_name):
        hook = GCSHook(
        google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
        delegate_to=self.delegate_to)
        file_name = str(object_name)
        file_data = hook.download(self.target_gcs_bucket, object=file_name)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        read_file = pd.read_excel(file_data, engine='openpyxl')
        print('read_file')
        print(read_file)
        df = pd.DataFrame(pd.read_excel(file_data, engine='openpyxl'))
        return df


    def _get_files_list(self, context):
        logging.info("Get list of files from storage for processing...")
        hook = GCSHook(
        google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
        delegate_to=self.delegate_to)
        
        folder_path = '{base_file_location}{date_nodash}/'.format(
        date_nodash=context['ds_nodash'],
        base_file_location=self.base_file_location    )
        
        logging.info("Folder Path : {folder_path}".format(folder_path=folder_path))
        print(hook.list(bucket=self.target_gcs_bucket, prefix=folder_path))   
        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)



