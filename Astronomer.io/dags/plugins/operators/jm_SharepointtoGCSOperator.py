"""
Author: Gayathri Narayanasamy
This Operator can be used to copy files from Sharepoint to GCP
"""
import os
import sys
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
import tempfile
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import logging
import json
import io
import datetime as dt
import tempfile
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz


class SharepointtoGCSOperator(BaseOperator):
    """
       :param sharepoint_connection:                         Sharepoint connection id.
       :type sharepoint_connection:                          string
        :source_file_url:                                    File path to copy files from
        :source_file_url:                                    string
        :param google_storage_connection_id:                 The gcs connection id.
        :type google_storage_connection_id:                  string
        :param destination_bucket:                           The gcs bucket to be used to store the copied data.
        :type gcs_bucket:                                    string
        :param gcs_destination_path                          Folder location in GCS to copy file into
        :type                                                String
        """
    template_fields = ('gcs_destination_path',)
    ui_color = '#6b1cff'


    @apply_defaults
    def __init__(self,
                 sharepoint_connection,
                 source_file_url,
                 google_storage_connection_id,
                 destination_bucket,
                 gcs_destination_path,
                 mime_type = "text/plain",
                 delegate_to=None,
                 *args,
                 **kwargs):


        super(SharepointtoGCSOperator, self).__init__(*args, **kwargs)
        self.sharepoint_site = sharepoint_connection
        self.google_storage_connection = google_storage_connection_id
        self.file_url = source_file_url
        self.gcs_destination_path = gcs_destination_path
        self.mime_type = mime_type
        self.destination_bucket = destination_bucket
        self.delegate_to = delegate_to
        self.GCShook = GCSHook(
            google_cloud_storage_conn_id=self.google_storage_connection,
            delegate_to=self.delegate_to)


    """
    Function to authenticate and connect to sharepoint 
    and get the list of files from sharepoint folder and upload it to GCS
    """
    def _FileUploadtoGCS(self):
        logging.info("Getting Sharepoint connection details from Basehook")
        connection = BaseHook.get_connection(self.sharepoint_site)

        site_url = connection.host
        client_secret = connection.password
        client_username = connection.login
        logging.info("Site _url :{url}".format(url = self.file_url))



        # Authenticating the credentials and connect to sharepoint team site
        credentials = ClientCredential(client_username, client_secret)
        ctx = ClientContext(site_url).with_credentials(credentials)
        logging.info("Connection established and created context object")

        # Getting list of files from the folder
        print('file_url')
        print(self.file_url)
        libraryRoot = ctx.web.get_folder_by_server_relative_url(self.file_url).execute_query()

        files = libraryRoot.files
        ctx.load(files)
        ctx.execute_query()

        #creating empty dataframe to add list of files in the sharepoint folder
        #get_latestfile_df = pd.DataFrame(columns=['file', 'file_path', 'created_time'])

        j = 0
        finalfilelist=[]
        for file in files:
            list = file.properties.keys()
            file_path = file.properties["ServerRelativeUrl"]
            file_created_time = file.properties["TimeCreated"]
            file_name = os.path.basename(file_path)
            finalfilelist.append(file_path)


        if len(finalfilelist) >= 1:
            logging.info("The following files are present at the source..")
            logging.info(finalfilelist)
        else:
            logging.info("No files present...")

        ##Downloading latest file from Sharepoint into dataframe
        for file_path in finalfilelist:
            file_name = os.path.basename(file_path)
            logging.info("processing......  {file_name}".format(file_name=file_name))
            with tempfile.NamedTemporaryFile() as localFile:
                name = localFile.name
                file = ctx.web.get_file_by_server_relative_url(file_path).download(localFile).execute_query()
                localFile.seek(0)
                content = localFile.read()
                path = self.gcs_destination_path + file_name

                #uploading the raw file into GCS
                self.GCShook.upload(
                    bucket=self.destination_bucket,
                    object=path,
                    filename=name,
                    mime_type=self.mime_type,
                )

    def execute(self, context):
        logging.info("Connecting to sharepoint...")
        data_df = self._FileUploadtoGCS()








