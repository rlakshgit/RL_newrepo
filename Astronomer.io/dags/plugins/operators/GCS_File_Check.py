from airflow.models import BaseOperator
from plugins.hooks.jm_gcs import GCSHook
from airflow.models import Variable
import logging
import io
import pandas as pd
import datetime as dt

class GCSFileCheck(BaseOperator):
    def __init__(self,
                 project,
                 bucket,
                 target_file='NA',
                 approx_max_file_size_bytes=1900000000,
                 delegate_to=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 prefix_for_filename_search=None,
                 *args,
                 **kwargs):

        super(GCSFileCheck, self).__init__(*args, **kwargs)

        self.project = project
        self.bucket = bucket
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.delegate_to = delegate_to
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.prefix_for_filename_search = prefix_for_filename_search
        self.target_file = target_file

    def execute(self, context):
        hook = GCSHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)

        if "{{ ds_nodash }}" in self.prefix_for_filename_search:
            self.prefix_for_filename_search = self.prefix_for_filename_search.replace("{{ ds_nodash }}",context['ds_nodash'])
        print('Looking for files in...', self.prefix_for_filename_search)

        file_list = hook.list(self.bucket, maxResults=100,prefix=self.prefix_for_filename_search)

        try:
            Experian_file_Check = Variable.get("Experian_File_Check")
        except:
            Variable.set("Experian_File_Check", "False")

        if len(file_list) == 0:
            print('No Files Present / Location Not found')
            Variable.set("Experian_File_Check","False")
            return
        else:
            print('Found files in the location')
            Variable.set("Experian_file_Check","True")
            return


