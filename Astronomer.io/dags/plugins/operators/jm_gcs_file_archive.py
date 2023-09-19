"""
This operator would move files with prefix to archive folder
variable used: gcs_archive_file_prefix
"""


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_gcs import GCSHook
import logging

class GCSToGCSOperator(BaseOperator):

  #  template_fields = ('partition_date', )
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 source_project,
                 source_bucket,
                 destination_bucket,
                 source_file_prefix,
                 source_gcs_conn_id,
                 move_object = True,
                 *args,
                 **kwargs):
        super(GCSToGCSOperator, self).__init__(*args, **kwargs)
        self.source_project =source_project
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.source_file_prefix = source_file_prefix
        self.move_object = move_object
        self.source_gcs_conn_id = source_gcs_conn_id
        self.source_gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.source_gcs_conn_id)

    def execute(self, context):
        source_file_list = self.source_gcp_storage_hook.list(self.source_bucket,prefix=self.source_file_prefix)
        for file in source_file_list:
            self.source_gcp_storage_hook.copy(self.source_bucket, file,
                                             self.destination_bucket,
                                             'archive_' + file)
            self.source_gcp_storage_hook.delete(bucket=self.source_bucket,
                    object=file)
        return 0
