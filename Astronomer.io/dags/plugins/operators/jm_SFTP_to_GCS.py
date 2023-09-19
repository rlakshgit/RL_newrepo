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
"""
This module contains SFTP to Google Cloud Storage operator.
"""
import os
from tempfile import NamedTemporaryFile
from typing import Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_sftp import SFTPHook
from airflow.utils.decorators import apply_defaults


WILDCARD = "*"



class SFTPToGCSOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToGCSOperator`

    :param source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :type source_path: str
    :param destination_bucket: The bucket to upload to.
    :type destination_bucket: str
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
        If a wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :type destination_path: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param mime_type: The mime-type string
    :type mime_type: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :type gzip: bool
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    """

    template_fields = ("source_path", "destination_path", "destination_bucket")


    @apply_defaults
    def __init__(
        self,
        source_path: str,
        destination_bucket: str,
        destination_path: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        sftp_conn_id: str = "ssh_default",
        delegate_to: Optional[str] = None,
        mime_type: str = "application/octet-stream",
        gzip: bool = False,
        move_object: bool = False,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.source_path = source_path
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.sftp_conn_id = sftp_conn_id
        self.move_object = move_object

    def execute(self, context):
        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        sftp_hook = SFTPHook(self.sftp_conn_id)

        total_wildcards = self.source_path.count(WILDCARD)
        if total_wildcards > 1:
            raise AirflowException(
                "Only one wildcard '*' is allowed in source_path parameter. "
                "Found {} in {}.".format(total_wildcards, self.source_path)
            )

        prefix, delimiter = self.source_path.split(WILDCARD, 1)
        base_path = os.path.dirname(prefix)

        files = sftp_hook.list_directory(base_path)
        modified_date_full = sftp_hook.get_mod_time(base_path)
        modified_date = modified_date_full[:8]
        if context['ds_nodash'] != modified_date:
            print('No new files')

            return

        for file in files:
            destination_path = file.replace(base_path, self.destination_path, 1)
            source_path = base_path.replace('/*', '')
            source_path = os.path.join(source_path, file)
            self._copy_single_object(gcs_hook, sftp_hook, source_path, os.path.join(self.destination_path,file))



    def _copy_single_object(
        self,
        gcs_hook: GCSHook,
        sftp_hook: SFTPHook,
        source_path: str,
        destination_object: str,
    ) -> None:
        """
        Helper function to copy single object.
        """
        self.log.info(
            "Executing copy of %s to gs://%s/%s",
            source_path,
            self.destination_bucket,
            destination_object,
        )

        with NamedTemporaryFile("w") as tmp:
            sftp_hook.retrieve_file(source_path, tmp.name)

            gcs_hook.upload(
                bucket=self.destination_bucket,
                object=destination_object,
                filename=tmp.name,
                mime_type=self.mime_type,
            )

        # if self.move_object:
        #     self.log.info("Executing delete of %s", source_path)
        #     sftp_hook.delete_file(source_path)


    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""


    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")










#
#
#
#
# #
# # Licensed to the Apache Software Foundation (ASF) under one
# # or more contributor license agreements.  See the NOTICE file
# # distributed with this work for additional information
# # regarding copyright ownership.  The ASF licenses this file
# # to you under the Apache License, Version 2.0 (the
# # "License"); you may not use this file except in compliance
# # with the License.  You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing,
# # software distributed under the License is distributed on an
# # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# # KIND, either express or implied.  See the License for the
# # specific language governing permissions and limitations
# # under the License.
# """
# Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
# """
#
# import os
#
# from airflow import models
# from airflow.providers.google.cloud.operators.sftp_to_gcs import SFTPToGCSOperator
# from airflow.utils.dates import days_ago
#
# default_args = {"start_date": days_ago(1)}
#
# BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_1_SRC", "test-sftp-gcs")
#
# TMP_PATH = "/tmp"
# DIR = "tests_sftp_hook_dir"
# SUBDIR = "subdir"
#
# OBJECT_SRC_1 = "parent-1.bin"
# OBJECT_SRC_2 = "parent-2.bin"
# OBJECT_SRC_3 = "parent-3.txt"
#
#
# with models.DAG(
#     "example_sftp_to_gcs", default_args=default_args, schedule_interval=None
# ) as dag:
#     # [START howto_operator_sftp_to_gcs_copy_single_file]
#     copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
#         task_id="file-copy-sftp-to-gcs",
#         source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_1),
#         destination_bucket=BUCKET_SRC,
#     )
#     # [END howto_operator_sftp_to_gcs_copy_single_file]
#
#     # [START howto_operator_sftp_to_gcs_move_single_file_destination]
#     move_file_from_sftp_to_gcs_destination = SFTPToGCSOperator(
#         task_id="file-move-sftp-to-gcs-destination",
#         source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_2),
#         destination_bucket=BUCKET_SRC,
#         destination_path="destination_dir/destination_filename.bin",
#         move_object=True,
#     )
#     # [END howto_operator_sftp_to_gcs_move_single_file_destination]
#
#     # [START howto_operator_sftp_to_gcs_copy_directory]
#     copy_directory_from_sftp_to_gcs = SFTPToGCSOperator(
#         task_id="dir-copy-sftp-to-gcs",
#         source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*"),
#         destination_bucket=BUCKET_SRC,
#     )
#     # [END howto_operator_sftp_to_gcs_copy_directory]
#
#     # [START howto_operator_sftp_to_gcs_move_specific_files]
#     move_specific_files_from_gcs_to_sftp = SFTPToGCSOperator(
#         task_id="dir-move-specific-files-sftp-to-gcs",
#         source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*.bin"),
#         destination_bucket=BUCKET_SRC,
#         destination_path="specific_files/",
#         move_object=True,
#     )
#     # [END howto_operator_sftp_to_gcs_move_specific_files]