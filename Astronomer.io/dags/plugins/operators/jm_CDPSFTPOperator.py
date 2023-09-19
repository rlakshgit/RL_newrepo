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
import pandas as pd
import datetime as dt
import logging
from airflow.models import Variable
from plugins.hooks.jm_bq_hook import BigQueryHook

WILDCARD = "*"


class CDPtoSFTPOperator(BaseOperator):
    """
    Run a input query and transfer it to GCS and SFTP.
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToGCSOperator`
    :param sftp_dest_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :type sftp_dest_path: str
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

    template_fields = ("sftp_dest_path", "destination_path", "destination_bucket", "sql", "file_name")

    @apply_defaults
    def __init__(
            self,
            sftp_dest_path: str,
            destination_bucket: str,
            sql: str,
            file_name: str,
            sql_filter_list = [],
            destination_path: Optional[str] = None,
            gcp_conn_id: str = "google_cloud_default",
            bigquery_conn_id: str = "google_cloud_default",
            sftp_conn_id: str = "ssh_default",
            delegate_to: Optional[str] = None,
            mime_type: str = "application/octet-stream",
            gzip: bool = False,
            move_object: bool = False,
            audit_location: str = 'NA',

            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.sftp_dest_path = sftp_dest_path
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.file_name = file_name
        self.sql_filter_list = sql_filter_list
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.sftp_conn_id = sftp_conn_id
        self.move_object = move_object
        self.audit_location = audit_location
        self.bigquery_conn_id = bigquery_conn_id

    def execute(self, context):
        logging.info('rlaksh in operator execute ')            
        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        logging.info('rlaksh in operator execute after gcs_hook')            
        sftp_hook = SFTPHook(self.sftp_conn_id)

        logging.info('rlaksh in operator execute after sftp_hook')            
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)

        logging.info('rlaksh in operator execute gcp_bq_hook')            
        object_name = os.path.join(self.destination_path, self.file_name.replace('_Delta', '') + context['ds_nodash'] + '.csv')
        logging.info('rlaksh in operator execute after gcp_bq_hook')            
        logging.info('rlaksh in operator execute ', object_name)            
        data_df = self.gcp_bq_hook.get_pandas_df(self.sql)

        logging.info('rlaksh in operator execute object_name', object_name)            
        # setting data type of 'ItemNumber' to INT
        if 'ItemNumber' in data_df.dtypes:
            data_df['ItemNumber']  = data_df['ItemNumber'].fillna(0).astype(int)

        logging.info('Fetching data from BigQuery ... COMPLETE')            
        gcs_hook.upload(
            bucket=self.destination_bucket,
            object=object_name,
            data=data_df.to_csv(index=False),                
            mime_type=self.mime_type,
        )        
        logging.info('GCS upload complete starting SFTP upload')            

        #sftp_dest_path is destination path in SFTP folder
        sftp_dest_path = '{base}/{filename}{date}.csv'.format(date=context['ds_nodash'],filename=self.file_name.replace('_Delta', ''),base=self.sftp_dest_path)
        logging.info('filename is .........')
        logging.info(sftp_dest_path)

        #transfer from a tmp file
        with NamedTemporaryFile("w") as tmp:
            data_df.to_csv(tmp.name, index=False)
            logging.info(tmp.name)
            logging.info(sftp_dest_path)
            sftp_hook.store_file(sftp_dest_path, tmp.name)
            logging.info('SFTP upload completed')
        return

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
