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
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_sftp import SFTPHook
from airflow.utils.decorators import apply_defaults
import pandas as pd
import json
import logging
import datetime as dt
import io



WILDCARD = "*"



class NCOAToGCSOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from SFTP server of NCOA.

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

    template_fields = ("source_path", "destination_path", "destination_bucket", "schema_file_path", "metadata_file_name", "norm_file_path")


    @apply_defaults
    def __init__(
        self,
        source_path: str,
        destination_bucket: str,
        norm_file_path: str,
        source: str,
        history_check: str,
        destination_path: Optional[str] = None,
        schema_file_path: Optional[str] = None,
        metadata_file_name: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        sftp_conn_id: str = "ssh_default",
        delegate_to: Optional[str] = None,
        mime_type: str = "application/json",
        gzip: bool = False,
        move_object: bool = False,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.source_path = source_path
        self.destination_path = self._set_destination_path(destination_path)
        self.schema_file_path = schema_file_path
        self.norm_file_path = norm_file_path
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.source = source
        self.history_check = history_check
        self.sftp_conn_id = sftp_conn_id
        self.move_object = move_object
        self.metadata_file_name = metadata_file_name
        self.gcs_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.sftp_hook = SFTPHook(self.sftp_conn_id)

    def execute(self, context):

        Variable.set(self.source + '_gcs_to_bq', 'True')

        if (self.history_check.upper() == 'TRUE') and (self.gcs_hook.exists(self.destination_bucket, self.destination_path)):
            logging.info('File already landed.........')
            logging.info('Processing the following file: ' + self.destination_path)
            self._process_from_l1_gcs(context, self.destination_path)
            return 0

        if (self.history_check.upper() == 'TRUE'):
            logging.info('Checking PROD-EDL for the file...........')
            # logging.info('Processing the following file: ' + self.destination_path)
            prod_edl_source_path = 'l1/ncoa/' + context['ds_nodash']
            prod_edl_source_bucket = 'jm_prod_edl_lnd'
            prod_edl_connector = 'prod_edl'
            result_return = self._process_from_prod_edl_l1_gcs(context, self.destination_path,prod_edl_connector,prod_edl_source_bucket,prod_edl_source_path)
            if result_return == 0:
                return 0



        total_wildcards = self.source_path.count(WILDCARD)
        if total_wildcards > 1:
            raise
            # raise AirflowException(https://community.powerbi.com/t5/Power-Query/BigQuery-Connector-for-write-custom-SQL-Query-to-filter-Date-and/m-p/839498#M28117
            #     "Only one wildcard '*' is allowed in source_path parameter. "
            #     "Found {} in {}.".format(total_wildcards, self.source_path)
            # )

        prefix, delimiter = self.source_path.split(WILDCARD, 1)
        base_path = os.path.dirname(prefix)


        files, _, _ = self.sftp_hook.get_tree_map(
            base_path, prefix=prefix, delimiter=delimiter
        )


        for file in files:
            if context['ds_nodash'] in file:
                source_path = base_path.replace('/*', '')
                source_path = os.path.join(source_path, file)
                self._copy_single_object_json(context,self.gcs_hook, self.sftp_hook, source_path, self.destination_path)
                Variable.set(self.source + '_gcs_to_bq', 'True')
                break
            elif self.gcs_hook.exists(self.destination_bucket, self.destination_path):
                logging.info('File already landed.........')
                logging.info('Processing the following file: ' + self.destination_path)
                self._process_from_l1_gcs(context, self.destination_path)
                Variable.set(self.source + '_gcs_to_bq', 'True')
                break
            else:
                logging.info('no files to process')
                Variable.set(self.source + '_gcs_to_bq', 'False')
        return



    def _copy_single_object(
        self,
        context,
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

    def _copy_single_object_json(
        self,
        context,
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
            df = pd.read_csv(tmp.name)

            json_data = df.to_json(orient='records', lines='\n', date_format='iso')
            gcs_hook.upload(
                bucket=self.destination_bucket,
                object=self.destination_path,
                data=json_data,
                mime_type=self.mime_type)
        self._normalize_data(df,context)
        return

    def _normalize_data(self, df, context):

        df['MoveDate'] = df['MoveDate'].astype(str).str.rstrip('.0')
        df['MoveDate'] = pd.to_datetime(df['MoveDate'])
        df = self._standardize_datatypes(df)
        file_date_time = pd.to_datetime(context['ds_nodash'], format='%Y%m%d').strftime('%Y-%m-%d')

        # Create time_central and time_utc for consistency
        run_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        extract_date = dt.datetime.strptime(context['ds_nodash'], '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
        df['run_date'] = run_date
        df['extract_date'] = extract_date
        df['run_date'] = pd.to_datetime(df['run_date'])
        df['extract_date'] = pd.to_datetime(df['extract_date'])
        df['time_utc'] = pd.to_datetime(df['extract_date'])
        df['time_central'] = df.time_utc.dt.tz_localize('UTC', ambiguous=True).dt.tz_convert(
            'US/Central').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        df['time_central'] = pd.to_datetime(df['time_central'], errors='coerce')
        logging.info('added Time fields')
        json_data = df.to_json(orient='records', lines='\n', date_format='iso')

        self.gcs_hook.upload(
            bucket=self.destination_bucket,
            object=self.norm_file_path,
            data=json_data,
            mime_type=self.mime_type,
        )
        self._upload_schema_to_gcs(df)
        self._upload_metadata(df=df, context=context)

    def _standardize_datatypes(self, df):
        for column_name, dtype in df.dtypes.iteritems():
            if (dtype.kind != 'M'):
                df[column_name] = df[column_name].astype(str)
        return df


    def _schema_generator(self, df, default_type='STRING'):

        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            # print(column_name,dtype.kind)
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return_string = "[{fields}]".format(fields=",".join(fields))
        return json.dumps(fields)  # return_string

    def _upload_schema_to_gcs(self, json_normalized_df):
        schema_data = self._schema_generator(json_normalized_df)

        # file_name = '{base_gcs_folder}_schema/{source}/{date_nodash}/ncoa_schema.json'.format(
        #     source=self.source_abbr.lower(),
        #     date_nodash=context['ds_nodash'],
        #     base_gcs_folder=self.base_gcs_folder)
        self.gcs_hook.upload(bucket=self.destination_bucket, object=self.schema_file_path, data=schema_data)

        return

    def _upload_metadata(self, context, df):
        audit_df = pd.DataFrame()

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': len(df), 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        self.gcs_hook.upload(bucket=self.destination_bucket, object=self.metadata_file_name, data=json_data)

        del json_data
        del audit_df

        return

    def _l1_data_check(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        if hook.exists(self.destination_bucket, object):
            return object
        else:
            return 'NONE'


        # file_prefix = '{base_gcs_folder}/{source}/{api_name}/{date_nodash}/l1_'.format(
        #     source=self.api_configuration[self.api_name]['api_source_name'],
        #     date_nodash=context['ds_nodash'],
        #     base_gcs_folder=self.base_gcs_folder,
        #     api_name=self.api_name)
        # file_list = []
        # file_list = hook.list(self.target_gcs_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        # if len(file_list) == 0:
        #     return 'NONE'
        # elif len(file_list) == 1:
        #     return file_list[0]
        # else:
        #     df = pd.DataFrame()
        #     for f in file_list:
        #         df_len = len(df)
        #         df.loc[df_len, 'filename'] = f
        #         df.loc[df_len, 'filename_base'] = f.replace('.json', '')
        #
        #     df['counter_value'] = df.filename_base.str.split('_').str[-1]
        #     df['file_dt'] = df.filename_base.str.split('_').str[-2]
        #     df['file_datetime'] = pd.to_datetime(df['file_dt'], format='%Y%m%d%H%M%S')
        #
        #     if return_type == 'FIRST':
        #         df.sort_values(by=['file_datetime'], ascending=[True], inplace=True)
        #         df.reset_index(drop=True, inplace=True)
        #         df = df[df['file_dt'] == df.loc[0, 'file_dt']]
        #     else:
        #         df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
        #         df.reset_index(drop=True, inplace=True)
        #         df = df[df['file_dt'] == df.loc[0, 'file_dt']]
        #
        #     return ','.join(df['filename'].values.tolist())
        #
        # return 'NONE'

    def _process_from_l1_gcs(self, context, object):

        file_data = self.gcs_hook.download(self.destination_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        self._normalize_data(df, context)
        return 0

    def _process_from_prod_edl_l1_gcs(self, context, object,source_connector,source_bucket,source_path):
        prod_hook = GCSHook(google_cloud_storage_conn_id=source_connector,
                                               delegate_to=self.delegate_to)

        file_list = prod_hook.list(source_bucket, prefix=source_path)
        if len(file_list) == 0:
            return -1

        if len(file_list) > 1:
            print('Found too many files')
            print(file_list)
            raise
            return -2

        for f in file_list:
            if '.json' not in f:
                continue

            file_data = prod_hook.download(source_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            self.gcs_hook.upload(
                bucket=self.destination_bucket,
                object=object,
                data=file_data
            )
            self._normalize_data(df, context)
        ##
        return 0

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
