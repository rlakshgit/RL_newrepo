"""Script to pull all Jeweler Member Benefits Statement data and produce report"""

################################################################################
# Base and third-party libraries
################################################################################


import os
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
import tempfile
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import logging
import json
import io
import datetime as dt
from common.dq_common import dq_common


class sharepointtogcsoperator(BaseOperator):
    template_fields = ('gcs_destination_path',)
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 sharepoint_connection,
                 google_storage_connection_id,
                 source_file_url,
                 gcs_destination_path,
                 base_gcs_folder,
                 source,
                 history_check,
                 mime_type = "text/plain",
                 destination_bucket ='jm-edl-landing-wip',
                 delegate_to=None,
                 *args,
                 **kwargs):


        super(sharepointtogcsoperator, self).__init__(*args, **kwargs)
        self.sharepoint_site = sharepoint_connection
        self.google_storage_connection = google_storage_connection_id
        self.file_url = source_file_url
        self.gcs_destination_path = gcs_destination_path
        self.mime_type = mime_type
        self.destination_bucket = destination_bucket
        self.delegate_to = delegate_to
        self.base_gcs_folder =base_gcs_folder
        self.source = source
        self.history_check = history_check

    def auth(self,context):

        logging.info('start processing')

        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")



        connection = BaseHook.get_connection(self.sharepoint_site)

        site_url = connection.host
        client_secret = connection.password
        client_username = connection.login

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_storage_connection,
            delegate_to=self.delegate_to)

        # Authenticating the credentials and connect to sharepoint team site
        credentials = ClientCredential(client_username, client_secret)
        ctx = ClientContext(site_url).with_credentials(credentials)


        #Getting list of files from the folder
        print('file_url')
        print(self.file_url)
        libraryRoot = ctx.web.get_folder_by_server_relative_url(self.file_url).execute_query()

        files = libraryRoot.files
        ctx.load(files)
        ctx.execute_query()
        active_clients_df = pd.DataFrame(columns=['file','modified_time'])
        report_submission_df = pd.DataFrame(columns=['file', 'modified_time'])
        i = 0

        ###logic to get latest file by type

        for file in files:
            file_path = file.properties["ServerRelativeUrl"]
            file_last_modified_time = file.properties["TimeLastModified"]
            file_name = os.path.basename(file_path)
            if 'submission' in file_name.lower():
                report_submission_df.loc[i,'file'] = file
                report_submission_df.loc[i, 'modified_time'] = file_last_modified_time
            elif 'active' in file_name.lower():
                active_clients_df.loc[i, 'file'] = file
                active_clients_df.loc[i, 'modified_time'] = file_last_modified_time
            i = i+1

        print('before timestmap conversion')
        print(active_clients_df)
        print('______________')
        print(report_submission_df)
        active_clients_df['modified_time'] = pd.to_datetime(active_clients_df['modified_time'])
        report_submission_df['modified_time'] = pd.to_datetime(report_submission_df['modified_time'])
        print('after timestmap conversion')
        print(active_clients_df)
        print('______________')
        print(report_submission_df)
        active_clients_df.sort_values('modified_time',inplace=True,ascending=False)
        report_submission_df.sort_values('modified_time', inplace=True,ascending=False)
        print('after sorting')
        print(active_clients_df)
        print('______________')
        print(report_submission_df)

        report_submission_df.reset_index(drop=True,inplace=True)
        report_submission_df.reset_index(drop=True, inplace=True)
        final_file_list = []
        print(active_clients_df)
        print('______________')
        print(report_submission_df)

        if len(report_submission_df) >= 1:
            final_file_list.append(active_clients_df.iloc[0]['file'])
            final_file_list.append(report_submission_df.iloc[0]['file'])
        else:
            final_file_list.append(active_clients_df.iloc[0]['file'])




        for myfile in final_file_list:
            file_path = myfile.properties["ServerRelativeUrl"]
            file_last_modified_time = myfile.properties["TimeLastModified"].split('T')

            # file_last_modified_time = file_last_modified_time[0]
            # if file_last_modified_time != context['ds']:
            #     continue

            logging.info("File path is: {file_path}".format(file_path = file_path))
            logging.info("file_last_modified_time: {file_last_modified_time}".format(file_last_modified_time=file_last_modified_time))

            #getting file name from the file path
            file_name = os.path.basename(file_path)
            logging.info("File name is: {file_name}".format(file_name = file_name))
            if ("Active Clients" not in file_name) and ("submission" not in file_name.lower()):
                logging.info("Not processing...... file name is: {file_name}".format(file_name=file_name))
                continue

            #copying the file to named temporaryfiles and upload it to GCS
            #Install xlrd >= 1.0.0 for Excel support Use pip or conda to install xlrd
            logging.info("processing...... file name is: {file_name}".format(file_name=file_name))
            with tempfile.NamedTemporaryFile() as localFile:
                name=localFile.name
                file = ctx.web.get_file_by_server_relative_url(file_path).download(localFile).execute_query()
                localFile.seek(0)
                content = localFile.read()
                path = self.gcs_destination_path + file_name
                hook.upload(
                    bucket=self.destination_bucket,
                    object=path,
                    filename=name,
                    mime_type=self.mime_type,
                )

                file_data = hook.download(self.destination_bucket, path)
                file_stream = io.BufferedReader(io.BytesIO(file_data))
                if "submission" not in file_name.lower():
                    norm_path = self.gcs_destination_path.replace('l1',
                                                                  'l1_norm') + 'l1_norm_{source}_active_clients.json'.format(
                        source=self.source)
                    schema_path = self.gcs_destination_path.replace('l1',
                                                                    'l1_schema') + 'l1_schema_{source}_active_clients.json'.format(
                        source=self.source)
                    audit_path = self.gcs_destination_path + 'l1_metadata_{source}_active_clients.json'.format(
                        source=self.source)
                    n = 0
                    encoding = 'utf-8'
                else:
                    norm_path = self.gcs_destination_path.replace('l1',
                                                                  'l1_norm') + 'l1_norm_{source}_report_submission.json'.format(
                        source=self.source)
                    schema_path = self.gcs_destination_path.replace('l1',
                                                                    'l1_schema') + 'l1_schema_{source}_report_submission.json'.format(
                        source=self.source)
                    audit_path = self.gcs_destination_path + 'l1_metadata_{source}_report_submission.json'.format(
                        source=self.source)
                    n = 3
                    encoding = 'cp1252'
                if '.xlsx' in file_name:
                    #data_df = pd.read_excel(file_stream, orient='records', lines='\n',engine='openpyxl', skiprows=n, encoding = encoding)
                    data_df = pd.read_excel(file_stream,  engine='openpyxl', skiprows=n)
                elif '.csv' in file_name:
                    data_df = pd.read_csv(file_stream,skiprows=n, encoding=encoding)
                else:
                    print('wrong file format present')
                    raise ()
                #data_df = pd.read_excel(localFile.name)

                self._normalize_data(data_df,context,hook,norm_path,schema_path,audit_path)






        return 0

    def execute(self, context):
        self.auth(context)

    def _schema_generator(self, df, context,hook,schema_path, default_type='STRING'):

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
        hook.upload(
            bucket=self.destination_bucket,
            object=schema_path,
            data=json.dumps(fields),
            mime_type=self.mime_type,
        )
        return 1
        # return_string



    def _upload_schema_to_gcs(self, json_normalized_df, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df, context)

        file_name = '{base_gcs_folder}_schema/{source}/{date_nodash}/l1_schema_{source}.json'.format(
            source=self.source.lower(),
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder)
        hook.upload(bucket=self.destination_bucket, object=file_name, data=schema_data)

        return

    def _normalize_data(self, df, context,hook,norm_path,schema_path,audit_path):
        dq = dq_common()
        new_cols = []
        for col in df.columns:
            col = col.lower()
            col = col.replace('.', '_').replace('-', '_').replace(',', '_').replace('#', 'num').replace(' ', '_')
            col = col.replace('$', '').replace('/', '_').replace('?', '')
            col = col.replace('__', ':')
            col = col.replace(':_', '_')
            col = col.replace(':', '_')
            new_cols.append(col)

        df.columns = new_cols
        df.reset_index(drop=True, inplace=True)
        data = dq.dq_column_group_cleanup(df)
        logging.info('clean up done')
        data = dq.gcp_bq_data_encoding(data)
        self._schema_generator(data, context, hook, schema_path)
        self._upload_metadata(context,data,audit_path)
        json_data = data.to_json(orient='records', lines='\n', date_format='iso')


        hook.upload(
            bucket=self.destination_bucket,
            object=norm_path,
            data=json_data,
            mime_type=self.mime_type,
        )
        return 1

    def _upload_metadata(self, context, df, audit_path):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_storage_connection,
            delegate_to=self.delegate_to)

        audit_df = pd.DataFrame()



        file_exists = hook.exists(self.destination_bucket, audit_path)
        logging.info('file_exists value is ' + str(file_exists))

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': len(df), 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        hook.upload(bucket=self.destination_bucket, object=audit_path, data=json_data)

        del json_data
        del audit_df

        return