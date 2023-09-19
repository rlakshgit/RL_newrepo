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
from pandas.io.json import json_normalize

desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class NiceToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('api_name', 'api_configuration', 'target_gcs_bucket', 'target_env')
    ui_color = '#d4f77c'

    @apply_defaults
    def __init__(self,
                 api_connection_id='NiceAPI',
                 http_access_request_conn_id='NiceAPIAccessKey',
                 api_name='NA',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encryped=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=50,
                 delegate_to=None,
                 base_gcs_folder='',
                 history_check='True',
                 metadata_filename='',
                 normalized_location='NA',
                 schema_location='NA',
                 *args,
                 **kwargs):

        super(NiceToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.http_access_request_conn_id = http_access_request_conn_id
        self.api_key_encrypted = api_key_encryped
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.api_name = api_name
        self.api_configuration = api_configuration
        self.target_gcs_bucket = target_gcs_bucket
        self.target_env = target_env
        self.max_loop_count = max_loop_count
        self.delegate_to = delegate_to
        self.offset_enable = False
        self.offset_df = pd.DataFrame()
        self.base_gcs_folder = base_gcs_folder
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.normalized_location = normalized_location
        self.schema_location = schema_location
        self.bearer_token = None
        self.api_url = None

    def execute(self, context):

        if self.history_check.upper() == 'TRUE':
            pull_new = False
            stream_response = self._check_history(context)
            # Failure capture method for errors in pulled data.
            ###{'error': 'invalid_request', 'error_description': 'CONTACTS_CMPLT8: Action handler threw an exception.'}
            if 'invalid_request' in str(stream_response):
                stream_response = None
            if stream_response == None:
                pull_new = True
            else:
                print('Pulling data set from history....')

        if pull_new:
            print('Pulling new data set....')
            # Get the access bearer token for the API
            nice_connection = BaseHook.get_connection(self.api_connection_id)
            self._get_access_token(nice_connection.login, nice_connection.password)
            # Build the header to handle passing to bearer token
            self._build_api_header()
            # Build the API URL
            self._build_api_url()
            # Build the payload for the API
            self._build_api_payload(context)
            # Get the API Data
            stream_response = self._get_api_data()
            print('>' * 20)
            # print(stream_response)
            print(self.api_url)
            print('<' * 20)

        # Preform data test to see if data is present
        try:
            stream_data = json.loads(stream_response)
        except:
            print('No data available for api...' + self.api_name + 'for date...' + context['ds'])
            print('Writing blank schema, normalization file')
            df_normalized = pd.DataFrame()
            self._upload_normalized_data_gcs(df_normalized, context)
            self._upload_schema_to_gcs(df_normalized, context)
            self._metadata_upload(context, len(df_normalized))
            return

        # Write Raw data to L1 folder
        self._upload_raw_data_gcs(stream_response, context)

        # Process Data for L1 Norm

        df_normalized = self._process_api_data(context, stream_data)

        if df_normalized.empty:
            print('No data available for api...' + self.api_name + 'for date...' + context['ds'])
            print('Writing blank schema, normalization file')
            df_normalized = pd.DataFrame()
            self._upload_normalized_data_gcs(df_normalized, context)
            self._upload_schema_to_gcs(df_normalized, context)
            self._metadata_upload(context, len(df_normalized))
            return

        # Normalize the data
        self._upload_normalized_data_gcs(df_normalized, context)
        # Generate Schema File
        self._upload_schema_to_gcs(df_normalized, context)
        if self.metadata_filename != None:
            self._metadata_upload(context, len(df_normalized))
        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        base_api_prefix = '{base_location}{base_api}/{date}/'.format(base_location=self.base_gcs_folder,
                                                                     base_api=self.api_name,
                                                                     date=context['ds_nodash'])

        print(base_api_prefix)
        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=base_api_prefix)

        print(base_file_list)
        file_data = None
        for f in base_file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, f)

        return file_data

    def _process_api_data(self, context, stream_data):
        if self.api_configuration[self.api_name]['api_parse_field'] != 'NA':
            if self.api_configuration[self.api_name]["api_record_path"] == 'NA':
                try:
                    df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']])
                except:
                    print('Parsing failure >>>>>>>>>>>>>>>>>>')
                    print(stream_data)
                    print(self.api_configuration[self.api_name]['api_parse_field'])
                    print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
                    pass

                    # raise

            elif self.api_configuration[self.api_name]['api_meta'] != "NA" and self.api_configuration[self.api_name][
                "api_record_path"] != 'NA':
                df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']],
                                         record_path=self.api_configuration[self.api_name]['api_record_path'],
                                         meta=self.api_configuration[self.api_name]['api_meta'])

            else:
                df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']],
                                         record_path=[self.api_configuration[self.api_name]['api_record_path']])

        else:
            try:
                df_norm = pd.DataFrame([stream_data])


            except:
                df_norm = pd.DataFrame(stream_data)

        new_cols = []
        for col in df_norm.columns:
            if 'date' in col:
                df_norm[col] = pd.to_datetime(df_norm[col])
                print('Column...', col, '....Converted to DateTime')
            else:
                # print('Column...',col,'....Converted to String')
                df_norm[col] = df_norm[col].astype(str)

            new_cols.append(col.replace('.', '_'))

        df_norm.columns = new_cols

        # print('*' * 20)
        # print(df_norm)
        # print('*' * 20)

        return df_norm

    def _upload_raw_data_gcs(self, data, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_{api_name}_{counter}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data)
        return

    def _upload_normalized_data_gcs(self, df_in, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_{api_name}_{counter}.json'.format(
            base_gcs_folder=self.normalized_location,
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _get_api_data(self):
        http_hook = HttpHook(method='DIRECT_GET', http_conn_id=self.api_connection_id)
        if self.api_payload == {}:
            response = http_hook.run(self.api_url, headers=self.api_header)
        else:
            response = http_hook.run(self.api_url, headers=self.api_header, data=self.api_payload)
        return response.text

    def _get_access_token(self, accessID, accessKEY):
        access_http = HttpHook(method='DIRECT_POST', http_conn_id=self.http_access_request_conn_id)
        header_param = {"Content-Type": "application/json"}
        payload = {"accessKeyId": accessID, "accessKeySecret": accessKEY}
        response = access_http.run('', headers=header_param, data=payload)
        json_data = json.loads(response.text)
        self.bearer_token = json_data['access_token']
        return

    def _build_api_url(self, supporting_filter='NA'):
        if supporting_filter == 'NA':
            api_url = self.api_configuration[self.api_name]['api_url'].format(
                version=self.api_configuration[self.api_name]['api_version'])
            self.api_url = '{API_URL}'.format(API_URL=api_url)
            return
        else:
            api_url = self.api_configuration[self.api_name][supporting_filter + '_url'].format(
                version=self.api_configuration[self.api_name]['api_version'],
                sub_index='__TBD__')
            self.api_url = '{API_URL}'.format(API_URL=api_url)
            return

    def _build_api_header(self):
        self.api_header = {}
        self.api_header['Authorization'] = "bearer {ACCESS_TOKEN}".format(ACCESS_TOKEN=self.bearer_token)
        self.api_header['content-Type'] = "application/json"
        return

    def _build_api_payload(self, context, supporting_filter='NA'):
        factors = {'startDate': context['ds'],
                   'endDate': context['next_ds'],
                   'updatedSince': context['ds'],
                   }

        if supporting_filter == 'NA':
            payload_items = self.api_configuration[self.api_name]['api_payload_params'].split(';')
        else:
            payload_items = self.api_configuration[self.api_name][supporting_filter + '_payload_params'].split(';')

        self.api_payload = {}
        for p in payload_items:
            if p == 'None':
                continue
            p_split = p.split(':')
            if len(p_split) == 1:
                self.api_payload[p] = factors[p]
            else:
                try:
                    self.api_payload[p_split[0]] = int(p_split[1])
                except:
                    self.api_payload[p_split[0]] = str(p_split[1])
        return

    def _upload_schema_to_gcs(self, json_normalized_df, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df)

        file_name = '{schema_location}{api_name}/{date_nodash}/l1_schema_{source}_{api_name}.json'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            schema_location=self.schema_location,
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return

    def _schema_generator(self, df, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',
        for col in df.columns:
            if 'date' in col:
                df[col] = df[col].fillna(pd.NaT)
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print('Column...', col, '....Converted to DateTime')
            else:
                # print('Column...',col,'....Converted to String')
                df[col] = df[col].astype(str)

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
        return json.dumps(fields)

    def _metadata_upload(self, context, row_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_prefix = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.normalized_location,
            api_name=self.api_name)

        # if '{date_nodash}' in self.metadata_filename:
        self.metadata_filename = self.metadata_filename.replace('date_nodash', context['ds_nodash'])

        print('Checking File Prefix:', file_prefix)
        print('Metadata File - ', self.metadata_filename)

        if check_landing:
            file_list = []
            file_list = gcs_hook.list(self.target_gcs_bucket, versions=True, maxResults=1000, prefix=file_prefix,
                                      delimiter=',')

            l1_count = 0
            file_counter = 0
            for f in file_list:
                file_counter += 1
                stream_data = gcs_hook.download(self.target_gcs_bucket, f)
                file_stream = io.BufferedReader(io.BytesIO(stream_data))
                df = pd.read_json(file_stream, orient='records', lines='\n')
                l1_count += len(df)
                del df
        else:
            l1_count = 0

        json_metadata = {
            'source_count': row_count,
            'l1_count': l1_count,
            'dag_execution_date': context['ds']
        }

        print(json_metadata)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)
        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return


class NiceSupportingAPIToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('api_name', 'api_configuration', 'target_gcs_bucket', 'target_env')
    ui_color = '#edd539'

    @apply_defaults
    def __init__(self,
                 api_name,
                 base_api_name,
                 base_api_location,
                 api_connection_id='NiceAPI',
                 http_access_request_conn_id='NiceAPIAccessKey',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encryped=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=50,
                 delegate_to=None,
                 base_gcs_folder='',
                 history_check='True',
                 metadata_filename='',
                 normalized_location='NA',
                 schema_location='NA',
                 *args,
                 **kwargs):

        super(NiceSupportingAPIToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.base_api_name = base_api_name
        self.base_api_location = base_api_location
        self.http_access_request_conn_id = http_access_request_conn_id
        self.api_key_encrypted = api_key_encryped
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.api_name = api_name
        self.api_configuration = api_configuration
        self.target_gcs_bucket = target_gcs_bucket
        self.target_env = target_env
        self.max_loop_count = max_loop_count
        self.delegate_to = delegate_to
        self.offset_enable = False
        self.offset_df = pd.DataFrame()
        self.base_gcs_folder = base_gcs_folder
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.normalized_location = normalized_location
        self.schema_location = schema_location
        self.bearer_token = None
        self.api_url = None
        self.history_check = 'True'

    def execute(self, context):

        if self.history_check.upper() == 'TRUE':
            # The history check will read all the files in the directory and return the row count.
            #
            row_count = self._check_history(context)
            if row_count != None:
                print('Pulling data set from history....')
                self._history_metadata_upload(context, row_count)
                return

        # Get data from the bucket tied to the dag run and base API
        base_data_df = self._get_base_data(context)

        if len(base_data_df) == 0:
            print('No data to process in the base API of ', self.base_api_name)
            df_normalized = pd.DataFrame()
            # Normalize the data
            self._upload_normalized_data_gcs(df_normalized, context)
            # Generate Schema File
            self._upload_schema_to_gcs(df_normalized, context)
            if self.metadata_filename != None:
                self._metadata_upload(context, len(df_normalized))
            return

        # Get the access bearer token for the API
        nice_connection = BaseHook.get_connection(self.api_connection_id)
        self._get_access_token(nice_connection.login, nice_connection.password)
        # Build the header to handle passing to bearer token
        self._build_support_api_header()
        # Build the API URL
        self._build_support_api_url(supporting_filter=self.api_name)
        # Build the payload for the API
        self._build_api_payload(context)

        # print(base_data_df)
        df_normalized = pd.DataFrame()
        output_json = []
        counter = 0
        try:
            get_list = base_data_df[self.api_configuration[self.api_name]['api_supporting_apis_index']].unique()
        except:
            print('Target column ', self.api_configuration[self.api_name]['api_supporting_apis_index'],
                  ' not present in base api...', self.base_api_name)
            raise

        base_url = self.api_url
        for id in base_data_df[self.api_configuration[self.api_name]['api_supporting_apis_index']].unique():
            if id == None:
                continue
            self.api_url = base_url.replace('__TBD__', str(id))
            # Get the API Data
            stream_response = self._get_api_data()
            if len(stream_response) == 0:
                continue

            stream_data = json.loads(stream_response)
            stream_data.update(
                {'base_api_' + self.api_configuration[self.api_name]['api_supporting_apis_index']: str(id)})
            output_json.append(stream_data)

            df_id_data = self._process_supporting_api_data(context, stream_data)
            if len(df_id_data) > 0:
                df_id_data['base_api_' + self.api_configuration[self.api_name]['api_supporting_apis_index']] = str(id)
                df_normalized = pd.concat([df_normalized, df_id_data])
            counter += 1

        df_normalized.reset_index(drop=True, inplace=True)
        df_raw = pd.DataFrame(output_json)

        # Write Raw data to L1 folder
        self._upload_raw_data_gcs(df_raw.to_json(orient='records', lines='\n', date_format='iso'), context)

        if len(df_normalized) > 0:
            print('df norm length more than 0')
        # print(df_normalized)
        # raise

        # Normalize the data
        self._upload_normalized_data_gcs(df_normalized, context)
        # Generate Schema File
        self._upload_schema_to_gcs(df_normalized, context)
        if self.metadata_filename != None:
            self._metadata_upload(context, len(df_normalized))

        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        base_api_prefix = '{base_location}{base_api}/{date}/'.format(base_location=self.base_gcs_folder,
                                                                     base_api=self.api_name,
                                                                     date=context['ds_nodash'])

        print(base_api_prefix)
        base_api_prefix = base_api_prefix.replace('l1/', 'l1_norm/')
        print(base_api_prefix)
        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=base_api_prefix)

        print(base_file_list)
        if len(base_file_list) == 0:
            return None
        file_data = None
        row_count = 0
        for f in base_file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            row_count += len(df)
        print(row_count)
        if row_count > 0:
            return row_count
        else:
            return None
        return None

    def _history_metadata_upload(self, context, row_count):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_prefix = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.normalized_location,
            api_name=self.api_name)

        # if '{date_nodash}' in self.metadata_filename:
        self.metadata_filename = self.metadata_filename.replace('date_nodash', context['ds_nodash'])

        print('Checking File Prefix:', file_prefix)
        print('Metadata File - ', self.metadata_filename)

        json_metadata = {
            'source_count': row_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        print(json_metadata)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)
        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _get_base_data(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        base_api_prefix = '{base_location}{base_api}/{date}/'.format(base_location=self.base_api_location,
                                                                     base_api=self.base_api_name,
                                                                     date=context['ds_nodash'])
        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=base_api_prefix)
        df_return = pd.DataFrame()
        for f in base_file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            df_return = pd.concat([df_return, df])
        df_return.reset_index(drop=True, inplace=True)
        return df_return

    def _process_supporting_api_data(self, context, stream_data):
        if self.api_configuration[self.api_name]['api_parse_field'] != 'NA':
            if self.api_configuration[self.api_name]["api_record_path"] == 'NA':
                df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']])

            elif self.api_configuration[self.api_name]['api_meta'] != "NA" and self.api_configuration[self.api_name][
                "api_record_path"] != 'NA':
                df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']],
                                         record_path=self.api_configuration[self.api_name]['api_record_path'],
                                         meta=self.api_configuration[self.api_name]['api_meta'])

            else:
                df_norm = json_normalize(data=stream_data[self.api_configuration[self.api_name]['api_parse_field']],
                                         record_path=[self.api_configuration[self.api_name]['api_record_path']])

        else:
            try:
                df_norm = pd.DataFrame([stream_data])


            except:
                df_norm = pd.DataFrame(stream_data)

        for col in df_norm.columns:
            if 'date' in col:
                df_norm[col] = pd.to_datetime(df_norm[col])
                print('Column...', col, '....Converted to DateTime')
            else:
                # print('Column...',col,'....Converted to String')
                df_norm[col] = df_norm[col].astype(str)

        return df_norm

    def _upload_raw_data_gcs(self, data, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_{api_name}_{counter}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=data)
        return

    def _upload_normalized_data_gcs(self, df_in, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_{api_name}_{counter}.json'.format(
            base_gcs_folder=self.normalized_location,
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _get_api_data(self):
        http_hook = HttpHook(method='DIRECT_GET', http_conn_id=self.api_connection_id)
        if self.api_payload == {}:
            response = http_hook.run(self.api_url, headers=self.api_header)
        else:
            response = http_hook.run(self.api_url, headers=self.api_header, data=self.api_payload)
        return response.text

    def _get_access_token(self, accessID, accessKEY):
        access_http = HttpHook(method='DIRECT_POST', http_conn_id=self.http_access_request_conn_id)
        header_param = {"Content-Type": "application/json"}
        payload = {"accessKeyId": accessID, "accessKeySecret": accessKEY}
        response = access_http.run('', headers=header_param, data=payload)
        json_data = json.loads(response.text)
        self.bearer_token = json_data['access_token']
        return

    def _build_support_api_url(self, supporting_filter='NA'):
        if supporting_filter == 'NA':
            api_url = self.api_configuration[self.api_name]['api_url'].format(
                version=self.api_configuration[self.api_name]['api_version'])
            self.api_url = '{API_URL}'.format(API_URL=api_url)
            return
        else:
            api_url = self.api_configuration[self.api_name]['api_url'].format(
                version=self.api_configuration[self.api_name]['api_version'],
                sub_index='__TBD__')

            self.api_url = '{API_URL}'.format(API_URL=api_url)
            return

    def _build_support_api_header(self):
        self.api_header = {}
        self.api_header['Authorization'] = "bearer {ACCESS_TOKEN}".format(ACCESS_TOKEN=self.bearer_token)
        self.api_header['content-Type'] = "application/json"
        return

    def _build_api_payload(self, context, supporting_filter='NA'):
        factors = {'startDate': context['ds'],
                   'endDate': context['next_ds'],
                   'updatedSince': context['ds'],
                   }

        if supporting_filter == 'NA':
            payload_items = self.api_configuration[self.api_name]['api_payload_params'].split(';')
        else:
            payload_items = self.api_configuration[self.api_name][supporting_filter + '_payload_params'].split(';')

        self.api_payload = {}
        for p in payload_items:
            if p == 'None':
                continue
            p_split = p.split(':')
            if len(p_split) == 1:
                self.api_payload[p] = factors[p]
            else:
                try:
                    self.api_payload[p_split[0]] = int(p_split[1])
                except:
                    self.api_payload[p_split[0]] = str(p_split[1])
        return

    def _upload_schema_to_gcs(self, json_normalized_df, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df)

        file_name = '{schema_location}{api_name}/{date_nodash}/l1_schema_{source}_{api_name}.json'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            schema_location=self.schema_location,
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return

    def _schema_generator(self, df, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',

        for col in df.columns:
            if 'date' in col:
                df[col] = df[col].fillna(pd.NaT)
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print('Column...', col, '....Converted to DateTime')
            else:
                # print('Column...',col,'....Converted to String')
                df[col] = df[col].astype(str)

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
        return json.dumps(fields)

    def _metadata_upload(self, context, row_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_prefix = '{base_gcs_folder}{api_name}/{date_nodash}/l1_data_nice_'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.normalized_location,
            api_name=self.api_name)

        # if '{date_nodash}' in self.metadata_filename:
        self.metadata_filename = self.metadata_filename.replace('date_nodash', context['ds_nodash'])

        print('Checking File Prefix:', file_prefix)
        print('Metadata File - ', self.metadata_filename)

        if check_landing:
            file_list = []
            file_list = gcs_hook.list(self.target_gcs_bucket, versions=True, maxResults=1000, prefix=file_prefix,
                                      delimiter=',')

            l1_count = 0
            file_counter = 0
            for f in file_list:
                file_counter += 1
                stream_data = gcs_hook.download(self.target_gcs_bucket, f)
                file_stream = io.BufferedReader(io.BytesIO(stream_data))
                df = pd.read_json(file_stream, orient='records', lines='\n')
                l1_count += len(df)
                del df
        else:
            l1_count = 0

        json_metadata = {
            'source_count': row_count,
            'l1_count': l1_count,
            'dag_execution_date': context['ds']
        }

        print(json_metadata)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)
        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return