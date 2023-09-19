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
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class HubspotToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('api_name', 'api_configuration', 'target_gcs_bucket', 'target_env')
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 api_connection_id='HubspotAPI',
                 api_name='NA',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encryped=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=200,
                 check_limit=True,
                 delegate_to=None,
                 base_gcs_folder='',
                 history_check='True',
                 metadata_filename=None,
                 file_prefix='',
                 *args,
                 **kwargs):

        super(HubspotToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
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
        self.check_limit = check_limit
        self.offset_df = pd.DataFrame()
        self.base_gcs_folder = base_gcs_folder
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.file_prefix = file_prefix

    def execute(self, context):
        # Variable.set(self.api_name + '_gcs_to_bq', 'True')
        file_dt = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        if ('_details' in self.api_name) or ('contactsInLists' == self.api_name):
            print('Hit Details...API')
            if self.api_name == 'contactsInLists':
                file_list = self._l1_norm_data_check(context, 'contactLists').split(',')
            else:
                file_list = self._l1_norm_data_check(context, self.api_name.replace('_details', '')).split(',')

            print(file_list)
            total_row_counter = 0
            counter = 0
            for f in file_list:
                if str(f) == 'NONE':
                    continue
                print('Processing based API information for getting detail information....', f)
                details_id_list = self._process_details_from_l1_gcs(context, f)
                url = self._build_url(offset_string='', context=context)
                df_details = pd.DataFrame()
                for id in details_id_list:
                    details_url = url.replace(
                        '{id_value}'.format(id_value=self.api_configuration[self.api_name]['details_target']), str(id))
                    details_url = details_url.replace('{', '').replace('}', '')
                    df = self._call_api_return_df(details_url, self.api_connection_id)
                    df_details = pd.concat([df_details, df])
                counter = f.split('_')[-1]
                counter = counter.replace('.json', '')
                new_cols = []
                for col in df_details.columns:
                    if col == None:
                        del df_details[col]
                        continue
                    new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))

                df_details.columns = new_cols
                df_details.reset_index(drop=True, inplace=True)
                total_row_counter += len(df_details)
                write_return = self._upload_df_to_gcs(df_details, context, file_dt, counter)
            if self.metadata_filename != None:
                self._metadata_upload(context, total_row_counter, counter)

            return

        # self.history_check = 'False'
        if self.history_check.upper() == 'TRUE':
            if self._l1_data_check(context) != 'NONE':
                file_list = self._l1_data_check(context).split(',')
                base_file_df = pd.DataFrame()
                max_counter = -1
                temp_filename_store = ''
                for f in file_list:
                    if 'l1_data_' not in f:
                        continue
                    file_split = f.split('/')
                    target_filename = file_split[len(file_split) - 1]
                    fn_split = target_filename.split('.')
                    fn_split = fn_split[0].split('_')
                    counter_value = fn_split[len(fn_split) - 1]
                    if int(counter_value) > max_counter:
                        max_counter = int(counter_value)
                        temp_filename_store = f

                history_count = 0
                counter = 0
                for f in file_list:
                    if 'l1_data_' not in f:
                        continue
                    print('Processing the following file: ', f)
                    file_row_count = self._process_from_l1_gcs(context, f, history_count, temp_filename_store)
                    history_count += file_row_count
                    counter += 1

                if self.metadata_filename != None:
                    self._metadata_upload(context, history_count, counter)
                return 0

        ## Get Offset file if present:
        if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
            self.offset_df = self._get_offset_information(context)
            self.offset_df['dag_execution_date'] = self.offset_df['dag_execution_date'].astype('str')
            get_ds_offsetdata = self.offset_df[(self.offset_df['dag_execution_date'] == str(context['ds_nodash']))]
            get_ds_offsetdata.reset_index(drop=True, inplace=True)
            get_index = get_ds_offsetdata.loc[0, 'idx']
            offsetValue = self.offset_df.loc[get_index, 'start_offset']
        else:
            offsetValue = 'NA'

        print('>> Start Config information ----->>>>>>')
        print(context)
        print(offsetValue)
        print('<< End Config information ----- <<<<<<<')

        ### This is for just getting a base API.  We will need to then determine if it there is support or detail information required.
        df = pd.DataFrame()
        counter = 0
        url = ''
        url = self._build_url(offset_string=self._build_offset_string(offsetValue), context=context)

        start_offset_value = offsetValue
        df = self._call_api_return_df(url, self.api_connection_id)

        # Get the last row of data to get the offset value
        if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
            df_end = df.tail(1)
            df_end.reset_index(drop=True, inplace=True)
            if len(df_end) > 0:
                try:
                    end_offset_value = df_end.loc[0, self.api_configuration[self.api_name]['api_offset_key']]
                except:
                    offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('_', '-')
                    end_offset_value = df_end.loc[0, offset_string]
            else:
                end_offset_value = offsetValue
        else:
            end_offset_value = 'NA'

        write_return = self._upload_df_to_gcs(df, context, file_dt, counter)
        if write_return == -1:
            print('No data present ending further processing...')
            df = pd.DataFrame()
            Variable.set(self.api_name + '_gcs_to_bq', 'False')
            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                self._update_offset_information(context, self.offset_df, start_offset_value, end_offset_value)

            if self.metadata_filename != None:
                self._metadata_upload(context, 0, 0, False)
            return -1

        total_row_counter = len(df)

        counter += 1

        if self.api_configuration[self.api_name]['api_has_more_enabled'].upper() == 'TRUE':
            df_end = df.tail(1)
            df_end.reset_index(drop=True, inplace=True)
            if len(df_end) > 0:
                if self.api_configuration[self.api_name]['api_offset_key'] in df_end.columns:
                    end_offset_value = df_end.loc[0, self.api_configuration[self.api_name]['api_offset_key']]
                elif '_' in self.api_configuration[self.api_name]['api_offset_key']:
                    offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('_', '-')
                    end_offset_value = df_end.loc[0, offset_string]
                else:
                    offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('-', '_')
                    end_offset_value = df_end.loc[0, offset_string]
            else:
                end_offset_value = offsetValue

            try:
                end_while = df_end.loc[0, self.api_configuration[self.api_name]['api_has_more_label']]
            except:
                label_check = self.api_configuration[self.api_name]['api_has_more_label'].replace('-', '_')
                end_while = df_end.loc[0, label_check]

            while end_while:
                df_hasmore = pd.DataFrame()
                url = self._build_url(offset_string=self._build_offset_string(end_offset_value), context=context)
                df_hasmore = self._call_api_return_df(url, self.api_connection_id)

                if len(df_hasmore) == 0:
                    break
                self._upload_df_to_gcs(df_hasmore, context, file_dt, counter)

                df_end = df_hasmore.tail(1)
                df_end.reset_index(drop=True, inplace=True)
                try:
                    end_while = df_end.loc[0, self.api_configuration[self.api_name]['api_has_more_label']]
                except:
                    label_check = self.api_configuration[self.api_name]['api_has_more_label'].replace('-', '_')
                    end_while = df_end.loc[0, label_check]

                if self.api_configuration[self.api_name]['api_offset_key'] in df_end.columns:
                    end_offset_value = df_end.loc[0, self.api_configuration[self.api_name]['api_offset_key']]
                elif '_' in self.api_configuration[self.api_name]['api_offset_key']:
                    offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('_', '-')
                    end_offset_value = df_end.loc[0, offset_string]
                else:
                    offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('-', '_')
                    end_offset_value = df_end.loc[0, offset_string]

                total_row_counter += len(df_hasmore)

                print('Offset Value: ', start_offset_value, end_offset_value)
                if counter > self.max_loop_count and self.check_limit:
                    print('Hit max count allotment....breaking loop.')
                    break

                counter += 1

        if self.api_configuration[self.api_name]['api_limit_enabled'].upper() == 'TRUE':

            def calculate_page(df_page, page_counter):
                try:
                    total_counter = df_page.loc[0, 'total']
                except:
                    total_counter = df_page.loc[0, 'total_count']

                if int(page_counter) < int(total_counter):
                    end_while_val = True
                else:
                    end_while_val = False

                return end_while_val

            df_end = df.tail(1)
            df_end.reset_index(drop=True, inplace=True)
            if 'limit' in df_end.columns.values.tolist():
                print('offset value to start is: {}'.format(total_row_counter))
                end_while = calculate_page(df_end, total_row_counter)
                end_offset_value = total_row_counter

                while end_while:
                    df_hasmore = pd.DataFrame()
                    url = self._build_url(offset_string=self._build_offset_string(end_offset_value), context=context)
                    df_hasmore = self._call_api_return_df(url, self.api_connection_id)

                    if len(df_hasmore) == 0:
                        break
                    self._upload_df_to_gcs(df_hasmore, context, file_dt, counter)

                    try:
                        end_offset_value += len(df_hasmore)
                    except:
                        print('Could not update offset value')

                    df_end = df_hasmore.tail(1)
                    df_end.reset_index(drop=True, inplace=True)

                    end_while = calculate_page(df_end, end_offset_value)
                    total_row_counter += len(df_hasmore)

                    print('Offset Value: ', start_offset_value, end_offset_value)
                    if counter > self.max_loop_count and self.check_limit:
                        print('Hit max count allotment....breaking loop.')
                        break

                    counter += 1

        if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
            self._update_offset_information(context, self.offset_df, start_offset_value, end_offset_value)

        if self.metadata_filename != None:
            self._metadata_upload(context, total_row_counter, counter)
        Variable.set(self.api_name + '_gcs_to_bq', 'True')
        return 0

    def _metadata_upload(self, context, row_count, file_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_prefix = '{base_gcs_folder}/{source}/{api_name}/{date_nodash}/l1_data_hubspot_'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder,
            api_name=self.api_name)

        print('Checking File Prefix:', file_prefix)

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
            'file_count': file_count,
            'dag_execution_date': context['ds']
        }

        print(json_metadata)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)
        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename.format(ds_nodash=context['ds_nodash']),
                        df.to_json(orient='records', lines='\n'))
        return

    def _to_millis(self, dt):
        return int(pd.to_datetime(dt).value / 10 ** 6)

    def _call_api_return_df(self, url, http_con_id):
        http = HttpHook(method='GET', http_conn_id=http_con_id)

        retry_count = 0
        # restResponse = http.run(url)
        # print('*' * 20)
        # print(restResponse)
        # print('*' * 20)

        while True:
            try:
                restResponse = http.run(url)
                break
            except:
                if retry_count > 5:
                    print('URL response failed....Failing the TASK')
                    sleep(1)
                    raise
                retry_count += 1

        if restResponse == {}:
            return pd.DataFrame()

        for line in restResponse.iter_lines():
            json_data = json.loads(line)
        try:
            df = pd.DataFrame.from_dict(json_data)
        except:
            json_data_list = [json_data]
            df = pd.DataFrame(json_data_list)

        try:
            if self.api_configuration[self.api_name]['api_output_enable'].upper() == 'TRUE':
                print(df)
        except:
            pass

        return df

    def _build_offset_string(self, offsetValue):
        if offsetValue != 'NA':
            offset_string = "&{offset_key}={offset_value}".format(
                offset_key=self.api_configuration[self.api_name]["api_offsetKeyForURL"],
                offset_value=offsetValue)
        else:
            offset_string = ''
        return offset_string

    def _update_offset_information(self, context, df, start_offset, end_offset):

        df_dag_date = df[(df['dag_execution_date'] == context['ds_nodash'])]
        df_dag_date.reset_index(drop=True, inplace=True)
        df_idx = df_dag_date.loc[0, 'idx']

        df.loc[df_idx, 'start_offset'] = start_offset
        df.loc[df_idx, 'end_offset'] = end_offset
        df.loc[df_idx + 1, 'dag_execution_date'] = context['tomorrow_ds_nodash']
        df.loc[df_idx + 1, 'start_offset'] = end_offset
        print('============ DF_INDEX ===============')
        print(df_idx)
        print('+' * 20)
        del df['idx']
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        file_name = '{prefix}offset_files/{source}/offset_{api_name}.conf'.format(
            source=self.api_configuration[self.api_name]['api_source_name'].lower(),
            api_name=self.api_name, prefix=self.file_prefix)
        json_data = df.to_json(orient='records', lines='\n')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)

        return

    def _get_offset_information(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)  # , delegate_to=self.delegate_to)
        offsetFileName = '{prefix}offset_files/{source}/offset_{api_name}.conf'.format(
            source=self.api_configuration[self.api_name]['api_source_name'].lower(),
            api_name=self.api_name, prefix=self.file_prefix)
        try:
            stream_data = hook.download(self.target_gcs_bucket, offsetFileName)  # .decode()
            file_stream = io.BufferedReader(io.BytesIO(stream_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            df.sort_values(by=['dag_execution_date', 'start_offset'], ascending=[True, True], inplace=True)
            df.reset_index(drop=True, inplace=True)
        except:
            print('no offset file')
            df = pd.DataFrame()
            df.loc[0, 'dag_execution_date'] = context['ds_nodash']
            df.loc[0, 'start_offset'] = 'NA'
            df.loc[0, 'end_offset'] = 'NA'

        if 'index' in df.columns:
            del df['index']
        df.reset_index(drop=False, inplace=True)
        df['idx'] = df['index']
        del df['index']
        return df

    def _build_url(self, offset_string, context):
        API_SECRET_KEY = self._process_api_key()

        if self.api_configuration[self.api_name]['api_time_based'].upper() == 'TRUE':
            if self.api_configuration[self.api_name]['api_date_type'].upper() == 'ISO':
                start_date = context['ds_nodash']
                end_date = context['ds_nodash']
            else:
                start_date = self._to_millis(context['prev_ds'])
                end_date = self._to_millis(context['ds'])

            time_string = '&{start_key}={start_date_value}&{end_key}={end_date_value}'.format(
                start_key=self.api_configuration[self.api_name]['api_start_date_key'],
                start_date_value=start_date,
                end_key=self.api_configuration[self.api_name]['api_end_date_key'],
                end_date_value=end_date)
        else:
            time_string = ''

        if self.api_configuration[self.api_name]['api_limit'] != 'NA':
            limit_string = self.api_configuration[self.api_name]['api_limit']
        else:
            limit_string = ''

        if self.api_configuration[self.api_name]['api_filter'] != 'NA':
            filter_string = self.api_configuration[self.api_name]['api_filter']
        else:
            filter_string = ''

        url = '{api_url}?{api_secret_key_label}={API_SECRET_KEY}{limit_string}{offset_string}{time_string}{filter_string}'.format(
            api_url=self.api_configuration[self.api_name]['api_url'],
            API_SECRET_KEY=API_SECRET_KEY,
            api_secret_key_label=self.api_configuration[self.api_name]['api_secret_key_label'],
            limit_string=limit_string,
            offset_string=offset_string,
            time_string=time_string,
            filter_string=filter_string)
        return url

    def _process_api_key(self):
        if self.api_key_encrypted:
            connection = BaseHook.get_connection(self.api_connection_id)
            API_SECRET_KEY = str(base64.b64decode(connection.password))
        else:
            API_SECRET_KEY = self.api_connection_id

        API_SECRET_KEY = API_SECRET_KEY.replace("b'", '').replace("'", '')
        return API_SECRET_KEY

    def _standardize_datatypes(self, df):
        for column_name, dtype in df.dtypes.iteritems():
            if (dtype.kind != 'M'):
                df[column_name] = df[column_name].astype(str)
        return df

    def _normalize_data(self, df, context):

        file_date_time = pd.to_datetime(context['ds_nodash'], format='%Y%m%d').strftime('%Y-%m-%d')

        if self.api_configuration[self.api_name]['api_normalization_required'].upper() == 'FALSE':
            df = self._standardize_datatypes(df)

            t_now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df['data_land_date'] = file_date_time
            df['data_land_date'] = pd.to_datetime(df['data_land_date'], format='%Y-%m-%d %H:%M:%S')  # ,unit='s')
            df['data_run_date'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
            df['time_central'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
            return df

        print(self.api_name)
        print(self.api_configuration[self.api_name]['api_normalization_required'])

        fields_to_normalize = self.api_configuration[self.api_name]['api_normalization_fields'].split(':')
        if self.api_configuration[self.api_name]['api_normalization_fields'] == 'NA':
            fields_to_normalize = []
        for field in fields_to_normalize:
            print('>>>>>>>>> ----   ', field)
            B = df[field]
            if field == context['ds']:
                A = pd.DataFrame((flatten(d) for d in B))
            else:
                try:
                    A = pd.DataFrame((flatten(d) for d in B)).add_prefix(field + '.')
                except:
                    print('Unable to flatten direct, must load as json.....')
                    A = pd.DataFrame((flatten(json.loads(d.replace("'", '"'))) for d in B)).add_prefix(field + '.')
            del df[field]
            for col_sub in A.columns:
                df[col_sub] = A[col_sub].copy(deep=True)

        new_cols = []
        for col in df.columns:
            if col == None:
                del df[col]
                continue
            new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))

        df.columns = new_cols
        df.reset_index(drop=True, inplace=True)
        df = self._standardize_datatypes(df)
        t_now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['data_land_date'] = file_date_time
        df['data_land_date'] = pd.to_datetime(df['data_land_date'], format='%Y-%m-%d %H:%M:%S')  # ,unit='s')
        df['data_run_date'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
        df['time_central'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
        return df

    def _schema_generator(self, df, context, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',
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

    def _upload_normalized_to_gcs(self, df, context, file_dt, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        json_normalized_df = self._normalize_data(df, context)
        schema_data_previous = 'NA'
        schema_check = True
        print(':::::>>>>>>', counter)
        if int(counter) > 0:
            schema_file_name = '{base_gcs_folder}_schema/{source}/{api_name}/{date_nodash}/l1_schema_hubspot_{api_name}.json'.format(
                source=self.api_configuration[self.api_name]['api_source_name'],
                date_nodash=context['ds_nodash'],
                date_time=file_dt,  # context['ts_nodash'],
                base_gcs_folder=self.base_gcs_folder,
                api_name=self.api_name)
            schema_data_previous = hook.download(self.target_gcs_bucket, schema_file_name)
            schema_check = True
        else:
            print('Loading yesterday schema file')
            try:
                schema_file_name = '{base_gcs_folder}_schema/{source}/{api_name}/{date_nodash}/l1_schema_hubspot_{api_name}.json'.format(
                    source=self.api_configuration[self.api_name]['api_source_name'],
                    date_nodash=context['prev_ds_nodash'],
                    date_time=file_dt,  # context['ts_nodash'],
                    base_gcs_folder=self.base_gcs_folder,
                    api_name=self.api_name)
                schema_data_previous = hook.download(self.target_gcs_bucket, schema_file_name)
                schema_check = True
            except:
                schema_check = False

        if schema_check:
            schema_df = pd.read_json(schema_data_previous)
            for col in schema_df['name'].values.tolist():
                if col not in json_normalized_df.columns:
                    json_normalized_df[col] = None

        file_name = '{base_gcs_folder}_norm/{source}/{api_name}/{date_nodash}/l1_data_hubspot_{api_name}_{counter}.json'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            date_time=file_dt,  # context['ts_nodash'],
            counter=str(counter),
            base_gcs_folder=self.base_gcs_folder,
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=json_normalized_df.to_json(orient='records', lines='\n', date_format='iso'))
        return json_normalized_df

    def _upload_schema_to_gcs(self, json_normalized_df, context, file_dt, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df, context)

        file_name = '{base_gcs_folder}_schema/{source}/{api_name}/{date_nodash}/l1_schema_hubspot_{api_name}.json'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            date_time=file_dt,  # context['ts_nodash'],
            counter=str(counter),
            base_gcs_folder=self.base_gcs_folder,
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return

    def _upload_df_to_gcs(self, df, context, file_dt, counter=0):
        if df.empty:
            print('Dataframe is empty.  Returning to main.....')
            return -1

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        json_data = df.to_json(orient='records', lines='\n', date_format='iso')
        # file_name = '{base_gcs_folder}/{source}/{api_name}/{date_nodash}/hubspot_{api_name}_{date_time}_{counter}.json'.format(
        #     source=self.api_configuration[self.api_name]['api_source_name'],
        #     date_nodash=context['ds_nodash'],
        #     date_time=file_dt,#context['ts_nodash'],
        #     counter=str(counter),
        #     base_gcs_folder=self.base_gcs_folder,
        #     api_name=self.api_name)
        file_name = '{base_gcs_folder}/{source}/{api_name}/{date_nodash}/l1_data_hubspot_{api_name}_{counter}.json'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            base_gcs_folder=self.base_gcs_folder,
            api_name=self.api_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)
        del json_data

        json_normalized_df = self._upload_normalized_to_gcs(df, context, file_dt, counter)

        del df
        self._upload_schema_to_gcs(json_normalized_df, context, file_dt, counter)

        return 0

    def _l1_data_check(self, context, counter=0, return_type='LAST'):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_prefix = '{base_gcs_folder}/{source}/{api_name}/{date_nodash}/'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder,
            api_name=self.api_name)
        file_list = []
        file_list = hook.list(self.target_gcs_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        if len(file_list) == 0:
            return 'NONE'
        elif len(file_list) == 1:
            return file_list[0]
        else:
            df = pd.DataFrame()
            for f in file_list:
                df_len = len(df)
                df.loc[df_len, 'filename'] = f
            #     df.loc[df_len, 'filename_base'] = f.replace('.json','')
            #
            # df['counter_value'] = df.filename_base.str.split('_').str[-1]
            # df['file_dt'] = df.filename_base.str.split('_').str[-2]
            # df['file_datetime'] = pd.to_datetime(df['file_dt'],format='%Y%m%d%H%M%S')
            #
            # if return_type == 'FIRST':
            #     df.sort_values(by=['file_datetime'],ascending=[True],inplace=True)
            #     df.reset_index(drop=True, inplace=True)
            #     df = df[df['file_dt'] == df.loc[0,'file_dt']]
            # else:
            #     df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
            #     df.reset_index(drop=True, inplace=True)
            #     df = df[df['file_dt'] == df.loc[0, 'file_dt']]

            return ','.join(df['filename'].values.tolist())

        return 'NONE'

    def _l1_norm_data_check(self, context, api_name, counter=0, return_type='LAST'):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_prefix = '{base_gcs_folder}_norm/{source}/{api_name}/{date_nodash}/'.format(
            source=self.api_configuration[self.api_name]['api_source_name'],
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.base_gcs_folder,
            api_name=api_name)

        file_list = []
        print('*' * 20)
        print(file_prefix)
        print('*' * 20)
        file_list = hook.list(self.target_gcs_bucket, versions=True, maxResults=100, prefix=file_prefix, delimiter=',')
        if len(file_list) == 0:
            return 'NONE'
        elif len(file_list) == 1:
            return file_list[0]
        else:
            df = pd.DataFrame()
            for f in file_list:
                df_len = len(df)
                df.loc[df_len, 'filename'] = f
                df.loc[df_len, 'filename_base'] = f.replace('.json', '')

            df['counter_value'] = df.filename_base.str.split('_').str[-1]
            df['file_dt'] = df.filename_base.str.split('/').str[-2]
            df['file_datetime'] = pd.to_datetime(df['file_dt'], format='%Y%m%d')
            if return_type == 'FIRST':
                df.sort_values(by=['file_datetime'], ascending=[True], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]
            else:
                df.sort_values(by=['file_datetime'], ascending=[False], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df = df[df['file_dt'] == df.loc[0, 'file_dt']]

            return ','.join(df['filename'].values.tolist())

        return 'NONE'

    def _process_from_l1_gcs(self, context, object, history_count=0, max_filename='NA'):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_data = hook.download(self.target_gcs_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        file_row_count_return = len(df)
        filename_parsing = object
        filename_parsing = filename_parsing.replace('.json', '')
        filename_parsing_split = filename_parsing.split('_')
        file_dt = filename_parsing_split[len(filename_parsing_split) - 2]
        counter = filename_parsing_split[len(filename_parsing_split) - 1]

        if object == max_filename:
            start_offsetValue = 'NA'
            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                self.offset_df = self._get_offset_information(context)
                self.offset_df['dag_execution_date'] = self.offset_df['dag_execution_date'].astype('str')
                get_ds_offsetdata = self.offset_df[(self.offset_df['dag_execution_date'] == str(context['ds_nodash']))]
                get_ds_offsetdata.reset_index(drop=True, inplace=True)
                get_index = get_ds_offsetdata.loc[0, 'idx']
                offsetValue = self.offset_df.loc[get_index, 'start_offset']
                start_offsetValue = offsetValue
            else:
                offsetValue = 'NA'

            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                df_end = df.tail(1)
                df_end.reset_index(drop=True, inplace=True)
                if len(df_end) > 0:
                    try:
                        end_offset_value = df_end.loc[0, self.api_configuration[self.api_name]['api_offset_key']]
                    except:
                        offset_string = self.api_configuration[self.api_name]['api_offset_key'].replace('_', '-')
                        end_offset_value = df_end.loc[0, offset_string]
                else:
                    end_offset_value = offsetValue
            else:
                end_offset_value = 'NA'

            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                self._update_offset_information(context, self.offset_df, start_offsetValue, end_offset_value)

        json_normalized_df = self._upload_normalized_to_gcs(df, context, file_dt, counter)
        del df
        self._upload_schema_to_gcs(json_normalized_df, context, file_dt, counter)

        return file_row_count_return

    def _process_details_from_l1_gcs(self, context, object):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_data = hook.download(self.target_gcs_bucket, object)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        details_field_list = df[self.api_configuration[self.api_name]['details_target']].unique()
        return details_field_list