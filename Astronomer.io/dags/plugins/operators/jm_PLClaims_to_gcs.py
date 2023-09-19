import json
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import io
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_http import HttpHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook as  BigQueryHook
from airflow.models import Variable
from plugins.operators.jm_gcs import GCSHook
import base64
from pandas.io.json import json_normalize
import logging

import numpy as np

desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class plclaimsGraphQLToGCS(BaseOperator):
    template_fields = (
        'target_gcs_bucket', 'target_env', 'metadata_filename', 'schema_location', 'begin_pull', 'end_pull',)
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 api_connection_id='plclaimsGraphQLID_PROD',
                 http_access_request_conn_id='plclaimsGraphQLAccessID_PROD',
                 table_name='AuthUser',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encrypted=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=50,
                 delegate_to=None,
                 base_gcs_folder='',
                 history_check='True',
                 metadata_filename='',
                 normalized_location='NA',
                 schema_location='NA',
                 query='',
                 ref_dataset='',
                 bq_project='',
                 bigquery_conn_id='NA',
                 query_type='AuthUser',
                 airflow_queries_var_set='NA',
                 begin_pull="{{ ds }}",
                 end_pull="{{ tomorrow_ds }}",
                 *args,
                 **kwargs):

        super(plclaimsGraphQLToGCS, self).__init__(*args, **kwargs)
        self.http_access_request_conn_id = http_access_request_conn_id
        self.api_key_encrypted = api_key_encrypted
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.table_name = table_name
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
        self.jwt_token = None
        self.api_url = None
        self.query = query
        self.query_type = query_type
        self.airflow_queries_var_set = airflow_queries_var_set
        self.begin_pull = begin_pull
        self.end_pull = end_pull
        self.l1_count = 0
        self.bigquery_conn_id = bigquery_conn_id
        self.schema_data_new = []
        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)
        self.ref_dataset = ref_dataset
        self.bq_project = bq_project

    def execute(self, context):

        pull_new = True
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)

        if self.history_check.upper() == 'TRUE':
            logging.info("Inside History check")
            if self._check_history(context) is not None:
                logging.info("Files present...")
                file_list = self._check_history(context)
                counter = 0
                length_df = 0
                source_count = 0
                for f in file_list:
                    file_data = gcs_hook.download(self.target_gcs_bucket, f)
                    stream_response = json.loads(file_data)

                    # pd.DataFrame(stream_response['data'])
                    try:
                        df = json_normalize(data=stream_response, record_path=['data', self.table_name])
                    except:
                        df = pd.DataFrame()
                    source_count = source_count + len(df)
                    del df

                    df_normalized = self._process_api_data(context, stream_response)
                    for col in df_normalized.columns:
                        if df_normalized[col].dtypes != 'datetime64[ns]':
                            df_normalized[col] = df_normalized[col].astype(str)
                        elif '_date' in col or '_ts' in col:
                            df_normalized[col] = pd.to_datetime(df_normalized[col])


                    # Normalize the data
                    self._upload_normalized_data_gcs(df_normalized, context, counter=counter)
                    self._upload_schema_to_gcs(df_normalized, context)
                    length_df = length_df + len(df_normalized)

                    counter = counter + 1
                    if self.metadata_filename is not None:
                        self._metadata_upload(context, length_df, length_df)

                pull_new = False

        if pull_new:
            print('Pulling new data set....')
            # Get the access bearer token for the API
            self._get_access_token()
            # Build the header to handle passing to bearer token
            self._build_api_header()
            # Get the API Data
            self.api_payload = self.query
            self.api_payload_full = self.query

            stream_response = self._get_api_data(context)
            # Build the payload for the API from the schema fields gathered in the previous query
            #self._build_api_payload(context, stream_response)

            stream_response = self._get_api_data(context)
        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_'.format(
            base_gcs_folder=self.base_gcs_folder,
            date_nodash=context['ds_nodash'],
            table=self.table_name)

        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=file_name)
        logging.info("Files are - {files}".format(files=base_file_list))
        if len(base_file_list) == 0:
            return None
        else:
            return base_file_list

    def _process_api_data(self, context, stream_data):
        detail_object = self.query_type.split('.')[0]
        path = self.query_type.split('.')[1]
        if detail_object.upper() == 'TRUE':
            df_norm = pd.DataFrame(stream_data['data'][path])
        else:
            df_norm = pd.DataFrame(stream_data['data'][self.table_name][path])

        return df_norm

    def _upload_raw_data_gcs(self, data, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        if self.query_type.split(".")[0].upper() == 'TRUE':
            file_name = '{base_gcs_folder}{table}/l1_data_plclaims_{table}_{counter}.json'.format(
                base_gcs_folder=self.base_gcs_folder,
                counter=str(counter),
                table=self.table_name)
        else:

            file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_plclaims_{table}_{counter}.json'.format(
                base_gcs_folder=self.base_gcs_folder,
                date_nodash=context['ds_nodash'],
                counter=str(counter),
                table=self.table_name)
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json.dumps(data))
        return

    def _upload_normalized_data_gcs(self, df_in, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        if self.query_type.split(".")[0].upper() == 'TRUE':
            file_name = '{base_gcs_folder}{table}/l1_norm_plclaims_{table}_{counter}.json'.format(
                base_gcs_folder=self.normalized_location,
                counter=str(counter),
                table=self.table_name)
        else:

            file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_norm_plclaims_{table}_{counter}.json'.format(
                base_gcs_folder=self.normalized_location,
                date_nodash=context['ds_nodash'],
                counter=str(counter),
                table=self.table_name)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _get_api_data(self, context):
        print('query')
        #print(self.api_payload)
        def run_query(query, url):
            import requests
            request = requests.post(url, json={'query': query}, headers=self.api_header, verify=False)

            if 'errors' in request.json():
                return 'error'
            elif request.status_code == 200:
                return request.json()
            else:
                raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

        """Each request will be paged. We need to try to get the data until the returned count < page_size."""
        self.page_size = 100  # pass this in?
        page_iteration = 0
        page_count = 0
        norm_df_length = 0
        get_data_count = -1

        detail_object = self.query_type.split('.')[0]
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        self.audit_detail_filename = '{base_gcs_folder}{table}/l1_data_plclaims_{table}_audit_detail.json'.format(
                base_gcs_folder=self.base_gcs_folder,

                table=self.table_name)

        if detail_object.upper() == 'TRUE':
            """Pulling data based on the list of id's form the parent object"""

            ssql = '''select distinct {col_name} from `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{date}" '''.format(dataset=self.ref_dataset,
                                                                                            table=self.query_type.split('.')[2],
                                                                                            project=self.bq_project,
                                                                                            col_name =self.query_type.split('.')[3],
                                                                                            date=context["ds"])


            # df_bqt = self.gcp_bq_hook.get_pandas_df(ssql)
            id_list = self.gcp_bq_hook.get_data(ssql)


            for index, row in id_list.iterrows():
                id = row[self.query_type.split('.')[3]]
                print(id)
                page_payload = self.api_payload.replace('external_id',id)
                request = run_query(page_payload, self.api_url)
                if request == 'error':
                    logging.info("Query failed to run. {}".format(page_payload))
                else:
                    self._upload_raw_data_gcs(request, context, counter=id)

                    df_normalized = self._process_api_data(context, request)
                    for col in df_normalized.columns:
                        if df_normalized[col].dtypes != 'datetime64[ns]':
                            df_normalized[col] = df_normalized[col].astype(str)

                    #norm_df_length = norm_df_length + len(df_normalized)
                    # Normalize the data
                    self._upload_normalized_data_gcs(df_normalized, context, counter=id)

                    self._upload_schema_to_gcs(df_normalized, context)
                    ###add code for storing counts as dictionary and writing it to gcs use dict.update to replace/overwrite
                    file_exists = gcs_hook.exists(self.target_gcs_bucket, self.audit_detail_filename)
                    logging.info('file_exists value is ' + str(file_exists))
                    if file_exists:
                        file_data = gcs_hook.download(self.target_gcs_bucket, object=self.audit_detail_filename)
                        #file_stream = io.BufferedReader(io.BytesIO(file_data))
                        audit_dict = json.loads(file_data)
                        audit_dict.update({id: len(df_normalized)})
                        gcs_hook.upload(bucket=self.target_gcs_bucket, object=self.audit_detail_filename, data=json.dumps(audit_dict))

                    else:
                        audit_dict = {}
                        audit_dict.update({id: len(df_normalized)})
                        gcs_hook.upload(bucket=self.target_gcs_bucket, object=self.audit_detail_filename,
                                             data=json.dumps(audit_dict))

            if self.metadata_filename is not None:
                file_exists = gcs_hook.exists(self.target_gcs_bucket, self.audit_detail_filename)
                logging.info('file_exists value is ' + str(file_exists))
                if file_exists:
                    file_data = gcs_hook.download(self.target_gcs_bucket, object=self.audit_detail_filename)
                    #file_stream = io.BufferedReader(io.BytesIO(file_data))
                    audit_dict = json.loads(file_data)
                    norm_df_length = sum(audit_dict.values())
                else:
                    raise Exception('audit detail file not found')

                self._metadata_upload(context, norm_df_length, norm_df_length)
            return 1

        data_count_left = 0
        while (data_count_left >= self.page_size) or (get_data_count == -1):
            final_list = []
            page_payload = self.api_payload.replace('start_date',self.begin_pull).replace(
                                                   'end_date',self.end_pull).replace(
                                                   'page_count',str(page_iteration)).replace(
                                                   'page_size',str(self.page_size)
                                                   )
            # Try the request with date first.  If error, then do a full pull.
            request = run_query(page_payload, self.api_url)


            if request == 'error':
                raise Exception("Query failed to run. {}".format(page_payload))

            try:
                get_data_count = request['data'][self.table_name]['count']
                print("Data count is {}".format(get_data_count))
            except:
                get_data_count = 0


            page_count += get_data_count
            source_count = page_count
            data_count_left = int(get_data_count) - (page_iteration * self.page_size)
            self._upload_raw_data_gcs(request, context, counter=page_iteration)
            df_normalized = self._process_api_data(context, request)
            for col in df_normalized.columns:
                if df_normalized[col].dtypes != 'datetime64[ns]':
                    df_normalized[col] = df_normalized[col].astype(str)
                elif '_date' in col or '_ts' in col:
                    df_normalized[col] = pd.to_datetime(df_normalized[col])


            norm_df_length = norm_df_length + len(df_normalized)



            # Normalize the data
            self._upload_normalized_data_gcs(df_normalized, context, counter=page_iteration)

            self._upload_schema_to_gcs(df_normalized, context)

            page_iteration += 1
        if self.metadata_filename is not None:
            self._metadata_upload(context, norm_df_length, get_data_count)

        return 1

    def _get_access_token(self):
        plclaims_access_connection = BaseHook.get_connection(self.http_access_request_conn_id)
        clientId = plclaims_access_connection.login
        userPoolId = plclaims_access_connection.password
        self.access_url = plclaims_access_connection.host

        plclaims_connection = BaseHook.get_connection(self.api_connection_id)
        userId = plclaims_connection.login
        password = plclaims_connection.password
        self.api_url = plclaims_connection.host

        header_param = {"Content-Type": "application/json",
                        "cache-control": "no-cache"}
        payload = {"userId": userId,
                   "password": password,
                   "grantType": "PASSWORD",
                   "clientId": clientId,
                   "userPoolId": userPoolId}

        import requests
        response = requests.post(self.access_url, headers=header_param,
                                 json=payload, verify=False)

        json_data = json.loads(response.text)
        self.bearer_token = json_data['accessToken']
        self.jwt_token = json_data['idToken']
        return

    def _build_api_header(self):
        self.api_header = {}
        self.api_header['authorization'] = "Bearer {ACCESS_TOKEN}".format(ACCESS_TOKEN=self.bearer_token)
        self.api_header['x-jwt-assertion'] = "{JwT_TOKEN}".format(JwT_TOKEN=self.jwt_token)
        return

    def _build_api_payload(self, context, stream_response):
        schema_df = pd.DataFrame(stream_response['data']['__schema']['types'])
        query_df = schema_df[schema_df['name'] == self.query_type]
        if len(query_df) == 0:
            self.api_payload = ''
            raise Exception('No type match for query {query}'.format(query=self.table_name))

        def _build_query(query_string, all_df, type_match, level, seen_type_list=[], max_level=3):
            type_df = all_df[all_df['name'] == type_match]
            schema_list = type_df['fields'].values[0]
            if len(schema_list) > 0:
                for s in schema_list:
                    if s['name'] in ['tenant_id']:
                        continue
                    if s['type']['kind'] == 'SCALAR':
                        query_string = query_string + '\n' + s['name']
                    elif s['type']['kind'] == 'LIST':
                        if s['type']['ofType']['kind'] == 'SCALAR':
                            query_string = query_string + '\n' + s['name']
                        elif s['type']['ofType']['kind'] == 'OBJECT':
                            if level < max_level:
                                #   if (level < max_level) and (s['type']['ofType']['ofType']['name'] not in seen_type_list):
                                seen_type_list.append(s['type']['ofType']['ofType']['name'])
                                query_string = query_string + '\n' + s['name'] + ' {'
                                query_string = _build_query(query_string, all_df, s['type']['ofType']['ofType']['name'],
                                                            level + 1, seen_type_list=seen_type_list)
                                query_string = query_string + '\n' + '}'
                            else:
                                continue
                    elif s['type']['kind'] == 'OBJECT':
                        if level < max_level:
                            #   if (level < max_level) and (s['type']['name'] not in seen_type_list):
                            seen_type_list.append(s['type']['name'])
                            query_string = query_string + '\n' + s['name'] + ' {'
                            query_string = _build_query(query_string, all_df, s['type']['name'], level + 1,
                                                        seen_type_list=seen_type_list)
                            query_string = query_string + '\n' + '}'
                    elif s['type']['ofType']['kind'] == 'SCALAR':
                        query_string = query_string + '\n' + s['name']
                    elif s['type']['ofType']['kind'] == 'LIST':
                        if s['type']['ofType']['ofType']['ofType']['kind'] == 'SCALAR':
                            query_string = query_string + '\n' + s['name']
                        elif s['type']['ofType']['ofType']['ofType']['kind'] == 'OBJECT':
                            if level < max_level:
                                #   if (level < max_level) and (s['type']['ofType']['ofType']['ofType']['name'] not in seen_type_list):
                                seen_type_list.append(s['type']['ofType']['ofType']['ofType']['name'])
                                query_string = query_string + '\n' + s['name'] + ' {'
                                query_string = _build_query(query_string, all_df,
                                                            s['type']['ofType']['ofType']['ofType']['name'], level + 1,
                                                            seen_type_list=seen_type_list)
                                query_string = query_string + '\n' + '}'
                            else:
                                continue
                    elif s['type']['ofType']['kind'] == 'OBJECT':
                        if level < max_level:
                            # if (level < max_level) and (s['type']['ofType']['name'] not in seen_type_list):
                            seen_type_list.append(s['type']['ofType']['name'])
                            query_string = query_string + '\n' + s['name'] + ' {'
                            query_string = _build_query(query_string, all_df, s['type']['ofType']['name'], level + 1,
                                                        seen_type_list=seen_type_list)
                            query_string = query_string + '\n' + '}'
                return query_string

        schema_list = query_df['fields'].values[0]

        table_query = ''
        if len(schema_list) > 0:
            table_query = table_query + '{ '
            table_query = _build_query(table_query, schema_df, self.query_type, 0, [])
            table_query = table_query + '}'
        table_query = table_query + '\n}'
        self.api_payload_full = 'query{' + self.table_name + '(page: { num: 0, size: 100 }) ' + table_query
        # print(self.api_payload_full)

        self.api_payload = 'query{' + self.table_name + \
                           '(page: { num: 0, size: 100 } ' + \
                           'dateRange: { from:"' + self.begin_pull + '", to:"' + self.end_pull + '" }) ' + \
                           table_query

        print(self.api_payload)
        return

    def _upload_schema_to_gcs(self, json_normalized_df, context, counter=0):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        schema_data = self._schema_generator(json_normalized_df)

        schema_data_intermediate = [items for items in schema_data if items not in self.schema_data_new]
        self.schema_data_new.extend(schema_data_intermediate)
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_schema_plclaims_{table}.json'.format(
            base_gcs_folder=self.schema_location,
            date_nodash=context['ds_nodash'],
            table=self.table_name)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json.dumps(self.schema_data_new))

        return

    def _schema_generator(self, df, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',
        type_mapping = {
            'i': 'INTEGER',
            'b': 'BOOLEAN',
            'f': 'NUMERIC',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'DATETIME'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            if '_date' in column_name or '_ts' in column_name:
                fields.append({'name': column_name,
                               'type': 'DATETIME',
                               'mode': 'NULLABLE'})
            else:


            # print(column_name,dtype.kind)
                fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return json.dumps(fields)
        return fields

    def _metadata_upload(self, context, row_count, source_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        print('Metadata File - ', self.metadata_filename)
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        self.metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return