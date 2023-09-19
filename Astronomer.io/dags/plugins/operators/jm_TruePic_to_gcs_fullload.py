import json
import pandas as pd
import numpy as np
import io
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from plugins.operators.jm_gcs import GCSHook
from pandas.io.json import json_normalize
from flatten_json import flatten
import logging
import requests
import time

desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class TruePicToGCS(BaseOperator):
    template_fields = (
        'target_gcs_bucket', 'target_env', 'metadata_filename', 'additional_metadata_filenames', 'schema_location','begin_pull','end_pull')
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 begin_pull,
                 end_pull,
                 api_connection_id='TruePic_ID',
                 access_connection_id='TruePic_access_ID',
                 event_connection_id='TruePic_event_ID',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encrypted=False,
                 table='events',
                 additional_tables=['events_timeline', ],
                 additional_column_names=['timeline', 'photos', 'list', ],
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_events=5000,
                 delegate_to=None,
                 base_gcs_folder='',
                 history_check='False',
                 metadata_filename='',
                 additional_metadata_filenames=[''],
                 normalized_location='NA',
                 schema_location='NA',
                 *args,
                 **kwargs):

        super(TruePicToGCS, self).__init__(*args, **kwargs)
        self.begin_pull = begin_pull
        self.end_pull = end_pull
        self.api_key_encrypted = api_key_encrypted
        self.api_connection_id = api_connection_id
        self.access_connection_id = access_connection_id
        self.event_connection_id = event_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.api_configuration = api_configuration
        self.target_gcs_bucket = target_gcs_bucket
        self.table_name = table
        self.additional_table_names = additional_tables
        self.additional_column_names = additional_column_names
        self.target_env = target_env
        self.max_events = max_events
        self.delegate_to = delegate_to
        self.base_gcs_folder = base_gcs_folder
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.additional_metadata_filenames = additional_metadata_filenames
        self.normalized_location = normalized_location
        self.schema_location = schema_location
        self.bearer_token = None
        self.api_url = None

    def execute(self, context):
        logging.info("step 1: Check if the data is loaded already..")
        pull_new = True
        if self.history_check.upper() == 'TRUE':
            if not self._check_history(context).empty:
                stream_response = self._check_history(context)
                pull_new = False

        if pull_new:
            logging.info('Data not present..Pulling new data set....')
            # Get the access bearer token for the API
            logging.info("Step 2: Gettting access token and building url for making API request..")
            self._get_access_token()
            logging.info("Getting Access token step completed..")
            # Build the header to handle passing to bearer token
            self._build_api_header()
            logging.info("Building API url step completed..")
            # Get the API Data
            logging.info("Step 3: Making request to events api..")
            stream_response,total_results = self._get_api_data(context)


        ##Handling no data issue
        if total_results == 0:
            logging.info("Exiting the process...")
            return
        # Write Raw data to L1 folder
        logging.info("Step 6:Uploading raw data to GCS..")
        self._upload_raw_data_gcs(stream_response, context)

        #sys.exit(1)

        # Process Data for L1 Norm
        df_normalized, df_additional_normalized_list = self._process_api_data(stream_response, context)
        for col in df_normalized.columns:
            if df_normalized[col].dtype != 'datetime64[ns]':
                df_normalized[col] = df_normalized[col].astype(str)

        # Upload the normalized data
        self._upload_normalized_data_gcs(df_normalized, self.table_name, context)

        # Generate Schema and metadata files
        self._upload_schema_to_gcs(df_normalized, self.table_name, context)
        if self.metadata_filename is not None:
            self._metadata_upload(self.table_name, self.metadata_filename, context, total_results)

        # For each of the additional tables to process...
        table_counter = 0
        for df_additional_normalized in df_additional_normalized_list:
            for col in df_additional_normalized.columns:
                print(col)
                if df_additional_normalized[col].dtype != 'datetime64[ns]':
                    df_additional_normalized[col] = df_additional_normalized[col].astype(str)

            # Upload the normalized data
            self._upload_normalized_data_gcs(df_additional_normalized, self.additional_table_names[table_counter],
                                             context)

            # Generate Schema and metadata files
            self._upload_schema_to_gcs(df_additional_normalized, self.additional_table_names[table_counter], context)
            if self.additional_metadata_filenames[table_counter] is not None:
                self._metadata_upload(self.additional_table_names[table_counter],
                                      self.additional_metadata_filenames[table_counter], context,
                                      len(df_additional_normalized))
            table_counter += 1

        return

    def _check_history(self, context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_'.format(
            base_gcs_folder=self.base_gcs_folder,
            date_nodash=context['ds_nodash'],
            table=self.table_name)

        base_file_list = gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=file_name)

        file_data = None
        df = pd.DataFrame()
        for f in base_file_list:
            file_data = gcs_hook.download(self.target_gcs_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')

        return df

    def _upload_raw_data_gcs(self, data, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_truepic_{table}_{counter}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            table=self.table_name)

        json_data = data.to_json(orient='records', lines='\n')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)
        return

    def _upload_normalized_data_gcs(self, df_in, table, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_norm_truepic_{table}_{counter}.json'.format(
            base_gcs_folder=self.normalized_location,
            date_nodash=context['ds_nodash'],
            counter=str(counter),
            table=table)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=df_in.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _get_api_data(self, context):
        def run_query(url,params):
            import requests
            request = requests.get(url, headers=self.api_header,params = params)  # , verify=False)
            if request.status_code == 200:
                logging.info ("Request - success..")
                return request.json()
            else:
                # Get the access bearer token for the API
                self._get_access_token()
                # Build the header to handle passing to bearer token
                self._build_api_header()
                request = requests.get(url, headers=self.api_header,params = params)  # , verify=False)
                if request.status_code == 200:
                    return request.json()
                else:
                    raise Exception(
                        "Request failed to run by returning code of {}. {}".format(request.status_code, request.json()))

        def get_new_events():

            # Get Event API endpoint URL from connection
            # For each page in the response, add to the master list.
            all_events_id = []
            df_merge = pd.DataFrame()
            truepic_connection = BaseHook.get_connection(self.event_connection_id)
            self.event_url = '{base_url}?{page_string}'.format(base_url=truepic_connection.host,
                                                               page_string='page[size]=100&page[number]=1')

            params = {'filter[created_at][>=]': self.begin_pull, 'filter[created_at][<=]': self.end_pull}
            logging.info(params)
            logging.info("Requesting page 1.")
            request_event = run_query(self.event_url,params)
            try:
                df = pd.DataFrame(request_event['result'])
                df_merge = df_merge.append(df)
            except:
                json_data_list = [request_event['result']]
                df = pd.DataFrame(json_data_list)
                df_merge = df_merge.append(df)
            if df.empty:
                print("no data received for the page")
                df = pd.DataFrame()
                return df,0
            else:
                total_pages = request_event['pageCount']
                total_results = request_event['totalResults']
                if total_pages > 1:
                    for i in range(2, total_pages + 1):
                        logging.info("Requesting page - {i}".format(i=i))
                        url = 'https://vision-api.truepic.com/v2/events?page[size]=100&page[number]={page}'.format(
                            page=i)
                        request = requests.get(url, headers=self.api_header, params=params)
                        request_event = request.json()
                        df = pd.DataFrame(request_event['result'])
                        df_merge = df_merge.append(df)
                        time.sleep(2)
                print(len(df_merge))
                print(total_results)
                if len(df_merge) == total_results:
                    logging.info("Landing count matches with source")

                else:
                    raise Exception('Count does not match..')

                logging.info("Step 4 - Getting the list of id's from events api to get the details of those events...")
                all_events_id.extend(df_merge['id'].tolist())
                unique_all_events = list(set(all_events_id))
                print(unique_all_events)
                final_df = pd.DataFrame()
                if len(unique_all_events) > 0:
                    truepic_connection = BaseHook.get_connection(self.api_connection_id)
                    self.api_url = truepic_connection.host
                    event_counter = 0
                    logging.info("Step 5 - Making requests to events_details API.. ")

                    for eventID in unique_all_events:
                        use_url = self.api_url.replace('{eventId}', str(eventID))
                        request = run_query(use_url,params)
                        try:
                            df = pd.DataFrame.from_dict(request['result'])
                            print(df)
                            time.sleep(2)
                        except:
                            print("Inside except block")
                            json_data_list = [request['result']]
                            df = pd.DataFrame(json_data_list)
                            time.sleep(2)



                        final_df = pd.concat([final_df, df])
                        event_counter += 1
                    print(final_df)
                    print("-----------------------------")
                    print(event_counter)
                    print("------------------------------")
                else:
                    print("No Data received")
                return final_df,total_results

            #   print(all_events)
        return get_new_events()

    def _process_api_data(self, df, context):
        for col in df.columns:
            if col not in ('photos', 'list', 'timeline'):
                B = df[col]
                try:
                    logging.info("Flattening column:{col}".format(col =col))
                    A = pd.DataFrame((flatten(d) for d in B)).add_prefix(col + '.')
                    logging.info("Deleting unflattened column and adding flattened column..")
                    del df[col]
                    for col_sub in A.columns:
                        df[col_sub] = A[col_sub].copy(deep=True)
                except:
                    print('Could not flatten column {}'.format(col))
        print(df.head(5))
        new_cols = []
        for col in df.columns:
            print(col)
            if col is None:
                del df[col]
                continue
            new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))

        df.columns = new_cols
        df.reset_index(drop=True, inplace=True)

        additional_tables_list = []
        table_counter = 0
        for additional_table in self.additional_column_names:
            # select the columns contains the photos
            if additional_table == 'photos':

                df_additional_id = df[['id', additional_table]]
                df_additional = df_additional_id.rename(columns={'id': 'event_id'}, inplace=False)
                ids, x = zip(*[(id_, value) for id_, sub in
                               zip(df_additional["event_id"], df_additional[additional_table].values.tolist())
                               for value in sub])
                df_additional_flat = pd.json_normalize(x)
                df_additional_flat['event_id'] = pd.Series(ids)

            else:
                df_additional = df[[additional_table]]

                ids, x = zip(*[(id_, value) for id_, sub in
                               zip(df_additional[additional_table], df_additional[additional_table].values.tolist()) for
                               value in sub])
                df_additional_flat = pd.json_normalize(x)

            new_cols = []
            for col in df_additional_flat.columns:
                if col is None:
                    del df_additional_flat[col]
                    continue
                if col.replace('.', '_').replace('-', '_').replace(',', '_') in new_cols:
                    new_cols.append('_' + col.replace('.', '_').replace('-', '_').replace(',', '_'))
                    continue
                new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))

            df_additional_flat.columns = new_cols
            df_additional_flat.reset_index(drop=True, inplace=True)
            additional_tables_list.append(df_additional_flat)
            table_counter += 1


        return df, additional_tables_list

    def _get_access_token(self):
        truepic_access_connection = BaseHook.get_connection(self.access_connection_id)
        client_id = truepic_access_connection.login
        client_secret = truepic_access_connection.password
        self.access_url = truepic_access_connection.host

        payload = {
            "audience": "https://vision-api.truepic.com",
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }

        header_param = {'Content-Type': 'application/json',
                        'cache-control': 'no-cache'}

        import requests
        response = requests.post(self.access_url, headers=header_param, json=payload)  # , verify=False)
        json_data = json.loads(response.text)
        self.bearer_token = json_data['access_token']
        return

    def _build_api_header(self):
        self.api_header = {}
        self.api_header['authorization'] = "Bearer {ACCESS_TOKEN}".format(ACCESS_TOKEN=self.bearer_token)
        return

    def _upload_schema_to_gcs(self, json_normalized_df, table, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        schema_data = self._schema_generator(json_normalized_df)

        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_schema_truepic_{table}.json'.format(
            base_gcs_folder=self.schema_location,
            date_nodash=context['ds_nodash'],
            table=table)

        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

        return

    def _schema_generator(self, df, default_type='STRING'):
        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        return json.dumps(fields)

    def _metadata_upload(self, table, metadata_file_name, context, row_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        file_prefix = '{base_gcs_folder}{table}/{date_nodash}/l1_norm_'.format(
            date_nodash=context['ds_nodash'],
            base_gcs_folder=self.normalized_location,
            table=table)

        print('Checking File Prefix:', file_prefix)
        print('Metadata File - ', metadata_file_name)

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

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_file_name,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return

