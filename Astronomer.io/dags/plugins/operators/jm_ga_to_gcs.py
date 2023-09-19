import json
import io
import pandas as pd
from datetime import datetime
import datetime as dt
import logging
from tempfile import NamedTemporaryFile
from common.dq_common import dq_common

from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_google_analytics_hook import GoogleAnalyticsHook
from airflow.models import BaseOperator
from airflow.models import Variable


class GoogleAnalyticsToGoogleCloudStorageOperator(BaseOperator):
    """
    Google Analytics Reporting To Google Cloud Storage Operator
    :param google_analytics_conn_id:    The Google Analytics connection id.
    :type google_analytics_conn_id:     string
    :param view_id:                     The view id for associated report.
    :type view_id:                      string/array
    :param since:                       The date up from which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type since:                        string
    :param until:                       The date up to which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type until:                        string
    :param gcs_conn_id:                  The gcs connection id.
    :type gcs_conn_id:                   string
    :param gcs_bucket:                   The gcs bucket to be used to store
                                        the Google Analytics data.
    :type gcs_bucket:                    string
    """

    template_fields = ('since',
                       'until',
                       'gcs_filename',
                       'table_name',
                       'prefix_norm',
                       'metadata_file',
                       'target_schema_base')
    #ui_color = '#00fff7'
    ui_color= '#deb4c2'

    def __init__(self,
                 google_analytics_conn_id,
                 view_id,
                 since,
                 until,
                 dimensions,
                 metrics,
                 gcs_conn_id,
                 gcs_bucket,
                 gcs_filename,
                 table_name,
                 bq_dataset,
                 bq_table,
                 gcp_project,
                 prefix_norm,
                 target_schema_base,
                 event_tables,
                 event_table_name,
                 bq_rename,
                 metadata_file,
                 history_check='True',
                 source_count=0,
                 for_refinement=False,
                 page_size=1000,
                 include_empty_rows=True,
                 sampling_level=None,
                 normalize=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_analytics_conn_id = google_analytics_conn_id
        self.view_id = view_id
        self.since = since
        self.until = until
        self.sampling_level = sampling_level
        self.dimensions = dimensions
        self.metrics = metrics
        self.page_size = page_size
        self.include_empty_rows = include_empty_rows
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filename = gcs_filename
        self.normalize = normalize
        self.table_name = table_name
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.gcp_project = gcp_project
        self.target_schema_base = target_schema_base
        self.event_tables = event_tables
        self.event_table_name = event_table_name
        self.bq_rename = bq_rename
        self.prefix_norm = prefix_norm
        self.for_refinement = for_refinement
        self.source_count = source_count
        self.metadata_file = metadata_file
        self.history_check = history_check

        self.metricMap = {
            'METRIC_TYPE_UNSPECIFIED': 'varchar(255)',
            'CURRENCY': 'decimal(20,5)',
            'INTEGER': 'int(11)',
            'FLOAT': 'decimal(20,5)',
            'PERCENT': 'decimal(20,5)',
            'TIME': 'time'
        }

        if self.page_size > 10000:
            raise Exception('Please specify a page size equal to or lower than 10000.')

        self.gcs_conn = GCSHook(self.gcs_conn_id)
        self.bq_conn = BigQueryHook(self.gcs_conn_id)

        if not isinstance(self.include_empty_rows, bool):
            raise Exception('Please specify "include_empty_rows" as a boolean.')

    def execute(self, context):

        if self.normalize:
            self._normalize(context)
            return

        if self.history_check.upper() == 'TRUE':
            pull_new = False
            stream_response = self._check_history(context)
            if stream_response is None:
                pull_new = True
            else:
                print('Pulling data set from history....')
                data_json_upload = stream_response
                data_json = data_json_upload.split(b'\n')
                self.source_count = len(data_json)

        if pull_new:
            ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id)

            try:
                since_formatted = datetime.strptime(self.since, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                since_formatted = str(self.since)
            try:
                until_formatted = datetime.strptime(self.until, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                until_formatted = str(self.until)

            report = ga_conn.get_analytics_report(self.view_id,
                                                  since_formatted,
                                                  until_formatted,
                                                  self.sampling_level,
                                                  self.dimensions,
                                                  self.metrics,
                                                  self.page_size,
                                                  self.include_empty_rows)

            columnHeader = report.get('columnHeader', {})
            # Right now all dimensions are hardcoded to varchar(255), will need a map if any non-varchar dimensions are used in the future
            # Unfortunately the API does not send back types for Dimensions like it does for Metrics (yet..)
            dimensionHeaders = [
                {'name': header.replace('ga:', ''), 'type': 'varchar(255)'}
                for header
                in columnHeader.get('dimensions', [])
            ]
            metricHeaders = [
                {'name': entry.get('name').replace('ga:', ''),
                 'type': self.metricMap.get(entry.get('type'), 'varchar(255)')}
                for entry
                in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            ]

            data_json = []
            rows = report.get('data', {}).get('rows', [])
            for row_counter, row in enumerate(rows):
                root_data_obj = {}
                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])

                for index, dimension in enumerate(dimensions):
                    header = dimensionHeaders[index].get('name').lower()
                    root_data_obj[header] = dimension

                for metric in metrics:
                    data = {}
                    data.update(root_data_obj)

                    for index, value in enumerate(metric.get('values', [])):
                        header = metricHeaders[index].get('name').lower()
                        data[header] = value

                    table_parts = str(self.table_name).split('_')
                    data['table_name'] = table_parts[1]
                    data_json.append(json.dumps(data))
                    self.source_count += 1

            data_json_upload = '\n'.join(str(d) for d in data_json)

        self.gcs_conn.upload(self.gcs_bucket, self.gcs_filename, data=data_json_upload, filename='', mime_type='application/json', gzip=False)

        self._upload_metadata(context, len(data_json), 1, self.source_count)
        return

    def _normalize(self, context):

        # Get the list of files from l1 that match the passed in prefix
        # There are two levels of normalization done with this function:
        #   1. L1 to L1_norm to combine multiples of same base file
        #   2. L1_norm to L1_norm client_session_all for the bulk of the data
        #   This results in 3 BigQuery tables for refined: client_session_all, users (minus users_1), and pl_portal
        #   Users and pl_portal final are taken care of in normalization task because they are single-topic combined
        #   client_session_all needs normalization first then combination task

        file_list = self.gcs_conn.list(self.gcs_bucket, prefix=self.prefix_norm)
        num_files = len(file_list)
        # For each file, download, normalize, and merge into dataframe on join fields
        results_df = pd.DataFrame()
        for source_file in file_list:
            try:
                source_check = source_file.split('l1_norm_data_')[1].replace('.json', '')
            except:
                source_check = source_file.split('l1_data_')[1].replace('.json', '')

            if self.event_table_name not in self.target_schema_base and source_check in self.event_tables:
                num_files -= 1
                continue

            fs = self.gcs_conn.download(self.gcs_bucket, source_file)
            file_JSON = [json.loads(row.decode('utf-8')) for row in fs.split(b'\n') if row]
            normalized_df = pd.io.json.json_normalize(file_JSON)
            try:
                normalized_df = normalized_df.drop(['table_name'], axis=1)
            except:
                logging.info('table_name not found in data')

            if results_df.empty:
                results_df = normalized_df
            elif normalized_df.empty:
                pass
            else:
                join_columns = normalized_df.columns.intersection(results_df.columns).tolist()

                results_df = pd.merge(results_df, normalized_df, 'outer', on=join_columns)

        # If this is prep for refinement, do some renaming and parsing
        if self.for_refinement:

            # Rename fields to be more descriptive
            results_df.rename(columns=self.bq_rename, inplace=True)

            # For the pl_portal data, parse out the policy number and activity from the screenname field
            if 'screenname' in results_df.columns:
                def parse_account(x):
                    if 'PLPortal' not in str(x):
                        return None
                    x_split = x.split('/')
                    for i in x_split:
                        if i.startswith('24-') or i.startswith('45-'):
                            return i
                    return None

                def parse_activity(x):
                    if 'PLPortal' not in str(x):
                        return None
                    x_split = x.split('/')
                    return x_split[len(x_split) - 1]

                def parse_jewelryid(x):
                    if 'PLPortal' not in str(x):
                        return None
                    x_split = x.split('jewelryId=')
                    return x_split[len(x_split) - 1]

                def parse_claimconfirmation(x):
                    if 'PLPortal' not in str(x):
                        return None
                    x_split = x.split('Confirmation?r=')
                    return x_split[len(x_split) - 1]

                results_df['policy_number'] = results_df['screenname'].apply(parse_account)
                results_df['activity'] = results_df['screenname'].apply(parse_activity)
                results_df['jewelry_id'] = results_df['screenname'].apply(parse_jewelryid)
                results_df['claim_confirmation'] = results_df['screenname'].apply(parse_claimconfirmation)

        # If there is data, write it to l1_norm

        if len(results_df) > 0:
            if self.for_refinement:
                t_now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results_df['load_timestamp'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
                results_df['time_central'] = pd.to_datetime(t_now, format='%Y-%m-%d %H:%M:%S')
                results_df['time_utc'] = results_df['time_central']
                results_df.time_utc = results_df.time_central.dt.tz_localize('US/Central', ambiguous=True).dt.tz_convert(
                    'UTC').dt.strftime('%Y-%m-%d %H:%M:%S')
                results_df['time_utc'] = pd.to_datetime(results_df['time_utc'], errors='coerce')

                dq = dq_common()
                results_df = dq.dq_column_group_cleanup(results_df)
                logging.info('clean up done')

                # Generating schema from the dataframe
                schema_list = []
                fields = self._generate_schema(results_df)
                [schema_list.append(x) for x in fields if x not in schema_list]

                self._upload_schema(schema_list, directory_date=context['ds_nodash'])
                
            json_data = results_df.to_json(orient='records', lines='\n', date_format='iso')

            self.gcs_conn.upload(bucket=self.gcs_bucket,
                                object=self.gcs_filename,
                                data=json_data,
                                mime_type='application/json')    

        self._upload_metadata(context, len(results_df), num_files, len(results_df))



        return 0

    def _generate_schema(self, data, default_type='STRING'):
        field = []
        type_mapping = {
                'i': 'INTEGER',
                'b': 'BOOLEAN',
                'f': 'FLOAT',
                'O': 'STRING',
                'S': 'STRING',
                'U': 'STRING',
                'M': 'TIMESTAMP'
            }

        data_columns = []
        for column_name, column_dtype in data.dtypes.iteritems():
            data_columns.append(column_name)
            field.append({'name': column_name,
                          'type': type_mapping.get(column_dtype.kind, default_type),
                          'mode': 'NULLABLE'})

        try:
            table_schema = self.bq_conn.get_schema(self.bq_dataset, self.bq_table, self.gcp_project)['fields']
        except:
            table_schema = {}

        table_columns = []
        for row in table_schema:
            table_columns.append(row['name'])
            if row['name'] not in data_columns:
                field.append({'name': row['name'],
                              'type': row['type'],
                              'mode': 'NULLABLE'})

        return field

    def _upload_schema(self, file_schema, directory_date):
        schema = json.dumps(file_schema, sort_keys=True)
        schema = schema.encode('utf-8')

        self.gcs_conn.upload(
            bucket=self.gcs_bucket,
            object=self.target_schema_base.format(directory_date),
            data=schema
        )

        return

    def _upload_metadata(self, context, row_count, file_count, l1_count=0):
        gcs_hook = self.gcs_conn
        json_metadata = {
                         'source_count': row_count,
                         'l1_count': l1_count,
                         'file_count': file_count,
                         'dag_execution_date': context['ds']
                         }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.gcs_bucket,
                        self.metadata_file,
                        df.to_json(orient='records', lines='\n'))
        return

    def _check_history(self, context):
        gcs_hook = self.gcs_conn

        # Get all the files in the date directory, then compare to the filename for the specific file to find.
        history_prefix = '/'.join(str(self.gcs_filename).split('/')[:-1])
        # Isolate the core file name to accommodate for historical vs. newer file naming.
        file_match = str(self.gcs_filename).split('/')[-1].replace('l1_data_', '').replace('.json', '')
        base_file_list = gcs_hook.list(self.gcs_bucket, maxResults=1000, prefix=history_prefix)
        print(base_file_list)
        print(file_match)

        file_data = None
        for f in base_file_list:
            if (file_match in f) and ('metadata' not in f):
                file_data = gcs_hook.download(self.gcs_bucket, f)

        return file_data
