import json
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_google_analytics_hook import GoogleAnalyticsHook
from airflow.models import BaseOperator
from airflow.models import Variable
import pandas as pd
from pandas.io.json import json_normalize


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
                       'metadata_file',
                       'target_schema_base'
                      )
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
                 gcp_project,
                 metadata_file,
                 target_schema_base,
                 history_check='True',
                 #include_empty_rows=True,
                 sampling_level=None,
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
        #self.include_empty_rows = include_empty_rows
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filename = gcs_filename
        self.gcp_project = gcp_project
        self.metadata_file = metadata_file
        self.history_check = history_check
        self.target_schema_base = target_schema_base
        self.page_size = 10000
        self.source_count = 0
        self.include_empty_rows = True



        self.gcs_conn = GCSHook(self.gcs_conn_id)
        self.bq_conn = BigQueryHook(self.gcs_conn_id)


    #Function to upload metadata to landing
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


    #Function to upload schema to landing
    def _upload_schema(self, file_schema, directory_date):
        schema = json.dumps(file_schema, sort_keys=True)
        schema = schema.encode('utf-8')

        self.gcs_conn.upload(
            bucket=self.gcs_bucket,
            object=self.target_schema_base.format(directory_date),
            data=schema
        )

        return

    def execute(self, context):
        """Since GA is a cube source the data will not change at the source.
        When rerunning the dag it is going to pull data from source instead of reprocessing history from storage layer"""
        pull_new = True
        if pull_new:
            #Iniializing GoogleAnalyticsHook object
            ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id)
            # query to API to get the report
            report = ga_conn.get_analytics_report(self.view_id,
                                                  self.since,
                                                  self.until,
                                                  self.sampling_level,
                                                  self.dimensions,
                                                  self.metrics,
                                                  self.page_size,
                                                  self.include_empty_rows)

            #Getting Column headers from response
            columnHeader = report.get('columnHeader', {})
            print(columnHeader)
            #Getting dimensionsHeaders from ColumnHeaders
            dimensionHeaders = [{'name': header.replace('ga:', ''), 'type': 'STRING','mode': 'NULLABLE'} for header in columnHeader.get('dimensions', [])]
            print(dimensionHeaders)
            #Getting MetricsHeaders from ColumnHeaders
            metricHeaders = [
                {'name': entry.get('name').replace('ga:', ''),
                 'type': entry.get('type'),'mode': 'NULLABLE'}
                for entry
                in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            ]
            print(metricHeaders)
            #Creating SChema file from metrics and dimensions headers
            schema = []
            schema.extend(dimensionHeaders)
            schema.extend(metricHeaders)
            schema.extend([
                {'name': 'bq_load_date',
                 'type': 'DATE','mode': 'NULLABLE'}])

            rows = report.get('data', {}).get('rows', [])
            print(rows)
            df_final= pd.DataFrame()

            #Getting data for each dimensions and metrics
            for row_counter, row in enumerate(rows):
                root_data_obj = {}
                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])

                for index, dimension in enumerate(dimensions):
                    header = dimensionHeaders[index].get('name')
                    root_data_obj[header] = dimension

                for metric in metrics:
                    data = {}
                    data.update(root_data_obj)

                    for index, value in enumerate(metric.get('values', [])):
                        header = metricHeaders[index].get('name')
                        data[header] = value

                    #Appending data to dataframe
                    df = pd.DataFrame.from_records(data=[data])
                    df['bq_load_date'] = context['ds']
                    df_final=df_final.append(df)
                    self.source_count += 1

            print(df_final.head(5))
            result = df_final.to_json(orient='records', date_format='iso', lines='\n')

        #uploading data to storage - l1 bucket
        self.gcs_conn.upload(self.gcs_bucket, self.gcs_filename, data=result, filename='',
                             mime_type='application/json', gzip=False)

        #uploading Metadata to storage
        self._upload_metadata(context, len(df_final), 1, self.source_count)

        #uploading Schema to Storage
        self._upload_schema(schema, directory_date=context['ds_nodash'])
        return

