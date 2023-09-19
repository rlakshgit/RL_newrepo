import json
from datetime import datetime
from datetime import timedelta
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_google_analytics_hook import GoogleAnalyticsHook
from airflow.models import BaseOperator
from airflow.models import Variable
import pandas as pd
import os
from pandas.io.json import json_normalize
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta import DateRange
from google.analytics.data_v1beta import Dimension
from google.analytics.data_v1beta import Metric
from google.analytics.data_v1beta import RunReportRequest


class GoogleAnalyticsToGoogleCloudStorageOperator(BaseOperator):
 
    # template_fields = ('since',
    #                    'until',
    #                    'metadata_filename',
    #                    'target_schema_base'
    #                   )
    #  template_fields = ('since',
    #                    'until',
    #                    'metadata_file',
    #                    'target_schema_base'
    #                   )
    # #ui_color = '#00fff7'
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
                gcp_project,
                source,
                l1_storage_location,
                l2_storage_location,
                metadata_filename,
                schema_storage_location,
                audit_storage_location,
                history_check='True',
                #include_empty_rows=True,
                sampling_level=None,
                delegate_to=None,
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
        self.source = source
        self.l1_storage_location = l1_storage_location
        self.l2_storage_location = l2_storage_location
        self.metadata_filename = metadata_filename
        self.schema_storage_location = schema_storage_location
        self.audit_storage_location = audit_storage_location
        self.gcp_project = gcp_project
        self.history_check = history_check
        self.page_size = 10000
        self.source_count = 0
        self.include_empty_rows = True
        self.gcs_conn = GCSHook(self.gcs_conn_id)
        self.bq_conn = BigQueryHook(self.gcs_conn_id)
        self.delegate_to = delegate_to


# l1_storage_location       DEV_l1/GA_Careplan/EmailEvents/20220401/file.json
# l2_storage_location       DEV_l2/GA_Careplan/
# offset_storage_location   DEV_offset/GA_Careplan/
# metadata_storage_location DEV_metadata/GA_Careplan/
# schema_storage_location   DEV_schema/GA_Careplan/
# audit_storage_location    DEV_audit/GA_Careplan/


    def execute(self, context):
        print('-'*100)
        print(context)
        print('-'*100)

        start_date = context['ds']
        end_date = context['next_ds']
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        end_date = end_date-timedelta(days=1)
        end_date = str(end_date.date())

        print('Start Date:', start_date)
        print('End Date', end_date)
        
        gcp_hook = GoogleBaseHook(gcp_conn_id= self.gcs_conn_id)
        keyfile_dict = gcp_hook._get_field('keyfile_dict')
        creds = json.loads(keyfile_dict)

        filename = 'creds.json'
        with open(filename, 'w') as fd:
            json.dump(creds, fd)
        
        cwd = os.getcwd()
        creds_path = cwd+'/creds.json'

        print('creds file name >>>>>')
        print(creds_path)

        os.environ['GOOGLE_APPLICATION_CREDENTIALS']=creds_path
    
        client = BetaAnalyticsDataClient()

        request = RunReportRequest(
        property=f"properties/213616124",
        dimensions=[Dimension(name="Year"),Dimension(name="Month")],
        metrics=[Metric(name="Sessions"),Metric(name="screenPageViews"),Metric(name="screenPageViewsPerSession")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
        response = client.run_report(request)
        print(response)

        source_count = response.row_count


        os.remove(creds_path)

        output = []

        for row in response.rows:
            output.append({'Year':row.dimension_values[0].value, 'Month':row.dimension_values[1].value, 'Session':row.metric_values[0].value, 'screenPageViews':row.metric_values[1].value, 'screenPageViewsPerSession':row.metric_values[2].value })
        
        df = pd.DataFrame(output)
        l1_count = len(df)
        upload_json = self._upload_json_to_gcs(context, response)
        upload_csv = self._upload_df_to_gcs(context, df)

        print(df)

        schema = self._generate_schema(df)
        upload_schema = self._upload_schema_to_gcs(context, schema)
        metadata = self._update_metadata(context, source_count, l1_count, start_date, end_date)
        
        print('File name >>>>')
        print(self.gcs_bucket)

        return 
    
    def _update_metadata(self, context, source_count=0, l1_count=0, start_date='', end_date=''):
                gcs_hook = GCSHook(
                            google_cloud_storage_conn_id=self.gcs_conn_id,
                            delegate_to=self.delegate_to)
        
                json_metadata = {
                        'source_count':source_count,
                        'l1_count': l1_count,
                        'start_date': start_date,
                        'end_date': end_date,
                        'dag_execution_date': context['ds']
                    }

                print(json_metadata)
                df = pd.DataFrame.from_dict(json_metadata, orient='index')
                df = df.transpose()
                print(df)

                gcs_hook.upload(bucket=self.gcs_bucket,
                                object=self.metadata_filename.format(ds_nodash=context['ds_nodash']),
                                data=df.to_json(orient='records', lines='\n'))
                return




    def _upload_schema_to_gcs(self, context, schema):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcs_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{schema_storage_location}{date}/{source}_{date}.json'.format(
            schema_storage_location=self.schema_storage_location,
            source=self.source,
            date=context['ds_nodash'])

        hook.upload(bucket=self.gcs_bucket, object=file_name,
                    data= schema)    


    def _upload_json_to_gcs(self, context, response):
        file_name = '{l1_storage_location}{date}/{source}_{date}.json'.format(
            l1_storage_location=self.l1_storage_location,
            source=self.source,
            date=context['ds_nodash'])

        self.gcs_conn.upload(self.gcs_bucket, file_name, data=str(response), filename='',
                             mime_type='application/json', gzip=False)



    def _upload_df_to_gcs(self, context, df):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcs_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l2_storage_location}{date}/{source}_{date}.json'.format(
            l2_storage_location=self.l2_storage_location,
            source=self.source,
            date=context['ds_nodash'])

        hook.upload(bucket=self.gcs_bucket, object=file_name,
                    data= df.to_csv(index=False))
        
        print('FILE PATH:')
        print(file_name)
        return 'upload completed'


    def _generate_schema(self, df):
        cols_list = list(df.columns)
        headers_schema = [
            {'name':col_name, 'type':'string', 'mode':'NULLABLE'} for col_name in cols_list
        ]
        return json.dumps(headers_schema)


