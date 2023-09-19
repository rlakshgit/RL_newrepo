import json
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import io
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_http import HttpHook
from airflow.models import Variable
from plugins.operators.jm_gcs import GCSHook
import base64
import logging
import requests
from pandas import json_normalize

import numpy as np

desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class AppInsightToGCS(BaseOperator):
    template_fields = (
        'target_gcs_bucket', 'metadata_filename', 'schema_location', 'begin_pull', 'end_pull',)
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 api_connection_id,
                 google_cloud_storage_conn_id,
                 base_gcs_folder,
                 target_gcs_bucket,
                 normalized_location,
                 schema_location,
                 metadata_filename,
                 history_check,
                 query,
                 begin_pull,
                 end_pull,
                 source_name,
                 *args,
                 **kwargs):

        super(AppInsightToGCS, self).__init__(*args, **kwargs)
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.base_gcs_folder = base_gcs_folder
        self.target_gcs_bucket = target_gcs_bucket
        self.normalized_location = normalized_location
        self.schema_location = schema_location
        self.metadata_filename = metadata_filename
        self.history_check = history_check
        self.query = query
        self.begin_pull = begin_pull
        self.end_pull = end_pull
        self.source_name = source_name
        self.api_url = None
        self.delegate_to = None


    def execute(self, context):

        # Creating GCS object to perform GCS operations
        self.gcs_hook = GCSHook(self.google_cloud_storage_conn_id, delegate_to=self.delegate_to)
        logging.info("History check - {check}".format(check = self.history_check))

        ##Getting APPInsight connection
        appinsight_connection = BaseHook.get_connection('AppInsight_Connector')
        self.APPId = appinsight_connection.login
        self.api_key = appinsight_connection.password
        self.api_url = appinsight_connection.host
        self.new_pull = True
        ##History check
        if self.history_check == True:
            logging.info("Checking for Files to reprocess....")
            list_length, list_of_files = self._check_history(context)
            print(list_length)
            print(list_of_files)
            l1_norm_count = 0
            page_counter = 0
            if list_length > 0:
                logging.info("Files are available for data reprocessing... ")
                for f in list_of_files:
                    logging.info("FList of files to process are : {file}".format(file =f))
                    file_data = self.gcs_hook.download(self.target_gcs_bucket, f)
                    logging.info("File downloaded...")
                    json_data = json.loads(file_data)
                    df,df_count = self._process_raw_data(json_data,page_counter,context)
                    l1_norm_count = l1_norm_count + df_count
                    page_counter = page_counter + 1
                self.new_pull = False
            else:
                logging.info("No file available to reprocess. so pulling from API....")
                self.new_pull = True

        if self.new_pull == True:
            ## sending request to API
            logging.info("New pull - sending API request step...")
            count_json_data = self.api_request_to_get_count(context)
            logging.info(count_json_data)
            source_count = count_json_data['tables'][0]['rows'][0][0]
            loop = int(source_count / 10000)+1
            start_count = 1
            end_count = 10000
            page_counter = 0
            l1_norm_count = 0
            logging.info("Raw data step...")
            for i in range(1, loop+1):

                json_data = self.api_request_to_get_data(start_count,end_count,context)
                ## upload raw data to l1 bucket
                self._upload_raw_data_gcs(data = json_data ,page_count=page_counter, context = context)
                ##Normalizing raw data before updating into l1_norm bucket
                logging.info("At Normalization step...")
                df,df_count = self._process_raw_data(json_data,page_counter,context)

                l1_norm_count = l1_norm_count+df_count
                start_count = start_count + 10000
                end_count = end_count + 10000
                page_counter = page_counter+1

        ## upload schema
        logging.info("At generate and upload schema step...")
        self._upload_schema_to_gcs(df, context)

        ##upload metadata
        if self.new_pull == False:
            source_count = l1_norm_count
        l1_norm_length = l1_norm_count
        #print(l1_norm_count)
        logging.info("At metadata generation step....")
        self._metadata_upload(context,source_count,l1_norm_length)

    def api_request_to_get_count(self,context):
        count_query_parameters = self.query + "|where timestamp > datetime('{begin_time}')and timestamp < datetime('{end_time}') |count".format(
            begin_time=self.begin_pull,
            end_time=self.end_pull)
        print(count_query_parameters)

        data_count= '{"query":' + '"' + count_query_parameters + '"}'
        headers = {
            'x-api-key': self.api_key,
            'Content-Type': 'application/json',
        }
        building_url = self.api_url+self.APPId+'/query'

        #Request to get count
        logging.info("Requesting API to get the count")
        count_response = requests.post(building_url, headers=headers, data=data_count, verify=False)

        count_json_data  = json.loads(count_response.text)
        return count_json_data


    def api_request_to_get_data(self,start_row,end_row,context):
        #Building Query
        query_parameters = self.query + "| where timestamp > datetime('{begin_time}')and timestamp < datetime('{end_time}')| order by timestamp asc|extend rn=row_number() |where rn >={start_row} and rn <= {end_row}  ".format(
                                                                                                                begin_time = self.begin_pull ,
                                                                                                                end_time = self.end_pull,
                                                                                                                start_row = start_row,
                                                                                                                end_row = end_row)
        print(query_parameters)

        data = '{"query":' + '"' + query_parameters + '"}'

        headers = {
            'x-api-key': self.api_key,
            'Content-Type': 'application/json',
        }
        building_url = self.api_url+self.APPId+'/query'
        #Request to get the data
        logging.info("Requesting API to get the data")
        response = requests.post(building_url, headers=headers,data=data, verify=False)
        json_data = json.loads(response.text)
        return json_data

    def _upload_raw_data_gcs(self, data,page_count,context):
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_{source}_{table}_{count}.json'.format(
            base_gcs_folder=self.base_gcs_folder,
            date_nodash=context['ds_nodash'],
            source = self.source_name,
            count=page_count,
            table=self.query)
        logging.info("uploading raw data to gcs...")
        self.gcs_hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json.dumps(data))
        return

    def _process_raw_data(self,json_data,page_counter,context):
        columns = json_normalize(data=json_data, record_path=['tables', 'columns'])
        columns = columns['name']
        # print(columns)
        df = json_normalize(data=json_data, record_path=['tables', 'rows'])
        df.columns = columns
        df['bq_load_date'] = self.end_pull
        for col in df.columns:
            if df[col].dtypes != 'datetime64[ns]':
                df[col] = df[col].astype(str)

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        ## upload normalized data

        self._upload_normalized_data_gcs(df, page_counter, context)
        return df,len(df)



    def _upload_normalized_data_gcs(self,df,counter,context):
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_norm_{source}_{table}_{counter}.json'.format(
            base_gcs_folder=self.normalized_location,
            date_nodash=context['ds_nodash'],
            source = self.source_name,
            counter = counter,
            table=self.query)

        result = df.to_json(orient='records', date_format='iso',lines='\n')
        logging.info("uploading normalized data to gcs...")
        self.gcs_hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data=result)
        return

    def _upload_schema_to_gcs(self, json_normalized_df, context):
        schema_data = self._schema_generator(json_normalized_df)
        #self.schema_data_new.extend(schema_data_intermediate)
        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_schema_{source}_{table}.json'.format(
            base_gcs_folder=self.schema_location,
            date_nodash=context['ds_nodash'],
            source = self.source_name,
            table=self.query)
        logging.info("uploading schema to gcs")
        self.gcs_hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json.dumps(schema_data))

        return

    def _schema_generator(self, df, default_type='STRING'):
        # 'i': 'INTEGER',
        # 'b': 'BOOLEAN',
        # 'f': 'FLOAT',
        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP',
            #'i': 'INTEGER',
            #'f': 'FLOAT'
        }
        fields = []
        #df['bq_load_date'] =  pd.to_datetime(df['bq_load_date'])

        for column_name, dtype in df.dtypes.iteritems():
            #print(column_name,dtype.kind)
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return json.dumps(fields)
        return fields

    def _metadata_upload(self, context, l1_count,l1_norm_count):

        #print('Metadata File - ', self.metadata_filename)
        file_name = '{base_gcs_folder}/{table}/{date}/l1_metadata_{source}_{table}.json'.format(
            base_gcs_folder=self.metadata_filename,
            source = self.source_name,
            date=context['ds_nodash'],
            table=self.query)
        json_metadata = {
            'source_count': l1_count,
            'l1_count': l1_norm_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        logging.info("uploading meta data to gcs...")
        self.gcs_hook.upload(self.target_gcs_bucket,
                        file_name,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return

    def _check_history(self, context):

        file_name = '{base_gcs_folder}{table}/{date_nodash}/l1_data_'.format(
            base_gcs_folder=self.base_gcs_folder,
            date_nodash=context['ds_nodash'],
            table=self.query)

        base_file_list = self.gcs_hook.list(self.target_gcs_bucket, maxResults=1000, prefix=file_name)
        logging.info("Files are - {files}".format(files=base_file_list))
        if len(base_file_list) == 0:
            return len(base_file_list),None
        else:
            return len(base_file_list),base_file_list



































