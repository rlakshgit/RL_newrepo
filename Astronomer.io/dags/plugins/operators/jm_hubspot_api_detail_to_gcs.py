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
import requests
from pandas.io.json import json_normalize
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class HubspotDetailToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('api_name', 'api_configuration', 'target_gcs_bucket', 'target_env')
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 api_connection_id='HubspotAPI',
                 api_name='NA',
                 parent_api='NA',
                 api_configuration={},
                 google_cloud_storage_conn_id='NA',
                 api_key_encryped=False,
                 target_gcs_bucket='NA',
                 target_env='DEV',
                 max_loop_count=200,
                 check_limit=True,
                 delegate_to=None,
                 base_gcs_folder='',
                 l1_storage_location='',
                 l2_storage_location='',
                 offset_storage_location='',
                 schema_storage_location='',
                 metadata_storage_location='',
                 history_check='True',
                 metadata_filename=None,
                 file_prefix='',
                 last_modified = '',
                 after = '',
                 *args,
                 **kwargs):

        super(HubspotDetailToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.api_key_encrypted = api_key_encryped
        self.api_connection_id = api_connection_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.api_name = api_name
        self.parent_api = parent_api
        self.api_configuration = api_configuration
        self.target_gcs_bucket = target_gcs_bucket
        self.target_env = target_env
        self.max_loop_count = max_loop_count
        self.delegate_to = delegate_to
        self.offset_enable = False
        self.check_limit = check_limit
        self.offset_df = pd.DataFrame()
        self.base_gcs_folder = base_gcs_folder
        self.l1_storage_location = l1_storage_location
        self.l2_storage_location = l2_storage_location
        self.offset_storage_location = offset_storage_location
        self.schema_storage_location = schema_storage_location
        self.metadata_storage_location = metadata_storage_location
        self.history_check = history_check
        self.metadata_filename = metadata_filename
        self.file_prefix = file_prefix
        self.last_modified = last_modified
        self.after = after

    def execute(self, context):
        json_row_count = 0
        flat_row_count = 0 
        json_page_count = 0
        flat_page_count = 0
        # Get the list of files 
        file_list = self._get_files_list(context)
        # loop over the files and extract the ids - add all to a list
        counter=0
        for file in file_list:
            file_ids = []
            file_df = self._load_file(context, file)
            file_ids = file_df['id'].to_list()
            raw_data = []
            combined_df = pd.DataFrame()
            for id in file_ids:
                url = self._build_url(id_str=id)
                nested_df, response = self._call_api_return_df(url, self.api_connection_id)
                raw_data.append(response)
                # flatten data
                flat_data = flatten(response)
                df = pd.json_normalize(flat_data)
                combined_df = pd.concat([df, combined_df])
            counter+=1
            json_row_count+=len(raw_data)
            flat_row_count+=len(combined_df)

            print(f'--- loading {counter} of {len(file_list)} ---')
            print(len(combined_df))
        
             # store the raw and flat data to gcs 
            upload_raw = self._upload_json_to_gcs(data=raw_data, context=context, counter=counter)
            print('[S5] ROW FILE UPLOADED SUCCESSFULLY')
            upload_flatten = self._upload_csv_to_gcs(combined_df, context, counter=counter)
            print('[S6] FLAT FILE UPLOADED SUCCESSFULLY') 

        print('>>>>>>',json_page_count, json_row_count)
        print('>>>>>>',flat_page_count, flat_row_count)
        # update metadata files 
        metadata = self._update_metadata(context, row_count=json_row_count ,flat_row_count=flat_row_count)



    def _update_metadata(self, context, row_count=0 ,flat_row_count=0):
        # gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{metadata_storage_location}{api_name}/{date_nodash}/l1_metadata_data_hubspot_{api_name}.json'.format(
            date_nodash=context['ds_nodash'],
            metadata_storage_location = self.metadata_storage_location,
            api_name=self.api_name)     

        json_metadata = {
            'source_count':row_count,
            'l1_count': flat_row_count,
            'dag_execution_date': context['ds']
        }



        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()

        hook.upload(self.target_gcs_bucket,
                        file_name,
                        df.to_json(orient='records', lines='\n'))
        return



    def _upload_csv_to_gcs(self, df, context,counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l2_storage_location}{api_name}/{date_nodash}/l1_data_hubspot_v2_{api_name}_{count}.csv'.format(
            date_nodash=context['ds_nodash'],
            l2_storage_location=self.l2_storage_location,
            api_name=self.api_name,
            count=counter)
        
        csv_data =  df.to_csv(encoding='utf-8', index=False)
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        print('>>>>>>>>>> file converted to csv')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= csv_data)
        print('upload completed')
        return 'upload completed'

    def _upload_json_to_gcs(self, data, context, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l1_storage_location}{api_name}/{date_nodash}/l1_data_hubspot_v2_{api_name}_{count}.json'.format(
            date_nodash=context['ds_nodash'],
            l1_storage_location = self.l1_storage_location,
            api_name=self.api_name,
            count=counter)
        
        json_data =  json.dumps(data)
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= json_data)
        return 'upload completed'
    


    def _get_files_list(self, context):
        hook = GCSHook(
        google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
        delegate_to=self.delegate_to)

        folder_path = '{l2_storage_location}{api_name}/{date_nodash}/'.format(
            date_nodash=context['ds_nodash'],
            l2_storage_location=self.l2_storage_location,
            api_name=self.parent_api)
        print(folder_path)

        return hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)



    def _load_file(self, context, object_name):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_name = str(object_name)
        file_data = hook.download(self.target_gcs_bucket, object=file_name)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_csv(file_stream)
        return df

        
    def _build_url(self, id_str):
        API_SECRET_KEY = self._process_api_key()

        #url = '{api_url}{id_string}?{api_secret_key_label}={API_SECRET_KEY}'.format(
        #url = '{hubspotAPI}{api_url}{id_string}?{api_secret_key_label}={API_SECRET_KEY}'.format(
        url = '{hubspotAPI}{api_url}{id_string}?'.format(
            hubspotAPI='https://api.hubapi.com/',
            api_url=self.api_configuration[self.api_name]['api_url'],
            #API_SECRET_KEY=API_SECRET_KEY,
            #api_secret_key_label=self.api_configuration[self.api_name]['api_secret_key_label'],
            id_string=id_str)
            
        return url

    def _call_api_return_df(self, url, http_con_id):
        #http = HttpHook(method='GET', http_conn_id=http_con_id)
        headers_param = { 
                    'authorization': 'Bearer pat-na1-ec9757ff-7969-4b96-afcf-eed10e42d6b4',
                    }

        retry_count = 0
        while True:
            try:
                #restResponse = http.run(url)
                restResponse = requests.get(url, headers=headers_param)
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

        # try:
        #     if self.api_configuration[self.api_name]['api_output_enable'].upper() == 'TRUE':
        #         print('api_output_enable is enabled')
        # except:
        #     pass

        return df, restResponse.json()


    def _process_api_key(self):
        if self.api_key_encrypted:
            connection = BaseHook.get_connection(self.api_connection_id)
            API_SECRET_KEY = str(base64.b64decode(connection.password))
        else:
            API_SECRET_KEY = self.api_connection_id

        API_SECRET_KEY = API_SECRET_KEY.replace("b'", '').replace("'", '')
        return API_SECRET_KEY