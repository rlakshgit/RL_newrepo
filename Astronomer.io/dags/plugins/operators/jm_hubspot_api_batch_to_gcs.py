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
import requests
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class HubspotAPIToGoogleCloudStorageOperator(BaseOperator):
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

        super(HubspotAPIToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
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
        # get updated records list file name from airflwo variabls 
        updated_records_file = Variable.get('hubspot_updated_ids_list')
        ids_list, ids_df = self.get_updated_rec_list(context, object_name=updated_records_file)
        if ids_list == False:
            source_count = 0
            json_row_count = 0
            json_page_count = 0
            flat_records_count = 0
            flat_page_count = 0
            
        else:
            source_count = len(ids_list)
            print(f'[S1] GETTING THE LIST OF IDS FROM: {updated_records_file}')
            sublist_size = 100
            flattened_rec_ids = []
            counter = 0
            json_row_count = 0
            flat_records_count = 0
            json_page_count = 0
            flat_page_count = 0
            for i in range(0, len(ids_list), sublist_size):
                sublist = ids_list[i: i + sublist_size]
                ids_str = '&vid='.join(str(e) for e in sublist)
                ids_str = '&vid='+ids_str
                print(f'[S2] LOADING BATCH {counter} OF {len(ids_list)/sublist_size}')
                combined_df = pd.DataFrame()
                file_dt = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
                url = self._build_url(context=context, ids_str=ids_str)
                try:
                    df, response = self.call_api_return_df(url, self.api_connection_id)
                    print('[S3] RECEIVED RESPONSE FROM ENDPOINT')
                    landed_rec_ids = self._extract_ids_from_response(response)
                    for record in response:
                        try:
                            flat_data = flatten(response[record])
                            flat_df = pd.json_normalize(flat_data)
                            # flat_df = pd.DataFrame.from_dict(flat_data, orient='index')
                            # flat_df = flat_df.T
                            flat_df.dropna(how='all', axis=1, inplace=True)
                            flat_df['bq_load_date']=context['ds_nodash']
                            combined_df = pd.concat([flat_df, combined_df])
                            flattened_rec_ids.append(id) # list of ids of records that were flattened

                        except Exception as err:
                            print(f'Flattening Error - File was not flattened - Record ID: {id} ')
                            print(err)
                    print('[S4] RESPONSE FLATTENED')
                    # metadata
                    flat_records_count += len(combined_df)
                    json_row_count += len(response)
                except Exception as err:
                        print(f'API Error - File was not downloaded - Record ID: {id}')
                        print(err)
                
                combined_df = combined_df.reset_index(drop=True) 
                flatten_rec_ids = combined_df['vid'].to_list()              
                # uploading row and falt datat to gcs
                upload_row = self._upload_json_to_gcs(df=df, context=context, file_dt=file_dt, counter=counter, contact_id=10)
                print('[S5] ROW FILE UPLOADED SUCCESSFULLY')
                upload_flatten = self._upload_csv_to_gcs(combined_df, context, file_dt, counter=counter, contact_id=0)
                print('[S6] FLAT FILE UPLOADED SUCCESSFULLY')
                counter +=1 

                # metadata
                json_page_count += 1
                flat_page_count += 1
        

                update_landed = self._update_rec_list(ids_df=ids_df, update_ids_list=landed_rec_ids, col_name='landed', file_dt=file_dt, context=context)
                update_flattened = self._update_rec_list(ids_df=ids_df, update_ids_list=flatten_rec_ids, col_name='flattened', file_dt=file_dt, context=context)
                print('[S8] TRACKING FILE UPLOADED SUCCESSFULLY')


        # update metadata files 
        metadata = self._update_metadata(context, source_count=source_count, l1_count=json_row_count, l1_file_count=json_page_count, l2_count=flat_records_count, l2_file_count=flat_page_count, start_date=context['prev_ds'], end_date=context['ds'])
    



    def _update_metadata(self, context, source_count=0, l1_count=0, l1_file_count=0, l2_count=0, l2_file_count=0, start_date='', end_date=''):
            gcs_hook = GCSHook(self.google_cloud_storage_conn_id)        

            file_name = '{metadata_storage_location}{api_name}/{date_nodash}/l1_metadata_data_hubspot_{api_name}.json'.format(
                date_nodash=context['ds_nodash'],
                metadata_storage_location = self.metadata_storage_location,
                api_name=self.api_name)  

            json_metadata = {
                    'source_count':source_count,
                    'l1_count': l1_count,
                    'l1_file_count':l1_file_count,
                    'l2_count': l2_count,
                    'l2_file_count':l2_file_count,
                    'start_date': start_date,
                    'end_date': end_date,
                    'dag_execution_date': context['ds']
                }

            print(json_metadata)
            df = pd.DataFrame.from_dict(json_metadata, orient='index')
            df = df.transpose()
            print(df)
            gcs_hook.upload(self.target_gcs_bucket,
                            file_name,
                            df.to_json(orient='records', lines='\n'))
            return




    def _extract_ids_from_response(self, response):
        ids_list = []
        for id in response:
            ids_list.append(id)
        return ids_list

    def _build_url(self, ids_str, context):
        API_SECRET_KEY = self._process_api_key()

        #url = '{hubspotAPI}{api_url}?{api_secret_key_label}={API_SECRET_KEY}{ids_string}{membership}'.format(
        url = '{hubspotAPI}{api_url}?{ids_string}{membership}'.format (
            hubspotAPI='https://api.hubapi.com',
            api_url=self.api_configuration[self.api_name]['api_url'],
            #API_SECRET_KEY=API_SECRET_KEY,
            #api_secret_key_label=self.api_configuration[self.api_name]['api_secret_key_label'],
            ids_string=ids_str,
            membership='&showListMemberships=true')
        return url


    def _process_api_key(self):
        if self.api_key_encrypted:
            connection = BaseHook.get_connection(self.api_connection_id)
            API_SECRET_KEY = str(base64.b64decode(connection.password))
        else:
            API_SECRET_KEY = self.api_connection_id

        API_SECRET_KEY = API_SECRET_KEY.replace("b'", '').replace("'", '')
        return API_SECRET_KEY
  


    def _update_rec_list(self, ids_df, update_ids_list, col_name, file_dt, context):
        '''
        ids_df:             dataframe of the ids file that was loaded from storage
        update_ids_list:    list of ids that their flag will be changed to 'Y' in the new file
        col_name:           name of the column to be updated
        file_dt:            date to be used in the file path 
        context:            context 
        '''
        for id in update_ids_list:
            ids_df.loc[(ids_df['id'] == id), col_name] = 'Y'
        x =self._upload_df_to_gcs(df=ids_df, context=context, file_dt=file_dt, counter=0)



    def _upload_df_to_gcs(self, df, context, file_dt, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l2_gcs_folder}{api_name}/{date_nodash}/l1_data_hubspot_v2_{api_name}.csv'.format(
            date_nodash=context['ds_nodash'],
            date_time=file_dt,  # context['ts_nodash'],
            l2_gcs_folder=self.l2_storage_location,
            api_name='search_contacts')

        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= df.to_csv(index=False))
        # store file name in airflow variables
        Variable.set('hubspot_updated_ids_list', file_name)
        return 'upload completed'
        

    def _upload_json_to_gcs(self, df, context, file_dt, counter=0, contact_id=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l1_storage_location}{api_name}/{date_nodash}/l1_data_hubspot_v2_{api_name}_{count}.json'.format(
            date_nodash=context['ds_nodash'],
            l1_storage_location = self.l1_storage_location,
            api_name=self.api_name,
            count=counter,
            id=contact_id)
        
        json_data =  df.to_json(orient='records', lines='\n', date_format='iso')
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= json_data)
        return 'upload completed'

    
    def _upload_csv_to_gcs(self, df, context, file_dt, counter=0, contact_id=0):
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





    def call_api_return_df(self, url, http_con_id):
        headers_param = {
                    'authorization': 'Bearer pat-na1-ec9757ff-7969-4b96-afcf-eed10e42d6b4',
                    }
                    #'Content-Type': 'application/json'
                    
        #http = HttpHook(method='GET', http_conn_id=http_con_id)
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

        try:
            if self.api_configuration[self.api_name]['api_output_enable'].upper() == 'TRUE':
                print(df)
        except:
            pass

        return df, restResponse.json()




        
    def get_updated_rec_list(self, context, object_name):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_name = str(object_name)
        file_data = hook.download(self.target_gcs_bucket, object=file_name)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_csv(file_stream)
        if df.empty:
            print('No Data to Load')
            return False, False
        else:
            print('*' * 50)
            print(df)
            ids_list = df['id'].values.tolist()
            return ids_list, df
