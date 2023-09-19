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
import sys
import requests

sys.setrecursionlimit(10000)

class HubspotSearchToGoogleCloudStorageOperator(BaseOperator):
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

        super(HubspotSearchToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
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

        file_dt = dt.datetime.now().strftime('%Y%m%d-%H%M%S')

        start_date = self._to_millis(context['prev_ds'])
        end_date = self._to_millis(context['ds'])
        print('Start Date', start_date, context['prev_ds'])
        print('End Date', end_date, context['ds'])
        print('**********************')


        # landing updated records from search end point 
        if self.api_configuration[self.api_name]['api_records_type'] == 'search':
            print('----- [[ THIS IS DYNAMIC API ]] -----', self.api_name)

            url = self._build_url(offset_string='', context=context)
            print('url:' , url)

            # set payload for the post request
            payload = self.set_payload(start_last_modified=start_date, end_last_modified=end_date)
            # send request and get back df & json response
            df, response = self._call_api_return_df_search(url, self.api_connection_id, payload)

            print('TOTAL RECORDS TO BE LOADED:', response['total'] )
            # extract the next_page value from the response
            next_page = self._next_page(response)
            ids_list = []

            # Extract IDs from response and append to list 
            list_of_ids = self._create_list(response, ids_list)
           
            # Recursive Function - continiously his the end point until it loads all the data
            list_of_ids = self._process_data(list_of_ids, next_page, start_date, end_date, url, list_of_ids)
            

            # Create DF for Tracking Columns 
            ids_df = pd.DataFrame(list_of_ids)
            ids_df['landed']='N'
            ids_df['flattened']='N'
            ids_df['loaded']='N' 

            # Drop Duplicates
            print('Length of DF BEFORE dropping duplicste', len(ids_df))
            ids_df.drop_duplicates(subset=['id'],inplace=True)
            print('Length of DF after dropping duplicste', len(ids_df))
           
            # get length of df 
            row_count = len(ids_df) 
           
            # upload IDs DF to GCS  
            upload = self._upload_df_to_gcs(df=ids_df, context=context, file_dt=file_dt, counter=0)
            
            if upload == 'upload completed':
                # update metadata file 
                self._update_metadata(context=context, row_count=row_count, start_date=context['prev_ds'], end_date=context['ds'], api_rec_count= response['total'])
                print('Metadata File Updated')  
            else:
                print('File failed to upload to GCS')

            
        
        # land other data based on offset file
        elif self.api_configuration[self.api_name]['api_records_type'] == 'static':
            print('----- [[ THIS IS STATIC API ]] -----', self.api_name)
            json_row_count = 0
            flat_row_count = 0
            json_page_count = 0
            flat_page_count = 0

            # Load offset data 
            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                self.offset_df = self._get_offset_information(context)
                self.offset_df['dag_execution_date'] = self.offset_df['dag_execution_date'].astype('str')
                get_ds_offsetdata = self.offset_df[(self.offset_df['dag_execution_date'] == str(context['ds_nodash']))]
                get_ds_offsetdata.reset_index(drop=True, inplace=True)
                get_index = get_ds_offsetdata.loc[0, 'idx']
                offsetValue = self.offset_df.loc[get_index, 'start_offset']

            else:
                offsetValue = 'NA'
   

            # Build URL
            df = pd.DataFrame()
            counter = 0
            url = ''
            url = self._build_url(offset_string='', context=context)
            print('url:' , url)

            start_offset_value = offsetValue

            

            # send first request 
            df, response = self._call_api_return_df(url, self.api_connection_id)
            flat_data_df = self.flatten_data(context, response)

            res_array = self.api_configuration[self.api_name]['api_array_key']
            json_row_count += len(response[res_array])
            flat_row_count += len(flat_data_df)
            json_page_count += 1
            flat_page_count += 1
            print('>>>>>>',json_page_count, json_row_count)
            print('>>>>>>',flat_page_count, flat_row_count)

            
        
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


            # upload the first file to GCS
            write_return = self._upload_json_to_gcs(df=df, context=context, file_dt=file_dt, counter=counter)
            print('uploading file to gcs')
            print('write_return >>>>>>', write_return)
            # upload flatten data 
            upload_flatten = self._upload_flat_to_gcs(flat_data_df, context, file_dt, counter=counter)

            # if file isn't uploaded to GCS
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

            # Check if there's more data and download it 
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


                ##Loop to keep loading data 
                while end_while:
                    df_hasmore = pd.DataFrame()
                    url = self._build_url(offset_string=self._build_offset_string(end_offset_value), context=context)
                    df_hasmore, restResponse = self._call_api_return_df(url, self.api_connection_id)

                    if len(df_hasmore) == 0:
                        break
                    self._upload_json_to_gcs(df_hasmore, context, file_dt, counter)

                    print('row data upload success')
                    flat_data_df = self.flatten_data(context, restResponse)
                    print('object flattened success')
                    upload_flatten = self._upload_flat_to_gcs(flat_data_df, context, file_dt, counter=counter)
                    print('flat object upload success')
                    
                    json_row_count += len(restResponse[res_array])
                    flat_row_count += len(flat_data_df)
                    json_page_count += 1
                    flat_page_count += 1

                    print('>>>>>>',json_page_count, json_row_count)
                    print('>>>>>>',flat_page_count, flat_row_count)

                    
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

        
                    if counter > self.max_loop_count and self.check_limit:
                        print('Hit max count allotment....breaking loop.')
                        break

                    counter += 1


            # check if there's more data for marketingEmail endpoint only
            if self.api_configuration[self.api_name]['api_name'] == 'marketingEmails':
                total = response['total'] - len(df)
                print('Has More Total =', total)
                
                offset = self.api_configuration[self.api_name]['api_limit']
                while total > 0:
                    url = self._build_url(offset_string=self._build_offset_string(offset), context=context)
                    df_hasmore, restResponse = self._call_api_return_df(url, self.api_connection_id)
                    
                    if len(df_hasmore) == 0:
                        break
                    self._upload_json_to_gcs(df_hasmore, context, file_dt, counter)

                    print('row data upload success')
                    flat_data_df = self.flatten_data(context, restResponse)
                    print('object flattened success')
                    upload_flatten = self._upload_flat_to_gcs(flat_data_df, context, file_dt, counter=counter)
                    print('flat object upload success')
                    
                    json_row_count += len(restResponse[res_array])
                    flat_row_count += len(flat_data_df)
                    json_page_count += 1
                    flat_page_count += 1

                    print('>>>>>>',json_page_count, json_row_count)
                    print('>>>>>>',flat_page_count, flat_row_count)
                    total -= len(flat_data_df)
                    offset += self.api_configuration['api_name']['api_limit']




            # check if limit is ebabled 
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
                    end_while = calculate_page(df_end, total_row_counter)
                    end_offset_value = total_row_counter

                    while end_while:
                        df_hasmore = pd.DataFrame()
                        url = self._build_url(offset_string=self._build_offset_string(end_offset_value), context=context)
                        df_hasmore, restResponse = self._call_api_return_df(url, self.api_connection_id)

                        if len(df_hasmore) == 0:
                            break
                        self._upload_json_to_gcs(df_hasmore, context, file_dt, counter)

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


            # upload offset file 
            if self.api_configuration[self.api_name]['offset_enabled'].upper() == 'TRUE':
                self._update_offset_information(context, self.offset_df, start_offset_value, end_offset_value)
                print('-=-=-=-=-=-=-=-=-=-=-=-=--= UPDATING OFFSET FILE ')
                print(self.offset_df)
                print(start_offset_value)
                print(end_offset_value)

            # Generate and upload schema file
            # column_list = self.api_configuration[self.api_name]["Columns_to_BQ"]
            # schema_wrtie_retrun = self._upload_schema_to_gcs( column_list, context, file_dt, counter=0)

            print('[TOTAL RECORDS LANDED]:', total_row_counter)

            # update metadata files 
            metadata = self._update_metadata(context, row_count=json_row_count ,flat_row_count=flat_row_count,json_page_count=json_page_count,flat_page_count=flat_page_count)
 



    def _update_metadata(self, context, row_count=0, start_date='', end_date='',flat_row_count=0,json_page_count=0,flat_page_count=0, api_rec_count=0):
        # gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{metadata_storage_location}{api_name}/{date_nodash}/l1_metadata_data_hubspot_{api_name}.json'.format(
            date_nodash=context['ds_nodash'],
            metadata_storage_location = self.metadata_storage_location,
            api_name=self.api_name)     

        if self.api_configuration[self.api_name]['api_records_type'] == 'search':
            json_metadata = {
                'source_count':row_count,
                'l1_count': row_count,
                'api_rec_count': api_rec_count,
                'start_date': start_date,
                'end_date': end_date,
                'dag_execution_date': context['ds']
            }

        elif self.api_configuration[self.api_name]['api_records_type'] == 'static':
            json_metadata ={
                'source_count':row_count,
                'l1_count': row_count,
                'l1_file_count':json_page_count,
                'l2_count': flat_row_count,
                'l2_file_count':flat_page_count,
                'start_date': start_date,
                'end_date': end_date,
                'dag_execution_date': context['ds']
            }

        print(json_metadata)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        print(df)
        hook.upload(self.target_gcs_bucket,
                        file_name,
                        df.to_json(orient='records', lines='\n'))
        return






    def _build_url(self, offset_string, context):
        API_SECRET_KEY = self._process_api_key()

        if self.api_configuration[self.api_name]['api_time_based'].upper() == 'TRUE':
            if self.api_configuration[self.api_name]['api_date_type'].upper() == 'ISO':
                start_date = context['ds_nodash']
                end_date = context['ds_nodash']

            else:
                start_date = self._to_millis(context['prev_ds'])
                end_date = self._to_millis(context['ds'])

            if self.api_configuration[self.api_name]['api_start_date_key'] != 'NA':
                start_key=self.api_configuration[self.api_name]['api_start_date_key']
            else:
                start_key=''

            
            if self.api_configuration[self.api_name]['api_end_date_key'] != 'NA':
                end_key=self.api_configuration[self.api_name]['api_end_date_key']
            else:
                end_key = ''

            # marketingEmail has different query implementation 
            if self.api_configuration[self.api_name]['api_name'] == 'marketingEmails':
                time_string = '&updated__range={start_date_value},{end_date_value}'.format(
                    start_date_value=start_date,
                    end_date_value=end_date
                )

            else:
                time_string = '&{start_key}={start_date_value}&{end_key}={end_date_value}'.format(
                    start_key=start_key,
                    start_date_value=start_date,
                    end_key=end_key,
                    end_date_value=end_date)
        
        else:
            time_string = ''

        if self.api_configuration[self.api_name]['api_limit'] != 'NA':
            limit_string = self.api_configuration[self.api_name]['api_limit']
            limit_label = '&limit='
        else:
            limit_string = ''
            limit_label = ''

        if self.api_configuration[self.api_name]['api_filter'] != 'NA':
            filter_string = self.api_configuration[self.api_name]['api_filter']
        else:
            filter_string = ''

        
        if self.api_configuration[self.api_name]['api_name'] == 'marketingEmails':
            #url = '{api_url}?{api_secret_key_label}={API_SECRET_KEY}{limit_label}{limit_string}{time_string}{offset_string}'.format(
            url = '{hubspotAPI}{api_url}?{limit_label}{limit_string}{time_string}{offset_string}'.format(
                hubspotAPI='https://api.hubapi.com/',
                api_url=self.api_configuration[self.api_name]['api_url'],
                #API_SECRET_KEY=API_SECRET_KEY,
                #api_secret_key_label=self.api_configuration[self.api_name]['api_secret_key_label'],
                limit_label=limit_label,
                limit_string=limit_string,
                offset_string=offset_string,
                time_string=time_string,
            )

        else:
            url = '{hubspotAPI}{api_url}?{limit_label}{limit_string}{offset_string}{time_string}{filter_string}'.format(
                hubspotAPI='https://api.hubapi.com/',
                api_url=self.api_configuration[self.api_name]['api_url'],
                #API_SECRET_KEY=API_SECRET_KEY,
                #api_secret_key_label=self.api_configuration[self.api_name]['api_secret_key_label'],
                limit_label=limit_label,
                limit_string=limit_string,
                offset_string=offset_string,
                time_string=time_string,
                filter_string=filter_string)
        return url


    def set_payload(self, after='', start_last_modified='', end_last_modified=''):
        payload = json.dumps({
        "filterGroups": [
            {
            "filters": [
                {
                "propertyName": "lastmodifieddate",
                "operator": "GTE",
                "value": start_last_modified
                },
                {
                "propertyName": "lastmodifieddate",
                "operator": "LTE",
                "value": end_last_modified
                }
            ]
            }
        ],
            "sorts": [
        {
            "propertyName": "lastmodifieddate",
            "direction": "ASCENDING"
        }
        ],
        "after": after,
        "limit": 100
        })
        return payload


    def _call_api_return_df_search(self, url, http_con_id, payload):
        headers_param = {
                    'authorization': 'Bearer pat-na1-ec9757ff-7969-4b96-afcf-eed10e42d6b4',
                    'Content-Type': 'application/json'
                    }
        #http = HttpHook(method='POST', http_conn_id=http_con_id)

        retry_count = 0
        #url = url+"content-type=application/json"
        while True:
            try:
                #restResponse = http.run(url, headers=headers, data=payload)
                restResponse = requests.post(url, headers=headers_param, data=payload)                
                break
            except:
                if retry_count > 5:
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

        return df, restResponse.json()


    def _next_page(self, response):
        try:
            next_page = response['paging']['next']['after']
            return next_page
        except:
            return False  


    def _create_list(self, response, ids_list):
        results = response['results']
        for item in results:
            ids_list.append({'id':item['id'],
                            'lastmodified':item['properties']['lastmodifieddate'],
                            'timestamp':dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        return ids_list



    def _process_data(self, list_of_ids, next_page, start_date, end_date, url, ids_list, res=''):
        if next_page == False:
            print('>>>>>>>>>>>>>> Processing completed')
            return list_of_ids
        elif next_page == '' or int(next_page) <= 9900:  
            payload = self.set_payload(after=next_page, start_last_modified=start_date, end_last_modified=end_date)
            df, response = self._call_api_return_df_search(url, self.api_connection_id, payload)
            next_page = self._next_page(response)
            list_of_ids = self._create_list(response, ids_list)
            list_size = str(sys.getsizeof(list_of_ids)) + "bytes"
            print('list Size is:', list_size)
            print('Length of list', len(list_of_ids))
            sleep(.25)
            self._process_data(list_of_ids, next_page, start_date, end_date, url, list_of_ids, res=response)
        else:
            next_page = ''
            start_last_modified = res['results'][-1]["properties"]['lastmodifieddate']
            # check if time fraction is in timestamp
            char_index = start_last_modified.rfind('.')
            if char_index == -1:
                start_last_modified = start_last_modified[0:19]+'.0'+start_last_modified[19:]
            start_last_modified = dt.datetime.strptime(start_last_modified, '%Y-%m-%d'+'T'+'%H:%M:%S.%f'+'Z')
            start_last_modified = int(start_last_modified.timestamp() * 1000)
            end_last_modified = end_date
            self._process_data(list_of_ids, next_page, start_date=start_last_modified, end_date=end_last_modified, url=url, ids_list=list_of_ids)

        return list_of_ids


    def _upload_df_to_gcs(self, df, context, file_dt, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l2_gcs_folder}{api_name}/{date_nodash}/l1_data_hubspot_v2_{api_name}.csv'.format(
            date_nodash=context['ds_nodash'],
            date_time=file_dt,  # context['ts_nodash'],
            l2_gcs_folder=self.l2_storage_location,
            api_name='search_contacts')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                    data= df.to_csv(index=False))
        # store file name in airflow variables
        Variable.set('hubspot_updated_ids_list', file_name)
        print('FILE PATH:')
        print(file_name)
        return 'upload completed'


    def flatten_data(self, context, response):
        res_array = self.api_configuration[self.api_name]['api_array_key']
        combined_df = pd.DataFrame()
        if (self.api_configuration[self.api_name]['api_name'] == 'marketingEmails') or (self.api_configuration[self.api_name]['api_name'] == 'emailSubscriptionTypes'):
            for object in response[res_array]:
                try:
                    flat_data = flatten(object)
                    flat_df = pd.json_normalize(flat_data)
                    # flat_df = pd.DataFrame.from_dict(flat_data, orient='index')
                    # flat_df = flat_df.T
                    flat_df.dropna(how='all', axis=1, inplace=True)
                    flat_df['bq_load_date'] = context['ds_nodash']
                    combined_df = pd.concat([combined_df, flat_df])
                except Exception as err:
                    print(err)
                    print('_' * 100)
                    print(object)
        else: 
            has_more = response['hasMore']
            offset = response['offset']

            for event in response[res_array]:
                try:
                    flat_data = flatten(event)
                    flat_df = json_normalize(flat_data)
                    # flat_df = pd.DataFrame.from_dict(flat_data, orient='index')
                    # flat_df = flat_df.T
                    flat_df.dropna(how='all', axis=1, inplace=True)
                    flat_df['hasMore1'] = has_more
                    flat_df['offset'] = offset
                    flat_df['bq_load_date'] = context['ds_nodash']
                    combined_df = pd.concat([combined_df, flat_df])
                except Exception as err:
                    print(err)
                    print('_' * 100)
                    print(event)


        return combined_df



    # STATIC EP - Get offset file 
    def _get_offset_information(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)  # , delegate_to=self.delegate_to)
        offsetFileName = '{base_gcs_folder}{api_name}/offset_{api_name}.conf'.format(
            api_name=self.api_name, base_gcs_folder=self.offset_storage_location)
        try:
            stream_data = hook.download(self.target_gcs_bucket, offsetFileName)  # .decode()
            file_stream = io.BufferedReader(io.BytesIO(stream_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            df.sort_values(by=['dag_execution_date', 'start_offset'], ascending=[True, True], inplace=True)
            df.reset_index(drop=True, inplace=True)
        except:
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
    




    # static 
    def _build_offset_string(self, offsetValue):
        if offsetValue != 'NA':
            offset_string = "&{offset_key}={offset_value}".format(
                offset_key=self.api_configuration[self.api_name]["api_offsetKeyForURL"],
                offset_value=offsetValue)
        else:
            offset_string = ''
        return offset_string

    def _process_api_key(self):
        if self.api_key_encrypted:
            connection = BaseHook.get_connection(self.api_connection_id)
            API_SECRET_KEY = str(base64.b64decode(connection.password))
        else:
            API_SECRET_KEY = self.api_connection_id

        API_SECRET_KEY = API_SECRET_KEY.replace("b'", '').replace("'", '')
        return API_SECRET_KEY



    def _to_millis(self, dt):
        return int(pd.to_datetime(dt).value / 10 ** 6)

    # STATIC 
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


        return df, restResponse.json()


    def _upload_json_to_gcs(self, df, context, file_dt, counter=0, contact_id=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l1_storage_location}{api_name}/{date_nodash}/l1_hubspot_v2_{api_name}_{count}.json'.format(
            date_nodash=context['ds_nodash'],
            l1_storage_location = self.l1_storage_location,
            api_name=self.api_name,
            count=counter,
            id=contact_id)
        
        if df.empty:
            return 'No Data to upload'
        else:
            json_data =  df.to_json(orient='records', lines='\n', date_format='iso')

            hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                        data= json_data)
            return 'upload completed'


    def _update_offset_information(self, context, df, start_offset, end_offset):

        df_dag_date = df[(df['dag_execution_date'] == context['ds_nodash'])]
        df_dag_date.reset_index(drop=True, inplace=True)
        df_idx = df_dag_date.loc[0, 'idx']

        df.loc[df_idx, 'start_offset'] = start_offset
        df.loc[df_idx, 'end_offset'] = end_offset
        df.loc[df_idx + 1, 'dag_execution_date'] = context['tomorrow_ds_nodash']
        df.loc[df_idx + 1, 'start_offset'] = end_offset

        del df['idx']
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        file_name = '{base_gcs_folder}{api_name}/offset_{api_name}.conf'.format(
            api_name=self.api_name, base_gcs_folder=self.offset_storage_location)

        json_data = df.to_json(orient='records', lines='\n')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=json_data)

        return


    def _upload_flat_to_gcs(self, df, context, file_dt, counter=0):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        file_name = '{l2_storage_location}{api_name}/{date_nodash}/l2_data_hubspot_v2_{api_name}_{count}.csv'.format(
            date_nodash=context['ds_nodash'],
            l2_storage_location=self.l2_storage_location,
            api_name=self.api_name,
            count=counter)
            
        if df.empty:
            return 'No Data to Upload'
        else: 
            csv_data =  df.to_csv(encoding='utf-8', index=False)

            hook.upload(bucket=self.target_gcs_bucket, object=file_name,
                        data= csv_data)

            return 'upload completed'

    # Generate Schema 
    # def _schema_generator(self, col_list):
    #     fields = []
    #     for column_name in sorted(col_list):
    #         if '-' in column_name:
    #             column_name = column_name.replace('-', '_')
    #             fields.append({'name': column_name,
    #                         'type': 'String',
    #                         'mode': 'NULLABLE'})
    #         else:
    #             fields.append({'name': column_name,
    #                         'type': 'String',
    #                         'mode': 'NULLABLE'})
    #     return json.dumps(fields)  # return_string

    # Upload Schema File 
    # def _upload_schema_to_gcs(self, column_list, context, file_dt, counter=0):
    #     """_summary_

    #     Args:
    #         column_list (_type_): _description_
    #         context (_type_): _description_
    #         file_dt (_type_): _description_
    #         counter (int, optional): _description_. Defaults to 0.
    #     """
    #     hook = GCSHook(
    #         google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
    #         delegate_to=self.delegate_to)

    #     schema_data = self._schema_generator(column_list)

    #     file_name = '{schema_storage_location}{api_name}/{date_nodash}/schema_hubspot_v2_{api_name}.json'.format(
    #         date_nodash=context['ds_nodash'],
    #         date_time=file_dt,  # context['ts_nodash'],
    #         counter=str(counter),
    #         schema_storage_location=self.schema_storage_location,
    #         api_name=self.api_name)

    #     hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=schema_data)

    #     return        