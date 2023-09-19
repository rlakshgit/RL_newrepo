# -*- coding: utf-8 -*-

import sys
#reload(sys)
#sys.setdefaultencoding('utf8')
import pandas as pd
import datetime as dt
import logging
import json
import os
import numpy as np
from pandas.io.json import json_normalize
from flatten_json import flatten
import base64

from airflow import DAG
from airflow import models
from airflow import configuration
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.gcs_hook import GCSHook
from airflow.operators.python import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from datetime import timedelta


if Variable.get("GCPCloudComposer") == 'True':
# if os.environ["GCPCloudComposer"] == 'True':
    sa = 'NA'
else:
    sa = os.path.join(configuration.get('core', 'dags_folder'), r'common/dev-edl.json')



break_count = 25

from hubspot.common import gcp_common
from hubspot.common import dq_common

pd.options.mode.chained_assignment = None
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')


def to_millis(dt):
    return int(pd.to_datetime(dt).value / 10 ** 6)

def baseEncode(simpleString):
    encryptedString = base64.b64encode(simpleString)
    return encryptedString


def baseDecode(encryptedString):
    decryptedString = base64.b64decode(encryptedString)
    return decryptedString

def convert_to_dicts(ld):
    if type(ld) == list:
        if str(ld) == '[]':
            return {}
        for item in ld:
            if type(item) == dict:
                return item
            if type(item[0]) != list:
                return item[0]
    else:
        return ld

def get_data_hapi_with_time(base_url, hapikey, limit, apiName,target_type, start_date, end_date, offset='NA', id=-1,
                            app=-1):

    # session = requests.Session()
    # session.mount('https://', HTTPAdapter(max_retries=5))

    limit_temp = limit.split('=')
    limit = limit_temp[1]

    if target_type == 'calendarEvents':
        hapi_url = '{target_url}?hapikey={hapikey}&limit={limit}&startDate={start_date}&endDate={end_date}'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )

    elif target_type == 'totals':
        hapi_url = 'analytics/v2/reports{target_url}/summarize/daily?hapikey={hapikey}&limit={limit}&start={start_date}&end={end_date}'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
    elif (target_type == 'sessions') or (target_type == 'sources'):
        hapi_url = 'analytics/v2/reports{target_url}/daily?hapikey={hapikey}&limit={limit}&start={start_date}&end={end_date}'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
    elif target_type.startswith('utm-'):
        filter_input = ''
        if (target_type == 'utm-mediums') or (target_type == 'utm-terms'):
            filter_input = '&f=social&f=web&f=blog'
        if target_type == 'utm-contents':
            filter_input = '&f=landing-pages&f=standard-pages&f=blog-posts&f=listing-pages&f=knowledge-articles'
        if target_type == 'utm-sources':
            filter_input = '&f=search&f=referrals&f=social-media&f=paid-social&f=paid-search&f=paid&f=direct&f=other&f=offline'
        if target_type == 'utm-campaigns':
            filter_input = '&f=landing-pages'

        hapi_url = 'analytics/v2/reports{target_url}/daily?hapikey={hapikey}&limit={limit}&start={start_date}&end={end_date}{filter}'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            filter=filter_input
        )


    elif target_type.startswith('geolocation'):
        #https://api.hubapi.com/
        hapi_url = 'analytics/v2/reports{target_url}/daily?hapikey={hapikey}&limit={limit}&start={start_date}&end={end_date}&f=us'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )


    else:
        hapi_url = '{target_url}?hapikey={hapikey}&limit={limit}&startTimestamp={start_date}&endTimestamp={end_date}'.format(
            hapikey=hapikey,
            target_url=base_url,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )

    if (id != -1) and (app != -1):
        hapi_url = '{base_hapi_url_load}&campaignId={campID}&appId={appID}'.format(
            base_hapi_url_load=hapi_url,
            campID=id, appID=app
        )

    hapi_url_no_offset = hapi_url
    if offset != 'NA':
        hapi_url = '{base_hapi_url_load}&offset={offsetParameter}'.format(
            base_hapi_url_load=hapi_url,
            offsetParameter=offset
        )

    http = HttpHook(method='GET', http_conn_id='HubspotAPI')
    logging.info("Calling HTTP method")
    try:
        restResponse = http.run(hapi_url)
        restResponse.raise_for_status()
    except:
        restResponse = http.run(hapi_url)
        restResponse.raise_for_status()

    if offset == 'NA':
        build_df_out = pd.DataFrame()
        hapi_offset_return = 'NA'
        for line in restResponse.iter_lines():
            json_data = json.loads(line, encoding='utf8')
            try:
                df = pd.DataFrame.from_dict(json_data)
            except:
                df = pd.DataFrame.from_dict(json_data, orient='index')
            build_df_out = pd.concat([build_df_out, df])
            build_df_out.reset_index(drop=True, inplace=True)

            if 'offset' in df.columns:
                hapi_offset_return = df['offset'].max()
            else:
                hapi_offset_return = 'NA'

    else:
        build_df_out = pd.DataFrame()
        hapi_offset_return = 'NA'
        for line in restResponse.iter_lines():
            json_data = json.loads(line, encoding='utf8')
            df = pd.DataFrame.from_dict(json_data)
            build_df_out = pd.concat([build_df_out, df])
            build_df_out.reset_index(drop=True, inplace=True)
            hapi_offset_return = df['offset'].max()

    if 'hasMore' in df.columns.values.tolist():
        if len(df) == 0:
            hapi_offset_return = offset
            build_df_out = pd.DataFrame()
            return build_df_out, hapi_offset_return

        if df['hasMore'].max():
            offset_get = True
        else:
            offset_get = False
        counter = 0
        while offset_get:
            counter += 1
            if counter > break_count:
                break

            logging.info("Reading more data from :" + apiName + ' -- loop counter value ' + str(counter))
            connection_info = BaseHook.get_connection('HubspotAPI')
            hapi_url_no_offset.replace(connection_info.host,'')
            ##To me....
            hapi_url = '{hapi_url}&offset={offsetParameter}'.format(
                hapi_url=hapi_url_no_offset,
                offsetParameter=hapi_offset_return)

            restResponse = http.run(hapi_url)
            restResponse.raise_for_status()
            for line in restResponse.iter_lines():
                json_data = json.loads(line, encoding='utf8')
                df = pd.DataFrame.from_dict(json_data)
            build_df_out = pd.concat([build_df_out, df])
            build_df_out.reset_index(drop=True, inplace=True)
            hapi_offset_return = df['offset'].max()
            if df['hasMore'].max():
                offset_get = True
            else:
                offset_get = False
    return build_df_out, hapi_offset_return

def get_data_hapi_no_time(base_url, hapikey, limit, apiName,target_type, offset='NA'):

    try:
        limit_temp = limit.split('=')
        limit_field = limit_temp[0]
        limit = limit_temp[1]
    except:
        limit_field = 'limit'
        limit = limit




    try:
        offset_temp = offset.split('=')
        offset_field = offset_temp[0]
        offset = offset_temp[1]
        offset_field_url = offset_temp[2]
    except:
        offset_field = 'offset'
        offset = offset
        offset_field_url = 'offset'


    import pandas as pd
    from pandas.io.json import json_normalize
    if limit != 'NA':
        if offset == 'NA':
            #https://api.hubapi.com
            hapi_url = '{target_url}?hapikey={hapikey}&{limit_field}={limit}'.format(
                hapikey=hapikey,
                target_url=base_url,
                limit_field=limit_field,
                limit=limit)
        else:
            hapi_url = '{target_url}?hapikey={hapikey}&{limit_field}={limit}&{offset_field}={offsetParameter}'.format(
                hapikey=hapikey,
                target_url=base_url,
                limit_field=limit_field,
                limit=limit,
                offset_field=offset_field_url,
                offsetParameter=offset)
    else:
        if offset == 'NA':
            hapi_url = '{target_url}?hapikey={hapikey}'.format(
                hapikey=hapikey,
                target_url=base_url)
        else:
            hapi_url = '{target_url}?hapikey={hapikey}&{offset_field}={offsetParameter}'.format(
                hapikey=hapikey,
                target_url=base_url,
                offset_field=offset_field_url,
                offsetParameter=offset)

    if target_type.lower() == 'contacts':
        hapi_url = hapi_url + '&propertyMode=value_and_history&formSubmissionMode=all&showListMemberships=true'

    http = HttpHook(method='GET', http_conn_id='HubspotAPI')
    try:
        restResponse = http.run(hapi_url)
    except:
        restResponse = http.run(hapi_url)



    # restResponse = session.get(hapi_url)
    # restResponse.raise_for_status()
    build_df_out = pd.DataFrame()
    hapi_offset_return = 'NA'
    for line in restResponse.iter_lines():
        json_data = json.loads(line, encoding='utf8')
    try:
        df = pd.DataFrame.from_dict(json_data)
    except:
        json_data_list = [json_data]
        df = pd.DataFrame(json_data_list)

    # except:
    #     logging.info("Failed to get data for the following URL")
    #     msg = 'FAILED URL: ' + str(hapi_url)
    #     logging.info(msg)
    #     return pd.DataFrame(), None

    new_col_list = []
    for col in df.columns:
        if col == 'has-more':
            new_col_list.append('hasMore')
        else:
            new_col_list.append(col)

    df.columns = new_col_list

    if (target_type == 'event_campaign') or (target_type == 'campaigns'):
        df_normalized = json_normalize(json_data, 'campaigns', errors='ignore')
        df_base = df.join(df_normalized)
        build_df_out = pd.concat([build_df_out, df_base])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()
    elif target_type == 'deals':
        df_normalized = json_normalize(json_data, 'deals', errors='ignore')
        df_base = df.join(df_normalized)
        build_df_out = pd.concat([build_df_out, df_base])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()
    elif (target_type == 'companies') or (target_type == 'companiesContacts'):
        df_normalized = json_normalize(json_data, 'companies', errors='ignore')
        df_base = df.join(df_normalized)
        build_df_out = pd.concat([build_df_out, df_base])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()
    elif target_type == 'contactLists':
        df_normalized = json_normalize(json_data, 'lists', errors='ignore')
        df_base = df.join(df_normalized)
        build_df_out = pd.concat([build_df_out, df_base])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()
    elif target_type == 'contactsInLists':
        df_normalized = json_normalize(json_data, 'contacts', errors='ignore')
        df_base = df.join(df_normalized)
        build_df_out = pd.concat([build_df_out, df_base])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()
    elif 'hasMore' not in df.columns:
        json_data = json.loads(line, encoding='utf8')
        try:
            df = pd.DataFrame.from_dict(json_data)
        except:
            json_data_list = [json_data]
            df = pd.DataFrame(json_data_list)
        df.reset_index(drop=False, inplace=True)
        df.rename(columns={"index": "counter_type"}, inplace=True)
        build_df_out = pd.concat([build_df_out, df])
        build_df_out.reset_index(drop=True, inplace=True)
    elif target_type == 'event_summary':
        json_data = json.loads(line, encoding='utf8')
        df = pd.DataFrame.from_dict(json_data)
        df.reset_index(drop=False, inplace=True)
        df.rename(columns={"index": "counter_type"}, inplace=True)
        build_df_out = pd.concat([build_df_out, df])
        build_df_out.reset_index(drop=True, inplace=True)
    else:
        build_df_out = pd.concat([build_df_out, df])
        build_df_out.reset_index(drop=True, inplace=True)
        hapi_offset_return = df[offset_field].max()

    if 'hasMore' in df.columns.values.tolist():
        if len(df) == 0:
            hapi_offset_return = offset
            build_df_out = pd.DataFrame()
            return build_df_out, hapi_offset_return

        if df['hasMore'].max():
            offset_get = True
        else:
            offset_get = False
        counter = 0
        while offset_get:
            counter += 1
            if counter > break_count:
                break
            logging.info("Reading more data from :" + target_type + ' -- loop counter value ' + str(counter))

            hapi_url = '{target_url}?hapikey={hapikey}&{limit_field}={limit}&{offset_field}={offsetParameter}'.format(
                hapikey=hapikey,
                target_url=base_url,
                limit_field=limit_field,
                limit=limit,
                offset_field=offset_field_url,
                offsetParameter=hapi_offset_return)
            try:
                try:
                    restResponse = http.run(hapi_url)
                    restResponse.raise_for_status()
                except:
                    restResponse = http.run(hapi_url)
                    restResponse.raise_for_status()

                for line in restResponse.iter_lines():
                    json_data = json.loads(line, encoding='utf8')
                    df = pd.DataFrame.from_dict(json_data)

                new_col_list = []
                for col in df.columns:
                    if col == 'has-more':
                        new_col_list.append('hasMore')
                    else:
                        new_col_list.append(col)

                df.columns = new_col_list

                if (target_type == 'event_campaign') or (target_type == 'campaigns'):
                    df_normalized = json_normalize(json_data, 'campaigns', errors='ignore')
                    df_base = df.join(df_normalized)
                    build_df_out = pd.concat([build_df_out, df_base])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = df[offset_field].max()
                elif target_type == 'deals':
                    df_normalized = json_normalize(json_data, 'deals', errors='ignore')
                    df_base = df.join(df_normalized)
                    build_df_out = pd.concat([build_df_out, df_base])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = df[offset_field].max()
                elif target_type == 'contactLists':
                    df_normalized = json_normalize(json_data, 'lists', errors='ignore')
                    df_base = df.join(df_normalized)
                    build_df_out = pd.concat([build_df_out, df_base])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = df[offset_field].max()
                elif target_type == 'contactsInLists':
                    df_normalized = json_normalize(json_data, 'contacts', errors='ignore')
                    df_base = df.join(df_normalized)
                    build_df_out = pd.concat([build_df_out, df_base])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = df[offset_field].max()
                elif target_type == 'engagementDispositions':
                    build_df_out = pd.concat([build_df_out, df])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = 'NA'
                else:
                    build_df_out = pd.concat([build_df_out, df])
                    build_df_out.reset_index(drop=True, inplace=True)
                    hapi_offset_return = df[offset_field].max()
                if df['hasMore'].max():
                    offset_get = True
                else:
                    offset_get = False
            except:
                logging.info('Failed on response with offset {offset}.'.format(offset=hapi_offset_return))
                offset_get = False

    if 'limit' in df.columns.values.tolist():
        if len(df) == 0:
            hapi_offset_return = offset
            build_df_out = pd.DataFrame()
            return build_df_out, hapi_offset_return

        if df['limit'].max():
            offset_get = True
        else:
            offset_get = False
        counter = 0
        hapi_offset_return = df['limit'].max()
        while offset_get:
            counter += 1
            logging.info("Reading more data from :" + apiName + ' -- loop counter value ' + str(counter))
            hapi_url = '{target_url}?hapikey={hapikey}&{limit_field}={limit}&{offset_field}={offsetParameter}'.format(
                hapikey=hapikey,
                target_url=base_url,
                limit_field=limit_field,
                limit=limit,
                offset_field=offset_field_url,
                offsetParameter=hapi_offset_return)

            if target_type.lower() == 'contacts':
                hapi_url = hapi_url + '&propertyMode=value_and_history&formSubmissionMode=all&showListMemberships=true'

            try:
                restResponse = http.run(hapi_url)
                restResponse.raise_for_status()
                for line in restResponse.iter_lines():
                    json_data = json.loads(line, encoding='utf8')
                    df = pd.DataFrame.from_dict(json_data)

                build_df_out = pd.concat([build_df_out, df])
                build_df_out.reset_index(drop=True, inplace=True)

                if not np.isnan(df['limit'].max()):
                    offset_get = True
                    hapi_offset_return = hapi_offset_return + df['limit'].max()

                else:
                    offset_get = False
            except:
                logging.info('Failed on response with offset {offset}.'.format(offset=hapi_offset_return))
                offset_get = False

        hapi_offset_return = 'NA'
    return build_df_out, hapi_offset_return



def new_process_data(element, target_dataset_name, module_name, gcp_project_name, target_bucket):
    elemet_split = element.split('/')

    dq = dq_common.dq_common()
    service_account = sa
    if ':\\' in os.getcwd():
        gcp = gcp_common.gcp_common(service_account, False)
    elif service_account.upper() != 'NA':
        gcp = gcp_common.gcp_common(service_account, False)
    else:
        gcp = gcp_common.gcp_common(None, True)

    data = gcp.gcp_storage_download_json_file(target_bucket, element, 'NA')


    logging.info(element)

    #Works for metaData but not for filters.
    if 'analyticsBreakdowns_sources' in element:
        if len(data.columns) > 2:
            assign_date_time = pd.to_datetime(elemet_split[3], format='%Y%m%d').strftime('%Y-%m-%d')
            new_df = pd.DataFrame()
            new_df[assign_date_time] = pd.Series(data.values.ravel('F'))
            new_df = new_df[new_df[assign_date_time] != 'None']
            new_df.dropna(inplace=True)
            if len(new_df.columns) == 1:
                new_df_2 = pd.DataFrame()
                for col in new_df.columns:
                    B = new_df[col].apply(convert_to_dicts)
                    A = pd.DataFrame((flatten(d) for d in B))
                    A['data_date'] = pd.to_datetime(col)
                    A['base_data'] = new_df[col]
                    new_df_2 = pd.concat([new_df_2, A])
                new_df = new_df_2.copy(deep=True)

            data = pd.DataFrame()
            data = new_df.copy(deep=True)
        else:
            new_df = pd.DataFrame()
            for col in data.columns:
                new_col = pd.to_datetime(col).strftime('%Y-%m-%d')
                B = data[new_col].apply(convert_to_dicts)
                A = pd.DataFrame((flatten(d) for d in B))
                A['data_date'] = pd.to_datetime(col)
                A['base_data'] = data[new_col]
                new_df = pd.concat([new_df, A])
            data = new_df.copy(deep=True)

    elif 'analyticsBreakdowns' in element:
        if 'utm-' in element:
            new_df = pd.DataFrame()
            for col in data.columns:
                try:
                    B = data[col].apply(convert_to_dicts)
                    A = pd.DataFrame((flatten(d) for d in B))
                    A['base_data'] = data[col]
                    new_df = pd.concat([new_df, A])
                except:
                    data = data.dropna()
                    B = data[col].apply(convert_to_dicts)
                    A = pd.DataFrame((flatten(d) for d in B))
                    A['base_data'] = data[col]
                    new_df = pd.concat([new_df, A])
            data = new_df.copy(deep=True)
        else:
            new_df = pd.DataFrame()

            if ('geolocation' in element) & (len(data.columns) == 1):
                folder_date = elemet_split[len(elemet_split)-2]
                folder_date_setup = pd.to_datetime(folder_date,format='%Y%m%d').strftime('%Y-%m-%d')
                data.columns = [folder_date_setup]
                data[folder_date_setup] = data[folder_date_setup].fillna('NA')
                data = data[data[folder_date_setup] != 'NA']

            for col in data.columns:
                new_col = pd.to_datetime(col).strftime('%Y-%m-%d')
                B = data[new_col].apply(convert_to_dicts)
                A = pd.DataFrame((flatten(d) for d in B))
                A['data_date'] = pd.to_datetime(col)
                A['base_data'] = data[new_col]
                new_df = pd.concat([new_df, A])
            data = new_df.copy(deep=True)

    else:
        logging.info('Hit the else condition - not AnalyticsBreakdowns!')
        for col in data.columns:
            try:

                B = data[col].apply(convert_to_dicts)

                if ('cosLayouts' in element) or ('cosSiteMap' in element):
                    A = json_normalize(B).add_prefix(col + '.')
                else:
                    A = pd.DataFrame((flatten(d) for d in B)).add_prefix(col + '.')

                del data[col]
                for col_sub in A.columns:
                    data[col_sub] = A[col_sub].copy(deep=True)
            except:
                continue

    new_cols = []
    for col in data.columns:
        if col == None:
            del data[col]
            continue
        new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))

    data.columns = new_cols
    data.reset_index(drop=True, inplace=True)

    data = gcp.gcp_bq_data_encoding(data)
    logging.info('->Running QA check ...')
    data = dq.dq_duplicate_check(data)
    data = dq.dq_column_group_cleanup(data)

    file_date_time = pd.to_datetime(elemet_split[3], format='%Y%m%d').strftime('%Y-%m-%d')
    data['data_land_date'] = file_date_time
    data['data_land_date'] = pd.to_datetime(data['data_land_date'])
    data['data_run_date'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data['data_run_date'] = pd.to_datetime(data['data_run_date'])
    data['time_central'] = data['data_land_date'].copy(deep=True)
    data['time_central'] = pd.to_datetime(data['data_land_date'])

    try:
        gcp.gcp_bq_create_dateset(target_dataset_name)
    except:
        pass

    elemet_split[2] = elemet_split[2].replace('-', '_')
    target_table_name = 't_' + module_name + '_' + elemet_split[2]
    try:
        get_schema = gcp.gcp_bq_columns_get(gcp_project_name, target_dataset_name, target_table_name)
    except:
        get_schema = pd.DataFrame()


    if len(get_schema) > 0:
        for col in data.columns:
            if col not in get_schema:
                if data[col].dtypes == 'datetime64[ns]':
                    pass_dtype = 'TIMESTAMP'
                else:
                    pass_dtype = 'STRING'
                gcp.gcp_bq_table_schema_update(target_dataset_name, target_table_name, col, datatype=pass_dtype, null_base='NULLABLE')

        for col in get_schema:
            if col not in data.columns.values.tolist():
                data[col] = None

    else:
        create_table_result = gcp.gcp_bq_create_partition_table(data, target_dataset_name, target_table_name)

    partition_table = str(target_table_name) + '$' + str(file_date_time.replace('-', ''))
    if not data.empty:
        result = gcp.gcp_bq_write_table(data, gcp_project_name, target_dataset_name, partition_table, 'APPEND', True)
    return

def hubspot_refined_dataflow(**kwargs):

    '''
    This python script will take landing data as input, performs cleaning on the data and expose processed data into BigQuery.
    :return:
    '''

    logging.info('START')
    logging.info(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    from hubspot.common import gcp_common

 #   try:
    module_name = 'hubspot'
    target_dataset_name = 'jm_ref_hubspot'
    gcp_project_name = 'dev-edl' #os.environ["GCP_PROJECT"]
    api_name = kwargs['params']['api_name']

    #  service_account = args['params']['service_account']
    service_account = sa
    partition_date = kwargs['params']['partition_date']

    airflow_exec_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"].replace('T00:00:00+00:00', '').replace('-', '')
    if len(airflow_exec_date) > 0:
        partition_date = airflow_exec_date

    confFilePath = os.path.join(configuration.get('core', 'dags_folder'), 'hubspot', 'ini', 'hubspot_ingestion_direct.conf')

    if gcp_project_name.lower().startswith('dev'):
        env = 'dev01'
    elif gcp_project_name.lower().startswith('qa'):
        env = 'qa01'
    elif gcp_project_name.lower().startswith('stage'):
        env = 'stage'
    elif gcp_project_name.lower().startswith('prod'):
        env = 'prod'

    input_bucket_name = 'jm_' + env + '_edl_lnd'

    logging.info("Passed api name : " + api_name)
    logging.info("Configuration file path : " + confFilePath)

    service_account = sa
    if ':\\' in os.getcwd():
        gcp = gcp_common.gcp_common(service_account, False)
    elif service_account.upper() != 'NA':
        gcp = gcp_common.gcp_common(service_account, False)
    else:
        gcp = gcp_common.gcp_common(None, True)

    # Get the list of files for the passed in API
    file_list = gcp.gcp_storage_simple_file_list(input_bucket_name, 'l1/hubspot/' + api_name + '/' + partition_date)
    logging.info(file_list)
    # Read INI file to determine if supporting or details APIs are associated with the API passed in
    with open(confFilePath, 'r') as configFile:
        confJSON = json.loads(configFile.read())
    logging.info("Successfully read configuration file for all APIs.")

    apiConfigSet = False
    for apiObject in confJSON:
        if api_name == apiObject['apiName']:
            apiConfigSet = True
            logging.info("Passed api name matched with api name in configuration file.")
        else:
            apiConfigSet = False
            continue

        if (apiObject['detailsApiExists'] == 'True'):
            details_apis_list = apiObject['details_apis_list'].split(';')
            for one_detail_api in details_apis_list:
                if one_detail_api != api_name:
                    one_file_list = gcp.gcp_storage_simple_file_list(input_bucket_name, 'l1/hubspot/' + one_detail_api + '/' + partition_date)
                    file_list.extend(one_file_list)

        if (apiObject['api_has_supporting_apis'] == 'True'):
            support_apis_list = apiObject['api_supporting_apis_list'].split(';')
            for one_support_api in support_apis_list:
                one_file_list = gcp.gcp_storage_simple_file_list(input_bucket_name, 'l1/hubspot/' + api_name + '_' + one_support_api + '/' + partition_date)
                file_list.extend(one_file_list)

    for one_file in file_list:
        new_process_data(one_file, target_dataset_name, module_name, gcp_project_name, input_bucket_name)

    logging.info('COMPLETE')
    logging.info(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return


def hubspot_landing(**kwargs):
    module_name = 'hubspot'
    gcp_project_name = 'dev-edl'  # os.environ["GCP_PROJECT"]
    apiName = kwargs['params']['api_name']
    airflow_exec_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"].replace('T00:00:00+00:00', '').replace('-', '')

    if gcp_project_name.lower().startswith('dev'):
        env = 'dev01'
        target_bucket = 'jm_dev01_edl_lnd'
    elif gcp_project_name.lower().startswith('qa'):
        env = 'qa01'
        target_bucket = 'jm_qa01_edl_lnd'
    elif gcp_project_name.lower().startswith('stage'):
        env = 'stage'
        target_bucket = 'jm_stg_edl_lnd'
    elif gcp_project_name.lower().startswith('prod'):
        env = 'prod'
        target_bucket = 'jm_prod_edl_lnd'

    service_account = sa
    if ':\\' in os.getcwd():
        gcp = gcp_common.gcp_common(service_account, False)
    elif service_account.upper() != 'NA':
        gcp = gcp_common.gcp_common(service_account, False)
    else:
        gcp = gcp_common.gcp_common(None, True)

    logging.info('COMPLETE')
    logging.info(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    ### Get Base Information about the API connection
    connection = BaseHook.get_connection('HubspotAPI')
    hubSpotSecretKey = str(baseDecode(connection.password))
    hubSpotSecretKey = hubSpotSecretKey.replace("b'",'').replace("'",'')

    print(kwargs)
    start_date = pd.to_datetime(kwargs['ds'])
    end_date = pd.to_datetime(kwargs['next_ds'])
    directoryDate = pd.to_datetime(start_date).strftime("%Y%m%d")
    start_date_ms = to_millis(start_date)
    end_date_ms = to_millis(end_date)


    confFilePath = os.path.join(configuration.get('core', 'dags_folder'), 'hubspot', 'ini',
                                'hubspot_ingestion_direct.conf')
    with open(confFilePath, 'r') as configFile:
        confJSON = json.loads(configFile.read())
    logging.info("Successfully read configuration file for all APIs.")


    apiConfigSet = False
    for apiObject in confJSON:
        #logging.info("api name from conf file - apiObject['apiName'] : " + apiObject['apiName'])
        if apiName == apiObject['apiName']:
            apiConfigSet = True
            logging.info("Passed api name matched with api name in configuration file.")
            break
        else:
            apiConfigSet = False
            continue

    if apiConfigSet == False:
        logging.warning("Passed api name DOES NOT MATCH with any api name in configuration file.")
        sys.exit(1)
        return


    offsetFileName = 'offset_files/hubspot/offset_{apiName}.conf'.format(apiName=apiName)
    offsetConfFileBucket= "jm_dev01_edl_lnd"
    logging.info("Reading offset file for last offset value.")
    # print(offsetConfFileBucket,offsetFileName)

    file_list = gcp.gcp_storage_simple_file_list(offsetConfFileBucket,'offset_files/hubspot','.conf')
    if offsetFileName in file_list:
        stream_data = gcp.gcp_storage_return_file_stream(offsetConfFileBucket,offsetFileName)
        json_data = json.loads(stream_data)
        df = pd.DataFrame([json_data])
        print(df)
    else:
        df = pd.DataFrame()
    # return

    offsetObject = {}
    try:
        offsetObject['offsetValue'] = df.loc[0,'offsetValue']
        offsetObject['apiName'] = apiName
        logging.info("Successfully read offset file for API :" + apiName)

    except:
        offsetObject['offsetValue'] = 'NA'
        offsetObject['apiName'] = apiName

    print(offsetObject,offsetObject['offsetValue'])
    offsetValue = offsetObject['offsetValue']
    if offsetValue == 'None':
        offsetValue = 'NA'
        offsetObject['offsetValue'] = 'NA'

    logging.info("offsetValue : " + offsetValue)
    offset_details = apiObject['api_offsetKey'] + '=' + offsetObject['offsetValue'] + '=' + apiObject[
        'api_offsetKeyForURL']

    df_campaigns = pd.DataFrame()
    df_appdata_full = pd.DataFrame()
    df_appdata = pd.DataFrame()

    if apiName == 'analyticsBreakdowns':
        delta_days = int((pd.to_datetime(end_date) - pd.to_datetime(start_date)) / np.timedelta64(1, 'D'))
        if delta_days == 0:
            delta_days = 1

        logging.info('Total number of days processing...' + str(delta_days))

        for day_counter in range(delta_days):
            logging.info('Processing the day counter....' + str(day_counter + 1))
            start_date_ms = to_millis(
                (pd.to_datetime(start_date) + timedelta(days=day_counter)).strftime("%Y%m%d"))
            end_date_ms = to_millis(
                (pd.to_datetime(start_date) + timedelta(days=day_counter + 1)).strftime("%Y%m%d"))
            directoryDate = (pd.to_datetime(start_date) + timedelta(days=day_counter)).strftime("%Y%m%d")
            logging.info('Target directory date...' + str(directoryDate))

            data_file_time_stamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
            loopCounter = 0
            if apiObject['api_has_supporting_apis'] == 'True':
                support_apis_list = apiObject['api_supporting_apis_list'].split(';')
                for sup_api in support_apis_list:
                    logging.info("Breakdown API Data...." + sup_api)
                    if apiObject[str(sup_api) + '_time_based'] == 'False':
                        logging.info('Extracting data based on no time')
                        df_sub_api, hapi_offset_return = get_data_hapi_no_time(
                            apiObject[sup_api + '_url'],
                            hubSpotSecretKey,
                            apiObject[sup_api + '_limit'],apiName,
                            sup_api,
                            apiObject[sup_api + '_offset'])
                    else:
                        logging.info('Extracting data based on time')
                        df_sub_api, hapi_offset_return = get_data_hapi_with_time(
                            apiObject[sup_api + '_url'],
                            hubSpotSecretKey,
                            apiObject[sup_api + '_limit'],apiName,
                            sup_api, start_date_ms,
                            end_date_ms,
                            apiObject[sup_api + '_offset'])


                    outputFileName = '/l1/hubspot/{apiName}_{sup_api}/{directoryDate}/hubspot_{apiName}_{sup_api}_{loopCounter}_{data_file_time_stamp}.json'.format(apiName=apiName,
                                                                                                                                                                    sup_api=sup_api,
                                                                                                                                                                    directoryDate=directoryDate,
                                                                                                                                                                    loopCounter=loopCounter,
                                                                                                                                                                    data_file_time_stamp=data_file_time_stamp)
                    logging.info(" outputFileName : " + outputFileName)
                    gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                                    df_sub_api.to_json(orient='records', lines='\n'),
                                                    from_memory=True)
                    loopCounter += 1


                    df.loc[0, 'offsetValue'] = str(hapi_offset_return)
                    gcp.gcp_storage_write_data_file(offsetConfFileBucket, offsetFileName, df.to_json(orient='records',lines='\n'),
                                                    from_memory=True)

        return



    if apiObject['api_has_supporting_apis'] == 'True':
        support_apis_list = apiObject['api_supporting_apis_list'].split(';')
        for sup_api in support_apis_list:
            logging.info("Gather Support API Data...." + sup_api)
            if sup_api == 'event_summary':

                df_appdata_full = pd.DataFrame()
                for id in df_campaigns['id'].unique():
                    df_appdata, hapi_offset_return = get_data_hapi_no_time(
                        apiObject[sup_api + '_url'] + str(id),
                        hubSpotSecretKey,
                        'NA',sup_api,
                        sup_api,
                        'NA')
                    df_appdata_full = pd.concat([df_appdata_full, df_appdata])
                    df_appdata_full.reset_index(drop=True, inplace=True)
                continue
            if apiObject[str(sup_api) + '_time_based'] == 'False':
                logging.info('Extracting data based on no time')
                df_sub_api, hapi_offset_return = get_data_hapi_no_time(
                    apiObject[sup_api + '_url'],
                    hubSpotSecretKey,
                    apiObject[sup_api + '_limit'],
                    sup_api,sup_api,
                    apiObject[sup_api + '_offset'])
            else:
                logging.info('Extracting data based on time')
                df_sub_api, hapi_offset_return = get_data_hapi_with_time(
                    apiObject[sup_api + '_url'],
                    hubSpotSecretKey,
                    apiObject[sup_api + '_limit'],
                    sup_api, sup_api,start_date_ms,
                    end_date_ms,
                    apiObject[sup_api + '_offset'])

            if sup_api == 'event_campaign':
                df_campaigns = df_sub_api.copy(deep=True)
                continue

    logging.info('Grabbing main API data based on date range.')
    delta_days = int((pd.to_datetime(end_date) - pd.to_datetime(start_date)) / np.timedelta64(1, 'D'))
    if delta_days == 0:
        delta_days = 1

    logging.info('Total number of days processing...' + str(delta_days))
    for day_counter in range(delta_days):
        loopCounter = 0
        logging.info('Processing the day counter....' + str(day_counter + 1))
        start_date_ms = to_millis(
            (pd.to_datetime(start_date) + timedelta(days=day_counter)).strftime("%Y%m%d"))
        end_date_ms = to_millis(
            (pd.to_datetime(start_date) + timedelta(days=day_counter + 1)).strftime("%Y%m%d"))
        directoryDate = (pd.to_datetime(start_date) + timedelta(days=day_counter)).strftime("%Y%m%d")
        logging.info('Target directory date...' + str(directoryDate))

        data_file_time_stamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")

        if len(df_campaigns) > 0:
            logging.info("Writing Campaign Information....")
            # outputFileName = "/l1/" + known_args.hubSpotOutputDir + "/" + apiName + "_event_campaign/" + directoryDate + '/hubspot_' + apiName + '_' + str(
            #     loopCounter) + '_' + data_file_time_stamp + '.json'

            outputFileName = '/l1/hubspot/{apiName}_{sup_api}/{directoryDate}/hubspot_{apiName}_{sup_api}_{loopCounter}_{data_file_time_stamp}.json'.format(
                apiName=apiName,
                sup_api='event_campaign',
                directoryDate=directoryDate,
                loopCounter=loopCounter,
                data_file_time_stamp=data_file_time_stamp)

            logging.info(" outputFileName : " + outputFileName)
            gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                            df_campaigns.to_json(orient='records', lines='\n'),
                                            from_memory=True)

        if len(df_appdata) > 0:
            logging.info("Writing App Information....")
            outputFileName = '/l1/hubspot/{apiName}_{sup_api}/{directoryDate}/hubspot_{apiName}_{sup_api}_{loopCounter}_{data_file_time_stamp}.json'.format(
                apiName=apiName,
                sup_api='event_summary',
                directoryDate=directoryDate,
                loopCounter=loopCounter,
                data_file_time_stamp=data_file_time_stamp)
            logging.info(" outputFileName : " + outputFileName)
            gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                            df_appdata_full.to_json(orient='records', lines='\n'),
                                            from_memory=True)

            logging.info("Gather Email Event Data based on Campaign and APP ID....")
            df_email_events = pd.DataFrame()
            for id in df_appdata_full['id'].unique():
                get_id_info = df_appdata_full[(df_appdata_full['id'] == id)]
                for app in get_id_info['appId'].unique():
                    df_email_events_partial, hapi_offset_return = get_data_hapi_with_time(
                        apiObject['api_url'],
                        hubSpotSecretKey, apiObject['api_limit'], apiName,apiName, start_date_ms,
                        end_date_ms,
                        'NA', id, app)

                    df_email_events = pd.concat([df_email_events, df_email_events_partial])
                    df_email_events.reset_index(drop=True, inplace=True)

            df_email_events.reset_index(drop=True, inplace=True)

            outputFileName = '/l1/hubspot/{apiName}/{directoryDate}/hubspot_{apiName}_{loopCounter}_{data_file_time_stamp}.json'.format(
                apiName=apiName,
                directoryDate=directoryDate,
                loopCounter=loopCounter,
                data_file_time_stamp=data_file_time_stamp)
            logging.info(" outputFileName : " + outputFileName)
            gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                            df_email_events.to_json(orient='records', lines='\n'),
                                            from_memory=True)
            continue

        logging.info('Pulling the base data!!!')
        print(offset_details)

        if apiObject['time_based'] == 'False':
            logging.info('Pulling base API information for No TIME')
            df_api, hapi_offset_return = get_data_hapi_no_time(apiObject['api_url'],
                                                               hubSpotSecretKey,
                                                               apiObject['api_limit'],
                                                               apiName,apiName,
                                                               offset_details)

            if len(df_api) > 0:
                outputFileName = '/l1/hubspot/{apiName}/{directoryDate}/hubspot_{apiName}_{loopCounter}_{data_file_time_stamp}.json'.format(
                    apiName=apiName,
                    directoryDate=directoryDate,
                    loopCounter=loopCounter,
                    data_file_time_stamp=data_file_time_stamp)

                logging.info(" outputFileName : " + outputFileName)
                gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                                df_api.to_json(orient='records', lines='\n'),
                                                from_memory=True)

                offsetObject['offsetValue'] = str(hapi_offset_return)
                df.loc[0, 'offsetValue'] = str(hapi_offset_return)
                gcp.gcp_storage_write_data_file(offsetConfFileBucket, offsetFileName,
                                                df.to_json(orient='records', lines='\n'), from_memory=True)


            # continue

        if apiObject['time_based'] == 'True':
            logging.info('Pulling base API information for with TIME')
            df_api, hapi_offset_return = get_data_hapi_with_time(
                apiObject['api_url'],
                hubSpotSecretKey, apiObject['api_limit'], apiName, apiName, start_date_ms, end_date_ms)

            outputFileName = '/l1/hubspot/{apiName}/{directoryDate}/hubspot_{apiName}_{loopCounter}_{data_file_time_stamp}.json'.format(
                apiName=apiName,
                directoryDate=directoryDate,
                loopCounter=loopCounter,
                data_file_time_stamp=data_file_time_stamp)
            logging.info(" outputFileName : " + outputFileName)
            gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                            df_api.to_json(orient='records', lines='\n'),
                                            from_memory=True)

            offsetObject['offsetValue'] = str(hapi_offset_return)
            df.loc[0, 'offsetValue'] = str(hapi_offset_return)
            gcp.gcp_storage_write_data_file(offsetConfFileBucket, offsetFileName,
                                            df.to_json(orient='records', lines='\n'), from_memory=True)

        logging.info('Pulling the details data!!!')
        if (apiObject['detailsApiExists'] == 'True'):
            exception_time_base_list = []

            # if apiObject['time_based'] == 'False':
            #     df_api, hapi_offset_return = get_data_hapi_no_time(apiObject['api_url'],
            #                                                        hubSpotSecretKey,
            #                                                        apiObject['api_limit'],
            #                                                        apiName,apiName,
            #                                                        offset_details)
            # elif apiObject['time_based'] == 'True':
            #     df_api, hapi_offset_return = get_data_hapi_with_time(apiObject['api_url'],
            #                                                          hubSpotSecretKey,
            #                                                          apiObject['api_limit'],
            #                                                          apiName,apiName,
            #                                                          start_date_ms,
            #                                                          end_date_ms)
            #
            # df_api_save = pd.DataFrame()
            # df_api_save = df_api.copy(deep=True)
            if apiName.lower() == 'contacts':
                B = df_api['contacts'].apply(convert_to_dicts)
                df_api = json_normalize(B)


            details_apis_list = apiObject['details_apis_list'].split(';')
            for detail_api in details_apis_list:
                logging.info("Gather Details API Data...." + detail_api)
                id_field = apiObject['id_field']
                #id_field = id_field.encode('utf-8')

                try:
                    # with open(offsetConfFilePath.replace(apiName, detail_api), 'r') as offsetConfFile:
                    #     detail_api_offset = json.loads(offsetConfFile.read())

                    stream_data = gcp.gcp_storage_return_file_stream(offsetConfFileBucket, offsetFileName.replace(apiName, detail_api))
                    json_data = json.loads(stream_data)
                    df_detail = pd.DataFrame([json_data])
                    #TO HERE
                    #detail_api_offset = df_detail.loc[0, 'offsetValue']
                    detail_api_offset = {}
                    detail_api_offset['offsetValue'] = df_detail.loc[0, 'offsetValue']
                    detail_api_offset['apiName'] = apiName
                    logging.info("Successfully read offset file for API :" + detail_api)
                except:
                    detail_api_offset = {}
                    detail_api_offset['offsetValue'] = 'NA'
                    detail_api_offset['apiName'] = apiName

                if detail_api_offset['offsetValue'] == 'None':
                    detail_api_offset['offsetValue'] = 'NA'

                df_detail_data_full = pd.DataFrame()

                #     logging.info(df_api[id_field].values.tolist())
                for index, row in df_api.iterrows():
                    id = row[id_field]
                    #    for id in df_api[id_field].unique():
                    try:
                        id_temp = str(id).split('.')
                        id = id_temp[0]
                    except:
                        id = id

                    if detail_api == apiName:
                        detail_api_name = detail_api + '_details'
                    else:
                        detail_api_name = detail_api

                    if str(id) != 'nan':
                        offset_details = apiObject[detail_api + '_offset'] + '=' + detail_api_offset[
                            'offsetValue'] + '=' + apiObject[detail_api + '_offset_url']

                        if apiObject[detail_api + '_time_based'] == 'False':
                            df_detail_data, hapi_offset_return = get_data_hapi_no_time(
                                apiObject[detail_api + '_url'].replace('{objectId}', str(id)),
                                hubSpotSecretKey,
                                apiObject[detail_api + '_limit'],
                                detail_api_name,detail_api_name,
                                offset_details)
                            #    detail_api_offset['offsetValue'])
                            offsetObject['offsetValue'] = str(hapi_offset_return)
                        elif apiObject[detail_api + '_time_based'] == 'True':
                            df_detail_data, hapi_offset_return = get_data_hapi_with_time(
                                apiObject[detail_api + '_url'].replace('{objectId}', str(id)),
                                hubSpotSecretKey,
                                apiObject[detail_api + '_limit'],
                                detail_api_name,detail_api_name,start_date_ms,end_date_ms,
                                offset_details)
                            #    detail_api_offset['offsetValue'])
                            offsetObject['offsetValue'] = str(hapi_offset_return)

                    if 'lastUpdatedTime' in df_api.columns.values.tolist():
                        df_detail_data['lastUpdatedTime'] = row['lastUpdatedTime']
                    df_detail_data_full = pd.concat([df_detail_data_full, df_detail_data])
                    df_detail_data_full.reset_index(drop=True, inplace=True)

            if len(df_detail_data_full) > 0:
                logging.info("Writing Detail App Information....")


                if detail_api != apiName:
                    logging.info("Writing Main App Information....")
                    outputFileName = '/l1/hubspot/{apiName}/{directoryDate}/hubspot_{apiName}_{loopCounter}_{data_file_time_stamp}.json'.format(
                        apiName=detail_api,
                        directoryDate=directoryDate,
                        loopCounter=loopCounter,
                        data_file_time_stamp=data_file_time_stamp)
                    logging.info(" outputFileName : " + outputFileName)
                    gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                                    df_detail_data_full.to_json(orient='records', lines='\n'),
                                                    from_memory=True)
                else:
                    outputFileName = '/l1/hubspot/{apiName}_detail/{directoryDate}/hubspot_{apiName}_detail_{loopCounter}_{data_file_time_stamp}.json'.format(
                        apiName=apiName,
                        # sup_api=sup_api,
                        directoryDate=directoryDate,
                        loopCounter=str(id),
                        data_file_time_stamp=data_file_time_stamp)
                    logging.info(" outputFileName : " + outputFileName)
                    gcp.gcp_storage_write_data_file(target_bucket, outputFileName,
                                                    df_detail_data_full.to_json(orient='records', lines='\n'),
                                                    from_memory=True)

            #continue





        loopCounter +=1

    return