import json
from fuzzywuzzy import fuzz
import os

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import configuration
#from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.hooks.base import BaseHook

import pandas_gbq as pd_gbq
import pandas as pd
import numpy as np
from datetime import datetime


class PlaceKeyOperator(BaseOperator):
    """
      Copies data from one BigQuery table to another.
      :param write_disposition: The write disposition if the table already exists.
      :type write_disposition: str
      :param bigquery_conn_id: reference to a specific BigQuery hook.
      :type bigquery_conn_id: str
      :param delegate_to: The account to impersonate, if any.
          For this to work, the service account making the request must have domain-wide
          delegation enabled.
      :type delegate_to: str
      :param labels: a dictionary containing labels for the job/query,
          passed to BigQuery
      :type labels: dict
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::
              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict
      """

    template_fields = ('destination_table', 'source_sql') 
    template_ext = ('.sql',)
    ui_color = '#e6f0e4'

    @apply_defaults
    def __init__(self,
                 source_sql,
                 chunk_size=500,
                 write_disposition='append',
                 bigquery_conn_id='bigquery_default',
                 google_places_conn_id='GooglePlacesDefault',
                 destination_table=None,
                 use_legacy_sql=False,
                 delegate_to=None,
                 labels=None,
                 encryption_configuration=None,
                 *args,
                 **kwargs):
        super(PlaceKeyOperator, self).__init__(*args, **kwargs)
        self.source_sql = source_sql
        self.chunk_size=chunk_size
        self.write_disposition = write_disposition
        self.bigquery_conn_id = bigquery_conn_id
        self.google_places_conn_id = google_places_conn_id
        self.destination_table = destination_table
        self.use_legacy_sql = use_legacy_sql,
        self.delegate_to = delegate_to
        self.labels = labels
        self.encryption_configuration = encryption_configuration

        self.http_hook = HttpHook(method='GET',
                             http_conn_id=self.google_places_conn_id)
        places_connection = self.http_hook.get_connection(self.google_places_conn_id)
        self.key = places_connection.password


    def execute(self, context):
        from google.oauth2 import service_account

      #  service_account_file = os.path.join(configuration.get('core', 'dags_folder'), r'common/semi-managed-reporting.json')
      #  credentials = service_account.Credentials.from_service_account_file(service_account_file, )
        bq_connection = BaseHook.get_connection(self.bigquery_conn_id)
        client_secrets = json.loads(bq_connection.extra_dejson['keyfile_dict'])
        credentials = service_account.Credentials.from_service_account_info(client_secrets)

        project = 'semi-managed-reporting' #self._get_field('project')

        df = pd_gbq.read_gbq(self.source_sql, project_id=project, dialect='standard', credentials=credentials)

        # process in chunks, log failure data to help restart load manually
        try:
            j = self.chunk_size
            for i in range(0,len(df),j):
                if (len(df) - i) < j:
                    j = len(df) - i
                sub_df=df[i:i+j].copy()

                pid_list = []
                name_list = []
                addr_list = []
                ptypes_list = []
                rjson_list = []
                for index, row in sub_df.iterrows():
                    pid, name, faddress, ptypes, rjson = self.pid_lookup(row['PlaceID_Lookup'],
                                                                    row['input_name'],
                                                                    row['input_address'])
                    pid_list.append(pid)
                    ptypes_list.append(ptypes)
                    # if it's a storefront, collect more details (costs more)
                    if ('establishment' in ptypes) | ('store' in ptypes):
                        rjson = self.pid_details(pid)
                        rjson_list.append(rjson)
                        name_list.append(rjson['result']['name'])
                        addr_list.append(rjson['result']['formatted_address'])
                    else:
                        name_list.append(name)
                        addr_list.append(faddress)
                        rjson_list.append(rjson)

                if 'random' in sub_df.columns: del sub_df['random']
                sub_df['places_pid'] = np.asarray(pid_list)
                sub_df['places_name'] = np.asarray(name_list)
                sub_df['places_address'] = np.asarray(addr_list)
                sub_df['places_types'] = pd.Series(ptypes_list)
                sub_df['places_json'] = np.asarray(rjson_list)
                sub_df['places_update_time'] = datetime.utcnow()

                if not sub_df.empty:
                    print(sub_df)
                    print(sub_df.dtypes)
                    pd_gbq.to_gbq(sub_df, destination_table=self.destination_table,
                                    project_id=project,
                                    if_exists=self.write_disposition,
                                    credentials=credentials)
        
        except:
            self.log.error("Error collecting pid: Uploaded {} of {} rows.".format(i, len(df)))
            raise

    def pid_lookup(self, lookup_text, input_name, input_address):
        r = self.http_hook.run(endpoint='/maps/api/place/textsearch/json?',
                          data={'input': '{}'.format(lookup_text),
                                'inputtype': 'textquery',
                                'fields': 'place_id,formatted_address,name,types',
                                'key': self.key})
        r_dict = r.json()
        try:
            if r_dict['status'] == 'OK':
                if 'route' not in r_dict['results'][0]['types']:
                    pid = r_dict['results'][0]['place_id']
                    name = r_dict['results'][0]['name'].replace('-', ' ')
                    formatted_address = r_dict['results'][0]['formatted_address'].replace('United States', 'USA')
                    ptypes = r_dict['results'][0]['types']
                    rjson = r_dict
                else:
                    self.log.info('NOT_FOUND - input: {}'.format(lookup_text))
                    self.log.info('NOT_FOUND - response: {}'.format(r_dict))
                    return 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND'
            else:
                self.log.info('NOT_FOUND - input: {}'.format(lookup_text))
                self.log.info('NOT_FOUND - response: {}'.format(r_dict))
                return 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND'
        except:
            print(lookup_text)
            print(r_dict)
            raise

        # FUZZY MATCH VERIFICATION
        print(r_dict)
        if (pid != 'NOT_FOUND') \
                & ('premise' not in ptypes) \
                & ('street_address' not in ptypes) \
                & (fuzz.ratio(self._prep_name(input_name), self._prep_name(name)) < 70) \
                & (fuzz.partial_ratio(self._prep_name(input_name), self._prep_name(name)) < 75) \
                & (fuzz.token_set_ratio(self._prep_name(input_name), self._prep_name(name)) < 58):
            self.log.info('Business name failed validation: {},{}'.format(input_name, name))
            return 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND'
        else:
            pass

        if (pid != 'NOT_FOUND') \
                & (fuzz.ratio(self._prep_address(input_address), self._prep_address(formatted_address)) < 70) \
                & (fuzz.token_set_ratio(self._prep_address(input_address), self._prep_address(formatted_address)) < 80):
            self.log.info('Business address failed validation: {},{}'.format(input_address, formatted_address))
            return 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND'
        else:
            pass

        return pid, name, formatted_address, ptypes, rjson

    def _prep_name(self, pr_name):
        remove_list = ['inc', 'Inc', 'INC', 'LLC', 'Llc', 'llc', 'LTD', 'Ltd', 'ltd', 'Corp', 'corp',
                       'Diamonds', 'diamonds', 'Diamond', 'diamond', 'Gems', 'gems',
                       'Gold', 'gold', 'Silver', 'silver',
                       'Jewelers', 'Jewellers', 'Jeweller', 'Jewelery', 'Jewelry', 'Jeweler', '\-', "'s", "'S",
                       "’s", "’S", ",", "\.", '-']
        if pr_name is not None:
            pr_name = pr_name.replace('|'.join(remove_list), '')
        else:
            pr_name = 'NOT_FOUND'

        return pr_name

    def _prep_address(self, pr_address):
        if pr_address is not None:
            addr_remove_list1 = ['United States', 'united states', 'USA', 'U.S.A.', 'Canada',
                                 ",", '\.', '\-', 'Avenue', 'Ave', 'Street', 'St', 'Drive', 'Dr', 'Road', 'Rd']
            pr_address = pr_address.replace('|'.join(addr_remove_list1), '')

            addr_remove_list2 = [' US', ' U.S.', ' CAN', ' Can']
            pr_address = pr_address.replace('|'.join(addr_remove_list2), '')
        else:
            pr_address = 'NOT_FOUND'
        return pr_address

    def pid_details(self, pid):
        if not pid == 'NOT_FOUND':
            r = self.http_hook.run(endpoint='/maps/api/place/details/json?',
                              data={'placeid': '{}'.format(pid),
                                    'key': self.key})
            rjson = r.json()
        else:
            rjson = 'NOT_FOUND'
        return rjson
