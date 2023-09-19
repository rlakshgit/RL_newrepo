import json
import os
import re

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import configuration
from airflow.hooks.base import BaseHook

import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import datetime as dt
from time import sleep
from pandas.io.json import json_normalize
from plugins.hooks.jm_bq_hook import BigQueryHook


class ExperianKeyOperator(BaseOperator):
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
                 destination_project,
                 destination_dataset,
                 chunk_size=500,
                 write_disposition='append',
                 bigquery_conn_id='bigquery_default',
                 experian_conn_id='ExperianDefault',
                 destination_table=None,
                 task_dataset=None,
                 task_table=None,
                 use_legacy_sql=False,
                 delegate_to=None,
                 labels=None,
                 encryption_configuration=None,
                 *args,
                 **kwargs):
        super(ExperianKeyOperator, self).__init__(*args, **kwargs)
        self.source_sql = source_sql
        self.destination_project = destination_project
        self.destination_dataset = destination_dataset
        self.chunk_size = chunk_size
        self.write_disposition = write_disposition
        self.bigquery_conn_id = bigquery_conn_id
        self.experian_conn_id = experian_conn_id
        self.destination_table = destination_table
        self.task_dataset = task_dataset
        self.task_table = task_table
        self.use_legacy_sql = use_legacy_sql,
        self.delegate_to = delegate_to
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.http_hook = HttpHook(method='POST',
                                  http_conn_id=self.experian_conn_id)

        exp_connection = self.http_hook.get_connection(self.experian_conn_id)
        # self.password = exp_connection.password
        client_details = json.loads(exp_connection.password)
        self.password = client_details.get('password')
        self.username = client_details.get('username')
        self.client_id = client_details.get('client_id')
        self.client_secret = client_details.get('client_secret')
        self.subcode = client_details.get('subcode')
        self.destination_table_split = self.destination_table.split('$')[0]
        self.destination_table_split = self.destination_table.split('$')[0]
        self.destination_table_split = self.destination_table_split.split('.')[1]

    def execute(self, context):
        from google.oauth2 import service_account

        #  service_account_file = os.path.join(configuration.get('core', 'dags_folder'), r'common/semi-managed-reporting.json')
        #  credentials = service_account.Credentials.from_service_account_file(service_account_file, )

        project = self.destination_project  # self._get_field('project')
        bq_connection = BaseHook.get_connection(self.bigquery_conn_id)
        client_secrets = json.loads(bq_connection.extra_dejson['keyfile_dict'])
        credentials = service_account.Credentials.from_service_account_info(client_secrets)
        df = pd.read_gbq(self.source_sql, project_id=project, dialect='standard', credentials=credentials)

        bearer_token = self.get_bearer_token()
        i = 0
        n = 25
        try:
            print('Starting BIN lookup per customer required')
            msg = 'Company count to be checked: ' + str(len(df))
            print(msg)
            bearer_start_time = dt.datetime.now()

            j = self.chunk_size
            for i in range(0, len(df), j):
                if (len(df) - i) < j:
                    j = len(df) - i
                sub_df = df[i:i + j].copy()

                for index, row in sub_df.iterrows():
                    bearer_time_now = dt.datetime.now()
                    time_diff = bearer_time_now - bearer_start_time
                    if time_diff.seconds >= n * i * 60:
                        i = i + 1
                        bearer_token = self.get_bearer_token()

                    # Parse the payload into the appropriate parts
                    if row['ExperianBIN_Lookup'] is not None:
                        temp_address_parts = row['ExperianBIN_Lookup'].split('||')
                        address_parts = []

                        # For addresses coming from places, name and address are in 2 pieces separated by ||
                        if len(temp_address_parts) == 2:
                            address_parts.extend([temp_address_parts[0]])
                            ap_parts = temp_address_parts[1].split(', ')
                            for ap in ap_parts:
                                if re.match(r'[A-Z]{2}\s+\d{5}', ap):
                                    ap_state_zip = ap.split(' ')
                                    address_parts.extend(ap_state_zip)
                                else:
                                    address_parts.extend([ap])

                        else:
                            address_parts.extend(temp_address_parts)

                        if len(address_parts) >= 5:
                            payload = {
                                "name": address_parts[0],
                                "street": address_parts[1].upper(),
                                "city": address_parts[2],
                                "state": self.get_state_abbrv(address_parts[3]),
                                "zip": address_parts[4],
                                "subcode": self.subcode
                            }
                        else:
                            payload = {}
                            self.log.warning('Invalid address, less than five address parts: {address}'.format(
                                address=address_parts))

                        if ((len(address_parts) >= 6)
                            and (address_parts[5].lower() != 'ca')) or ((len(address_parts) == 5)
                                                                        and (address_parts[4].lower() != 'canada')
                                                                        and (address_parts[4].lower() != 'ca')):

                            bin_value, reliability_number, returned_name, returned_phone, returned_address, bearer_token = self.get_bin_value(
                                payload, bearer_token)
                            print(bin_value, reliability_number, returned_name, returned_phone, returned_address)

                            # FUZZY MATCH VERIFICATION, business name only for experian
                            if ((fuzz.ratio(self._prep_name(payload['name']), self._prep_name(returned_name)) < 70)
                                    and (fuzz.partial_ratio(self._prep_name(payload['name']),
                                                            self._prep_name(returned_name)) < 75)
                                    and (fuzz.token_set_ratio(self._prep_name(payload['name']),
                                                              self._prep_name(returned_name)) < 58)
                                    and (reliability_number <= 90) and (reliability_number > 0)):

                                print("******************************")

                                print("Inside If")

                                sub_df.loc[index, 'experian_business_id'] = -2
                                sub_df.loc[index, 'experian_reliability_value'] = reliability_number
                                sub_df.loc[index, 'experian_street'] = 'POOR_MATCH'
                                sub_df.loc[index, 'experian_city'] = 'POOR_MATCH'
                                sub_df.loc[index, 'experian_state'] = 'POOR_MATCH'
                                sub_df.loc[index, 'experian_zip'] = 'POOR_MATCH'

                                # TESTING ONLY
                                sub_df.loc[index, 'payload'] = json.dumps(payload)
                                sub_df.loc[index, 'names'] = "{} | {}".format(self._prep_name(payload['name']),
                                                                              self._prep_name(returned_name))
                                sub_df.loc[index, 'fuzz_ratio'] = fuzz.ratio(self._prep_name(payload['name']),
                                                                             self._prep_name(returned_name))
                                sub_df.loc[index, 'partial_ratio'] = fuzz.partial_ratio(
                                    self._prep_name(payload['name']),
                                    self._prep_name(returned_name))
                                sub_df.loc[index, 'token_set_ratio'] = fuzz.token_set_ratio(
                                    self._prep_name(payload['name']),
                                    self._prep_name(returned_name))

                                self.log.info(
                                    'Business name failed validation: {},{}'.format(payload['name'], returned_name))
                                continue

                            elif bin_value == -1:
                                print("******************************")

                                print("Inside elseIf1")
                                sub_df.loc[index, 'experian_business_id'] = bin_value
                                sub_df.loc[index, 'experian_reliability_value'] = reliability_number
                                sub_df.loc[index, 'experian_street'] = 'NOT_FOUND'
                                sub_df.loc[index, 'experian_city'] = 'NOT_FOUND'
                                sub_df.loc[index, 'experian_state'] = 'NOT_FOUND'
                                sub_df.loc[index, 'experian_zip'] = 'NOT_FOUND'
                                # TESTING ONLY
                                sub_df.loc[index, 'payload'] = json.dumps(payload)
                                sub_df.loc[index, 'names'] = 'NOT_FOUND'
                                sub_df.loc[index, 'fuzz_ratio'] = -1
                                sub_df.loc[index, 'partial_ratio'] = -1
                                sub_df.loc[index, 'token_set_ratio'] = -1

                            elif (bin_value == -4) | (bin_value == -5):
                                print("******************************")

                                print("Inside elseIf2")
                                sub_df.loc[index, 'experian_business_id'] = bin_value
                                sub_df.loc[index, 'experian_reliability_value'] = reliability_number
                                sub_df.loc[index, 'experian_street'] = returned_address
                                sub_df.loc[index, 'experian_city'] = returned_address
                                sub_df.loc[index, 'experian_state'] = returned_address
                                sub_df.loc[index, 'experian_zip'] = returned_address
                                # TESTING ONLY
                                sub_df.loc[index, 'payload'] = json.dumps(payload)
                                sub_df.loc[index, 'names'] = returned_name
                                sub_df.loc[index, 'fuzz_ratio'] = bin_value
                                sub_df.loc[index, 'partial_ratio'] = bin_value
                                sub_df.loc[index, 'token_set_ratio'] = bin_value

                            else:
                                print("******************************")

                                print("Inside else")
                                sub_df.loc[index, 'experian_business_id'] = int(bin_value)
                                sub_df.loc[index, 'experian_reliability_value'] = int(reliability_number)
                                sub_df.loc[index, 'experian_name'] = returned_name  # address_parts[0]
                                print(returned_phone)
                                sub_df.loc[index, 'experian_phone'] = returned_phone
                                print(sub_df.loc[index, 'experian_phone'])
                                sub_df.loc[index, 'experian_street'] = returned_address.get('street',
                                                                                            'NOT_FOUND')  # address_parts[1]
                                sub_df.loc[index, 'experian_city'] = returned_address.get('city',
                                                                                          'NOT_FOUND')  # address_parts[2]
                                sub_df.loc[index, 'experian_state'] = returned_address.get('state',
                                                                                           'NOT_FOUND')  # address_parts[3]
                                sub_df.loc[index, 'experian_zip'] = returned_address.get('zip',
                                                                                         'NOT_FOUND')  # address_parts[4]
                                # TESTING ONLY
                                sub_df.loc[index, 'payload'] = json.dumps(payload)
                                sub_df.loc[index, 'names'] = "{} | {}".format(self._prep_name(payload['name']),
                                                                              self._prep_name(returned_name))
                                sub_df.loc[index, 'fuzz_ratio'] = fuzz.ratio(self._prep_name(payload['name']),
                                                                             self._prep_name(returned_name))
                                sub_df.loc[index, 'partial_ratio'] = fuzz.partial_ratio(
                                    self._prep_name(payload['name']),
                                    self._prep_name(returned_name))
                                sub_df.loc[index, 'token_set_ratio'] = fuzz.token_set_ratio(
                                    self._prep_name(payload['name']), self._prep_name(returned_name))

                        else:
                            sub_df.loc[index, 'experian_business_id'] = -3
                            sub_df.loc[index, 'experian_reliability_value'] = -3
                            sub_df.loc[index, 'experian_street'] = 'INVALID_ADDRESS'
                            sub_df.loc[index, 'experian_city'] = 'INVALID_ADDRESS'
                            sub_df.loc[index, 'experian_state'] = 'INVALID_ADDRESS'
                            sub_df.loc[index, 'experian_zip'] = 'INVALID_ADDRESS'
                            # TESTING ONLY
                            sub_df.loc[index, 'payload'] = json.dumps(payload)
                            sub_df.loc[index, 'names'] = 'INVALID_ADDRESS'
                            sub_df.loc[index, 'fuzz_ratio'] = -3
                            sub_df.loc[index, 'partial_ratio'] = -3
                            sub_df.loc[index, 'token_set_ratio'] = -3

                if returned_phone:
                    sub_df['experian_phone'] = returned_phone
                else:
                    sub_df['experian_phone'] = 'None'

                sub_df['experian_business_id'] = sub_df['experian_business_id'].astype(int)
                sub_df['experian_reliability_value'] = sub_df['experian_reliability_value'].astype(int)
                sub_df['experian_update_time'] = dt.datetime.utcnow()

                print(sub_df.dtypes)
                print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                print(sub_df["experian_phone"])
                print(sub_df)
                print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                hook = BigQueryHook(self.bigquery_conn_id)
                schema = hook.get_schema(project_id=self.destination_project, dataset_id=self.destination_dataset,
                                         table_id=self.destination_table_split)
                schema_info = json_normalize(data=schema['fields'])
                column_name = schema_info['name'].tolist()
                new_column = []
                sql = []

                print(column_name)
                for col in sub_df.columns:
                    if col not in column_name:
                        new_column.append(col)
                        print(col)
                if len(new_column) != 0:
                    self._update_schema(new_column)
                sub_df.to_gbq(destination_table=self.destination_table,
                              project_id=project,
                              if_exists=self.write_disposition,
                              credentials=credentials)

        except:
            self.log.error("Error collecting BIN: Uploaded {} of {} rows.".format(i, len(df)))
            raise

    def get_state_abbrv(self, state_name):
        us_state_abbrev = {
            'Alabama': 'AL',
            'Alaska': 'AK',
            'Arizona': 'AZ',
            'Arkansas': 'AR',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'Delaware': 'DE',
            'District of Columbia': 'DC',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Hawaii': 'HI',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Iowa': 'IA',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Maine': 'ME',
            'Maryland': 'MD',
            'Massachusetts': 'MA',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Mississippi': 'MS',
            'Missouri': 'MO',
            'Montana': 'MT',
            'Nebraska': 'NE',
            'Nevada': 'NV',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'New York': 'NY',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Vermont': 'VT',
            'Virginia': 'VA',
            'Washington': 'WA',
            'West Virginia': 'WV',
            'Wisconsin': 'WI',
            'Wyoming': 'WY',
            #  'British Columbia': 'BC',
            #  'Ontario': 'ON',
        }
        return us_state_abbrev.get(state_name, state_name)

    def get_bearer_token(self):
        try:
            payload = {'username': self.username, 'password': self.password}
            headers = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "content-type": "application/json"
            }
            #    r = requests.post(self.token_url, json=payload, headers=headers)
            r = self.http_hook.run(endpoint='/oauth2/v1/token?',
                                   data=json.dumps(payload),
                                   headers=headers)
            response_json_data = r.json()
            sleep(0.1)

            try:
                if str(response_json_data['issued_at']).upper() == 'NONE':
                    print(response_json_data)
                    return {}
            except:
                print(response_json_data)
                return {}

            return response_json_data

        except:
            #            r = requests.post(self.token_url, json=payload, headers=headers)
            r = self.http_hook.run(endpoint='/oauth2/v1/token?',
                                   data=json.dumps(payload),
                                   headers=headers)
            self.log.error("Error! Http status Code {}".format(r.json()))
            raise ()

            # sys.exit(1)

    def get_bin_value(self, payload, bearer_token_json):
        # bearer_token_json = self.get_bearer_token()

        headers = {'Authorization': bearer_token_json['token_type'] + ' ' + bearer_token_json['access_token'],
                   'content-type': 'application/json'}

        # response = requests.post(self.bin_url, data=json.dumps(payload), headers=headers)
        response = self.http_hook.run(endpoint='/businessinformation/businesses/v1/search?',
                                      data=json.dumps(payload),
                                      headers=headers,
                                      extra_options={'check_response': False})

        response_bin_json = response.json()

        # ERROR HANDLING
        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if response_bin_json['errors'][0]['errorType'] == 'Unauthorized':
                    self.log.info('Bearer token may have expired, retrieving new token.')
                    bearer_token_json = self.get_bearer_token()
                    headers = {
                        'Authorization': bearer_token_json['token_type'] + ' ' + bearer_token_json['access_token'],
                        'content-type': 'application/json'}

                    response = self.http_hook.run(endpoint='/businessinformation/businesses/v1/search?',
                                                  data=json.dumps(payload),
                                                  headers=headers,
                                                  extra_options={'check_response': False})
                    response_bin_json = response.json()
        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if (response_bin_json['errors'][0]['errorType'] == 'Request Error') \
                        | (response_bin_json['errors'][0]['errorType'] == 'steps.jsonthreatprotection.ExecutionFailed'):
                    self.log.info(payload)
                    self.log.error(response_bin_json)
                    return -4, -4, 'REQUEST_ERROR', 'REQUEST_ERROR', 'REQUEST_ERROR', bearer_token_json

        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if (response_bin_json['errors'][0]['errorType'] == 'App Error'):
                    self.log.info(payload)
                    self.log.error(response_bin_json)
                    return -5, -5, 'APP_ERROR', 'APP_ERROR', 'APP_ERROR', bearer_token_json

        self.log.info(response_bin_json)
        if response_bin_json['success'] and len(response_bin_json['results']) == 0:
            bin_value = -1
            reliability_value = -1
            experian_name = 'NOT_FOUND'
            experian_phone = 'NOT_FOUND'
            experian_address = 'NOT_FOUND'
        else:
            try:
                bin_value = response_bin_json['results'][0]['bin']
                reliability_value = response_bin_json['results'][0]['reliabilityCode']
                experian_name = response_bin_json['results'][0]['businessName']
                experian_phone = response_bin_json['results'][0]['phone']
                experian_address = response_bin_json['results'][0]['address']

            except:
                self.log.info(payload)
                self.log.error(response_bin_json)
                raise

        return bin_value, reliability_value, experian_name, experian_phone, experian_address, bearer_token_json

    def _prep_name(self, pr_name):
        pr_name = pr_name.lower()
        remove_list = [",", ".", 'inc', 'llc', 'l.l.c.', 'ltd', 'l.t.d.', 'corp',
                       'diamonds', 'diamond', 'gems',
                       'gold', 'silver',
                       'jewelers', 'jewellers', 'jeweller', 'jewelery', 'jewelry', 'jeweler', '-', "'s",
                       "’s", '-']

        if pr_name is not None:
            # pr_name = pr_name.replace('|'.join(remove_list), '')
            # re.sub(r'\w+', sub, pr_name)
            pr_name = re.sub(r'|'.join(map(re.escape, remove_list)), '', pr_name)
        else:
            pr_name = 'NOT_FOUND'

        return pr_name

    def _update_schema(self, new_column_list):
        hook = BigQueryHook(self.bigquery_conn_id, use_legacy_sql=False)
        sql = []
        for x in new_column_list:
            sql += ['ADD column {column_name} String'.format(column_name=x)]

        join_parts = ', '.join(sql)

        query = 'ALTER TABLE {project}.{dataset}.{table} {sql_part}'.format(table=self.destination_table_split,
                                                                            sql_part=join_parts,
                                                                            project=self.destination_project,
                                                                            dataset=self.destination_dataset)
        # query = 'ALTER TABLE {project}.GN_DEV_core_sales_JDP.{table} {sql_part}'.format(table='business_stage2', sql_part=join_parts)

        hook.run_query(query)