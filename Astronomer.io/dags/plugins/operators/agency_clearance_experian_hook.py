import json
import re
from fuzzywuzzy import fuzz
import datetime as dt
from time import sleep
from airflow.hooks.http_hook import HttpHook


class ExperianBin():
    def __init__(self):
        self.experian_conn_id='ExperianDefault'
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

        # exp_cred_path = 'credentials/experian_creds.json'
        # with open(exp_cred_path) as f:
        #   client_details = json.load(f)
        #
        # self.password = client_details.get('password')
        # self.username = client_details.get('username')
        # self.client_id = client_details.get('client_id')
        # self.client_secret = client_details.get('client_secret')
        # self.subcode = '0530260'
        self.token_url = client_details.get('token_url')
        self.bin_url = client_details.get('bin_url')

        self.bearer_start_time = dt.datetime.now()
        self.bearer_token = self.get_bearer_token()

    def execute(self, payload):
        from google.oauth2 import service_account

        # project = 'semi-managed-reporting' #_get_field('project')
        # bq_connection = BaseHook.get_connection(bigquery_conn_id)
        # client_secrets = json.loads(bq_connection.extra_dejson['extra__google_cloud_platform__keyfile_dict'])
        # credentials = service_account.Credentials.from_service_account_info(client_secrets)

        # df = pd.read_csv('Clearance_Analysis/data/Wexler_ActiveClients_805691178.csv')

        payload["subcode"] = self.subcode

        bearer_expiration_mins = 25
        try:
            # print('Starting BIN lookup per customer required')
            # msg='Company count to be checked: ' + str(len(df))
            # print(msg)


            bearer_time_now = dt.datetime.now()
            time_diff = bearer_time_now - self.bearer_start_time
            if time_diff.seconds >= bearer_expiration_mins*60:
                self.bearer_token = self.get_bearer_token()
                self.bearer_start_time = dt.datetime.now()

            bin_value, reliability_number, returned_name, returned_phone, returned_address, bearer_token = self.get_bin_value(payload, self.bearer_token)
            # print(bin_value, reliability_number, returned_name, returned_phone, returned_address)

            exp_data = {'experian_business_id': bin_value,
                        'experian_reliability_value': reliability_number,
                        'experian_name': returned_name,
                        'experian_phone': returned_phone,
                        'experian_street': returned_address,
                        'experian_city': returned_address,
                        'experian_state': returned_address,
                        'experian_zip': returned_address
                        }

            # FUZZY MATCH VERIFICATION, business name only for experian
            if ((fuzz.ratio(self._prep_name(payload['name']), self._prep_name(returned_name)) < 70)
                    and (fuzz.partial_ratio(self._prep_name(payload['name']), self._prep_name(returned_name)) < 75)
                        and (fuzz.token_set_ratio(self._prep_name(payload['name']), self._prep_name(returned_name)) < 58)
                            and (reliability_number <= 90) and (reliability_number > 0)):

                exp_data['experian_business_id'] = -2
                exp_data['experian_reliability_value'] = reliability_number
                exp_data['experian_street'] = 'POOR_MATCH'
                exp_data['experian_city'] = 'POOR_MATCH'
                exp_data['experian_state'] = 'POOR_MATCH'
                exp_data['experian_zip'] = 'POOR_MATCH'
                # TESTING ONLY
                exp_data['payload'] = json.dumps(payload)
                exp_data['names'] = "{} | {}".format(self._prep_name(payload['name']), self._prep_name(returned_name))
                exp_data['fuzz_ratio'] = fuzz.ratio(self._prep_name(payload['name']),
                                                             self._prep_name(returned_name))
                exp_data['partial_ratio'] = fuzz.partial_ratio(self._prep_name(payload['name']),
                                                                        self._prep_name(returned_name))
                exp_data['token_set_ratio'] = fuzz.token_set_ratio(self._prep_name(payload['name']),
                                                                            self._prep_name(returned_name))

                # log.info('Business name failed validation: {},{}'.format(payload['name'], returned_name))


            elif bin_value == -1:
                exp_data['experian_business_id'] = bin_value
                exp_data['experian_reliability_value'] = reliability_number
                exp_data['experian_street'] = 'NOT_FOUND'
                exp_data['experian_city'] = 'NOT_FOUND'
                exp_data['experian_state'] = 'NOT_FOUND'
                exp_data['experian_zip'] = 'NOT_FOUND'
                # TESTING ONLY
                exp_data['payload'] = json.dumps(payload)
                exp_data['names'] = 'NOT_FOUND'
                exp_data['fuzz_ratio'] = -1
                exp_data['partial_ratio'] = -1
                exp_data['token_set_ratio'] = -1

            elif (bin_value == -4) | (bin_value == -5):
                exp_data['experian_business_id'] = bin_value
                exp_data['experian_reliability_value'] = reliability_number
                exp_data['experian_street'] = returned_address
                exp_data['experian_city'] = returned_address
                exp_data['experian_state'] = returned_address
                exp_data['experian_zip'] = returned_address
                # TESTING ONLY
                exp_data['payload'] = json.dumps(payload)
                exp_data['names'] = returned_name
                exp_data['fuzz_ratio'] = bin_value
                exp_data['partial_ratio'] = bin_value
                exp_data['token_set_ratio'] = bin_value

            else:
                exp_data['experian_business_id'] = int(bin_value)
                exp_data['experian_reliability_value'] = int(reliability_number)

                exp_data['experian_name'] = returned_name #address_parts[0]
                exp_data['experian_phone'] = returned_phone
                exp_data['experian_street'] = returned_address.get('street', 'NOT_FOUND') # address_parts[1]
                exp_data['experian_city'] = returned_address.get('city', 'NOT_FOUND') # address_parts[2]
                exp_data['experian_state'] = returned_address.get('state', 'NOT_FOUND') #address_parts[3]
                exp_data['experian_zip'] = returned_address.get('zip', 'NOT_FOUND') #address_parts[4]
                # TESTING ONLY
                exp_data['payload'] = json.dumps(payload)
                exp_data['names'] = "{} | {}".format(self._prep_name(payload['name']),
                                                             self._prep_name(returned_name))
                exp_data['fuzz_ratio'] = fuzz.ratio(self._prep_name(payload['name']),
                                                             self._prep_name(returned_name))
                exp_data['partial_ratio'] = fuzz.partial_ratio(self._prep_name(payload['name']),
                                                                        self._prep_name(returned_name))
                exp_data['token_set_ratio'] = fuzz.token_set_ratio(
                    self._prep_name(payload['name']), self._prep_name(returned_name))

            # else:
            #     exp_data['experian_business_id'] = -3
            #     exp_data['experian_reliability_value'] = -3
            #     exp_data['experian_street'] = 'INVALID_ADDRESS'
            #     exp_data['experian_city'] = 'INVALID_ADDRESS'
            #     exp_data['experian_state'] = 'INVALID_ADDRESS'
            #     exp_data['experian_zip'] = 'INVALID_ADDRESS'
            #     # TESTING ONLY
            #     exp_data['payload'] = json.dumps(payload)
            #     exp_data['names'] = 'INVALID_ADDRESS'
            #     exp_data['fuzz_ratio'] = -3
            #     exp_data['partial_ratio'] = -3
            #     exp_data['token_set_ratio'] = -3

            # sub_df['experian_business_id'] = sub_df['experian_business_id'].astype(int)
            # sub_df['experian_reliability_value'] = sub_df['experian_reliability_value'].astype(int)
            exp_data['experian_update_time'] = str(dt.datetime.utcnow())

            # print(exp_data)
            # sub_df.to_gbq(destination_table=destination_table,
            #           project_id=project,
            #           if_exists=write_disposition,
            #           credentials=credentials)
            return(exp_data)

        except:
            # log.error("Error collecting BIN: Uploaded {} of {} rows.".format(i, len(df)))
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
                "client_id": self.client_id ,
                "client_secret": self.client_secret,
                "content-type": "application/json"
            }
   #         r = requests.post(self.token_url, json=payload, headers=headers)
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
         #   r = requests.post(self.token_url, json=payload, headers=headers)
            r = self.http_hook.run(endpoint='/oauth2/v1/token?',
                                   data=json.dumps(payload),
                                   headers=headers)
            # log.error("Error! Http status Code {}".format(r.json()))
            raise()


            #sys.exit(1)



    def get_bin_value(self, payload,bearer_token_json):
        #bearer_token_json = get_bearer_token()

        headers = {'Authorization': bearer_token_json['token_type'] + ' ' + bearer_token_json['access_token'],
                   'content-type': 'application/json'}

       # response = requests.post(self.bin_url, data=json.dumps(payload), headers=headers)
        response = self.http_hook.run(endpoint='/businessinformation/businesses/v1/search?',
                                      data=json.dumps(payload),
                                      headers=headers,
                                      extra_options={'check_response': False})

        try:
            response_bin_json = response.json()
        except:
            print(response.text)
            raise

        # ERROR HANDLING
        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if response_bin_json['errors'][0]['errorType'] == 'Unauthorized':
                    # log.info('Bearer token may have expired, retrieving new token.')
                    bearer_token_json = self.get_bearer_token()
                    headers = {
                        'Authorization': bearer_token_json['token_type'] + ' ' + bearer_token_json['access_token'],
                        'content-type': 'application/json'}

                #    response = requests.post(self.bin_url, data=json.dumps(payload), headers=headers,
                 #                                 extra_options={'check_response': False})
                    response = self.http_hook.run(endpoint='/businessinformation/businesses/v1/search?',
                                                  data=json.dumps(payload),
                                                  headers=headers,
                                                  extra_options={'check_response': False})
                    response_bin_json = response.json()
        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if (response_bin_json['errors'][0]['errorType'] == 'Request Error') \
                        | (response_bin_json['errors'][0]['errorType'] == 'steps.jsonthreatprotection.ExecutionFailed'):
                    # log.info(payload)
                    # log.error(response_bin_json)
                    return -4, -4, 'REQUEST_ERROR', 'REQUEST_ERROR', 'REQUEST_ERROR', bearer_token_json

        if 'errors' in response_bin_json:
            if len(response_bin_json['errors']) > 0:
                if (response_bin_json['errors'][0]['errorType'] == 'App Error'):
                    # log.info(payload)
                    # log.error(response_bin_json)
                    return -5, -5, 'APP_ERROR', 'APP_ERROR', 'APP_ERROR', bearer_token_json

        # log.info(response_bin_json)
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
                # log.info(payload)
                # log.error(response_bin_json)
                raise

        return bin_value,reliability_value,experian_name,experian_phone,experian_address, bearer_token_json

    def _prep_name(self, pr_name):
        pr_name = pr_name.lower()
        remove_list = [",", ".", 'inc', 'llc', 'l.l.c.', 'ltd', 'l.t.d.', 'corp',
                       'diamonds', 'diamond', 'gems',
                       'gold', 'silver',
                       'jewelers', 'jewellers', 'jeweller', 'jewelery', 'jewelry', 'jeweler', '-', "'s",
                       "’s",  '-']

        if pr_name is not None:
            # pr_name = pr_name.replace('|'.join(remove_list), '')
            # re.sub(r'\w+', sub, pr_name)
            pr_name = re.sub(r'|'.join(map(re.escape, remove_list)), '', pr_name)
        else:
            pr_name = 'NOT_FOUND'

        return pr_name