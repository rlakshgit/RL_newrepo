#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import gzip as gz
import os
import shutil
import warnings
import logging
import io
import pandas as pd
import filelock


from google.cloud import storage

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import pickle
import dill
from faker import Factory
from collections import defaultdict
from plugins.hooks.jm_gcs import GCSHook


class AnonHook(BaseHook):
    # Anon hook contains functions to anonymize the data and upload the scrubbed data into respective buckets

    """
    :param anon_cloud_storage_conn_id: Reference to a data-anonymization project Google cloud storage hook.
    :type anon_cloud_storage_conn_id: str
    """
    _conn = None


    def __init__(self,
                 anon_cloud_storage_conn_id,
                 anon_bucket = 'jm_anon_bucket',
                 anon_file = 'cloud_composer_anon_concurrent.pkl',
                 delegate_to=None,
                 *args,
                 **kwargs
                 ):

        self.anon_bucket = anon_bucket
        self.anon_file = anon_file
        self.anon_cloud_storage_conn_id = anon_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self._args = args
        self._kwargs = kwargs
        self.build_store_list = {}

    # The below would anonymize the production version of data and return a dataframe with scrubbed data
    #df_in - dataframe to anonymize
    #anon_mapping - Dataframe which contains mapping for the columns in dataframe

    def anon_data(self, df_in, anon_mapping):

        faker = Factory.create()
        anon_hook = GCSHook(
            google_cloud_storage_conn_id=self.anon_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        file_exist = anon_hook.exists(bucket = self.anon_bucket,object = self.anon_file)

        # This function would return the mapping field dictionary based on the string value passed
        #for example if we pass First_name it would return the first_name defaultdict
        def select_dictionary(element):
            return self.build_store_list[element]
                
        lock = filelock.FileLock("{}.lock".format(self.anon_file))
        with lock.acquire(timeout=10):
            # check if file exists
            #if file exists load the file into variable and get the annon key value pairs from the variable
            #if file does not exist, create a new pikle file - get values for the mapping fields from faker library
            #build a dictionary of initial key value mapping  and save the dictionary as a pickle file into anon bucket
            if file_exist:

                try:
                    anon_stream = anon_hook.download(bucket=self.anon_bucket, object=self.anon_file)
                    self.build_store_list = dill.loads(anon_stream)
                    print(self.build_store_list)
                    print('Loaded the file into a dictionary...')

                except Exception as ex:
                    print(ex)

            else:
                print("File not present")
                full_name = defaultdict(faker.name)
                email_address = defaultdict(faker.email)
                full_address = defaultdict(faker.address)
                street_address = defaultdict(faker.street_address)
                city = defaultdict(faker.city)
                state = defaultdict(faker.state_abbr)
                state_desc = defaultdict(faker.state)
                county = defaultdict(faker.state)
                zipcode = defaultdict(faker.zipcode)
                country = defaultdict(faker.country)
                phone_number = defaultdict(faker.phone_number)
                suite = defaultdict(faker.secondary_address)
                company = defaultdict(faker.company)
                ip = defaultdict(faker.ipv4_public)
                first_name = defaultdict(faker.first_name)
                last_name = defaultdict(faker.last_name)
                url = defaultdict(faker.url)
                longitude = defaultdict(faker.longitude)
                latitude = defaultdict(faker.latitude)
                domain_name = defaultdict(faker.domain_name)

                # Dictionary to save anon key value mapping.
        #        build_store_list = {}
                self.build_store_list["full_name"] = full_name
                self.build_store_list["email_address"] = email_address
                self.build_store_list["full_address"] = full_address
                self.build_store_list["street_address"] = street_address
                self.build_store_list["city"] = city
                self.build_store_list["state"] = state
                self.build_store_list["state_desc"] = state_desc
                self.build_store_list["county"] = county
                self.build_store_list["zip"] = zipcode
                self.build_store_list["country"] = country
                self.build_store_list["phone"] = phone_number
                self.build_store_list["suite"] = suite
                self.build_store_list["company"] = company
                self.build_store_list["ip"] = ip
                self.build_store_list["first_name"] = first_name
                self.build_store_list["last_name"] = last_name
                self.build_store_list["url"] = url
                self.build_store_list["longitude"] = longitude
                self.build_store_list["latitude"] = latitude
                self.build_store_list["domain_name"] = domain_name


            print(self.build_store_list)
            #get the unique value of mapping column from the anon mapping file
            anon_mapping_list = anon_mapping["mapping"].tolist()
            anon_mapping_list = list(dict.fromkeys(anon_mapping_list))
            logging.info("Got the unique mapping value from the mapping column..")

            #for each column in the dataframe, filter the mapping dataframe by column name
            #if the length of the filtered dataframe is zero, then the column does not present in the mapping dataframe, mask the values for that column in the dataframe
            for col in df_in.columns:

                col_mapping = anon_mapping[(anon_mapping['column'] == col)]
                col_mapping.reset_index(drop=True, inplace=True)

                if col == 'partition_date':
                    continue
                    
                if len(col_mapping) == 0:
                    df_in[col] = '****'
                    logging.info ('{col} not present in lookup file. masking the values for the column'.format(col =col))
                    continue

                for element in anon_mapping_list:

                    if col_mapping.loc[0, 'mapping'].lower() == element:
                        if element == 'skip':
                            logging.info('{col} - Mapping value skip'.format(col=col))
                            break
                        if element == 'mask':
                            logging.info('{col} - Mapping value Mask'.format(col=col))
                            df_in[col] = '****'
                            break
                        else:
                            default_dict = select_dictionary(element)
                            logging.info('{col} - Mapping value {element}'.format(col=col,element = element))
                            #print(default_dict)
                            df_in[col] = df_in[col].apply(lambda x:default_dict[str(x).upper()])

                            break
                            
            #upload updated anon mapping dictionary to anon bucket
            try:
                pickledObject = pickle.dumps(self.build_store_list)
                anon_hook.upload(bucket=self.anon_bucket, object=self.anon_file, data=pickledObject)
                logging.info("File uploaded to anon bucket")
            except lock.Timeout:
                print("Update pickle file timeout")
            except Exception as e:
                print(e)
        return df_in, self.build_store_list



    #This function would upload the anonymized data into gcs
    # df - dataframe to scrub the data
    # google_cloud_storage_connection_id - Reference to a jm_dl_landing project Google cloud storage hook
    # anon_mapping_bucket
    # destination_bucket - Name of the bucket to upload the scrubbed json data into
    # destination_file_name - Name of the file to upload into
    # anon_mapping_file - Name of the lookup file for column mapping
    # anon_mapping_bucket - Name of the bucket which has column mapping lookup file

    def _upload_anon_data_to_gcs(self, df,google_cloud_storage_connection_id,destination_bucket,destination_file_name,anon_mapping_file,anon_mapping_bucket = 'jm_anonymization_lookup_file'):
        logging.info("Inside upload anon data to gcs function")
        hook = GCSHook(
            google_cloud_storage_conn_id=google_cloud_storage_connection_id,
            delegate_to=self.delegate_to)

        logging.info("Connected to jm_dl_landing project ")


        mapping_stream = hook.download(bucket=anon_mapping_bucket, object=anon_mapping_file)
        logging.info("Downloaded the look up file..")
        mapping_data = io.BufferedReader(io.BytesIO(mapping_stream))
        mapping_df = pd.read_csv(mapping_data)
        logging.info("calling anon_data function....")
        df, anon_return = self.anon_data(df_in=df, anon_mapping=mapping_df)
        logging.info("Data successfully scrubbed..")


        hook.upload(bucket=destination_bucket, object=destination_file_name,
                    data=df.to_json(orient='records', lines='\n', date_format='iso'))
        logging.info("Anon file uploaded to Anon bucket...")

        return