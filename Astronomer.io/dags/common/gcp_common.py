###
###
###

from google.cloud import storage
from google.cloud import bigquery
import os
import pandas as pd
import pandas_gbq as pd_gbq
# from sklearn.externals import joblib
import io
import sys
import warnings
import datetime
import time
from google.oauth2 import service_account
import json
import re
from google.cloud.exceptions import NotFound


# Global Settings.
warnings.simplefilter(action='ignore', category=FutureWarning)


class gcp_credentials_common():
    def __init__(self, creds_data, project, implied_creds=False):
        self.creds_data = creds_data
        self.info = json.loads(creds_data)
        self.credentials = service_account.Credentials.from_service_account_info(self.info)
        self.project = project
        self.implied_creds = implied_creds

        if self.implied_creds:
            self.storage_client = storage.Client()
        else:
            self.storage_client = storage.Client(project=self.project, credentials=self.credentials)

        if self.implied_creds:
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = bigquery.Client(project=self.project, credentials=self.credentials)

    def gcp_storage_create_new_bucket(self, project, new_bucket):
        '''
        This function is for creating a new bucket on a targeted GCP Project.
        :param new_bucket: Name of the new bucket to be generated must be unique for all of the GCP instance.
        :return: Status return only.  1-Exists already,0 - Success, -1 - Failure.
        '''

        try:
            self.storage_client.get_bucket(new_bucket)
            return 1
        except:
            try:
                self.storage_client.create_bucket(new_bucket)
                return 0

            except:
                return -1

    def gcp_storage_write_data_file(self, target_bucket, target_file_name, data_source):
        '''
        This function is for writing any file type to a bucket on a GCP Project.
        :param target_bucket: The target bucket is the GCP Bucket where you want to put the file
        :param target_file_name: File naming convention for posting to GCP
        :param data_source: Data source can be any format when loading from memory or it can be a file path
                            and name from where you want to get the data
        :return: Default return of 0, no status configured at this time.
        '''
        if target_file_name[:1] == '/':
            target_file_name = target_file_name[1:]

        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.blob(target_file_name)
        try:
            blob.upload_from_string(data_source)
        except:
            bucket = self.storage_client.get_bucket(target_bucket)
            blob = bucket.blob(target_file_name)
            blob.upload_from_string(data_source)

        return 0

    def gcp_storage_download_csv_file(self, target_bucket, target_file):
        '''
        This function is for downloading a CSV file from GCP and gives you the option of converting to a dataframe,
        store in a local directory, and to set the flag to automatically remove once converted.
        :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
        :param target_file: The target file is the file you want from GCP
        :return: The return will be either a dataframe of the file name.  The default condition will be returning a
                 dataframe of the values and keeping the local copy.
        '''

        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.get_blob(target_file)
        try:
            file_data = blob.download_as_string()
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_csv(file_stream)
            return df
        except:
            return pd.DataFrame()

    def gcp_bq_get_dataset_list(self):
        dataset_list = list(self.bigquery_client.list_datasets())
        dataset_list = []
        for dataset_item in dataset_list:
            dataset = self.bigquery_client.get_dataset(dataset_item.reference)
            value = str(dataset_item.reference)
            value = value.replace('DatasetReference(', '').replace("'", '').replace(' ', '').replace(')', '').split(',')
            dataset_list.append(value[1])
        return dataset_list

    def gcp_bq_project_table_list(self):

        dataset_list = list(self.bigquery_client.list_datasets())
        tables_found = []
        table_schemas = pd.DataFrame()
        for dataset_item in dataset_list:
            dataset = self.bigquery_client.get_dataset(dataset_item.reference)
            value = str(dataset_item.reference)
            value = value.replace('DatasetReference(', '').replace("'", '').replace(' ', '').replace(')', '').split(',')
            tables_list = list(self.bigquery_client.list_tables(dataset))
            for table_item in tables_list:
                table = self.bigquery_client.get_table(table_item.reference)
                table_schema_single = self.gcp_bq_get_table_schema(value[1], table.table_id)
                table_value = table.table_id
                table_schema_single['table'] = table_value
                table_schema_single['dataset'] = value[1]
                if len(table_schemas) == 0:
                    table_schemas = table_schema_single.copy(deep=True)
                    continue
                table_schemas = pd.concat([table_schemas, table_schema_single])
                table_schemas.reset_index(drop=True, inplace=True)

        del table_schemas['mode'], table_schemas['description']
        table_schemas = table_schemas[['dataset', 'table', 'name', 'type']]
        table_schemas.columns = ['dataset', 'table', 'column', 'type']
        table_schemas.drop_duplicates(inplace=True)
        table_schemas.reset_index(drop=True, inplace=True)
        return table_schemas

    def gcp_bq_get_anon_target_tables(self):
        dataset_list = list(self.bigquery_client.list_datasets())
        tables_found = []
        table_schemas = pd.DataFrame()
        for dataset_item in dataset_list:
            dataset = self.bigquery_client.get_dataset(dataset_item.reference)
            value = str(dataset_item.reference)
            value = value.replace('DatasetReference(', '').replace("u'", '').replace("'", '').replace(' ', '').replace(
                ')', '').split(',')
            if 'jm_ref_' not in value[1]:
                if 'jm_gld_' not in value[1]:
                    continue

            if 'digital_dashboard' in value[1]:
                continue
            if 'jm_ref_test' == value[1]:
                continue

            tables_list = list(self.bigquery_client.list_tables(dataset))
            for table_item in tables_list:
                table = self.bigquery_client.get_table(table_item.reference)
                table_schema_single = self.gcp_bq_get_table_schema(value[1], table.table_id)
                table_value = table.table_id
                if 't_' not in table.table_id:
                    continue

                if 'jm_ref_mp_coded' == value[1]:
                    continue
                table_schema_single['table'] = table_value
                table_schema_single['dataset'] = value[1]
                if len(table_schemas) == 0:
                    table_schemas = table_schema_single.copy(deep=True)
                    continue
                table_schemas = pd.concat([table_schemas, table_schema_single])
                table_schemas.reset_index(drop=True, inplace=True)

        try:
            del table_schemas['mode']
        except:
            pass
        try:
            del table_schemas['description']
        except:
            pass
        table_schemas = table_schemas[['dataset', 'table', 'name', 'type']]
        table_schemas.columns = ['dataset', 'table', 'column', 'type']
        table_schemas.drop_duplicates(inplace=True)
        table_schemas.reset_index(drop=True, inplace=True)
        return table_schemas

    def __parse_record_field(self, row_data, schema_details, top_level_name=''):
        '''
        This function is to parse each of the schema types from the schema string.
        :param row_data: Schema value
        :param schema_details: Used for parsing of the values
        :param top_level_name: Used for nested tables.
        :return: Returns a tuple of the values.
        '''
        name = row_data['name']
        if top_level_name != '':
            name = top_level_name + '.' + name
        schema_details.append([{
            'name': name,
            'type': row_data['type'],
            'mode': row_data['mode'],
            'fields': pd.np.nan,
            'description': row_data['description']
        }])
        if type(row_data.get('fields', 0.0)) == float:
            return None
        for entry in row_data['fields']:
            self.__parse_record_field(entry, schema_details, name)

    def __unpack_all_schema(self, schema):
        '''
        This function will parse the schema data into a Dataframe
        :param schema: Schema read from BQ table
        :return: Dataframe of results.
        '''
        schema_details = []
        schema.apply(lambda row:
                     self.__parse_record_field(row, schema_details), axis=1)
        result = pd.concat([pd.DataFrame.from_dict(x) for x in schema_details])
        result.reset_index(drop=True, inplace=True)
        del result['fields']
        return result

    def gcp_bq_get_table_schema(self, dataset, table):
        '''
        This function is for getting the table schema from any dataset.table combination.
        :param dataset: The dataset is the subgrouping of tables
        :param table: Table from which you want the schema
        :return: Schema in a dictionary format, and will return INVALID if unable to find table.
        '''
        dataset_id = dataset
        table_id = table
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            table = self.bigquery_client.get_table(table_ref)  # API request
            original_schema = table.schema
            # Unnest table column names.
            schema = pd.DataFrame.from_dict([x.to_api_repr() for x in original_schema])
            if 'fields' in schema.columns:
                schema = self.__unpack_all_schema(schema)
            schema = schema[['name', 'type', 'mode', 'description']]

            return schema
        except:
            return pd.DataFrame()

    def gcp_bq_query_to_dataframe(self, project, ssql):
        '''
        This function is the support of the pandas_gbq library for gathering information.  By default it will try
        legacy format first and then try standard format.
        :param project: Project of the GCP where the BQ is located.
        :param ssql: The SQL statement for getting information.
        :return: Will return the contents of the SQL in dataframe format.  If the failure does occur is should print
                 The reason for the failure and return a blank dataframe.
        '''

        if self.implied_creds:
            cred_local = None
        else:
            cred_local = self.creds_data

        try:
            df = pd_gbq.read_gbq(str(ssql), project_id=self.project, dialect='legacy',
                                 private_key=cred_local)
        except Exception as e:
            try:
                df = pd_gbq.read_gbq(str(ssql), project_id=self.project, dialect='standard',
                                     private_key=cred_local)
            except Exception as f:
                print('Legacy failed for the following Exception:', e)
                print('Standard failed for the following Exception:', f)
                df = pd.DataFrame()

        return df

    def gcp_bq_query_to_df_non_gbq(self, project, sql):
        results = self.bigquery_client.query(sql)
        return results.to_dataframe()

    def gcp_bq_row_count(self, dataset, table):
        table_ref = self.bigquery_client.dataset(dataset).table(table)
        previous_rows = self.bigquery_client.get_table(table_ref).num_rows
        return previous_rows

    def gcp_bq_linked_tabled_update(self, dataset, table, uri='NA', data_mangement='WRITE_TRUNCATE',
                                    error_count_allowed=0):
        dataset_ref = self.bigquery_client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True

        if data_mangement == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif data_mangement == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.max_bad_records = 10000000
        load_job = self.bigquery_client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            location='US',  # Location must match that of the destination dataset.
            job_config=job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        destination_table = self.bigquery_client.get_table(dataset_ref.table(table))
        return destination_table.num_rows

    def gcp_bq_linked_tabled_update_csv(self, dataset, table, uri='NA', data_mangement='WRITE_TRUNCATE',
                                        error_count_allowed=0):
        dataset_ref = self.bigquery_client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'

        job_config.schema = [
            bigquery.SchemaField('dataset', 'STRING'),
            bigquery.SchemaField('table', 'STRING'),
            bigquery.SchemaField('column', 'STRING'),
            bigquery.SchemaField('type', 'STRING'),
            bigquery.SchemaField('mapping', 'STRING'),
            bigquery.SchemaField('prev_mapping', 'STRING'),
            bigquery.SchemaField('possible_match', 'STRING')

        ]

        if data_mangement == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif data_mangement == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # job_config.source_format = bigquery.SourceFormat.CSV
        job_config.max_bad_records = 10000000
        load_job = self.bigquery_client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            location='US',  # Location must match that of the destination dataset.
            job_config=job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        destination_table = self.bigquery_client.get_table(dataset_ref.table(table))
        return destination_table.num_rows

    def gcp_bq_table_exist(self, dataset, table):
        '''
        Simple python function for the determination if the table does exist.
        :param dataset: Dataset is in the reference to the dataset in the Big Query
        :param table: Table name in the Big Query
        :return: Return True for present, False for not present.
        '''

        dataset = self.bigquery_client.dataset(dataset)
        table_ref = dataset.table(table)
        from google.cloud.exceptions import NotFound
        try:
            self.bigquery_client.get_table(table_ref)
            return True
        except NotFound:
            return False


class gcp_common():
    def __init__(self, passed_creds=None, implied_creds=False, max_wait_seconds=180):
        self.explicit_creds = passed_creds
        self.implied_creds = implied_creds
        if self.implied_creds:
            self.credentials = None
        else:
            self.credentials = self.explicit_creds

        self.BYTES_PER_GB = 2 ** 30
        self.max_wait_seconds = max_wait_seconds
        self.total_gb_used_net_cache = 0

        if self.implied_creds:
            self.storage_client = storage.Client()
        else:
            self.storage_client = storage.Client.from_service_account_json(self.explicit_creds)

        if self.implied_creds:
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = bigquery.Client.from_service_account_json(self.explicit_creds)

        pass

    ### Storage Section
    def gcp_storage_return_file_stream(self, target_bucket, target_file):
        '''
        This function is for downloading a JSON file from GCP and gives you the option of converting to a dataframe,
        store in a local directory, and to set the flag to automatically remove once converted.
        :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
        :param target_file: The target file is the file you want from GCP
        :param system_store_loc:  The system_store_loc is the file path of where the file will be posted.  The same file
                                  name from GCP will be assumed for the local copy.
        :param from_memory: This flag indicates not to write the file local but to read directly into memory.
        :param convert_to_df: Convert to df flag is to auto load the data in to a dataframe and return the dataframe
                              otherwise it will return just the filename
        :param delete_local: The delete local will be a True/False flag for the removing the file right away after
                             processing, in this case with the flag set to True it will delete the local file after
                             it is converted to a dataframe.
        :return: The return will be either a dataframe of the file name.  The default condition will be returning a
                 dataframe of the values and keeping the local copy.
        '''
        file_split = target_file.split('/')

        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.get_blob(target_file)
        file_data = blob.download_as_string()
        # file_stream = io.BufferedReader(io.BytesIO(file_data))
        return file_data

    def gcp_storage_download_json_file(self, target_bucket, target_file, system_store_loc, from_memory=True,
                                       convert_to_df=True, delete_local=False):
        '''
        This function is for downloading a JSON file from GCP and gives you the option of converting to a dataframe,
        store in a local directory, and to set the flag to automatically remove once converted.
        :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
        :param target_file: The target file is the file you want from GCP
        :param system_store_loc:  The system_store_loc is the file path of where the file will be posted.  The same file
                                  name from GCP will be assumed for the local copy.
        :param from_memory: This flag indicates not to write the file local but to read directly into memory.
        :param convert_to_df: Convert to df flag is to auto load the data in to a dataframe and return the dataframe
                              otherwise it will return just the filename
        :param delete_local: The delete local will be a True/False flag for the removing the file right away after
                             processing, in this case with the flag set to True it will delete the local file after
                             it is converted to a dataframe.
        :return: The return will be either a dataframe of the file name.  The default condition will be returning a
                 dataframe of the values and keeping the local copy.
        '''
        file_split = target_file.split('/')
        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.get_blob(target_file)
        if from_memory:
            file_data = blob.download_as_string()
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            return df
        else:
            try:
                destination_file_name = system_store_loc + '\\' + file_split[len(file_split) - 1]
                blob.download_to_filename(destination_file_name)
                if convert_to_df:
                    df = pd.read_json(os.path.join(system_store_loc, destination_file_name), orient='records',
                                      lines='\n')
                    if delete_local:
                        os.remove(destination_file_name)
                    return df
                else:
                    return destination_file_name
            except:
                return None

    def gcp_storage_download_csv_file(self, target_bucket, target_file, system_store_loc, from_memory=True,
                                      convert_to_df=True, delete_local=False):
        '''
        This function is for downloading a CSV file from GCP and gives you the option of converting to a dataframe,
        store in a local directory, and to set the flag to automatically remove once converted.
        :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
        :param target_file: The target file is the file you want from GCP
        :param system_store_loc:  The system_store_loc is the file path of where the file will be posted.  The same file
                                  name from GCP will be assumed for the local copy.
        :param from_memory: This flag indicates not to write the file local but to read directly into memory.
        :param convert_to_df: Convert to df flag is to auto load the data in to a dataframe and return the dataframe
                              otherwise it will return just the filename
        :param delete_local: The delete local will be a True/False flag for the removing the file right away after
                             processing, in this case with the flag set to True it will delete the local file after
                             it is converted to a dataframe.
        :return: The return will be either a dataframe of the file name.  The default condition will be returning a
                 dataframe of the values and keeping the local copy.
        '''
        file_split = target_file.split('/')
        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.get_blob(target_file)
        if from_memory:
            file_data = blob.download_as_string()
            file_stream = io.BufferedReader(io.BytesIO(file_data))
            df = pd.read_csv(file_stream)
            return df
        else:
            try:
                destination_file_name = system_store_loc + '\\' + file_split[len(file_split) - 1]
                blob.download_to_filename(destination_file_name)
                if convert_to_df:
                    df = pd.read_csv(os.path.join(system_store_loc, destination_file_name))
                    if delete_local:
                        os.remove(destination_file_name)
                    return df
                else:
                    return destination_file_name
            except:
                return None

    # def gcp_storage_download_pkl_file_model(self, target_bucket, target_file, system_store_loc, delete_local=False):
    #     '''
    #     This function is for the downloading of the model pickle files that are targetted to be used for the GCP
    #     App Engine deployment.  This function will only return targeted sections of the pickle file and may require
    #     modifictions as model files or pickle files develop.
    #     :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
    #     :param target_file: The target file is the file you want from GCP
    #     :param system_store_loc:  The system_store_loc is the file path of where the file will be posted.  The same file
    #                               name from GCP will be assumed for the local copy.
    #     :param delete_local: The delete local will be a True/False flag for the removing the file right away after
    #                          processing, in this case with the flag set to True it will delete the local file after
    #                          it is read and provide the columns to keep, and model object.
    #     :return: The return from this func is the Columns to Keep (a list) and the Model Object.  If for any reason there
    #              is a failure in reading the data it will default to None for both outputs.
    #     '''
    #     file_split = target_file.split('/')
    #     bucket = self.storage_client.get_bucket(target_bucket)
    #     blob = bucket.get_blob(target_file)
    #     try:
    #         destination_file_name = system_store_loc + '\\' + file_split[len(file_split) - 1]
    #         blob.download_to_filename(destination_file_name)
    #         nested_model = joblib.load(destination_file_name)
    #         cols_to_keep = nested_model[0]
    #         model = nested_model[1]
    #         if delete_local:
    #             os.remove(destination_file_name)
    #         return cols_to_keep, model
    #
    #     except:
    #         return None, None
    #
    # def gcp_storage_load_model_pkl_memory(self, target_bucket, target_file):
    #     '''
    #     This function is for the reading GCP location into memory of the model pickle files that are targetted to be
    #     used for the GCP App Engine deployment.  This function will only return targeted sections of the pickle file
    #     and may requiremodifictions as model files or pickle files develop.
    #
    #     :param target_bucket: The target bucket is the GCP Bucket for where you want to get the file from.
    #     :param target_file: The target file is the file you want from GCP
    #     :return: The return from this func is the Columns to Keep (a list) and the Model Object.  If for any reason there
    #              is a failure in reading the data it will default to None for both outputs.
    #     '''
    #     bucket = self.storage_client.get_bucket(target_bucket)
    #     blob = bucket.get_blob(target_file)
    #     file_data = blob.download_as_string()
    #     file_stream = io.BufferedReader(io.BytesIO(file_data))
    #     nested_model = joblib.load(file_stream)
    #     cols_to_keep = nested_model[0]
    #     model = nested_model[1]
    #     return cols_to_keep, model

    def gcp_storage_create_new_bucket(self, new_bucket):
        '''
        This function is for creating a new bucket on a targeted GCP Project.
        :param new_bucket: Name of the new bucket to be generated must be unique for all of the GCP instance.
        :return: Status return only.  1-Exists already,0 - Success, -1 - Failure.
        '''
        try:
            try:
                self.storage_client.get_bucket(new_bucket)
                return 1
            except:
                self.storage_client.create_bucket(new_bucket)
                return 0
        except:
            return -1

    def gcp_storage_write_data_file(self, target_bucket, target_file_name, data_source, from_memory=False):
        '''
        This function is for writing any file type to a bucket on a GCP Project.
        :param target_bucket: The target bucket is the GCP Bucket where you want to put the file
        :param target_file_name: File naming convention for posting to GCP
        :param data_source: Data source can be any format when loading from memory or it can be a file path
                            and name from where you want to get the data
        :param from_memory: The from memory flag is for posting data directly to GCP from memory without saving
                            to local drive
        :return: Default return of 0, no status configured at this time.
        '''
        if target_file_name[:1] == '/':
            target_file_name = target_file_name[1:]
        bucket = self.storage_client.get_bucket(target_bucket)
        blob = bucket.blob(target_file_name)
        if from_memory:
            try:
                blob.upload_from_string(data_source)
            except:
                bucket = self.storage_client.get_bucket(target_bucket)
                blob = bucket.blob(target_file_name)
                blob.upload_from_string(data_source)
        else:
            blob.upload_from_filename(data_source)
            print('File {} uploaded to {}.'.format(
                data_source,
                target_file_name))
        return 0

    def gcp_storage_bucket_file_list(self, target_bucket, filter=None):
        '''
        This function is for getting all the files in a specific bucket on GCP and allow for a filter to be present
        to help reduce the number of files reviewed.  This type of information is used for the procesing of multiple
        files or to locate specific files and the directory information.
        :param target_bucket: The target bucket from where you are looking for files.
        :param filter: String parameter to be used for filter data, this is an instring command so ensure you do not
                       have the filter at the wrong level of detail.
        :return: The return is a dataframe with all the information that is collected in regards to file lists.
        '''

        def strip_date(x):
            '''
            This function is for supporting an apply, which will review each item of the series selected for ingestion
            :param x: X is the date from each row
            :return: Returns the data in a format of YYYYMMDD to be used later for filtering or further analysis.
            '''
            x_split = x.split('/')
            date_string = x_split[2]
            date_time_stamp = pd.to_datetime(date_string, format="%Y%m%d")
            return date_time_stamp

        bucket = self.storage_client.get_bucket(target_bucket)
        available_files = []
        for f in bucket.list_blobs():
            f_string = str(f).replace(' ', '').replace('<Blob:', '').replace('>', '')
            f_string_split = f_string.split(',')
            blob = bucket.get_blob(f_string_split[1])
            file_stream = blob.download_as_string()
            if filter == None:
                available_files.append((f_string_split[0], f_string_split[1], len(file_stream)))
            else:
                if filter in f_string_split[1]:
                    if 'sample_data' not in f_string_split[1]:
                        available_files.append((f_string_split[0], f_string_split[1], len(file_stream)))

        avail_df = pd.DataFrame(available_files, columns=['Bucket', 'File', 'FileSize'])
        try:
            avail_df['folder_date'] = avail_df['File'].apply(strip_date)
            avail_df['YEAR'] = pd.DatetimeIndex(avail_df['folder_date']).year
            avail_df['MONTH'] = pd.DatetimeIndex(avail_df['folder_date']).month
            avail_df['MONTH_YEAR'] = avail_df['MONTH'].astype(str) + '_' + avail_df['YEAR'].astype(str)
            avail_df.sort_values(by=['folder_date'], ascending=[True], inplace=True)
        except:
            pass
        avail_df.reset_index(drop=True, inplace=True)
        return avail_df

    def gcp_storage_bucket_file_list_light(self, target_bucket, filter=None):
        '''
        This function is for getting all the files in a specific bucket on GCP and allow for a filter to be present
        to help reduce the number of files reviewed.  This type of information is used for the procesing of multiple
        files or to locate specific files and the directory information.
        :param target_bucket: The target bucket from where you are looking for files.
        :param filter: String parameter to be used for filter data, this is an instring command so ensure you do not
                       have the filter at the wrong level of detail.
        :return: The return is a dataframe with all the information that is collected in regards to file lists.
        '''

        def strip_date(x):
            '''
            This function is for supporting an apply, which will review each item of the series selected for ingestion
            :param x: X is the date from each row
            :return: Returns the data in a format of YYYYMMDD to be used later for filtering or further analysis.
            '''
            x_split = x.split('/')
            date_string = x_split[2]
            date_time_stamp = pd.to_datetime(date_string, format="%Y%m%d")
            return date_time_stamp

        bucket = self.storage_client.get_bucket(target_bucket)
        available_files = []
        for f in bucket.list_blobs():
            f_string = str(f).replace(' ', '').replace('<Blob:', '').replace('>', '')
            f_string_split = f_string.split(',')
            blob = bucket.get_blob(f_string_split[1])
            # file_stream = blob.download_as_string()
            if filter == None:
                available_files.append((f_string_split[0], f_string_split[1]))  # ,len(file_stream)))
            else:
                if filter in f_string_split[1]:
                    if 'sample_data' not in f_string_split[1]:
                        available_files.append((f_string_split[0], f_string_split[1]))  # ,len(file_stream)))

        avail_df = pd.DataFrame(available_files, columns=['Bucket', 'File'])  # ,'FileSize'])
        try:
            avail_df['folder_date'] = avail_df['File'].apply(strip_date)
            avail_df['YEAR'] = pd.DatetimeIndex(avail_df['folder_date']).year
            avail_df['MONTH'] = pd.DatetimeIndex(avail_df['folder_date']).month
            avail_df['MONTH_YEAR'] = avail_df['MONTH'].astype(str) + '_' + avail_df['YEAR'].astype(str)
            avail_df.sort_values(by=['folder_date'], ascending=[True], inplace=True)
        except:
            pass
        avail_df.reset_index(drop=True, inplace=True)
        return avail_df

    def gcp_storage_bucket_list_files(self, target_bucket, filter=None):
        '''
        This function is for getting all the files in a specific bucket on GCP and allow for a filter to be present
        to help reduce the number of files reviewed.  This type of information is used for the procesing of multiple
        files or to locate specific files and the directory information.
        :param target_bucket: The target bucket from where you are looking for files.
        :param filter: String parameter to be used for filter data, this is an instring command so ensure you do not
                       have the filter at the wrong level of detail.
        :return: The return is a dataframe with all the information that is collected in regards to file lists.
        '''

        def strip_date(x):
            '''
            This function is for supporting an apply, which will review each item of the series selected for ingestion
            :param x: X is the date from each row
            :return: Returns the data in a format of YYYYMMDD to be used later for filtering or further analysis.
            '''
            x_split = x.split('/')
            date_string = x_split[2]
            date_time_stamp = pd.to_datetime(date_string, format="%Y%m%d")
            return date_time_stamp

        bucket = self.storage_client.get_bucket(target_bucket)

        df = pd.DataFrame()
        df['blob_list'] = list(bucket.list_blobs())

        print(df)

        def blob_cleanup(x):
            f_string = str(x).replace(' ', '').replace('<Blob:', '').replace('>', '')
            return f_string

        def blob_parse(x, target_value):
            x_string_split = x.split(',')
            if target_value.lower() == 'bucket':
                return x_string_split[0]
            if target_value.lower() == 'source':
                f = x_string_split[1]
                f_spilt = f.split('/')
                return f_spilt[1]
            if target_value.lower() == 'sub_source':
                f = x_string_split[1]
                f_spilt = f.split('/')

                if f_spilt[1] == 'hubspot':
                    return f_spilt[2]
                else:
                    return 'all'

            if target_value.lower() == 'date':
                f = x_string_split[1]
                p = re.compile(r'\d+')
                m = p.findall(f)
                return m[1]

            if target_value.lower() == 'layer':
                f = x_string_split[1]
                f_spilt = f.split('/')
                return f_spilt[0]

            return None

        df['blob_clean'] = df['blob_list'].apply(blob_cleanup)
        df['bucket'] = df['blob_clean'].apply(blob_parse, args=('bucket',))
        df['source'] = df['blob_clean'].apply(blob_parse, args=('source',))
        df['sub_source'] = df['blob_clean'].apply(blob_parse, args=('sub_source',))
        df['source_sub_source'] = df['source'] + '_' + df['sub_source']
        df['layer'] = df['blob_clean'].apply(blob_parse, args=('layer',))
        df['date'] = df['blob_clean'].apply(blob_parse, args=('date',))

        return df

    def gcp_storage_read_into_memory(self, target_bucket, target_file):
        '''
        This function is for reading any file type into memory and return the content in a string format
        :param target_bucket: Target bucket of where the file to read is located.
        :param target_file:  Target file that is going to be read with the appropriate folder/file structure.
        :return: The data content in string format, it may be in byte order.  If the read fails the default
                 is a NULL string.
        '''

        bucket = self.storage_client.get_bucket(target_bucket)
        try:
            blob = bucket.get_blob(target_file)
            file_data = blob.download_as_string()
            return file_data
        except:
            return None

    def gcp_storage_file_move(self, target_bucket, target_file, new_file_name):
        '''
        This function is for reading any file type into memory and return the content in a string format
        :param target_bucket: Target bucket of where the file to read is located.
        :param target_file:  Target file that is going to be read with the appropriate folder/file structure.
        :return: The data content in string format, it may be in byte order.  If the read fails the default
                 is a NULL string.
        '''
        source_bucket = self.storage_client.get_bucket(target_bucket)
        source_blob = source_bucket.blob(target_file)
        source_bucket.rename_blob(source_blob, new_file_name)
        return new_file_name

    def gcp_storage_simple_file_list(self, target_bucket, prefix):
        bucket = self.storage_client.get_bucket(target_bucket)

        blob_list = []
        blobObjects = [blobList for blobList in bucket.list_blobs(prefix=prefix)]
        for id, blob in enumerate(blobObjects):

            blob.name = str(blob.name)
            if ".json" in blob.name:
                blob_list.append(blob.name)

        return blob_list

    ### Big Query Section.....

    def gcp_bq_write_table_force_schema(self, df_in, project, dataset, table_name, control, schema):
        source_target = dataset + '.' + table_name

        '''
        This function will generate the schema and table in the Big Query
        :param
            df_in: this is the input data frame from the file
            control: This is for the control of the appending of the data or the replacing of the data.
                     Valid constructs must be ["APPEND","REPLACE"]
            schema_enable: This is for the control the pre-determining the schema and forcing this onto the table
        :return:
            will return a status flag of 0 for success and -1 for failure.
        '''

        if (('APPEND' != control.upper()) & ('REPLACE' != control.upper())):
            print('Invalid Control of:', control)
            print('Must be APPEND or REPLACE....')
            sys.exit(0)

        try:
            pd_gbq.to_gbq(df_in, destination_table=source_target,
                          project_id=project.lower(),
                          if_exists=control.lower(), private_key=self.credentials, verbose=True,
                          table_schema=schema)
            return 0
        except:
            pd_gbq.to_gbq(df_in, destination_table=source_target,
                          project_id=project.lower(),
                          if_exists=control.lower(), private_key=self.credentials, verbose=True,
                          table_schema=schema)
            return -1

    def gcp_bq_write_table(self, df_in, project, dataset, table_name, control, schema_enable=False, schema='NA'):
        source_target = dataset + '.' + table_name

        '''
        This function will generate the schema and table in the Big Query
        :param
            df_in: this is the input data frame from the file
            control: This is for the control of the appending of the data or the replacing of the data.
                     Valid constructs must be ["APPEND","REPLACE"]
            schema_enable: This is for the control the pre-determining the schema and forcing this onto the table
        :return:
            will return a status flag of 0 for success and -1 for failure.
        '''

        if (('APPEND' != control.upper()) & ('REPLACE' != control.upper())):
            print('Invalid Control of:', control)
            print('Must be APPEND or REPLACE....')
            sys.exit(0)

        # print(schema_gbq)

        try:
            if schema_enable:
                def generate_bq_schema(df, default_type='STRING'):
                    """ Given a passed df, generate the associated Google BigQuery schema.
                    Parameters
                    ----------
                    df : DataFrame
                    default_type : string
                        The default big query type in case the type of the column
                        does not exist in the schema.
                    """

                    type_mapping = {
                        'i': 'INTEGER',
                        'b': 'BOOLEAN',
                        'f': 'FLOAT',
                        'O': 'STRING',
                        'S': 'STRING',
                        'U': 'STRING',
                        'M': 'TIMESTAMP'
                    }

                    fields = []
                    for column_name, dtype in df.dtypes.iteritems():
                        fields.append({'name': column_name,
                                       'type': type_mapping.get(dtype.kind, default_type),
                                       'mode': 'REPEATABLE'})

                    return fields  # {'fields': fields

                schema_gbq = generate_bq_schema(df_in)
                print('Build with Schema')
                if schema == 'NA':
                    pd_gbq.to_gbq(df_in, destination_table=source_target,
                                  project_id=project.lower(),
                                  if_exists=control.lower(), private_key=self.credentials, verbose=True,
                                  table_schema=schema_gbq)
                else:
                    pd_gbq.to_gbq(df_in, destination_table=source_target,
                                  project_id=project.lower(),
                                  if_exists=control.lower(), private_key=self.credentials, verbose=True,
                                  table_schema=schema)
                print('Build with Schema --- Complete')
            else:
                print('Build No Schema')
                pd_gbq.to_gbq(df_in, destination_table=source_target,
                              project_id=project.lower(),
                              if_exists=control.lower(), private_key=self.credentials,
                              verbose=True)  # , table_schema=schema_gbq)
                print('Build No Schema---Complete')
            return 0
        except:
            pd_gbq.to_gbq(df_in, destination_table=source_target,
                          project_id=project.lower(),
                          if_exists=control.lower(), private_key=self.credentials,
                          verbose=True)  # , table_schema=schema_gbq)
            return -1

    def gcp_bq_table_exist(self, dataset, table):
        '''
        Simple python function for the determination if the table does exist.
        :param dataset: Dataset is in the reference to the dataset in the Big Query
        :param table: Table name in the Big Query
        :return: Return True for present, False for not present.
        '''

        dataset = self.bigquery_client.dataset(dataset)
        table_ref = dataset.table(table)
        from google.cloud.exceptions import NotFound
        try:
            self.bigquery_client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def gcp_bq_columns_get(self, project, dataset, table_name):
        '''
        This defintion is for the gathering of the current table and the columns populated.
        :param project: Project that the table and dataset is located in
        :param dataset: Dataset of where the table is located in
        :param table_name: The table that has the data that we want the columns
        :return: Returns a list of the columns.
        '''
        source_target = dataset + '.' + table_name
        sql = '''SELECT * from `{project}.{dataset}.{table_name}`'''.format(project=project,
                                                                            dataset=dataset,
                                                                            table_name=table_name)

        df = self.gcp_bq_query_to_df_non_gbq(sql)

        #ssql_build = 'SELECT * FROM [' + source_target + '] limit 1'
        # ssql_build = 'SELECT * FROM [' + source_target + '] limit 1'
        # df = pd_gbq.read_gbq(str(ssql_build), project_id=project,
        #                      private_key=self.credentials)

        return df.columns.values.tolist()

    def gcp_bq_force_table_schema_update(self, dataset, table, new_schema):
        '''
        This function will read the table schema and then append the new column that is required.  Also allow the
        programmer to send data type and null base as a factor as well.
        :param dataset: The dataset is the subgrouping of tables
        :param table: Table from which you want to change the schema
        :param new_col: New column name to be added to the table
        :param datatype: Data type of the new column.  By default it will be a string.
        :param null_base: Null type for either REPEATABLE or NULLABLE.  By default we want to use NULLABLE.
        :return: If the code is successful the code will return the new schema.  If the schema where to fail
                 to update the function will return FAILURE.
        '''

        dataset_id = dataset
        table_id = table
        table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
        table = self.bigquery_client.get_table(table_ref)  # API request
        table.schema = new_schema
        table_ref.update()
        return new_schema

    def gcp_bq_table_schema_update(self, dataset, table, new_col, datatype='STRING', null_base='NULLABLE'):
        '''
        This function will read the table schema and then append the new column that is required.  Also allow the
        programmer to send data type and null base as a factor as well.
        :param dataset: The dataset is the subgrouping of tables
        :param table: Table from which you want to change the schema
        :param new_col: New column name to be added to the table
        :param datatype: Data type of the new column.  By default it will be a string.
        :param null_base: Null type for either REPEATABLE or NULLABLE.  By default we want to use NULLABLE.
        :return: If the code is successful the code will return the new schema.  If the schema where to fail
                 to update the function will return FAILURE.
        '''

        dataset_id = dataset
        table_id = table
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            table = self.bigquery_client.get_table(table_ref)  # API request
            original_schema = table.schema
            new_schema = original_schema[:]  # creates a copy of the schema
            new_schema.append(bigquery.SchemaField(new_col, datatype, null_base))
            table.schema = new_schema
            self.bigquery_client.update_table(table, ['schema'])  # API request
            return new_schema
        except:
            return 'FAILED'

    def gcp_bq_query_to_dataframe(self, project, ssql):
        '''
        This function is the support of the pandas_gbq library for gathering information.  By default it will try
        legacy format first and then try standard format.
        :param project: Project of the GCP where the BQ is located.
        :param ssql: The SQL statement for getting information.
        :return: Will return the contents of the SQL in dataframe format.  If the failure does occur is should print
                 The reason for the failure and return a blank dataframe.
        '''
        try:
            df = pd_gbq.read_gbq(str(ssql), project_id=project, dialect='legacy',
                                 private_key=self.credentials)
        except Exception as e:
            try:
                df = pd_gbq.read_gbq(str(ssql), project_id=project, dialect='standard',
                                     private_key=self.credentials)
            except Exception as f:
                print('Legacy failed for the following Exception:', e)
                print('Standard failed for the following Exception:', f)
                df = pd.DataFrame()

        return df

    def gcp_bq_table_expiration_set(self, target_dataset, target_table, expiration_value_in_hours=24):
        '''
        This function is for setting a expiration timer on a BQ Table, once the timer is hit the table will self delete
        :param target_dataset: Dataset of where the table is located.
        :param target_table: Table name for the timer to be applied too.
        :param experation_value_in_hours: Number of hours that you wish the table to be present before a self delete
        :return: Default return of NONE.
        '''

        table_ref = self.bigquery_client.dataset(target_dataset).table(target_table)
        table = self.bigquery_client.get_table(table_ref)
        expiration = datetime.datetime.now() + datetime.timedelta(hours=expiration_value_in_hours)
        table.expires = expiration
        self.bigquery_client.update_table(table, ['expires'])
        return

    def gcp_bq_data_encoding(self, df_in):
        '''
        This function is to prepare the data for the uploading to a GCP Big Query Table.  Per documentation the
        GCP BQ Table can only support UTF-8 format
        :param df_in: This is the input dataframe to be converted to UTF-8
        :return: The dataframe with all target fields converted to UTF-8 except for time fields.
        '''

        def string_test(x):
            '''
            This function is to strip the "b '" that is prefixed in all string fields as a part of the converstion
            to UTF-8
            :param x: This is the value of each item for the apply.
            :return: Returns the scrubed data value that is uploaded into the appropriate cell location.
            '''

            if str(x)[-1] == "'":
                x_test = str(x)[:-1]
            else:
                x_test = str(x)

            if x_test[:2] == "b'":
                return x_test[2:]
            elif x_test[:2] == 'b"':
                return x_test[2:]
            else:
                return x_test

        def unicode_test(x):
            import urllib
            if ('\n' in str(x)) or ('\r' in str(x)):
                #   if isinstance(str(x), unicode):
                return urllib.quote(str(x))
            #    return x
            else:
                return x
            #    return urllib.quote(str(x))

        try:
            df_in['time_central'] = pd.to_datetime(df_in['time_central'])
            df_in['time_utc'] = pd.to_datetime(df_in['time_utc'])
        except:
            pass
        for col in df_in.columns:
            try:
                #    if 'time_' in col:
                if df_in[col].dtype.kind == 'M':
                    continue
            except:
                pass

            try:
                df_in[col] = df_in[col].astype(str)
            except:
                pass
            try:
                df_in[col] = df_in[col].str.encode('utf-8')
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(string_test)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(unicode_test)
            except:
                pass
        return df_in

    def gcp_bq_get_query_size(self, ssql):
        '''
        This function is used for estimating the size of data for the query.
        :param ssql: Input SQL statement.
        :return: Size in GB
        '''
        job_config = bigquery.job.QueryJobConfig()
        job_config.dry_run = True
        job = self.bigquery_client.query(ssql, job_config=job_config)
        return job.total_bytes_processed / self.BYTES_PER_GB

    def gcp_bq_query_nested_table(self, ssql):
        '''
        This query is for reading nested tables and non-nested tables
        :param ssql: Input SQL
        :return: Output dataframe
        '''

        job = self.bigquery_client.query(ssql)
        start_time = time.time()
        while not job.done():
            if (time.time() - start_time) > self.max_wait_seconds:
                print("Query Timed Out....SQL - ", ssql)
                self.bigquery_client.cancel_job(job.job_id)
                return None
            time.sleep(0.1)
        if job.total_bytes_billed:
            self.total_gb_used_net_cache += job.total_bytes_billed / self.BYTES_PER_GB
        return job.to_dataframe()

    def __parse_record_field(self, row_data, schema_details, top_level_name=''):
        '''
        This function is to parse each of the schema types from the schema string.
        :param row_data: Schema value
        :param schema_details: Used for parsing of the values
        :param top_level_name: Used for nested tables.
        :return: Returns a tuple of the values.
        '''
        name = row_data['name']
        if top_level_name != '':
            name = top_level_name + '.' + name
        schema_details.append([{
            'name': name,
            'type': row_data['type'],
            'mode': row_data['mode'],
            'fields': pd.np.nan,
            'description': row_data['description']
        }])
        if type(row_data.get('fields', 0.0)) == float:
            return None
        for entry in row_data['fields']:
            self.__parse_record_field(entry, schema_details, name)

    def __unpack_all_schema(self, schema):
        '''
        This function will parse the schema data into a Dataframe
        :param schema: Schema read from BQ table
        :return: Dataframe of results.
        '''
        schema_details = []
        schema.apply(lambda row:
                     self.__parse_record_field(row, schema_details), axis=1)
        result = pd.concat([pd.DataFrame.from_dict(x) for x in schema_details])
        result.reset_index(drop=True, inplace=True)
        del result['fields']
        return result

    def gcp_bq_get_table_schema(self, dataset, table):
        '''
        This function is for getting the table schema from any dataset.table combination.
        :param dataset: The dataset is the subgrouping of tables
        :param table: Table from which you want the schema
        :return: Schema in a dictionary format, and will return INVALID if unable to find table.
        '''

        dataset_id = dataset
        table_id = table
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            table = self.bigquery_client.get_table(table_ref)  # API request
            original_schema = table.schema
            # Unnest table column names.
            schema = pd.DataFrame.from_dict([x.to_api_repr() for x in original_schema])
            if 'fields' in schema.columns:
                schema = self.__unpack_all_schema(schema)
            schema = schema[['name', 'type', 'mode', 'description']]
            schema.columns = ['column', 'datatype', 'mode', 'description']
            return schema
        except:
            return 'INVALID'

    def gcp_bq_query_to_df_non_gbq(self, sql):
        results = self.bigquery_client.query(sql)
        return results.to_dataframe()

    def gcp_bq_linked_tabled_update_csv(self, dataset, table, uri='NA', data_mangement='WRITE_TRUNCATE',
                                        error_count_allowed=0):
        dataset_ref = self.bigquery_client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'

        job_config.schema = [
            bigquery.SchemaField('dataset', 'STRING'),
            bigquery.SchemaField('table', 'STRING'),
            bigquery.SchemaField('column', 'STRING'),
            bigquery.SchemaField('type', 'STRING'),
            bigquery.SchemaField('mapping', 'STRING'),
            bigquery.SchemaField('prev_mapping', 'STRING'),
            bigquery.SchemaField('possible_match', 'STRING')

        ]

        if data_mangement == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif data_mangement == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # job_config.source_format = bigquery.SourceFormat.CSV
        job_config.max_bad_records = 10000000
        load_job = self.bigquery_client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            location='US',  # Location must match that of the destination dataset.
            job_config=job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        destination_table = self.bigquery_client.get_table(dataset_ref.table(table))
        return destination_table.num_rows

    def gcp_bq_linked_tabled_update(self, dataset, table, uri='NA', data_mangement='WRITE_TRUNCATE',
                                    error_count_allowed=10000000):
        dataset_ref = self.bigquery_client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True

        if data_mangement == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif data_mangement == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.max_bad_records = error_count_allowed
        load_job = self.bigquery_client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            location='US',  # Location must match that of the destination dataset.
            job_config=job_config)  # API request
        # print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        destination_table = self.bigquery_client.get_table(dataset_ref.table(table))
        return destination_table.num_rows

    def gcp_bq_create_dateset(self, dataset):

        dataset_ref = self.bigquery_client.dataset(dataset)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        try:
            dataset = self.bigquery_client.create_dataset(dataset)
        except:
            pass
        return

    def gcp_bq_table_schema_builder(self, schema, new_col, datatype='STRING', null_base='NULLABLE'):
        '''
        This function will read the table schema and then append the new column that is required.  Also allow the
        programmer to send data type and null base as a factor as well.
        :param dataset: The dataset is the subgrouping of tables
        :param table: Table from which you want to change the schema
        :param new_col: New column name to be added to the table
        :param datatype: Data type of the new column.  By default it will be a string.
        :param null_base: Null type for either REPEATABLE or NULLABLE.  By default we want to use NULLABLE.
        :return: If the code is successful the code will return the new schema.  If the schema where to fail
                 to update the function will return FAILURE.
        '''

        try:
            schema.append(bigquery.SchemaField(new_col, datatype, null_base))
            return schema
        except:
            return 'FAILED'

    def gcp_bq_sql_schema_to_bq_schema(self, df_in, default_type='STRING'):
        sql_type_mapping = {
            'bigint': 'INTEGER',
            'int': 'INTEGER',
            'bit': 'BOOLEAN',
            'decimal': 'FLOAT',
            'varchar': 'STRING',
            'datetime2': 'TIMESTAMP',
            'varbinary': 'STRING'
        }

        schema = []

        df_in['schema_mapped'] = df_in['datatype'].map(sql_type_mapping)
        df_in['schema_mapped'] = df_in['schema_mapped'].fillna('STRING')
        for i in range(len(df_in)):
            schema.append(bigquery.SchemaField(str(df_in.loc[i, 'column']), df_in.loc[i, 'schema_mapped']))

        return schema

    def gcp_bq_linked_tabled_update_force_schema(self, dataset, table, df_in, uri='NA', data_mangement='WRITE_TRUNCATE',
                                                 error_count_allowed=0):
        if len(df_in) == 0:
            return 0

        # sql_type_mapping = {
        #     'bigint': 'INTEGER',
        #     'int': 'INTEGER',
        #     'int64': 'INTEGER',
        #     'i': 'INTEGER',
        #     'INTEGER': 'INTEGER',
        #     'bit': 'BOOLEAN',
        #     'BOOLEAN': 'BOOLEAN',
        #     'decimal': 'FLOAT',
        #     'FLOAT': 'FLOAT',
        #     'varchar': 'STRING',
        #     'STRING': 'STRING',
        #     'datetime2': 'TIMESTAMP',
        #     'TIMESTAMP': 'TIMESTAMP',
        #     'varbinary': 'STRING',
        #     'b': 'BOOLEAN',
        #     'f': 'FLOAT',
        #     'O': 'STRING',
        #     'S': 'STRING',
        #     'U': 'STRING',
        #     'M': 'TIMESTAMP'
        # }

        sql_type_mapping = {
            'bigint': 'FLOAT',
            'int': 'FLOAT',
            'int64': 'FLOAT',
            'i': 'FLOAT',
            'INTEGER': 'FLOAT',
            'bit': 'BOOLEAN',
            'BOOLEAN': 'BOOLEAN',
            'decimal': 'FLOAT',
            'FLOAT': 'FLOAT',
            'varchar': 'STRING',
            'STRING': 'STRING',
            'datetime2': 'TIMESTAMP',
            'datetime': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'varbinary': 'STRING',
            'b': 'BOOLEAN',
            'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        new_schema = []

        df_in['schema_mapped'] = df_in['datatype'].map(sql_type_mapping)
        df_in['schema_mapped'] = df_in['schema_mapped'].fillna('STRING')
        for i in range(len(df_in)):
            new_schema.append(bigquery.SchemaField(str(df_in.loc[i, 'column']), df_in.loc[i, 'schema_mapped']))

        # for i in range(len(df_in)):
        #     print(df_in.loc[i,'column'])
        #     print(df_in.loc[i,'datatype'])
        #     print(df_in.loc[i,'schema_mapped'])

        dataset_ref = self.bigquery_client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = new_schema

        if data_mangement == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif data_mangement == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.max_bad_records = error_count_allowed
        load_job = self.bigquery_client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            location='US',  # Location must match that of the destination dataset.
            job_config=job_config)  # API request
        # print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        destination_table = self.bigquery_client.get_table(dataset_ref.table(table))

        # table.schema = new_schema
        # table = client.update_table(table, ['schema'])

        # print('Table {table} Loaded {row_count} rows.'.format(table=destination_table,row_count = destination_table.num_rows))
        return destination_table.num_rows

    def gcp_bq_project_table_list(self):

        dataset_list = list(self.bigquery_client.list_datasets())
        tables_found = []
        table_schemas = pd.DataFrame()
        for dataset_item in dataset_list:
            dataset = self.bigquery_client.get_dataset(dataset_item.reference)
            value = str(dataset_item.reference)
            value = value.replace('DatasetReference(', '').replace("'", '').replace(' ', '').replace(')', '').split(',')
            tables_list = list(self.bigquery_client.list_tables(dataset))
            for table_item in tables_list:
                table = self.bigquery_client.get_table(table_item.reference)
                table_schema_single = self.gcp_bq_get_table_schema(value[1], table.table_id)
                table_value = table.table_id
                table_schema_single['table'] = table_value
                table_schema_single['dataset'] = value[1]
                if len(table_schemas) == 0:
                    table_schemas = table_schema_single.copy(deep=True)
                    continue
                table_schemas = pd.concat([table_schemas, table_schema_single])
                table_schemas.reset_index(drop=True, inplace=True)

        del table_schemas['mode'], table_schemas['description']
        table_schemas = table_schemas[['dataset', 'table', 'name', 'type']]
        table_schemas.columns = ['dataset', 'table', 'column', 'type']
        table_schemas.drop_duplicates(inplace=True)
        table_schemas.reset_index(drop=True, inplace=True)
        return table_schemas

    def gcp_bq_create_partition_table(self, df, target_dataset_name, target_table_name,
                                      data_type="STRING", mode="NULLABLE"):
        try:
            dataset_ref = self.bigquery_client.dataset(target_dataset_name)
            table_ref = dataset_ref.table(target_table_name)

            type_mapping = {
                'i': 'INTEGER',
                'b': 'BOOLEAN',
                'f': 'FLOAT',
                'O': 'STRING',
                'S': 'STRING',
                'U': 'STRING',
                'M': 'TIMESTAMP'
            }
            schema_list = []

            for column_name, dtype in df.dtypes.iteritems():
                schema_list.append(bigquery.SchemaField(column_name, type_mapping.get(dtype.kind, data_type), mode))

            table = bigquery.Table(table_ref, schema=schema_list)

            # creating partition table
            table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)

            self.bigquery_client.create_table(table)
            return True
        except Exception as e:
            if 'Already Exists' in str(e):
                return False
            print(e)
            return False


    def gcp_bq_table_exist(self,table):
        print('Checking if table exsits.....')
        try:
            table = self.bigquery_client.get_table(table)
            return 0
        except:
            return -1

    def gcp_bq_delete_table(self,dataset_name,table_name):
        print('Deleting table....')
        dataset_ref = self.bigquery_client.dataset(dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref)
        self.bigquery_client.delete_table(table)
        return

    def gcp_bq_create_table(self,dataset_name,table_name):
        print('Creating table....')
        dataset_ref = self.bigquery_client.dataset(dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref)
        self.bigquery_client.create_table(table)
        return

    def gcp_bq_table_present_check(self,dataset_name,table_name):
        dataset = self.bigquery_client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        try:
            self.bigquery_client.get_table(table_ref)
            return True
        except NotFound:
            return False