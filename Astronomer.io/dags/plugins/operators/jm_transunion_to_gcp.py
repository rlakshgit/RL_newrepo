# -*- coding: utf-8 -*-
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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from plugins.operators.jm_gcs import GCSHook
# from plugins.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from airflow.models import Variable

import os
import pandas as pd
import warnings
import numpy as np
import logging
import xmltodict
import sys
import io
import argparse
import datetime as dt
import urllib
import json
import re
from xml.etree import ElementTree


class TransunionToGoogleCloudStorageOperator(BaseOperator):
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 source_project,
                 target_project,
                 target_file_base,
                 target_schema_base,
                 target_metadata_base,
                 target_failed_archive_file_list,
                 target_archive_base,
                 source_gcs_conn_id,
                 target_gcs_conn_id,
                 source_bucket,
                 target_bucket,
                 target_dataset,
                 target_table,
                 target_prefix,
                 airflow_files_var_set,
                 airflow_present_files_var_set,
                 Full_load,
                 reprocess_history,
                 archive_project,
                 *args,
                 **kwargs):
        super(TransunionToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_project = source_project
        self.target_project = target_project
        self.source_file_list = []
        self.target_file_base = target_file_base
        self.target_schema_base = target_schema_base
        self.target_metadata_base = target_metadata_base
        self.target_failed_archive_file_list = target_failed_archive_file_list
        self.target_archive_base = target_archive_base
        self.source_gcs_conn_id = source_gcs_conn_id
        self.target_gcs_conn_id = target_gcs_conn_id
        self.source_gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.source_gcs_conn_id)
        self.target_gcp_storage_hook = GCSHook(google_cloud_storage_conn_id=self.target_gcs_conn_id)
        self.target_gcp_bq_hook = BigQueryHook(gcp_conn_id=self.target_gcs_conn_id)
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.target_dataset = target_dataset
        self.target_table = target_table
        self.airflow_files_var_set = airflow_files_var_set
        self.airflow_present_files_var_set = airflow_present_files_var_set
        self.overwrite_enable = False
        self.target_prefix = target_prefix
        self.source = 'transunion'
        self.Full_load = Full_load
        self.reprocess_history = True
        self.archive_project = archive_project

    def execute(self, context):
        pd.set_option('display.max_rows', None)
        # pd.set_option('display.max_columns', None)

        # if self.airflow_files_var_set != 'NA':
        #     # Get a list of files that are staged..
        #     file_list = self.source_gcp_storage_hook.list(self.source_bucket)
        #
        #     Variable.set(self.airflow_files_var_set, ','.join(x for x in file_list))
        #     return

        ##Put in file filter and then move forward.....
        # Three potential checks
        # 1. ensure it is an xml
        # 2. Ensure not in archive folder or l1
        # 3. Ensure not a duplicate
        # 4. Ensure DS matches archive folder (only valid if self.reprocess_history = TRUE
        skip_delta_process = False
        filtered_files = []
        files_to_process = []
        source_file_list = self.source_gcp_storage_hook.list(self.source_bucket)

        ######Transunion Full load ############
        if self.Full_load.lower() == 'true':

            logging.info("Full load ...processing all the files")
            for f in source_file_list:
                if '.xml' not in f:
                    continue
                if 'l1' not in f:
                    # print('Staging file for re-processing....', f)
                    filtered_files.append(f)
                    continue
            print(len(filtered_files))
            skipped_file_list = []
            for f in filtered_files:
                # print(f)
                count = 0
                for dup_check in filtered_files:
                    if dup_check in f:
                        count = count + 1
                if count > 1:
                    print('Duplicate {file} found....'.format(file = f))
                    skipped_file_list.append(f)
                    #print('Skipping file...')
                    continue
                files_to_process.append(f)
                continue
            print("*****************************************")
            print("Files that are skipped processing...")
            for i in skipped_file_list:
                print(i)
            print("*****************************************")



        else:
            logging.info("Delta Load ... ")
            ## source is transunion project
            ## Target is jm-dl-landing project
            logging.info("Getting list od files from source bucket - {source}".format(source=self.source_bucket))
            source_file_list = self.source_gcp_storage_hook.list(self.source_bucket)

            logging.info("Getting list od files from target bucket - {target} from transunion archive folder".format(
                target=self.target_bucket))
            target_file_list = self.target_gcp_storage_hook.list(self.target_bucket, prefix=self.target_prefix)

            logging.info(
                "Getting list od files from target bucket - {target} from transunion archive folder with run date prefix".format(
                    target=self.target_bucket))
            target_file_list_reprocess = self.target_gcp_storage_hook.list(self.target_bucket,
                                                                           prefix=self.target_prefix + '{date}/'.format(
                                                                               date=context['ds_nodash']))

            ####
            # Check and list files with .xml format from prod-transunion project. we don not want files saved in archive or l1 bucket in transunion project for delta load
            fileList_s = [f for f in source_file_list if '.xml' in f and 'l1' not in f and 'archive' not in f]

            # check and list files with .xml format from l1_archive/transunion bucket in jm-dl-landing project
            fileList_t = [fi for fi in target_file_list if '.xml' in fi and 'transunion' in fi]

            ####Logic to extract file name from  the full file path
            target_list = []
            logging.info("Extracting file name from file path for delta load comparison between source and target...")
            for l in fileList_t:
                list_2 = [val for val in l.split('/')]
                target_list.append(list_2[-1])

            # on rerun , instead of pulling files from source the logic below reprocess the data archived for the execution date
            if self.reprocess_history == True:
                logging.info(
                    "Reprocessing Archived files for the execution date - {date}".format(date=context['ds_nodash']))
                for f in target_file_list_reprocess:
                    if (context['ds_nodash'] in f):
                        logging.info('Staging file for re-processing....{f}'.format(f=f))
                        files_to_process.append(f)
                        self.source_bucket = self.target_bucket
                        logging.info("setting skip_delta_process_variable to True..")
                        skip_delta_process = True
                        continue

            ## Delta load logic
            # compare the difference between source files and target files and process only the files that are not in target_list
            if skip_delta_process == False:
                logging.info("Delta load new processing....")
                files_to_process = list(set(fileList_s) - set(target_list))
                logging.info(files_to_process)

        logging.info("File processing/parsing and uploading to target bucket starts ...")
        schema_list = []
        failed_files_list = []
        print("*************************************************************")
        logging.info("Number of files to process - {length}".format(length=len(files_to_process)))
        print("*************************************************************")
        for source_file in files_to_process:
            logging.info("Processing file {name}".format(name=source_file))

            ###Extracting file name from file path to upload to the target bucket
            f_temp = source_file.replace('.xml', '').replace('::', '')
            element_parts = [val for val in f_temp.split('/')]
            filename_parts = element_parts[-1]
            logging.info("File_name extracted from {file} is {result}".format(file=source_file, result=filename_parts))
            f_split = filename_parts.split('_')
            if len(f_split) > 3:
                transaction_id = '_'.join(f_split[0:len(f_split) - 3])
            else:
                transaction_id = '_'.join(f_split[0:len(f_split) - 1])

            if len(f_split) > 4:
                load_date = ':'.join(f_split[len(f_split) - 3:len(f_split)])

            else:
                load_date = f_split[len(f_split) - 1]

            try:
                load_date = pd.to_datetime(load_date)
            except:
                load_date = pd.to_datetime(context['ds'])

            results_df, archive_file, archive_variable = self._trans_xml_file_to_df(source_file, True,
                                                                                    self.source_bucket)

            results_df['filename'] = filename_parts + '.xml'
            results_df['transaction_ssid'] = transaction_id
            results_df['request_timestamp'] = load_date
            results_df['data_land_date'] = load_date
            results_df['time_central'] = load_date
            results_df['data_run_date'] = pd.to_datetime(context['ds'])

            json_data = results_df.to_json(orient='records', lines='\n')
            logging.info("uploading to l1_norm bucket ...")
            ### the following code executed only for reprocessing the data not for deltaload
            if skip_delta_process == True:
                logging.info("Reprocessed data upload to l1_norm bucket..")
                self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                                    object='{}.json'.format(
                                                        self.target_file_base.format(context['ds_nodash'],
                                                                                     filename_parts)),
                                                    data=results_df.to_json(orient='records', lines='\n',
                                                                            date_format='iso'))

                ###Archive and delete only if archive_variable = 1 and skip_delta_process = False. wont archive for rerun/reprocessing
            if archive_variable == 1 and skip_delta_process == False:
                logging.info("uploading to l1_norm bucket ...")
                self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                                    object='{}.json'.format(
                                                        self.target_file_base.format(context['ds_nodash'],
                                                                                     filename_parts)),
                                                    data=results_df.to_json(orient='records', lines='\n',
                                                                            date_format='iso'))
                self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                                    object='{}.xml'.format(
                                                        self.target_archive_base.format(context['ds_nodash'],
                                                                                        filename_parts)),
                                                    data=archive_file)
                #print(self.source_bucket)
                #print(files_to_process)
                self.source_gcp_storage_hook.delete(self.source_bucket, source_file)
                logging.info("Deleting file - {file}".format(file=source_file))

            if archive_variable == 0 and skip_delta_process == False:
                # else:
                logging.info("{File} not archived/uploaded to l1_norm bucket.skipping downstream process and continue with next file..".format(File=source_file))
                failed_files_list.append(source_file)
                continue

            # Generating schema from the dataframe
            fields = self._generate_schema(results_df)
            [schema_list.append(x) for x in fields if x not in schema_list]


            """if 'archive/' not in source_file:
                if self.archive_project == 'prod-edl' or self.archive_project == 'jm-dl-landing':
                    self.source_gcp_storage_hook.copy(self.source_bucket, source_file,
                                                      self.target_bucket, self.target_archive_base.format(context['ds_nodash'],source_file))"""

        ####

        print('Uploading schema file to the storage bucket...')
        self._upload_schema(schema_list, directory_date=context['ds_nodash'])

        print('Uploading metadata.....')

        Variable.set('transunion_archive_failed_files', failed_files_list)
        ###Upload of metadata a.k.a file_list as well as files failed to archive
        df_test = pd.DataFrame(files_to_process, columns=['files_list'])
        failed_files_df = pd.DataFrame(failed_files_list, columns=['Failed_files_list'])
        self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                            object=self.target_metadata_base.format(context['ds_nodash']),
                                            data=df_test.to_json(orient='records', lines='\n'))

        self.target_gcp_storage_hook.upload(bucket=self.target_bucket,
                                            object=self.target_failed_archive_file_list.format(context['ds_nodash']),
                                            data=failed_files_df.to_json(orient='records', lines='\n'))

        return

    def _generate_schema(self, data, default_type='STRING'):
        field = []
        type_mapping = {
            'i': 'INTEGER',
            'b': 'BOOLEAN',
            'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        data_columns = []
        for column_name, column_dtype in data.dtypes.iteritems():
            data_columns.append(column_name)
            field.append({'name': column_name,
                          'type': type_mapping.get(column_dtype.kind, default_type),
                          'mode': 'NULLABLE'})

        try:
            table_schema = \
            self.target_gcp_bq_hook.get_schema(self.target_dataset, self.target_table, self.target_project)['fields']
        except:
            table_schema = {}

        table_columns = []
        for row in table_schema:
            table_columns.append(row['name'])
            if row['name'] not in data_columns:
                field.append({'name': row['name'],
                              'type': row['type'],
                              'mode': 'NULLABLE'})

        return field

    def _upload_schema(self, file_schema, directory_date):
        schema = json.dumps(file_schema, sort_keys=True)
        schema = schema.encode('utf-8')

        self.target_gcp_storage_hook.upload(
            bucket=self.target_bucket,
            object=self.target_schema_base.format(directory_date),
            data=schema
        )
        return

    ####This function recursively parse the xml into a dataframe####
    ####returns dataframe, string of xml after removing SSN , archive_variable to decide whether to archive or not
    def _trans_xml_file_to_df(self, f, gcp_file=True, input_bucket='NA'):

        def parse_to_df(data, col_name):

            if (type(data).__name__ == 'list'):
                df = pd.DataFrame()
                i = 0
                for l in data:
                    df_sub = pd.DataFrame.from_dict(l, orient='index')

                    # New logic added to fix issue with parsing on column product_subject_subjectRecord_indicative_employment if it has only two columns
                    if len(df_sub) == 2 and len(
                            data) <= 2 and col_name == 'product_subject_subjectRecord_indicative_employment':
                        df_sub.reset_index(drop=False, inplace=True)
                        df_sub.columns = ['data_identifier', 'value']
                        df_sub['data_identifier'] = col_name + '_' + str(i) + '_' + df_sub['data_identifier']
                        df_sub['data_identifier'] = df_sub['data_identifier'].str.replace(
                            'product_subject_subjectRecord_', '')
                        df = pd.concat([df, df_sub])
                        i = i + 1
                        continue

                    if len(df_sub) > 2:
                        df_sub.reset_index(drop=False, inplace=True)
                        df_sub.set_index(['index'], inplace=True, drop=True)
                        df_sub = df_sub.transpose()
                        df = pd.concat([df, df_sub])
                        continue
                return df, 0

            try:
                df = pd.DataFrame.from_dict([data])
                df = df.transpose()
                df.reset_index(drop=False, inplace=True)
                if len(df.columns) == 3:
                    print("Length of column is 3")
                    df_sub1 = df[[df.columns[0], df.columns[1]]]
                    df_sub1['index'] = df_sub1['index'] + '_' + df.columns[1]
                    df_sub1.columns = ['data_identifier', 'value']
                    df_sub2 = df[[df.columns[0], df.columns[2]]]
                    df_sub2['index'] = df_sub2['index'] + '_' + df.columns[2]
                    df_sub2.columns = ['data_identifier', 'value']
                    df = pd.concat([df_sub1, df_sub2])
                    df['data_identifier'] = col_name + '_' + df['data_identifier']
                    return df

                df.columns = ['data_identifier', 'value']
                df['data_identifier'] = col_name + '_' + df['data_identifier']

            except:
                try:
                    df = pd.DataFrame.from_dict(data, orient='index')
                    df.reset_index(drop=False, inplace=True)
                    df.columns = ['data_identifier', 'value']
                    df['data_identifier'] = col_name + '_' + df['data_identifier']

                except:
                    return pd.DataFrame(), -1
            return df, 0

        if gcp_file:
            logging.info("Downloading file from storage -{file}".format(file=f))
            downloaded_file = self.source_gcp_storage_hook.download(input_bucket, f)
            file_stream = io.BufferedReader(io.BytesIO(downloaded_file))
            lines = file_stream.readlines()


        else:
            with open(f, 'r') as one_file:
                lines = one_file.readlines()

        line_string = ''

        for line in lines:
            line = line.decode('utf-8').strip()  # Python 3 has bytes type so this is needed to treat as string
            if '<?xml version="1.0" encoding="utf-8"?>' in line:
                line = line.replace('<?xml version="1.0" encoding="utf-8"?>', '')
            if '<?xml version="1.0" encoding="UTF-8"?>' in line:
                line = line.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
            if '<?xml version' in line:
                continue
            line_string = line_string + line

        ####Logic to remove Social security from the file string using regex functions #####
        line_string_archive = line_string
        try:
            logging.info("Removing PI...")
            line_string_archive = re.sub(r'<socialSecurity[\s\S].*<\/socialSecurity>', '', line_string_archive)
        except:
            logging.info("Issues removing PI..")

        ###second check to make sure that PI got removed###
        ###if PI did not get removed set the archive_variable to 0
        ###Archive_variable decides whether to archive the file or not
        ###If archive_variable = 0, PI did not removed so not archiving the file , archive_variable =1  PI removed so w ecan archive teh file
        z = re.findall('<socialSecurity', line_string_archive)
        if len(z) > 0:
            logging.info("PI field did not get removed...")
            archive_variable = 0
        else:
            archive_variable = 1
            # raise ValueError('Issues replacing the XML tag...')

        result = xmltodict.parse(line_string)
        result_df = pd.DataFrame.from_dict(result)

        result_df.reset_index(drop=False, inplace=True)
        result_df.columns = ['data_identifier', 'value']

        combined_result_df = pd.DataFrame()
        for i in range(len(result_df)):
            result, result_status = parse_to_df(result_df.loc[i, 'value'], result_df.loc[i, 'data_identifier'])
            combined_result_df = pd.concat([combined_result_df, result])
        combined_result_df.reset_index(drop=True, inplace=True)

        run_me = True
        round_counter = 0
        multiple_issues = 0
        while run_me:
            run_me = False

            for j in range(len(combined_result_df)):
                result, result_status = parse_to_df(combined_result_df.loc[j, 'value'],
                                                    combined_result_df.loc[j, 'data_identifier'])
                if result_status == 'value':
                    continue
                if result_status == -1:
                    continue

                if len(result.columns) > 2:
                    multiple_issues += 1
                    result.reset_index(drop=True, inplace=True)
                    result['key_index'] = combined_result_df.loc[j, 'data_identifier'] + '_' + result.index.astype(str)
                    result['key_index'] = result['key_index'].str.replace('product_subject_subjectRecord_', '')
                    result.set_index(['key_index'], inplace=True)
                    result = result.transpose()
                    result.reset_index(drop=False, inplace=True)
                    breakdown_df = pd.DataFrame()
                    for col in result.columns:
                        if col == 'index':
                            continue
                        tmp_df = pd.DataFrame()
                        tmp_df['data_identifier'] = result['index'].copy(deep=True)
                        tmp_df['data_identifier'] = col + '_' + tmp_df['data_identifier']
                        tmp_df['value'] = result[col].copy(deep=True)
                        breakdown_df = pd.concat([breakdown_df, tmp_df])
                    result = pd.DataFrame()
                    result = breakdown_df.copy(deep=True)

                if result_status == 0:
                    combined_result_df = combined_result_df.drop([j])
                    run_me = True

                if len(result) == 0:
                    logging.info("Length is 0 ... so continuing ...................")
                    continue

                combined_result_df = pd.concat([combined_result_df, result])
                combined_result_df.reset_index(drop=True, inplace=True)
                round_counter += 1

        combined_result_df.set_index(['data_identifier'], inplace=True)

        combined_result_df = combined_result_df.transpose()

        combined_result_df.reset_index(drop=False, inplace=True)
        new_col = []
        for col in combined_result_df.columns:
            new_col.append(col.replace('@', '').replace('#', ''))
        combined_result_df.columns = new_col

        ####Logic to remove SSN from dataframe in case if it is missed with regex replace

        if 'product_subject_subjectRecord_indicative_socialSecurity_number' in combined_result_df.columns:
            logging.info("SSN info present")
            combined_result_df.drop('product_subject_subjectRecord_indicative_socialSecurity_number', axis=1,
                                    inplace=True)
        else:
            logging.info("SSN info not present")

        return combined_result_df, line_string_archive, archive_variable

