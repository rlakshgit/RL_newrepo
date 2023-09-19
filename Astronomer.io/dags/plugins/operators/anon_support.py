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

# imports section
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
pd.options.mode.chained_assignment = None
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_anon_hook import AnonHook
from plugins.hooks.jm_bq_hook import BigQueryHook
import io
from airflow.models import Variable
import json
from airflow import macros
from pandas.io.json import json_normalize
import numpy as np
import datetime

class anon_support_concurrent(BaseOperator):
    @apply_defaults
    def __init__(self,
                 anon_project_id,
                 landing_project_id,
                 DEV_project_id, 
                 QA_project_id,
                 storage_type,
                 from_env,
                 to_env,
                 from_dataset,
                 to_dataset,
                 anon,
                 load_frequency,
                 load_type,
                 keyword,
                 days,
                 enabled,
                 anon_mapping_file,
                 *args,
                 **kwargs
                 ):

        super(anon_support_concurrent, self).__init__(*args, **kwargs)
        self.anon_project_id = anon_project_id
        self.landing_project_id = landing_project_id
        self.DEV_project_id = DEV_project_id
        self.QA_project_id = QA_project_id
        self.dialect = 'standard'
        self.delegate_to = None
        self.anon_mapping_bucket = 'jm_anonymization_lookup_file'
        self.storage_type=storage_type
        self.from_env=from_env
        self.to_env=to_env
        self.from_dataset=from_dataset
        self.to_dataset=to_dataset
        self.anon=anon
        self.load_frequency=load_frequency 
        self.load_type=load_type
        self.keyword=keyword
        self.days=days
        self.enabled=enabled
        self.anon_mapping_file=anon_mapping_file
        
    def execute(self, context):

        landing_hook = GCSHook(google_cloud_storage_conn_id=self.landing_project_id,
                                              delegate_to=self.delegate_to)
        anon_hook = AnonHook(anon_cloud_storage_conn_id=self.anon_project_id,
                             delegate_to=self.delegate_to)

        dev_bq_hook =  BigQueryHook(self.DEV_project_id)
        qa_bq_hook = BigQueryHook(self.QA_project_id)
        lnd_bq_hook = BigQueryHook(self.landing_project_id)

        # Determine which hook to use based on the specified environment for this entry
        if self.to_env == "qa-edl":
            hook = qa_bq_hook
            project = 'qa-edl'
        else:
            hook = dev_bq_hook
            project = 'dev-edl'
            
        logging.info ("Connections to GCP projects created.")

        # PARTITION DATE = DAG EXECUTION DATE
        partition_date = context['ds']
        PARTITION_DATE = macros.ds_format( partition_date, "%Y-%m-%d", "%m/%d/%Y")

        # Get list of tables from the dataset .. Assuming source is always prod
        logging.info("Getting list of tables from the dataset...")
        list = lnd_bq_hook.get_dataset_tables_list(project_id='prod-edl', dataset_id=self.from_dataset)
        df = pd.DataFrame(list)
        table_list = df['tableId'].tolist()
        logging.info("Gathered list of tables.")
        print(table_list)

        # Get Mapping Data into a Dataframe
        mapping_stream =landing_hook.download(bucket=self.anon_mapping_bucket, object=self.anon_mapping_file)
        logging.info("Mapping file downloaded.")
        mapping_data = io.BufferedReader(io.BytesIO(mapping_stream))
        mapping_df = pd.read_csv(mapping_data)
        logging.info("Completed reading the mapping data into a dataframe.")

        # For each table in the dataset...
        for tables in table_list:
            logging.info("Constructing dynamic sql...")
            if self.keyword == 'Rolling':
                end_date=partition_date
                self.days=self.days * -1
                start_date=macros.ds_add(partition_date, self.days)
                logging.info("start_date is {start_date}".format(start_date=start_date))
                logging.info("end_date is {end_date}".format(end_date=end_date))

                # Generate list of dates for rolling n days based on DAG execution date
                generate_dates_sql = '''SELECT GENERATE_DATE_ARRAY('{start_date}', '{end_date}', INTERVAL 1 DAY) AS date_array'''.format(start_date=start_date , end_date=end_date)
                df = hook.get_pandas_df(sql=generate_dates_sql, dialect=self.dialect)
                date_list = df.get_value(0,"date_array")
                print("_______________________")
                print(date_list)
                print("_______________________")

                # if table exist get distinct partition dates from the bq table as a list
                # check the list of dates to run query are already present in the bq table by comparing the two lists
                # Run only for dates which is not present int the BQ table (Incremental load)

                sql = '''SELECT ARRAY(
                           SELECT date(_PARTITIONTIME) as pt 
                           FROM `{project}.{dataset}.{table}` 
                           GROUP BY _PARTITIONTIME ORDER BY _PARTITIONTIME
                         ) as pt_array'''.format(project=project,
                                                 dataset=self.to_dataset, 
                                                 table=tables)

                try:
                    current_partitions = hook.get_pandas_df(sql=sql, dialect=self.dialect)
                    partition_list = current_partitions.get_value(0, "pt_array")
                except:
                    partition_list = []
                    
                if len(partition_list) > 0:
                    # Delete the previous date load from bq table just to keep rolling 90 days of data
                    date_list_delete = set(partition_list) - set(date_list)
                    print("***********DELETING DATES***************")
                    print(date_list_delete)
                    print("**************************")
                    if date_list_delete:
                        for date in date_list_delete:
                            delete_previous_day_sql = """DELETE from `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME)='{Date}'""".format(
                                project=project,
                                dataset=self.to_dataset,
                                table=tables,
                                Date=date)
                            print(delete_previous_day_sql)
                            hook.run_query(sql=delete_previous_day_sql, use_legacy_sql=False)
                            logging.info("Partition deleted fron BQ table - {date}".format(date = date))
                            
                date_list = set(date_list) -  set(partition_list)
                print("**********ADDING DATES***************")
                print(date_list)
                print("*************************")
                if not date_list:
                    logging.info("Data present for the load dates")
                    return

                # For each date, pull the data from prod, scrub it, and put it in the anonymized dataset/table by partition
                for one_date in date_list:
                    logging.info("Loading the data for {date}".format(date=one_date))
                    sql = ''' select * from `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{SD}" '''.format(
                                                                                                project='prod-edl',
                                                                                                dataset=self.from_dataset,
                                                                                                table=tables,
                                                                                                SD=one_date)
                    print(sql)
                    logging.info("Got the source data in a dataframe")
                    df = lnd_bq_hook.get_pandas_df(sql=sql, dialect=self.dialect)
                    
                    if(len(df) == 0):
                        logging.info("No data available for partition - {partition_date}".format(partition_date=partition_date))
                        continue

                    # Call anon_data function which anonymize the data
                    logging.info("Scrubbing the data...")
                    df_new, anon_return = anon_hook.anon_data(df_in=df, anon_mapping=mapping_df)
                    logging.info("Data anonymization completed")

                    # Get table schema from the prod source
                    schema =lnd_bq_hook.get_schema(project_id='prod-edl', dataset_id=self.from_dataset, table_id=tables)
                    schema_fields = schema['fields']

                    # If the anonymized table doesn't exist yet, create it as partition table with the schema
                    try:
                        hook.create_empty_table(
                            project_id=project,
                            dataset_id=self.to_dataset,
                            table_id=tables,
                            schema_fields=schema_fields,
                            time_partitioning={'type': 'DAY'})
                    except:
                        logging.info("Table {table} already exists.".format(table=tables))

                    # Write the scrubbed data to the partition
                    hook.gcp_bq_write_table(
                        df_in=df_new,
                        project=project,
                        dataset=self.to_dataset,
                        table_name='{table}${partition_date}'.format(table=tables,
                                                                     partition_date=str(one_date).replace('-','')),
                        control='APPEND',
                        schema_enable=True,
                        schema=schema_fields)


                # Audit - This is across all dates for this table.
                # Getting source count
                sql_prod_count = """select count(*) as prod_count FROM `{project}.{to_dataset}.{tables}` 
                                    WHERE DATE(_PARTITIONTIME) >= "{start_date}" and  DATE(_PARTITIONTIME) <= "{end_date}" """.format(project = 'prod-edl',
                                                                                                                                      to_dataset = self.from_dataset,
                                                                                                                                      tables = tables,
                                                                                                                                      start_date = start_date,
                                                                                                                                      end_date = end_date)

                prod_count_df = lnd_bq_hook.get_pandas_df(sql=sql_prod_count, dialect=self.dialect)
                source_count = prod_count_df.get_value(0,"prod_count")

                # Getting Destination count

                sql_lower_env_count = """select count(*) as destination_count FROM `{project}.{to_dataset}.{tables}` 
                                         WHERE DATE(_PARTITIONTIME) >= "{start_date}" 
                                         and DATE(_PARTITIONTIME) <= "{end_date}" """.format(project=project,
                                                                                             to_dataset=self.to_dataset,
                                                                                             tables=tables,
                                                                                             start_date=start_date,
                                                                                             end_date=end_date)

                destination_count_df = hook.get_pandas_df(sql=sql_lower_env_count, dialect=self.dialect)
                destination_count = destination_count_df.get_value(0, "destination_count")

            if source_count == destination_count:
                logging.info("Audit passed")
            else:
                raise("Audit Failed.  Count does not match.")




class anon_support(BaseOperator):
    @apply_defaults
    def __init__(self,
                 anon_project_id,
                 landing_project_id,
                 DEV_project_id,
                 QA_project_id,
                 *args,
                 **kwargs
                 ):

        super(anon_support, self).__init__(*args, **kwargs)
        self.anon_project_id = anon_project_id
        self.landing_project_id = landing_project_id
        self.DEV_project_id = DEV_project_id
        self.QA_project_id = QA_project_id
        self.dialect = 'standard'
        self.delegate_to = None
        self.anon_mapping_bucket = 'jm_anonymization_lookup_file'

    def execute(self, context):


        landing_hook = GCSHook(
            google_cloud_storage_conn_id=self.landing_project_id,
            delegate_to=self.delegate_to)
        anon_hook = AnonHook(
            anon_cloud_storage_conn_id=self.anon_project_id,
            delegate_to=self.delegate_to)

        dev_bq_hook =  BigQueryHook(self.DEV_project_id)
        qa_bq_hook = BigQueryHook(self.QA_project_id)
        lnd_bq_hook = BigQueryHook(self.landing_project_id)

        logging.info ("Connections to GCP projects created")

        SOURCE_LIST = Variable.get('data requirement', default_var='{"Default":{"storageType":"File"}}')
        json_data = json.loads(SOURCE_LIST)
        sources = json_data.keys()

        for source in sources:
            logging.info("pulling data from source - {source}".format(source = source))
            storage_type = json_data[source]["storageType"]
            from_env = json_data[source]["fromEnv"]
            to_env = json_data[source]["toEnv"]
            from_dataset = json_data[source]["fromDataSet"]
            to_dataset = json_data[source]["toDataSet"]
            anon =  json_data[source]["anon"]
            load_frequency = json_data[source]["loadFreq"]
            load_type = json_data[source]["loadType"]
            keyword = json_data[source]["Keyword"]
            days = json_data [source]["NumberOfDays"]
            #anon_mapping_file = '{source}_anon_mapping_table.csv'.format(source = source)
            anon_mapping_file = 'Mixpanel_People_anon_mapping_table.csv'
            logging.info("Meta data gethered from source..")

            # PARTITION DATE = DAG EXECUTION DATE
            partition_date = context['ds']
            PARTITION_DATE = macros.ds_format( partition_date, "%Y-%m-%d", "%m/%d/%Y")

            #Get list of tables from the dataset .. Assuming source is always prod
            logging.info("Getting list of tables from the dataset")
            list = lnd_bq_hook.get_dataset_tables_list(project_id='prod-edl', dataset_id=from_dataset)
            df = pd.DataFrame(list)
            table_list = df['tableId'].tolist()
            logging.info("Gathered lis of tables..")
            print(table_list)

            # Get Mapping Data into a Dataframe
            mapping_stream =landing_hook.download(bucket=self.anon_mapping_bucket, object=anon_mapping_file)
            logging.info("Mapping file downloaded")
            mapping_data = io.BufferedReader(io.BytesIO(mapping_stream))
            mapping_df = pd.read_csv(mapping_data)
            logging.info("Completed Reading the Mapping data into a dataframe")

            #creating dataset
            if to_env == "qa-edl":
                hook = qa_bq_hook
                project = 'qa-edl'
            else:
                hook = dev_bq_hook
                project = 'dev-edl'
            try:
                hook.create_empty_dataset(project_id=project, dataset_id=to_dataset)
            except Exception as e:
                if 'Already Exists' in str(e):
                    logging.info('Dataset already present!')
                else:
                    print(e)
                    raise


            for tables in table_list:
                logging.info("constructing dynamic sql ...")
                if keyword == 'Rolling':
                    end_date =  partition_date
                    days = days * -1
                    start_date =macros.ds_add(partition_date, days)
                    logging.info("start_date is {start_date}".format(start_date = start_date))
                    logging.info("end_date is {end_date}".format(end_date = end_date))
                    #Generate list of dates for rolling n days based on DAG execution date
                    generate_dates_sql = '''SELECT GENERATE_DATE_ARRAY('{start_date}', '{end_date}', INTERVAL 1 DAY) AS date_array'''.format(start_date = start_date , end_date = end_date)
                    df = hook.get_pandas_df(sql=generate_dates_sql, dialect=self.dialect)
                    date_list = df.get_value(0,"date_array")
                    print("_______________________")
                    print(date_list)
                    print("_______________________")

                    #check if table exists
                    # if table exist get distinct run date from the bq table as a list
                    # check the list of dates to run query are already present in the bq table by comparing the two lists
                    #Run only for dates which is not present int the BQ table (Incremental load)
                    TABLE_EXIST_CHECK = hook.table_exists(project_id = project,dataset_id = to_dataset,table_id = tables)
                    if TABLE_EXIST_CHECK == True:
                        get_run_date_sql = """SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 DAY) AS date_range
                                            FROM (
                                                    SELECT Min(date(cast(partition_date as TIMESTAMP))) as date_start,MAX(date(cast(partition_date as TIMESTAMP))) as date_end FROM `{project}.{to_dataset}.{tables}`
                                                  ) AS items""".format(project = project, to_dataset = to_dataset,tables = tables)
                        run_date_df = hook.get_pandas_df(sql=get_run_date_sql, dialect=self.dialect)
                        run_dates_list = run_date_df.get_value(0,"date_range")
                        print("************************")
                        print(run_dates_list)
                        print("************************")
                        #Delete the previous date load from bq table just to keep rolling 90 days of data
                        date_list_delete = set(run_dates_list) - set(date_list)
                        print("**************************")
                        print(date_list_delete)
                        print("**************************")
                        if date_list_delete:
                            for date in date_list_delete:
                                delete_previous_day_sql = """ DELETE  from  `{project}.{dataset}.{table}` WHERE date(cast(time_central as TIMESTAMP)) ='{Date}'""".format(
                                    project=project,
                                    dataset=to_dataset,
                                    table=tables,
                                    Date=date)
                                print(delete_previous_day_sql)
                                hook.run_query(sql=delete_previous_day_sql, use_legacy_sql=False)
                                logging.info("Partition deleted fron BQ table - {date}".format(date = date))
                        date_list = set(date_list) -  set(run_dates_list)
                        print("*************************")
                        print(date_list)
                        print("*************************")
                        if not date_list:
                            logging.info("Data present for the load dates")
                            return

                    for date in date_list:
                        logging.info("Loading the data for {date}".format(date = date))
                        sql = ''' select *, cast(DATE(_PARTITIONTIME) as TIMESTAMP) as partition_date from `{project}.{dataset}.{table}` WHERE DATE(_PARTITIONTIME) = "{SD}" '''.format(
                                                                                                    project = 'prod-edl',
                                                                                                    dataset = from_dataset,
                                                                                                    table = tables,
                                                                                                    SD = date)
                        print(sql)
                        logging.info("Got the SQL output as a Dataframe")
                        df = lnd_bq_hook.get_pandas_df(sql=sql, dialect=self.dialect)
                        print(len(df))
                        if(len(df) == 0):
                            logging.info("No data available for partition - {partition_date}".format(partition_date = partition_date))
                            continue

                    # call anon_data function which anonymize the data
                        logging.info("calling the anon function")
                        df_new, anon_return = anon_hook.anon_data(df_in=df, anon_mapping=mapping_df)


                        logging.info("Data anonymization completed")


                        #Get table schema
                        schema =lnd_bq_hook.get_schema(project_id='prod-edl', dataset_id=from_dataset,
                                                         table_id=tables)
                        print(schema)
                        schema_fields = schema['fields']
                        print(type(schema_fields))
                        schema_fields.append({'name': 'partition_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'})
                        print(schema_fields)

                        hook.gcp_bq_write_table(
                            df_in=df_new,
                            project=project,
                            dataset=to_dataset,
                            table_name=tables,
                            control='APPEND',
                            schema_enable=True,
                            schema=schema_fields)


                    #Audit
                    # Getting source count
                    sql_prod_count = """ select count(*) as prod_count FROM `{project}.{to_dataset}.{tables}` WHERE DATE(_PARTITIONTIME) >= "{start_date}" and  DATE(_PARTITIONTIME) <= "{end_date}" """.format(
                                                                                                                                                                        project = 'prod-edl',
                                                                                                                                                                        to_dataset = from_dataset,
                                                                                                                                                                        tables = tables,
                                                                                                                                                                        start_date = start_date,
                                                                                                                                                                        end_date = end_date)

                    prod_count_df = lnd_bq_hook.get_pandas_df(sql=sql_prod_count, dialect=self.dialect)
                    source_count = prod_count_df.get_value(0,"prod_count")


                    # Getting Destination count


                    sql_lower_env_count = """ select count(*) as destination_count FROM `{project}.{to_dataset}.{tables}` WHERE DATE(cast(partition_date as TIMESTAMP)) >= "{start_date}" and  DATE(cast(partition_date as TIMESTAMP)) <= "{end_date}" """.format(
                                                                                                                                                                            project=project,
                                                                                                                                                                            to_dataset=to_dataset,
                                                                                                                                                                            tables=tables,
                                                                                                                                                                            start_date=start_date,
                                                                                                                                                                            end_date=end_date)

                    destination_count_df = hook.get_pandas_df(sql=sql_lower_env_count, dialect=self.dialect)
                    destination_count = destination_count_df.get_value(0, "destination_count")


                if source_count == destination_count:
                    logging.info("Audit passed")
                else:
                    raise("Audit Failed. count does not match")










