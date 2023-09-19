from plugins.hooks.jm_salesforce import SalesforceHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
from airflow.models import Variable
from flatten_json import flatten
from pandas.io.json import json_normalize
from plugins.hooks.jm_bq_hook import BigQueryHook
import datetime as dt
import logging
import json
import pandas as pd
import numpy as np
import io
import os
from common.dq_common import dq_common
from simple_salesforce import Salesforce
pd.options.mode.chained_assignment = None
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 10)


class SalesforceToGCSOperator(BaseOperator):
    """
    Make a query against Salesforce and
    write the resulting data to a file.
    """
    #template_fields = ("query", )

    @apply_defaults
    def __init__(
            self,
            conn_id,
            dest_bucket,
            refine_table='NA',
            bq_project='NA',
            history_check='NA',
            google_cloud_storage_conn_id='google_cloud_default',
            google_bq_conn_id='google_cloud_default',
            refine_dataset='ref_salesforce',
            base_location='None',
            base_norm_location='l1/',
            base_schema_location='l1_schema',
            obj='NA',
            delegate_to=None,
            airflow_var_set='NA',
            sf_connection=None,
            *args,
            **kwargs
    ):
        """
        Initialize the operator
        :param conn_id:             name of the Airflow connection that has
                                    your Salesforce username, password and
                                    orgID
        :param obj:                 name of the Salesforce object we are
                                    fetching data from
        :param file_location:              name of the file where the results
                                    should be saved
        :param airflow_var_set:     *(optional)* airflow variable that has salesforce objects
        :param         dest_bucket:    GCS bucket
        :param         history_check:    Boolean Value, will process from l1 if set to true else pulls from source
        :param         google_cloud_storage_conn_id:    google cloud connection
        :param         base_location:    l1 file location
        :param         base_norm_location:    l1_norm file location
        :param         base_schema_location:    schema file location
        """

        super(SalesforceToGCSOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.base_location = base_location
        self.airflow_var_set = airflow_var_set
        self.object = obj
        self.base_norm_location = base_norm_location
        self.base_schema_location = base_schema_location
        self.delegate_to = delegate_to
        self.gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        self.hook = SalesforceHook(conn_id=self.conn_id)
        # self.hook = sf_connection
        self.source_count = 0
        self.dest_bucket = dest_bucket
        self.history_check = history_check
        self.refine_table = refine_table
        self.refine_dataset = refine_dataset
        self.google_bq_conn_id = google_bq_conn_id
        self.bq_hook = BigQueryHook(
            gcp_conn_id=self.google_bq_conn_id,
            delegate_to=self.delegate_to)
        self.bq_project = bq_project

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file in GCS.
        """
        logging.info("Prepping to gather data from Salesforce")

        if self.airflow_var_set != 'NA':
            description = self.hook.describe_all()
            #print(description)
            names = [obj['name'] for obj in description['sobjects'] if obj['queryable']]
            names = ', '.join([x for x in names])

            Variable.set(self.airflow_var_set, names)
            return

        logging.info('fetching data for:' + self.object)

        target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])

        if (self.history_check.upper() == 'TRUE') and (self.gcs_hook.exists(self.dest_bucket, target_file_name)):
            logging.info('File already landed.........')
            logging.info('Processing the following file: ' + target_file_name)
            self._process_from_l1_gcs(context, target_file_name)
            return 0

        try:
            # get object from salesforce
            fieldNames = self.hook.get_available_fields(self.object)
        except:
            print('Retrying the connection to SalesForce')
            self.hook = SalesforceHook(conn_id=self.conn_id)
            fieldNames = self.hook.get_available_fields(self.object)

        obj_describe = self.hook.describe_object(self.object)
        start_timestamp = context['ds'] + 'T00:00:00Z'
        #start_timestamp = '2020-01-01' + 'T00:00:00Z' # Remove after testing
        end_timestamp = context['next_ds'] + 'T00:00:00Z'
        logging.info("Making request for {0} fields from {1}".format(len(fieldNames), self.object))

        file_path = os.path.join('/home/airflow/gcs/dags',
                                 r'ini/salesforce_additional_info_tables.json')

        with open(file_path, 'rb') as inputJSON:
            confJson = json.load(inputJSON)
        logging.info("Successfully read configuration file for additional input tables list")


        for item in confJson:
            if self.object == item['objectName']:
                source_field = item['source_field']
                source_table = item['source_table']
                link_field=item['link_field']
                source_ids = pd.DataFrame(self.hook.make_query(
                    "SELECT {source_field} FROM {source_table} WHERE LastModifiedDate >= {start_timestamp} AND LastModifiedDate< {end_timestamp} ".format(
                        source_field=source_field, source_table=source_table, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))['records'])
                source_dict = source_ids.to_dict('records')
                source_ids = pd.DataFrame((flatten(d) for d in source_dict))
                results = []
                datefield = '_ALL_'
                if not source_ids.empty:
                    for one_id in source_ids[source_field].unique():
                        one_results = self.hook.make_query(
                            "SELECT {field_list} FROM {link_table} WHERE {link_field} = '{one_id}' ".format(
                                field_list=", ".join(fieldNames), link_table=self.object, link_field=link_field,
                                one_id=one_id))
                        #self.source_count = self.source_count + one_results['totalSize']
                        one_results = one_results['records']
                        results.append(one_results)

                    self.source_count = len(results)
                    results = pd.DataFrame(results)

                else:
                    results = pd.DataFrame()

                json_data = results.to_json(orient='records', lines='\n', date_format='iso')
                target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
                    base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
                logging.info("Writing query results to: {0}".format(target_file_name))
                self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
                self._normalize_data(context, results, self.object, datefield)
                return


        try:
            if 'LastModifiedDate' in fieldNames:
                results = self.hook.make_query(
                    "SELECT {source_field} FROM {source_table} WHERE LastModifiedDate >= {start_timestamp} AND LastModifiedDate< {end_timestamp} ".format(
                        source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))
                #self.source_count = results['totalSize']
                results = results['records']
                self.source_count = len(results)
                datefield = 'LastModifiedDate'

            elif 'CreatedDate' in fieldNames:
                results = self.hook.make_query(
                    "SELECT {source_field} FROM {source_table} WHERE CreatedDate >= {start_timestamp} AND CreatedDate< {end_timestamp} ".format(
                        source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))
                #self.source_count = results['totalSize']
                results = results['records']
                self.source_count = len(results)
                datefield = 'CreatedDate'

            else:
                results = self.hook.make_query("SELECT {source_field} FROM {source_table} ".format(
                    source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                    end_timestamp=str(end_timestamp)))
                #print(results)
                #self.source_count = results['totalSize']
                results = results['records']
                self.source_count = len(results)
                datefield = '_ALL_'
        except:
            results = pd.DataFrame()
            datefield = '_ALL_'
            logging.info(' {obj} table could not be queried without additional input (like a targeted ID)'.format(obj=self.object))
            try:
                additional_input_tables = Variable.get('salesforce_misbehaving_tables')
                if self.object not in additional_input_tables:
                    #additional_input_tables.append(self.object + ',')
                    additional_input_tables = additional_input_tables + self.object + ','
                Variable.set('salesforce_misbehaving_tables', additional_input_tables)
            except:
                Variable.set('salesforce_misbehaving_tables',self.object + ',')


        results = pd.DataFrame(results)
        json_data = results.to_json(orient='records', lines='\n',date_format='iso')
        target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
        logging.info("Writing query results to: {0}".format(target_file_name))
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
        self._normalize_data(context,results,self.object,datefield)

    def _normalize_data(self, context, df, obj, datefield):
        dq = dq_common()
        logging.info('obj is ' + self.object)
        current_date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if len(df) == 0:
            logging.info('No data to process.....')
            json_data = df.to_json(orient='records', lines='\n', date_format='iso')
            target_file_name = '{base_norm_location}{obj}/{extract_date}/l1_norm_sf_{obj}.json'.format(
                base_norm_location=self.base_norm_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
            self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
            self._upload_schema_to_gcs(obj, context, df)
            self._upload_metadata(context, df)
            return

        df_dict = df.to_dict('records')
        df = pd.DataFrame((flatten(d) for d in df_dict))
        df = dq.dq_column_group_cleanup(df)
        df = self._standardize_datatypes(df)

        date_target = context['ds']
        extract_date = context['ds_nodash']
        msg = 'Processing data for date ->' + str(date_target)
        logging.info(msg)

        #remove the additional timefields
        df['data_extract_date'] = pd.to_datetime(date_target).strftime("%Y-%m-%d %H:%M:%S")
        df['data_run_date'] = current_date
        file_date_time = pd.to_datetime(extract_date, format='%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
        df['data_land_date'] = file_date_time
        df['data_land_date'] = pd.to_datetime(df['data_land_date'])
        df['data_run_date'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['data_run_date'] = pd.to_datetime(df['data_run_date'])
        df['time_central'] = df['data_land_date'].copy(deep=True)
        df['time_central'] = pd.to_datetime(df['data_land_date'])

        new_cols = []
        for col in df.columns:
            new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))
        df.columns = new_cols
        df.reset_index(drop=True, inplace=True)

        time_fields = ['_at', '_time', '_created', '_date', 'created', 'At', 'updated', 'Date', 'stamp', '_Date__c']

        date_fields = []
        for col in df.columns:
            if col == 'IsUpToDate':
                continue
            if any(str(col).endswith(x) for x in time_fields):
                date_fields.append(col)



        def time_test(x):
            if 'T' in str(x):
                return pd.to_datetime(x, errors='coerce')

            if (':' not in str(x)) and ('-' not in str(x)) and ('/' not in str(x)):
                return pd.to_datetime('1970-01-01')

            try:
                x_temp = pd.to_datetime(x, unit='ms')
            except:
                x_temp = pd.to_datetime(x, errors='ignore')
            return x_temp

        for col in df.columns:
            if col in date_fields:
                df[col] = df[col].apply(time_test)




        if datefield != '_ALL_':
            df[datefield] = pd.to_datetime(df[datefield])

        table_presence = self.bq_hook.table_exists(project_id=self.bq_project,
                                                   dataset_id=self.refine_dataset,
                                                   table_id=self.refine_table)
        if table_presence:
            logging.info('getting old schema')
            old_cols = self.bq_hook.get_schema(project_id=self.bq_project,
                                               dataset_id=self.refine_dataset,
                                               table_id=self.refine_table)

            old_cols = json_normalize(data=old_cols['fields'])
        else:
            old_cols = pd.DataFrame()

        if len(old_cols) > 0:
            for col in old_cols.name.values:
                if old_cols.loc[old_cols['name'] == col, 'type'].iloc[0].lower() == 'string':
                    df[col] = df[col].astype(str)

        json_data = df.to_json(orient='records', lines='\n',date_format='iso')
        target_file_name = '{base_norm_location}{obj}/{extract_date}/l1_norm_sf_{obj}.json'.format(
            base_norm_location=self.base_norm_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
        self._upload_schema_to_gcs(obj,context,df)
        self._upload_metadata(context,df)

        return

    def _upload_metadata(self, context, df):


        audit_df = pd.DataFrame()

        target_file_name = '{base_location}{obj}/{extract_date}/l1_metadata_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])

        file_exists = self.gcs_hook.exists(self.dest_bucket, target_file_name)
        logging.info('file_exists value is ' + str(file_exists))

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': self.source_count, 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)

        del json_data
        del audit_df

        return

    def _upload_schema_to_gcs(self, obj, context,df):

        schema_data = self._schema_generator(df, context)

        target_file_name = '{base_schema_location}{obj}/{extract_date}/l1_schema_sf_{obj}.json'.format(
            base_schema_location=self.base_schema_location, obj=obj.lower(), extract_date=context['ds_nodash'])

        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=schema_data)

        return

    def _schema_generator(self, df, context, default_type='STRING'):

        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():

            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return_string = "[{fields}]".format(fields=",".join(fields))
        return json.dumps(fields)

    def _process_from_l1_gcs(self, context, target_file):

        file_data = self.gcs_hook.download(self.dest_bucket, target_file)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        self.source_count = len(df)
        fieldNames = list(df.columns)
        if 'LastModifiedDate' in fieldNames:
            datefield = 'LastModifiedDate'

        elif 'CreatedDate' in fieldNames:
            datefield = 'CreatedDate'

        else:
            datefield = '_ALL_'

        self._normalize_data(context,df,self.object,datefield)
        return 0

    def _standardize_datatypes(self, df):
        for column_name, dtype in df.dtypes.iteritems():
            if (dtype.kind != 'M'):
                df[column_name] = df[column_name].astype(str)
        return df


class SalesforceToGCSOperatorAllObjects(BaseOperator):
    """
    Make a query against Salesforce and
    write the resulting data to a file.
    """
    #template_fields = ("query", )

    @apply_defaults
    def __init__(
            self,
            conn_id,
            dest_bucket,
            refine_table='NA',
            bq_project='NA',
            history_check='NA',
            google_cloud_storage_conn_id='google_cloud_default',
            google_bq_conn_id='google_cloud_default',
            refine_dataset='ref_salesforce',
            base_location='None',
            base_norm_location='l1/',
            base_schema_location='l1_schema',
            obj_all='',
            delegate_to=None,
            airflow_var_set='NA',
            *args,
            **kwargs
    ):
        """
        Initialize the operator
        :param conn_id:             name of the Airflow connection that has
                                    your Salesforce username, password and
                                    orgID
        :param obj:                 name of the Salesforce object we are
                                    fetching data from
        :param file_location:              name of the file where the results
                                    should be saved
        :param airflow_var_set:     *(optional)* airflow variable that has salesforce objects
        :param         dest_bucket:    GCS bucket
        :param         history_check:    Boolean Value, will process from l1 if set to true else pulls from source
        :param         google_cloud_storage_conn_id:    google cloud connection
        :param         base_location:    l1 file location
        :param         base_norm_location:    l1_norm file location
        :param         base_schema_location:    schema file location
        """

        super(SalesforceToGCSOperatorAllObjects, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.base_location = base_location
        self.airflow_var_set = airflow_var_set
        self.object_all = obj_all
        self.base_norm_location = base_norm_location
        self.base_schema_location = base_schema_location
        self.delegate_to = delegate_to
        self.gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        self.source_count = 0
        self.dest_bucket = dest_bucket
        self.history_check = history_check
        self.refine_table = refine_table
        self.refine_dataset = refine_dataset
        self.google_bq_conn_id = google_bq_conn_id
        self.bq_hook = BigQueryHook(
            gcp_conn_id=self.google_bq_conn_id,
            delegate_to=self.delegate_to)
        self.bq_project = bq_project
        self.http_hook = HttpHook(method='GET',
                                  http_conn_id=self.conn_id)

    def _sf_connect(self, context):
        # Get credentials from the connection using http hook
        sf_connection = self.http_hook.get_connection(self.conn_id)
        sf_extra = json.loads(sf_connection.extra)
        self.sf_password = sf_connection.password
        self.sf_username = sf_connection.login
        self.sf_orgid = sf_extra.get('organizationId')
        self.sf_domain = sf_extra.get('domain')

        if self.sf_domain == 'test':
            self.sf = Salesforce(password=self.sf_password, username=self.sf_username, organizationId=self.sf_orgid, domain='test')
        else:
            self.sf = Salesforce(password=self.sf_password, username=self.sf_username, organizationId=self.sf_orgid)

        return 0

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file in GCS.
        """
        logging.info("Prepping to gather data from Salesforce")

        # Establish the connection via Simple Salesforce library
        self._sf_connect(context)

        # Get the list of tables to load.  Save the tables in a variable for task distribution in the refined step.
        description = self.sf.describe()
        names = [obj['name'] for obj in description['sobjects'] if obj['queryable']]
        names = ', '.join([x for x in names])
        logging.info("setting variables.....")
        Variable.set(self.airflow_var_set, names)
        self.object_all = names.split(', ')
        num_tables = len(self.object_all)
        table_counter = 0

        # For each table, build the refine table name and extract the data.
        for item in self.object_all:

            table_counter += 1
            self.object = item
            self.refine_table = self.refine_table.format(object=self.object)

            self._extract_data(context)


        logging.info('{seen} tables of {possible} processed'.format(seen=table_counter, possible=num_tables))

    def _extract_data(self, context):
        logging.info('fetching data for:' + self.object)
        self.source_count = 0

        target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])

        # As long as we are doing the history check, if the load all data task fails, rerun will pick up the already landed files.
        if (self.history_check.upper() == 'TRUE') and (self.gcs_hook.exists(self.dest_bucket, target_file_name)):
            logging.info('File already landed.........')
            logging.info('Processing the following file: ' + target_file_name)

            #skipping normalization for table with normalization issue/wrong json format
            SKIP_TABLES_LIST = Variable.get('salesforce_skip_tables_list',
                                            default_var='contentdocumentlink,contentfolderitem,contentfolderlink,contentfoldermember')
            skip_list = SKIP_TABLES_LIST.split(',')
            #table_skip_normalization_list = ['contentdocumentlink', 'contentfolderitem', 'contentfolderlink','contentfoldermember']
            if self.object.lower() in skip_list:
                logging.info('skipping normalization step for --> {table}'.format(table=self.object))
                return
            self._process_from_l1_gcs(context, target_file_name)
            return 0

        # Using the established SF object, get the table and fields to pull.
        # Run the query using date fields as available on table.
        try:
            salesforceObject = self.sf.__getattr__(self.object)
            fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]

        except:
            logging.info('Retrying the connection to SalesForce')
            self.sf_connect(context)
            salesforceObject = self.sf.__getattr__(self.object)
            fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]

        start_timestamp = context['ds'] + 'T00:00:00Z'
        end_timestamp = context['next_ds'] + 'T00:00:00Z'
        logging.info("Making request for {0} fields from {1}".format(len(fieldNames), self.object))

        file_path = os.path.join('/home/airflow/gcs/dags',
                                 r'ini/salesforce_additional_info_tables.json')

        with open(file_path, 'rb') as inputJSON:
            confJson = json.load(inputJSON)
        logging.info("Successfully read configuration file for additional input tables list")

        for item in confJson:
            if self.object == item['objectName']:
                source_field = item['source_field']
                source_table = item['source_table']
                link_field=item['link_field']
                source_ids = pd.DataFrame(self.sf.query_all(
                    "SELECT {source_field} FROM {source_table} WHERE LastModifiedDate >= {start_timestamp} AND LastModifiedDate< {end_timestamp} ".format(
                        source_field=source_field, source_table=source_table, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))['records'])
                source_dict = source_ids.to_dict('records')
                source_ids = pd.DataFrame((flatten(d) for d in source_dict))
                results = []
                datefield = '_ALL_'
                if not source_ids.empty:
                    for one_id in source_ids[source_field].unique():
                        one_results = self.sf.query_all(
                            "SELECT {field_list} FROM {link_table} WHERE {link_field} = '{one_id}' ".format(
                                field_list=", ".join(fieldNames), link_table=self.object, link_field=link_field,
                                one_id=one_id))
                        one_results = one_results['records']
                        results.append(one_results)

                    self.source_count = len(results)
                    results = pd.DataFrame(results)

                else:
                    results = pd.DataFrame()

                json_data = results.to_json(orient='records', lines='\n', date_format='iso')
                target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
                    base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
                logging.info("Writing query results to: {0}".format(target_file_name))
                self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
                self._normalize_data(context, results, self.object, datefield)
                return

        try:
            if 'LastModifiedDate' in fieldNames:
                results = self.sf.query_all(
                    "SELECT {source_field} FROM {source_table} WHERE LastModifiedDate >= {start_timestamp} AND LastModifiedDate< {end_timestamp} ".format(
                        source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))
                results = results['records']
                self.source_count = len(results)
                datefield = 'LastModifiedDate'

            elif 'CreatedDate' in fieldNames:
                results = self.sf.query_all(
                    "SELECT {source_field} FROM {source_table} WHERE CreatedDate >= {start_timestamp} AND CreatedDate< {end_timestamp} ".format(
                        source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                        end_timestamp=str(end_timestamp)))
                results = results['records']
                self.source_count = len(results)
                datefield = 'CreatedDate'

            else:
                results = self.sf.query_all("SELECT {source_field} FROM {source_table} ".format(
                    source_field=", ".join(fieldNames), source_table=self.object, start_timestamp=str(start_timestamp),
                    end_timestamp=str(end_timestamp)))
                results = results['records']
                self.source_count = len(results)
                datefield = '_ALL_'
        except:
            results = pd.DataFrame()
            self.source_count = len(results)
            datefield = '_ALL_'
            logging.info(' {obj} table could not be queried without additional input (like a targeted ID)'.format(obj=self.object))
            try:
                logging.info("Adding to Misbehaving table list ...")
                additional_input_tables = Variable.get('salesforce_misbehaving_tables')
                if self.object not in additional_input_tables:
                    additional_input_tables = additional_input_tables + self.object + ','
                Variable.set('salesforce_misbehaving_tables', additional_input_tables)
            except:
                Variable.set('salesforce_misbehaving_tables', self.object + ',')

        results = pd.DataFrame(results)
        json_data = results.to_json(orient='records', lines='\n', date_format='iso')
        target_file_name = '{base_location}{obj}/{extract_date}/l1_data_sf_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
        logging.info("Writing query results to: {0}".format(target_file_name))
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)

        # skipping normalize the data for tables with wrong json format /normalization issues
        SKIP_TABLES_LIST = Variable.get('salesforce_skip_tables_list',
                                        default_var='contentdocumentlink,contentfolderitem,contentfolderlink,contentfoldermember')
        skip_list = SKIP_TABLES_LIST.split(',')
        # table_skip_normalization_list = ['contentdocumentlink', 'contentfolderitem', 'contentfolderlink','contentfoldermember']
        if self.object.lower() in skip_list:
            logging.info('skipping normalization step for --> {table}'.format(table = self.object))
            return
        self._normalize_data(context, results, self.object, datefield)

    def _normalize_data(self, context, df, obj, datefield):
        dq = dq_common()
        logging.info('obj is ' + self.object)
        current_date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if len(df) == 0:
            logging.info('No data to process.....')
            json_data = df.to_json(orient='records', lines='\n', date_format='iso')
            target_file_name = '{base_norm_location}{obj}/{extract_date}/l1_norm_sf_{obj}.json'.format(
                base_norm_location=self.base_norm_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
            self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
            self._upload_schema_to_gcs(obj, context, df)
            self._upload_metadata(context, df)
            return

        df_dict = df.to_dict('records')
        df = pd.DataFrame((flatten(d) for d in df_dict))
        df = dq.dq_column_group_cleanup(df)
        df = self._standardize_datatypes(df)

        date_target = context['ds']
        extract_date = context['ds_nodash']
        msg = 'Processing data for date ->' + str(date_target)
        logging.info(msg)

        #remove the additional timefields
        df['data_extract_date'] = pd.to_datetime(date_target).strftime("%Y-%m-%d %H:%M:%S")
        df['data_run_date'] = current_date
        file_date_time = pd.to_datetime(extract_date, format='%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
        df['data_land_date'] = file_date_time
        df['data_land_date'] = pd.to_datetime(df['data_land_date'])
        df['data_run_date'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['data_run_date'] = pd.to_datetime(df['data_run_date'])
        df['time_central'] = df['data_land_date'].copy(deep=True)
        df['time_central'] = pd.to_datetime(df['data_land_date'])

        new_cols = []
        for col in df.columns:
            new_cols.append(col.replace('.', '_').replace('-', '_').replace(',', '_'))
        df.columns = new_cols
        df.reset_index(drop=True, inplace=True)

        time_fields = ['_at', '_time', '_created', '_date', 'created', 'At', 'updated', 'Date', 'stamp', '_Date__c']

        date_fields = []
        #print(df.columns)
        for col in df.columns:
            if col == 'IsUpToDate':
                continue
            if any(str(col).endswith(x) for x in time_fields):
                date_fields.append(col)


        def time_test(x):
            if 'T' in str(x):
                return pd.to_datetime(x, errors='coerce')

            if (':' not in str(x)) and ('-' not in str(x)) and ('/' not in str(x)):
                return pd.to_datetime('1970-01-01')
            try:
                x_temp = pd.to_datetime(x, unit='ms')
            except:
                x_temp = pd.to_datetime(x, errors='ignore')
            return x_temp

        for col in df.columns:
            if col in date_fields:
                df[col] = pd.to_datetime(df[col],errors='coerce')
                #df[col] = df[col].apply(time_test)

        if datefield != '_ALL_':
            df[datefield] = pd.to_datetime(df[datefield])

        table_presence = self.bq_hook.table_exists(project_id=self.bq_project,
                                                   dataset_id=self.refine_dataset,
                                                   table_id=self.refine_table)
        if table_presence:
            logging.info('getting old schema')
            old_cols = self.bq_hook.get_schema(project_id=self.bq_project,
                                               dataset_id=self.refine_dataset,
                                               table_id=self.refine_table)

            old_cols = json_normalize(data=old_cols['fields'])

        else:
            
            old_cols = pd.DataFrame()

        if len(old_cols) > 0:
            for col in df.columns:
#            for col in old_cols.name.values:
                if old_cols.loc[old_cols['name'] == col, 'type'].iloc[0].lower() == 'string':
                    df[col] = df[col].astype(str)
                    



        json_data = df.to_json(orient='records', lines='\n',date_format='iso')
        target_file_name = '{base_norm_location}{obj}/{extract_date}/l1_norm_sf_{obj}.json'.format(
            base_norm_location=self.base_norm_location, obj=self.object.lower(), extract_date=context['ds_nodash'])
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)
        self._upload_schema_to_gcs(obj, context, df)
        self._upload_metadata(context, df)

        return

    def _upload_metadata(self, context, df):
        audit_df = pd.DataFrame()

        target_file_name = '{base_location}{obj}/{extract_date}/l1_metadata_{obj}.json'.format(
            base_location=self.base_location, obj=self.object.lower(), extract_date=context['ds_nodash'])

        file_exists = self.gcs_hook.exists(self.dest_bucket, target_file_name)
        logging.info('file_exists value is ' + str(file_exists))

        audit_df = audit_df.append(
            {'l1_count': len(df), 'source_count': self.source_count, 'dag_execution_date': context['ds']},
            ignore_index=True)

        json_data = audit_df.to_json(orient='records', lines='\n', date_format='iso')
        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=json_data)

        del json_data
        del audit_df

        return

    def _upload_schema_to_gcs(self, obj, context,df):

        schema_data = self._schema_generator(df, context)

        target_file_name = '{base_schema_location}{obj}/{extract_date}/l1_schema_sf_{obj}.json'.format(
            base_schema_location=self.base_schema_location, obj=obj.lower(), extract_date=context['ds_nodash'])

        self.gcs_hook.upload(bucket=self.dest_bucket, object=target_file_name, data=schema_data)

        return

    def _schema_generator(self, df, context, default_type='STRING'):

        type_mapping = {
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }
        fields = []
        for column_name, dtype in df.dtypes.iteritems():

            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type),
                           'mode': 'NULLABLE'})
        # return_string = "[{fields}]".format(fields=",".join(fields))
        return json.dumps(fields)

    def _process_from_l1_gcs(self, context, target_file):

        file_data = self.gcs_hook.download(self.dest_bucket, target_file)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_json(file_stream, orient='records', lines='\n')
        self.source_count = len(df)
        fieldNames = list(df.columns)

        if 'LastModifiedDate' in fieldNames:
            datefield = 'LastModifiedDate'

        elif 'CreatedDate' in fieldNames:
            datefield = 'CreatedDate'

        else:
            datefield = '_ALL_'

        self._normalize_data(context,df,self.object,datefield)
        return 0

    def _standardize_datatypes(self, df):
        for column_name, dtype in df.dtypes.iteritems():
            if (dtype.kind != 'M'):
                df[column_name] = df[column_name].astype(str)
        return df
