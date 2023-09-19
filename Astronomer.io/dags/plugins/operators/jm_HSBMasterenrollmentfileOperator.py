"""Script to pull all Jeweler Member Benefits Statement data and produce report"""

################################################################################
# Base and third-party libraries
################################################################################


import os
import sys
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.jm_gcs import GCSHook
from plugins.hooks.jm_bq_hook_semimanaged import BigQueryHook
import tempfile
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import logging
import json
import io
import datetime as dt
import tempfile
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz


class HSBMasterenrollmentfileOperator(BaseOperator):
    template_fields = ('gcs_destination_path',)
    ui_color = '#6b1cff'

    @apply_defaults
    def __init__(self,
                 sharepoint_connection,
                 google_storage_connection_id,
                 bq_gcp_connection_id,
                 semi_managed_connection,
                 source_file_url,
                 gcs_destination_path,
                 base_gcs_folder,
                 source,
                 history_check,
                 project,
                 dataset,
                 table,
                 mime_type="text/plain",
                 destination_bucket='jm-edl-landing-wip',
                 delegate_to=None,
                 *args,
                 **kwargs):


        super(HSBMasterenrollmentfileOperator, self).__init__(*args, **kwargs)
        self.sharepoint_site = sharepoint_connection
        self.google_storage_connection = google_storage_connection_id
        self.bq_gcp_connection_id =bq_gcp_connection_id
        self.file_url = source_file_url
        self.gcs_destination_path = gcs_destination_path
        self.mime_type = mime_type
        self.destination_bucket = destination_bucket
        self.delegate_to = delegate_to
        self.base_gcs_folder =base_gcs_folder
        self.source = source
        self.history_check = history_check
        self.semi_managed_connection_id = semi_managed_connection
        self.hook = GCSHook(
            google_cloud_storage_conn_id=self.google_storage_connection,
            delegate_to=self.delegate_to)
        self.semi_managed_hook = GCSHook(
            google_cloud_storage_conn_id=self.semi_managed_connection_id,
            delegate_to=self.delegate_to)
        self.semimanaged_bq_hook =  BigQueryHook(gcp_conn_id=  self.semi_managed_connection_id)
        self.bq_hook =  BigQueryHook(gcp_conn_id= self.bq_gcp_connection_id)
        self.new_pull = True
        self.project = project
        self.dataset = dataset
        self.table = table

    """
    Function to authenticate and connect to sharepoint 
    and get the latest file from the folder
    """
    def auth(self):

        logging.info('start processing')
        logging.info("Getting Sharepoint connection details from Basehook")
        connection = BaseHook.get_connection(self.sharepoint_site)

        site_url = connection.host
        client_secret = connection.password
        client_username = connection.login
        logging.info("Site _url :{url}".format(url = self.file_url))



        # Authenticating the credentials and connect to sharepoint team site
        credentials = ClientCredential(client_username, client_secret)
        ctx = ClientContext(site_url).with_credentials(credentials)
        logging.info("Connection established and created context object")

        # Getting list of files from the folder
        print('file_url')
        print(self.file_url)
        libraryRoot = ctx.web.get_folder_by_server_relative_url(self.file_url).execute_query()

        files = libraryRoot.files
        ctx.load(files)
        ctx.execute_query()

        #creating empty dataframe to add list of files in the sharepoint folder
        get_latestfile_df = pd.DataFrame(columns=['file', 'file_path', 'created_time'])

        j = 0
        for file in files:
            list = file.properties.keys()

            #for i in list:
                #print("{i}:{value}".format(i=i, value=file.properties[i]))

            file_path = file.properties["ServerRelativeUrl"]
            file_created_time = file.properties["TimeCreated"]
            file_name = os.path.basename(file_path)

            ## Adding file metadata to dataframe
            get_latestfile_df.loc[j, 'file'] = file_name
            get_latestfile_df.loc[j, 'created_time'] = file_created_time
            get_latestfile_df.loc[j, 'file_path'] = file_path

            j = j + 1

        #Logic to get recently created file
        get_latestfile_df['created_time'] = pd.to_datetime(get_latestfile_df['created_time'])
        get_latestfile_df.sort_values('created_time', inplace=True, ascending=False)
        get_latestfile_df.reset_index(drop=True, inplace=True)
        logging.info("The latest file to process is - {file}".format(file= get_latestfile_df))
        final_file_list = []
        if len(get_latestfile_df) >= 1:
            final_file_list.append(get_latestfile_df.iloc[0]['file_path'])
            print(final_file_list)
        else:
            logging.info("No files present...")

        ##Downloading latest file from Sharepoint into dataframe
        for file_path in final_file_list:
            file_name = os.path.basename(file_path)
            logging.info("processing......  {file_name}".format(file_name=file_name))
            with tempfile.NamedTemporaryFile() as localFile:
                name = localFile.name
                file = ctx.web.get_file_by_server_relative_url(file_path).download(localFile).execute_query()
                localFile.seek(0)
                content = localFile.read()
                path = self.gcs_destination_path + file_name

                #uploading the raw file into GCS
                self.hook.upload(
                    bucket=self.destination_bucket,
                    object=path,
                    filename=name,
                    mime_type=self.mime_type,
                )
                file_data = self.hook.download(self.destination_bucket, path)
                file_stream = io.BufferedReader(io.BytesIO(file_data))
                data_df = pd.read_excel(file_stream, engine='openpyxl')
                return data_df

    """
    Function to check if file eists in storage for reprocessing
    """
    def _l1_data_check(self, context):


        file_prefix = '{base_gcs_folder}{source}/{date_nodash}/'.format(
                                                 source=self.source,
                                                 date_nodash=context['ds_nodash'],
                                                 base_gcs_folder=self.base_gcs_folder
                                                                                 )
        logging.info("Files prefix - {prefix}".format(prefix = file_prefix))
        #Getting List of files from the folder
        file_list = self.hook.list(self.destination_bucket, versions=True, maxResults=100, prefix=file_prefix)#, delimiter=',')
        logging.info("The files are...")
        logging.info(file_list)
        if len(file_list) >0:
            return len(file_list),file_list[0]
        else:
            return len(file_list) , 'None'


    """
    Data transformation(cleaning and filtering and uploading it to Bigquery
    
    """

    def data_transformation(self,df,context):

        ###Check with Julius  - This function is not used anywhere. Do we still need it?
        def locmatch(df1, df2, pol1, pol2, loc1, loc2):
            ##Create Lookup columns in both Dataframes
            df1['Lookups'] = df1[pol1] + '-' + df1[loc1]
            df2['Lookups2'] = df2[pol2] + '-' + df2[loc2]

            lookups = df1['Lookups'].to_list()

            rows = []
            final = pd.DataFrame()
            count = 0
            for i in lookups:
                matchlist = []
                matchlist.append(i)
                match = process.extractOne(i, df2['Lookups2'].to_list())
                # matchlist.append(match)
                matchlist.append([i for i in list(match)])
                rows.append(matchlist)



            # Create Dataframe from matches
            found = pd.DataFrame(rows, columns=['Lookups', 'Match'])
            found['Similarity'] = found['Match'].apply(pd.Series)[1]
            found['Match'] = found['Match'].apply(pd.Series)[0]

            # Drop matches where policy numbers do not equal
            found['1'] = found['Lookups'].str.split('-', expand=True)[[1]]
            found['2'] = found['Match'].str.split('-', expand=True)[[1]]
            found['drop'] = np.where(found['1'] != found['2'], 1, 0)

            # Drop not matches, and also drop extra columns
            found = found[found['drop'] != 1]
            found.drop(columns=['1', '2', 'drop'], inplace=True)

            final = df1.merge(found, on='Lookups', how='left')

            final2 = final.merge(df2[['Lookups2', 'req_status']], left_on='Match', right_on='Lookups2', how='left')

            final2.drop_duplicates(inplace=True)

            return (final2)
        ####Check with Julius -This HSB requirements is created by load_HSB_requirements_table task - should we run the task before this task?
        sql3 = '''SELECT * FROM `{project}.{dataset}.hsb_requirements`'''.format(project = self.project, dataset = self.dataset)
        req = self.bq_hook.get_pandas_df(sql = sql3)
        df = df.rename(columns={'optOutYN': 'Opt Out Y/N'})
        col = df.columns.to_list()[0]
        df = df[df[col].notna()]
        ##Make list of all columns to loop through and rename
        cols = df.columns.to_list()
        # Remove \n from column names
        newcols = []
        for i in cols:
            i = i.replace('\n', ' ')
            newcols.append(i)

        ##Rename columns
        df.columns = newcols

        #####Check with Julius - Codes to upload it to storage/BQ (This is what available in data folder)
        file_name = '{source}/data/{date_nodash}/hsb_enrollments.csv'.format(
           date_nodash=context['ds_nodash'],
            source=self.source
        )
        #self.semi_managed_hook.upload(bucket='semi-managed-reporting', object=file_name,
                    #data=df.to_csv(index= False))



        bq_df = df.copy()
        df_column = bq_df.columns.tolist()
        df_cols = [i.replace(' ', '_').replace('/', '_') for i in df_column]
        bq_df.columns = df_cols


        #writing to bigquery table hsb_enrollments
        self.bq_hook.gcp_bq_write_table(
            df_in=bq_df,
            project=self.project,
            dataset=self.dataset,
            table_name=self.table,
            control='REPLACE',
            schema_enable=True,
            schema='NA')
        ######################################

        df = df[df['Opt Out Y/N'].isna()]
        ##Format Account number for Salesforce (Remove location tags)
        df['accountNumber'] = df['accountNumber'].str[:9]
        df['install_status'] = np.where(df['installedDate'].notnull(), True, False)
        hold = df
        # Choose needed columns
        df = df[['accountNumber', 'accountAddress', 'install_status', 'shippedDate', 'installedDate']]
        ##Now read in latest file
        df = hold

        data2 = df[
            ['accountNumber', 'accountAddress', 'accountCity', 'accountState', 'accountZip', 'install_status',
             'shippedDate',
             'deliveredDate', 'reshippedDate', 'activatedDate', 'daysToInstall', 'installedDate']]


        req['req_status'] = 1

        # In[35]:

        data3 = data2.merge(req[['PolicyNbr', 'req_status', 'LocAddr1', 'InsuredName', 'LocNbr', 'BldgNbr']],
                            left_on='accountNumber', right_on='PolicyNbr', how='left')

        data3['accountAddress'] = data3['accountAddress'].astype('str')
        data3['LocAddr1'] = data3['LocAddr1'].astype('str')
        data3['accountAddress'] = data3['accountAddress'].str.lower()

        data3['LocAddr1'] = data3['LocAddr1'].str.lower()
        data3['Ratio'] = data3.apply(lambda x: fuzz.partial_ratio(x['accountAddress'], x['LocAddr1']), axis=1)
        data3 = data3.sort_values(by=['accountNumber', 'accountAddress', 'Ratio'], ascending=[True, True, False])
        data4 = data3.drop_duplicates(subset=['accountNumber', 'accountAddress'])
        print(data4.head(5))
        data5 = data4.copy()
        data5['req_status'] = np.where(data5['Ratio'] > 50, data5['req_status'], np.nan)
        data6 = data5.copy()
        data6.drop(columns=['LocAddr1', 'PolicyNbr'], inplace=True)
        print(data6.head(5))

        #Copying this to Bigquery for Josh to use?
        logging.info("Uploading to SF_Upload table...")
        self.bq_hook.gcp_bq_write_table(
            df_in=data6,
            project=self.project,
            dataset=self.dataset,
            table_name='SF_Upload',
            control='REPLACE',
            schema_enable=True,
            schema='NA')




    def execute(self, context):
        logging.info("History check - {check}".format(check = self.history_check))
        if self.history_check.upper() == 'TRUE':
            len_of_filelist,file_path = self._l1_data_check(context)
            logging.info("Number of files present - {numfiles}".format(numfiles = len_of_filelist))
            if len_of_filelist >= 1:
                logging.info("Files present for processing...")
                file_data = self.hook.download(self.destination_bucket, file_path)
                file_stream = io.BufferedReader(io.BytesIO(file_data))
                data_df = pd.read_excel(file_stream, engine='openpyxl')
                self.new_pull = False


            else:
                logging.info("Pulling from Sharepoint site ...")
                self.new_pull = True

        if self.new_pull == True:
            logging.info("Connecting to sharepoint...")
            data_df = self.auth()

        self.data_transformation(df = data_df,context = context)







