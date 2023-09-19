import pandas as pd
import datetime
import json
import io
import logging
from airflow.models import BaseOperator
from plugins.operators.jm_gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class PBIAuditLogOperator(BaseOperator):

    ui_color = '#e0aFFc'

    # template_fields = ('metadata_filename',)

    @apply_defaults
    def __init__(self,
                target_gcs_bucket,
                source,
                source_abbr,
                table,
                config_file,
                metadata_storage_location,
                base_file_location,
                base_normalized_location,
                base_backup_location,
                google_cloud_storage_conn_id,
                delegate_to = None,
                *args,
                **kwargs
                ):
        super(PBIAuditLogOperator,self).__init__(*args, **kwargs)
        self.target_gcs_bucket = target_gcs_bucket
        self.source = source
        self.source_abbr = source_abbr
        self.table = table
        self.config_file = config_file
        self.metadata_storage_location = metadata_storage_location
        self.base_file_location = base_file_location
        self.base_normalized_location = base_normalized_location
        self.base_backup_location = base_backup_location
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to


    def execute(self,context):
        column_list = self.config_file[self.table]["Columns_to_BQ"]
        counter = 0
        files = self._get_files_list(context)
        for file in files:
            if file.endswith('csv'):
                print(f'processing file {file}')
                df = self._load_data(file, context)
                print("load_data\'s df",df)
                source_count = len(df)
                logging.info("Data loaded ... ")
                self._retain_uploaded_csv_files(df, context)
                logging.info("File retained ... ")
                normalized_df = self._flatten_data(df, context)
                print("flatten_data\'s df", normalized_df)
                logging.info("Data flattened ... ")

                print("-"*100)
                all_columns = list(set(list(normalized_df.columns) + column_list))
                print("Number of all_columns",len(all_columns))
                normalized_df = normalized_df.reindex(columns=all_columns, fill_value='')
                Extracted_df = normalized_df.loc[:,column_list]
                Extracted_df = Extracted_df.reindex(sorted(normalized_df.columns),axis=1)
                print("Extracted columns: ", len(Extracted_df.columns))
                print("Final_df: ", Extracted_df)
                print("-"*100)

                self._upload_csv_to_gcs(Extracted_df,context,counter)
                row_count = len(Extracted_df)
                logging.info("File uploaded ... ")
                self._metadata_upload(context, source_count, row_count)
                counter += 1
                print("Total number of staging files: ", counter) 
            else:
                print('this is not a file')

    def _get_files_list(self, context):
        logging.info ("Get list of files from storage for processing...")
        hook = GCSHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        folder_path = '{base_file_location}{table}/{date_nodash}'.format(
            table=self.table,
            date_nodash=context['ds_nodash'],
            base_file_location=self.base_file_location)

        logging.info("Folder Path : {folder_path}".format(folder_path = folder_path))
        file_list = hook.list(bucket=self.target_gcs_bucket, prefix=folder_path)
        print(file_list)
        print("*************************************************************************")
        return file_list


    def _load_data(self,file, context):
        hook = GCSHook(google_cloud_storage_conn_id = self.google_cloud_storage_conn_id, delegate_to = self.delegate_to)
        file_name = str(file)
        file_data = hook.download(self.target_gcs_bucket, object = file_name)
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df = pd.read_csv(file_stream)
        return df

    def _flatten_data(self, df, context) -> pd.DataFrame:
        row_list = []
        for row in df.loc[:,'AuditData']:
            row = json.loads(row)
            row = pd.json_normalize(row)
            row_list.append(row)
        return pd.concat(row_list)

    def _upload_csv_to_gcs(self, df,context, counter):
        hook = GCSHook(google_cloud_storage_conn_id = self.google_cloud_storage_conn_id, delegate_to = self.delegate_to)
        file_name = '{base_normalized_location}{date_nodash}/l1_norm_{source}_{counter_suffix}.csv'.format(
            date_nodash=context['ds_nodash'],
            source = self.source_abbr,
            api_name = self.table,
            base_normalized_location = self.base_normalized_location,
			counter_suffix = counter)

        csv_data = df.to_csv(encoding='utf-8', index=False)
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        print('>>>>>>>>>> file converted to csv')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=csv_data)
        print('upload completed')
        return 

    def _metadata_upload(self, context, row_count, source_count, check_landing=True):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)

        metadata_filename = '{base_folder}{date}/l1_metadata_{source}.json'.format(
            base_folder=self.metadata_storage_location,
            source=self.source_abbr,
            date=context['ds_nodash'])

        print('Metadata File - ', metadata_filename)
        json_metadata = {
            'source_count': source_count,
            'l1_count': row_count,
            'dag_execution_date': context['ds']
        }

        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        df = df.transpose()
        gcs_hook.upload(self.target_gcs_bucket,
                        metadata_filename,
                        df.to_json(orient='records', lines='\n', date_format='iso'))
        return
        
    def _retain_uploaded_csv_files(self, df, context):
        hook = GCSHook(google_cloud_storage_conn_id = self.google_cloud_storage_conn_id, delegate_to = self.delegate_to)
        file_name = '{base_backup_location}{table}/{date_nodash}.csv'.format(
            date_nodash=context['ds_nodash'],
            table = self.table,
            base_backup_location = self.base_backup_location)

        csv_data = df.to_csv(encoding='utf-8', index=False)
        print(f'>>>>>>> FILE PATH >>>>> {file_name}')
        print('>>>>>>>>>> file converted to csv')
        hook.upload(bucket=self.target_gcs_bucket, object=file_name, data=csv_data)
        print('upload completed')
        return

