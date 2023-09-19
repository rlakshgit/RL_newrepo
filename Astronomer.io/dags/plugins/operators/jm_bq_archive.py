"""
This operator would archive tables/datasets from source to destination inside the same project
-- we have three options - we can copy entire dataset or copy list of tables inside a dataset or copy tables starts with some search string
   It can be achieved by manipulating search_string in Archival_variable through Airflow web frontend
   if search string ='' ----> copy entire dataset to destination dataset
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"","dataset_expiration_time":864000}
   if search string ='[table1,table2,table3]' -----> copy only list of tables in a source dataset to destination dataset
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"[table1,table2,table3]","dataset_expiration_time":864000}
   if search string = 'sometext' ------> copy only those tables which starts with search string
   
   Example :Archival_variable = {"project":"dev-edl","source_dataset":"GN_kimberlite_pctl_current","destination_dataset":"archive_GN_kimberlite_pctl_current","delete_source":"True","table_search_string":"pctl","dataset_expiration_time":864000}
Note:
    To delete the source table/dataset -->"delete_source" in archival variable has set to be True.
"""


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_bq_hook import BigQueryHook
import logging


class BigQueryArchiveOperator(BaseOperator):
    """
    base_gcp_connector ----> gcp_connection_id (type string)
    project ---> Name of the GCP project where we archive the dataset (string)
    source_dataset --> Name of the dataset which has to be copied /archived (string)
    destination_dataset ---> Name of the Destination Dataset to copt tables/entire source dataset into (string)
    table_search_string ---> string which would determine if the archiving is for full dataset or only some tables inside a dataset
    delete_source --> string which would decide if the dataset/tables has to be deleted
    """


    @apply_defaults
    def __init__(self,
                 base_gcp_connector,
                 project,
                 source_dataset,
                 destination_dataset,
                 table_search_string,
                 datasets_to_delete,
                 archive='True',
                 delegate_to = None,
                 delete_source = 'False',
                 *args, **kwargs):
        self.base_gcp_connector = base_gcp_connector
        self.project = project
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.table_search_string = table_search_string
        self.delete_source = delete_source
        self.archive = archive
        self.datasets_to_delete = datasets_to_delete
        self.delegate_to = delegate_to

        super(BigQueryArchiveOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook =BigQueryHook(self.base_gcp_connector)
        logging.info("Table list - {list}".format(list = self.table_search_string))

        if self.archive == 'False':
            logging.info("Archiving - False. Just deleting dataset...")
            if '[' in self.datasets_to_delete:
                logging.info("List of datasets to be deleted...")
                list_of_datasets = self.datasets_to_delete.replace("[", "", 1).replace("]", "", 1).replace("dbo.", "").split( ',')
                logging.info(list_of_datasets)
                for dataset in list_of_datasets:
                    hook.delete_dataset(project_id=self.project, dataset_id=dataset, delete_contents=True)
                    logging.info("{Dataset} - deleted from {project}".format(Dataset=dataset, project=self.project))
            return


        #if search_string = '' --> copy full dataset
        #if search_string contains '[' -->copy list of tables mentioned in the list using sql (in operator)
        #if serch_string contains some search string  ---> copy tables starts with search string using sql(like operator)


        if self.table_search_string =='':
            logging.info("Copying all tables in a dataset..")
            query = 'SELECT table_id FROM {project}.{source_dataset}.__TABLES__'.format(source_dataset = self.source_dataset,project=self.project)
        elif '[' in self.table_search_string:
            logging.info("Copying list of tables in a dataset..")
            list_of_tables = self.table_search_string.replace("[", "",1 ).replace("]", "", 1).replace("dbo.","").split(',')
            string_tables = ','.join("'{0}'".format(w) for w in list_of_tables)
            query = "SELECT table_id FROM {project}.{source_dataset}.__TABLES__ where table_id in ({search_string})".format(
                project=self.project, source_dataset=self.source_dataset,
                search_string=string_tables)
        else:
            logging.info("Copying list of tables using table name search in a dataset..")
            query = "SELECT table_id FROM {project}.{source_dataset}.__TABLES__ where table_id like '{search_string}%'".format(project=self.project,source_dataset=self.source_dataset,search_string = self.table_search_string)

        df = hook.get_pandas_df(sql=query,dialect='standard')
        source_count = len(df)
        logging.info('Number of tables needs to be copied - {length}'.format(length = source_count))
        table_list = df['table_id'].tolist()

        destination_count = 0
        # Copying the dataset/tables
        for items in table_list:
            source_dataset_tables = '{project}.{dataset}.{table}'.format(
                project=self.project,
                dataset = self.source_dataset,
                table = items)
            destination_dataset_tables = '{project}.{dataset}.{table}'.format(
                project=self.project,
                dataset=self.destination_dataset,
                table=items)
            # if table already present skip copying the tables from source to destination
            Table_check = hook.table_exists(project_id=self.project, dataset_id = self.destination_dataset, table_id=items)
            if(Table_check == False):
                hook.run_copy(source_project_dataset_tables =source_dataset_tables,destination_project_dataset_table = destination_dataset_tables )
                logging.info('copied table - {table}'.format(table = items))
            else:
                logging.info("Table already present")
            destination_count = destination_count+1


        #gchecking counts from source and destination datasets
        logging.info("source count - {sc}".format(sc = source_count))
        logging.info("destination count - {dc}".format(dc=destination_count))
        if(source_count == destination_count):
            logging.info("Number of table count matches")
        else:
            raise ValueError('Table count between source and destination does not match')

        #deleting the dataset / tables
        #delete the table only if number of tables copied from source matches with number of tables in destination and delete_source variable is set to True
        #if search_string variable ="" , Delete the entire source dataset otherwise deleting the list of tables from sourec dataset
        if (self.delete_source.upper() == 'TRUE')  and (source_count == destination_count) :
            table_delete_count =0
            if self.table_search_string == '':
                hook.delete_dataset(project_id = self.project, dataset_id = self.source_dataset, delete_contents = True)
                logging.info("{Dataset} - deleted from {project}".format(Dataset = self.source_dataset,project= self.project))

            else:
                for items in table_list:
                    logging.info("Deleting table = {table}".format(table = items))
                    source_dataset_tables = '{project}.{dataset}.{table}'.format(
                        project=self.project,
                        dataset=self.source_dataset,
                        table=items)
                    hook.run_table_delete( deletion_dataset_table = source_dataset_tables, ignore_if_missing = False)
                    table_delete_count =  table_delete_count +1
                logging.info("Number of tables deleted - {count}".format(count = table_delete_count))

                if(source_count == destination_count==table_delete_count):
                    logging.info("Archive Job Success...")
                else:
                    raise ValueError('Archive Job Failed..')
        else:
            logging.info("Tables/Dataset not deleted")



