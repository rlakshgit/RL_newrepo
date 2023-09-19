# imports section
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import ast
from airflow.models import Variable
import urllib.parse as urlparse
# from plugins.operators.dbhelper import DBHelper
from google.oauth2 import service_account
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_gcs import GCSHook
import pandas_gbq
import os
import pandas as pd
import datetime


class WebScraperFeeder(BaseOperator):
    @apply_defaults
    def __init__(self,
                 # anon_project_id,

                 *args,
                 **kwargs
                 ):

        super(WebScraperFeeder, self).__init__(*args, **kwargs)
        # self.anon_project_id = anon_project_id

    def execute(self, context):
        url_pid_dict = ast.literal_eval(Variable.get('url_pid_all'))
        last_ctr = int(Variable.get('last_ctr', default_var=0))
        new_dict = {}
        new_ctr = last_ctr + 1000

        i = -1
        for start_url, place_ids in url_pid_dict.items():
            i += 1
            if (i < last_ctr):
                continue
            new_dict[f'{start_url}'] = place_ids

            if (i == new_ctr):
                break

        Variable.set("last_ctr", new_ctr)
        Variable.set("url_pid_working", new_dict)

        return


class WebScraperPopulateTable(BaseOperator):
    @apply_defaults
    def __init__(self,
                 project_id,
                 sql,
                 google_cloud_bq_conn,
                 sql_dialect='standard',
                 myql_connector='webscraper_myql_conn',

                 *args,
                 **kwargs
                 ):

        super(WebScraperPopulateTable, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.sql = sql
        self.sql_dialect = sql_dialect
        self.myql_connector = myql_connector
        self.google_cloud_bq_conn = google_cloud_bq_conn

    def execute(self, context):
        # db = DBHelper()

        # Old##
        # credentials = service_account.Credentials.from_service_account_file('key.json')
        # df = pandas_gbq.read_gbq(self.sql, project_id=self.project_id, credentials=credentials, dialect=self.sql_dialect)

        bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn)
        df = bq_hook.get_pandas_df(self.sql)
        url_pid_dict = {}
        i = 0
        for row in df.itertuples():
            i += 1

            place_id = row.place_id
            try:
                url = row.website
            except:
                url = row.website_url

            url_parsed = urlparse.urlparse(url)
            start_url = url_parsed.scheme + "://" + url_parsed.netloc + "/"

            domain = url_parsed.netloc
            if domain.startswith('ww'):
                domain = domain.split(".", 1)[1]
            # print(place_id, url, start_url, domain)
            # populated = db.populate_table(place_id, url, start_url, domain)
            # if (populated == False):
            #     continue ####

            if (start_url in url_pid_dict):
                url_pid_dict[f'{start_url}'].append(place_id)
            else:
                url_pid_dict[f'{start_url}'] = [place_id]

        Variable.set("url_pid_all", url_pid_dict)
        return


class WebScraperGenerateTable(BaseOperator):
    @apply_defaults
    def __init__(self,
                 project_id='NA',
                 myql_connector='webscraper_myql_conn',

                 *args,
                 **kwargs
                 ):

        super(WebScraperGenerateTable, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.myql_connector = myql_connector

    def execute(self, context):
        # db = DBHelper()
        wordColumns = []
        # column loading
        file_path = os.path.join('/home/airflow/gcs/dags',
                                 r'web_crawler/ini/words.txt')
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if (line == "place_id"):
                    continue
                else:
                    wordColumns.append(line)

        # db.create_table(wordColumns)
        # del db
        return "Done"


class WebScraperGroupSites(BaseOperator):
    @apply_defaults
    def __init__(self,
                 project_id='NA',
                 myql_connector='webscrapper_myql_conn',

                 *args,
                 **kwargs
                 ):

        super(WebScraperGroupSites, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.myql_connector = myql_connector

    def execute(self, context):
        # initializing dictionary
        url_pid_dict = {}

        # database init and getting all records
        # db = DBHelper()
        # sites = db.get_all_records()
        # del db
        sites = []
        # grouping url by domains
        for site in sites:
            if (site.start_url in url_pid_dict):
                url_pid_dict[f'{site.start_url}'].append(site.place_id)
            else:
                url_pid_dict[f'{site.start_url}'] = [site.place_id]

        Variable.set("url_pid_all", url_pid_dict)


class WebScraperBackFill(BaseOperator):
    @apply_defaults
    def __init__(self,
                 project_id='NA',
                 myql_connector='webscrapper_myql_conn',

                 *args,
                 **kwargs
                 ):

        super(WebScraperBackFill, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.myql_connector = myql_connector

    def execute(self, context):
        AIRFLOW_ENV = Variable.get('ENV')

        if AIRFLOW_ENV.lower() != 'prod':
            base_bucket = 'jm-edl-landing-wip'
            project = 'jm-dl-landing'
            base_gcp_connector = 'jm_landing_dev'
        elif AIRFLOW_ENV.lower() == 'prod':
            base_bucket = 'jm-edl-landing'
            project = 'jm-dl-landing'
            base_gcp_connector = 'jm_landing_prod'
        else:
            print('Environment is not available please address....')
            raise

        hook = GCSHook(
            google_cloud_storage_conn_id=base_gcp_connector,
            delegate_to=None)

        save_date = datetime.datetime.now().strftime("%Y%m%d")
        prefix_file = 'web_crawler_data/{save_date}/webcrawler_data_'.format(save_date=save_date)
        list_files = hook.list(base_bucket, prefix=prefix_file)
        print('*' * 20)
        print(list_files)
        print('*' * 20)

        url_pid_dict = ast.literal_eval(Variable.get('url_pid_working'))
        list_urls = [(k, v) for k, v in url_pid_dict.items()]
        for start_url, place_ids in list_urls:
            domain = urlparse.urlparse(start_url).netloc
            if domain.startswith('ww'):
                domain = domain.split(".", 1)[1]

            place_ids_str = ','.join(place_ids)
            domain_new = ''.join(e for e in domain if e.isalnum())
            df = pd.DataFrame()
            counter = 0
            for place_id in place_ids_str.split(','):
                df.loc[counter, 'place_ids'] = place_id
                counter += 1

            df['start_url'] = domain
            df['domain'] = domain
            df['update_time'] = datetime.datetime.now()

            cols_list = ['spider_closed:BOOLEAN',
                         'twitter:STRING',
                         'pinterest:STRING',
                         'instagram:STRING',
                         'youtube:STRING',
                         'facebook:STRING',
                         'preowned:BOOLEAN',
                         'cash_for_gold:BOOLEAN',
                         'loose_stones:BOOLEAN',
                         'guns:BOOLEAN',
                         'pawn:BOOLEAN',
                         'phones:BOOLEAN',
                         'BretLeing:BOOLEAN',
                         'twentyfour_k_gold:BOOLEAN',
                         'Vacheron_Constantin:BOOLEAN',
                         'Omega:BOOLEAN',
                         'Girard_Perregaux:BOOLEAN',
                         'Audemars_Piguet:BOOLEAN',
                         'A_Lange_Sahne:BOOLEAN',
                         'service:BOOLEAN',
                         'cash4gold:BOOLEAN',
                         'electronics:BOOLEAN',
                         'repair:BOOLEAN',
                         'fourteen_k_gold:BOOLEAN',
                         'Patek_Philippe:BOOLEAN',
                         'diamond:BOOLEAN',
                         'Rolex:BOOLEAN',
                         'cash:BOOLEAN',
                         'Blancpain:BOOLEAN',
                         'Cartier:BOOLEAN',
                         'Jaeger_LeCoultre:BOOLEAN',
                         'gold:BOOLEAN',
                         'fourteen_carrat:BOOLEAN',
                         'twentyfour_carrat:BOOLEAN',
                         ]

            for col_type in cols_list:
                col_type_split = col_type.split(':')
                if col_type_split[1] == 'STRING':
                    df[col_type_split[0]] = 'None'
                if col_type_split[1] == 'BOOLEAN':
                    df[col_type_split[0]] = False

            # if len(df) == 1:
            #     continue

            target_file = 'web_crawler_data/{save_date}/webcrawler_data_{domain}.json'.format(save_date=save_date,
                                                                                              domain=domain.replace('.',
                                                                                                                    '_'))
            if target_file in list_files:
                continue

            hook.upload(base_bucket, target_file,
                        df.to_json(orient='records', lines='\n', date_format='iso', double_precision=4), 'NA',
                        'application/json', False)

        return