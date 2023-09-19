import pandas as pd
import datetime
from fuzzysearch import find_near_matches
from fuzzywuzzy import process
import urllib.parse as urlparse
from web_crawler.web_crawler.jm_gcs_webcrawler import GCSHookWeb
from airflow.models import Variable
import io
import random
# from dags.dbhelper import DBHelper



class WebCrawlerPipeline(object):

    def __init__(self):
        self.field_names = []
        self.place_ids = []
        # self.db = DBHelper()
        self.domain = 'NA'


    def open_spider(self, spider):
        pass
            
    def process_item(self, item, spider):
        content = item
        THRESHOLD = 50

        self.place_ids = item['place_ids']
        self.field_names = item['field_names']

        print('*' * 20)
        print(self.place_ids)
        print('*' * 20)
        df = pd.DataFrame()
        counter = 0

        for place_id in self.place_ids:
            df.loc[counter, 'place_ids'] = place_id
        # df.loc[counter, 'site'] = site
        for col in self.field_names:
            if col == 'start_url':
                continue
            df[col] = False

        df['update_time'] = datetime.datetime.now()

        domain = urlparse.urlparse(item['base_url']).netloc
        if domain.startswith('ww'):
            domain = domain.split(".", 1)[1]

        self.domain = domain
        df['domain'] = domain
        df['start_url'] = item['base_url']

        print('=' * 20)
        print(df)
        print('=' * 20)

        #####
        site = 'NA'
        df = self.process_social_media(item, site,df)
        for word in self.field_names:
            result = self.fuzzy_extract(word.lower(), item['url_content'], THRESHOLD)
            for match,index in result:
                if(match != None):
                    # self.db.update_value(word, True, site)
                    df[word] = True


        new_cols = []
        for col in df.columns:
            if col == '24k gold':
                new_cols.append('twentyfour_k_gold')
            elif col == '24 carrat':
                new_cols.append('twentyfour_carrat')
            elif col == '14k gold':
                new_cols.append('fourteen_k_gold')
            elif col == '14 carrat':
                new_cols.append('fourteen_carrat')
            elif col == 'Breguet & Fils':
                new_cols.append('Breguet_Fils')
            elif col == 'A. Lange & Söhne':
                new_cols.append('A_Lange_Sahne')
            else:
                new_cols.append(col.replace(' ','_').replace('-','_'))

        df.columns = new_cols

        cols_order = ['place_ids','domain','update_time','start_url']
        for col in df.columns:
            if col not in cols_order:
                cols_order.append(col)


        df = df[cols_order]
        df['spider_closed'] = False
        save_date = datetime.datetime.now().strftime("%Y%m%d")
        save_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        save_datetime = 'test'
        target_file = 'web_crawler_data/{save_date}/webcrawler_data_{domain}.json'.format(
            save_date=save_date, domain=self.domain.replace('.', '_'))
        self.upload_to_gcs(target_file, df_enable=True, df=df)





    def fuzzy_extract(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=1):
                match = word[match.start:match.end]
                index = ls.find(match)
                yield (match, index)

    def process_social_media(self, item, site,df):
        social_media_list = ['facebook','instagram','pinterest','youtube','twitter']
        for social_media in social_media_list:
            if (len(item[social_media]) > 0):
                if (isinstance(item[social_media], list)):
                    df[social_media] = item[social_media][0]
                else:
                    df[social_media] = item[social_media]

                df[social_media] = df[social_media].str.encode('ascii', 'ignore').str.decode('ascii')
            else:
                df[social_media] = 'None'

        
        # if (len(item['facebook']) > 0):
        #     if(isinstance(item['facebook'], list)):
        #         df['facebook'] = item['facebook'][0]
        #         # self.db.update_value('facebook', item['facebook'][0], site)
        #     else:
        #         df['facebook'] = item['facebook']
        #         # self.db.update_value('facebook', item['facebook'], site)
        #
        # if (len(item['instagram']) > 0):
        #     if(isinstance(item['instagram'], list)):
        #         df['instagram'] = item['instagram'][0]
        #         # self.db.update_value('instagram', item['instagram'][0], site)
        #     else:
        #         df['instagram'] = item['instagram']
        #         # self.db.update_value('instagram', item['instagram'], site)
        #
        # if (len(item['pinterest']) > 0):
        #     if(isinstance(item['pinterest'], list)):
        #         df['pinterest'] = item['pinterest'][0]
        #         # self.db.update_value('pinterest', item['pinterest'][0], site)
        #     else:
        #         df['pinterest'] = item['pinterest']
        #         # self.db.update_value('pinterest', item['pinterest'], site)
        #
        # if (len(item['youtube']) > 0):
        #     if(isinstance(item['youtube'], list)):
        #         df['youtube'] = item['youtube'][0]
        #         # self.db.update_value('youtube', item['youtube'][0], site)
        #     else:
        #         df['youtube'] = item['youtube']
        #         # self.db.update_value('youtube', item['youtube'], site)
        #
        #
        # if (len(item['twitter']) > 0):
        #     if(isinstance(item['twitter'], list)):
        #         df['twitter'] = item['twitter'][0]
        #         # self.db.update_value('twitter', item['twitter'][0], site)
        #     else:
        #         df['twitter'] = item['twitter']
        #         # self.db.update_value('twitter', item['twitter'], site)

        return df

    def upload_to_gcs(self,object_name, df_enable=False, df=pd.DataFrame(), raw_data=None):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
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


        hook = GCSHookWeb(
            google_cloud_storage_conn_id=base_gcp_connector,
            delegate_to=None)

        if df_enable:
            hook.upload(base_bucket, object_name,
                        df.to_json(orient='records', lines='\n', date_format='iso', double_precision=4), 'NA',
                        'application/json',False)
        elif raw_data != None:
            # This is for writing rawdata
            hook.upload(base_bucket, object_name, raw_data, 'NA',
                        'application/json',
                        False)
        else:
            return



    def close_spider(self, spider):
        ##
        AIRFLOW_ENV = Variable.get('ENV')

        if AIRFLOW_ENV.lower() != 'prod':
            base_bucket = 'jm-edl-landing-wip'
            project = 'jm-dl-landing'
            base_gcp_connector = 'jm_landing_dev'
        elif AIRFLOW_ENV.lower() == 'prod':
            base_bucket = 'jm-edl-landing-prod'
            project = 'jm-dl-landing'
            base_gcp_connector = 'jm_landing_prod'
        else:
            print('Environment is not available please address....')
            raise


        hook = GCSHookWeb(
            google_cloud_storage_conn_id=base_gcp_connector,
            delegate_to=None)
        save_date = datetime.datetime.now().strftime("%Y%m%d")

        try:
            target_file = 'web_crawler_data/{save_date}/webcrawler_data_{domain}.json'.format(
                save_date=save_date, domain=self.domain.replace('.', '_'))
            data = hook.download(base_bucket, target_file)
            file_stream = io.BufferedReader(io.BytesIO(data))
            df = pd.read_json(file_stream, orient='records', lines='\n')
            df['spider_closed'] = True

            self.upload_to_gcs(target_file, df_enable=True, df=df)
        except:
            print('Pipelines did not run due to no webpage crawled.')

        # site = self.db.get_store_by_place_id(self.place_ids)
        # self.db.set_true("spider_closed", site)
        # del self.db
        return
