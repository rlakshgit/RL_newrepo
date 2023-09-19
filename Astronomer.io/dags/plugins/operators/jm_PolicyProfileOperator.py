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

import json
import decimal
import sys
from airflow import configuration
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_gcs import GCSHook
from plugins.hooks.jm_mssql import MsSqlHook
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from plugins.hooks.jm_bq_hook import BigQueryHook
from plugins.hooks.jm_bq_hook_v2 import JMBQHook
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import io
import datetime as dt

class policy_profile():
    def __init__(self):
        pass
    def keyword_return(self,x, keyword_dict, default_value):
        try:
            x = x.upper()
        except:
            return default_value

        try:
            x = int(x)
            return default_value
        except:
            pass

        if str(x) == 'nan':
            return default_value

        counter_df = pd.DataFrame()
        for key, value in keyword_dict.items():
            total_count = 0
            for v in value:
                get_count = x.count(key) + x.count(v.upper())
                total_count += get_count
            get_len = len(counter_df)
            counter_df.loc[get_len, 'CLASSIFICATION'] = key
            counter_df.loc[get_len, 'COUNTS'] = total_count

        get_max = counter_df[(counter_df['COUNTS'] == counter_df['COUNTS'].max())]
        if counter_df['COUNTS'].max() == 0:
            return default_value
        if len(get_max) == 1:
            get_max.reset_index(drop=True, inplace=True)
            return get_max.loc[0, 'CLASSIFICATION']
        else:
            get_max.reset_index(drop=True, inplace=True)
            return_string = ''
            item_count = 0
            for item in get_max['CLASSIFICATION'].values.tolist():
                if item == 'UNKNOWN':
                    continue
                if item_count == 0:
                    return_string = item
                    item_count += 1
                else:
                    return_string = return_string + '_' + item

            return return_string
    def state_classifying(self,df_bqt, target_col):
        keyword_dict = {
            'FL': 'FL',
            'CA': 'CA',
            'NY': 'NY',
            'TX': 'TX',
            'NJ': 'NJ',
            'LA': 'LA',
            'UT': 'MidWest',
            'CO': 'MidWest',
            'AZ': 'MidWest',
            'NM': 'MidWest',
            'MT': 'MidWest',
            'WY': 'MidWest',
            'MI': 'North',
            'IL': 'North',
            'WI': 'North',
            'MN': 'North',
            'NE': 'North',
            'IN': 'North',
            'IA': 'North',
            'SD': 'North',
            'ND': 'North',
            'PA': 'NorthEast',
            'MD': 'NorthEast',
            'MA': 'NorthEast',
            'CT': 'NorthEast',
            'VA': 'NorthEast',
            'OH': 'NorthEast',
            'KY': 'NorthEast',
            'RI': 'NorthEast',
            'NH': 'NorthEast',
            'DE': 'NorthEast',
            'DC': 'NorthEast',
            'ME': 'NorthEast',
            'WV': 'NorthEast',
            'VT': 'NorthEast',
            'MO': 'South',
            'KS': 'South',
            'OK': 'South',
            'AR': 'South',
            'MS': 'South',
            'GA': 'SouthEast',
            'NC': 'SouthEast',
            'SC': 'SouthEast',
            'AL': 'SouthEast',
            'TN': 'SouthEast',
            'AP': 'SouthEast',
            'AE': 'SouthEast',
            'PR': 'SouthEast',
            'VI': 'SouthEast',
            'WA': 'West',
            'NV': 'West',
            'OR': 'West',
            'HI': 'West',
            'ID': 'West',
            'AK': 'West',
        }

        # default_value = "IDK"
        # df_bqt['region'] = df_bqt[target_col].apply(self.keyword_return, args=(keyword_dict, default_value))
        df_in_partial_list = pd.DataFrame()
        df_in_partial_list[target_col] = df_bqt[target_col].unique()
        df_in_partial_list['region'] = df_in_partial_list[target_col].map(keyword_dict)
        df_in_partial_list['region'] = df_in_partial_list['region'].fillna("IDK")


        keyword_dict = {
            'FL' : 'b00',
            'CA' : '03',
            'NY' : '01',
            'TX' : '02',
            'NJ' : '04',
            'LA' : '05',
            'MidWest' : '06',
            'North' : '07',
            'South' : '09',
            'NorthEast' : '08',
            'SouthEast' : '10',
            'West' : '11',
        }

        default_value = "IDK"
        df_in_partial_list['region_index'] = df_in_partial_list['region'].map(keyword_dict)
        df_in_partial_list['region_index'] = df_in_partial_list['region_index'].astype(str) + '>' + df_in_partial_list['region']

        df_bqt = pd.merge(df_bqt, df_in_partial_list, on=[target_col], how='left')
        df_bqt.reset_index(drop=True, inplace=True)

        return df_bqt
    def extract_policy_year(self,x):
        year_extracted = str(x)[:4]
        if int(year_extracted) == 2018:
            return 2017
        else:
            return(int(year_extracted))
    def type_classification(self,df_in):
        keyword_dict = {
                        'Engagement Ring': ['Engagement Ring', 'Ladies Engagement Ring', 'Gents Engagement Ring', 'Ladies Engagement',
                                            'Gents Engagement'],
                        'Bracelet': ['Bracelet', 'Ladies Bracelet', 'Gents Bracelet'],
                        'Gents Watch': ['Gents Watch', 'Gents Rolex Watch', 'Gents Smart Watch'],
                        'Ladies Watch': ['Ladies Watch', 'Ladies Rolex Watch', 'Ladies Smart Watch', 'Ladies Rolex Watc',
                                         'Ladies Smart Watc'],
                        'Chain': ['Chain', 'Gents Chain', 'Ladies Chain'],
                        'Wed Set': ['Wed Set', 'Ladies Wedding Band', 'Ladies Wedding Set', 'Gents Wedding Band', 'Gents Wedding Set',
                                    'Ladies Wedding Ba', 'Ladies Wedding Se', 'Gents Wedding Ban'],
                        'Other': ['Other', '?', 'Brooch'],
                        'Stone_SingER': ['Loose Stone', 'Single Earring'],
                        'Ladies Ring': ['Ladies Ring'],
                        'Pair Earrings': ['Pair Earrings'],
                        'Combination': ['Combination'],
                        'Gents Ring': ['Gents Ring'],
                        'Pendant': ['Pendant']
                    }

        df_in_partial_list = pd.DataFrame()
        df_in_partial_list['PolicyEffectiveDate'] = df_in['PolicyEffectiveDate'].unique()
        df_in_partial_list['policy_year2'] = df_in_partial_list['PolicyEffectiveDate'].apply(self.extract_policy_year)
        #df_in['policy_year2'] = df_in['PolicyEffectiveDate'].apply(self.extract_policy_year)

        df_in = pd.merge(df_in,df_in_partial_list,on=['PolicyEffectiveDate'],how='left')
        df_in.reset_index(drop=True,inplace=True)
        #print(df_in)

        df_in_partial_list = pd.DataFrame()
        df_in_partial_list['Risk_State'] = df_in['Risk_State'].unique()
        df_in_partial_list = self.state_classifying(df_in_partial_list, 'Risk_State')
        df_in = pd.merge(df_in, df_in_partial_list, on=['Risk_State'], how='left')
        df_in.reset_index(drop=True, inplace=True)
        #print(df_in)

        df_in_partial_list = pd.DataFrame()
        df_in_partial_list['ItemClassDescription'] = df_in['ItemClassDescription'].unique()
        df_in_partial_list['type_classifier_base'] = df_in_partial_list['ItemClassDescription'].apply(
            self.keyword_return,
            args=(keyword_dict, 'UNK'))

        df_in = pd.merge(df_in, df_in_partial_list, on=['ItemClassDescription'], how='left')
        df_in.reset_index(drop=True, inplace=True)



        #sys.exit(10)

        # df_in = self.state_classifying(df_in, 'Risk_State')
        # df_in['type_classifier_base'] = df_in['ItemClassDescription'].apply(self.keyword_return,args=(keyword_dict, 'UNK'))

        keyword_dict = {
                        'Engagement Ring': 'b00',
                        'Bracelet': '03',
                        'Gents Watch': '07',
                        'Ladies Watch': '08',
                        'Chain': '10',
                        'Wed Set': '12',
                        'Other': '11',
                        'Stone_SingER': '13',
                        'Ladies Ring': '01',
                        'Pair Earrings': '02',
                        'Combination': '04',
                        'Gents Ring': '05',
                        'Pendant': '09',
        }

        default_value = "IDK"
        df_in['type_class_base_index'] = df_in['type_classifier_base'].map(keyword_dict)
        df_in['type_class_base_index'] = df_in['type_class_base_index'].astype(str) + '>' + df_in['type_classifier_base']




        keyword_grouped = { 'Wed Rings' : ['Engagement Ring','Wed Set'],
                            'Ladies Item': ['Ladies Ring', 'Ladies Watch'],
                            'Gents Items': ['Gents Ring', 'Gents Watch','Chain'],
                            'Other': ['Pair Earrings', 'Bracelet', 'Combination','Necklace','Pendant','Other','Stone_SingER'],
                        }

        df_in_partial_list = pd.DataFrame()
        df_in_partial_list['type_classifier_base'] = df_in['type_classifier_base'].unique()
        df_in_partial_list['type_classifier'] = df_in_partial_list['type_classifier_base'].apply(self.keyword_return, args=(keyword_grouped, 'IDK'))
        #df_in['type_classifier'] = df_in['type_classifier_base'].apply(self.keyword_return, args=(keyword_grouped, 'IDK'))

        df_in = pd.merge(df_in, df_in_partial_list, on=['type_classifier_base'], how='left')
        df_in.reset_index(drop=True, inplace=True)

        return df_in
    def segmentation_assignment_total_value(self,df_in):
        df_in['total_item_count'] = df_in['total_item_count'].astype(int)
        df_itemone = df_in[(df_in['total_item_count'] <= 1)]
        df_itemmulti = df_in[(df_in['total_item_count'] > 1)]


        item_one_group_values = [-1,0,249,499,749,999,1249,1499,1749,1999,
                                 2499,2999,3499,3999,4499,4999,5999,6999,7999,9999,
                                 11999,13999,15999,19999,29999,49999,9999999999999]

        item_one_group_value_labels = ['ND','1-250','250-500','500-750','750-1K','1K-1.25K','1.25K-1.5K',
                                       '1.5K-1.75K','1.75K-2K','2K-2.5K','2.5K-3K','3K-3.5K','3.5K-4K',
                                       '4K-4.5K','4.5K-5K','5K-6K','6K-7K','7K-8K','8K-10K','10K-12K','12K-14K',
                                       '14K-16K','16K-20K','20K-30K','30K-50K','50K+']

        item_one_group_index_labels = ['ND','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17',
                                       'b18','19','20','21','22','23','24','25']

        # print(len(item_one_group_values),len(item_one_group_value_labels),len(item_one_group_index_labels))

        df_itemone['agg_total_value_bin'] = pd.cut(df_itemone['policy_item_value_sum'].astype(float), item_one_group_values,
                                               labels=item_one_group_value_labels)


        df_itemone['agg_total_value_label'] = pd.cut(df_itemone['policy_item_value_sum'].astype(float), item_one_group_values,
                                   labels=item_one_group_index_labels)





        item_two_group_values = [-1,0,499,999,1499,1999,
                                 2999,4499,5999,7999,
                                 11999,15999,29999,999999999999]

        item_two_group_value_labels = ['ND','1-500','500-1K','1-1.5K','1.5-2K','2-3K',
                                       '3-4.5K','4.5-6K','6-8K','8-12K',
                                       '12-16K','16-30K','30K+']

        item_two_group_index_labels = ['ND','1','2','3','4','5','b06','7','8','9','10','11','12']

        # print(len(item_two_group_values),len(item_two_group_value_labels),len(item_two_group_index_labels))



        df_itemmulti['agg_total_value_bin'] = pd.cut(df_itemmulti['policy_item_value_sum'].astype(float), item_two_group_values,
                                               labels=item_two_group_value_labels)


        df_itemmulti['agg_total_value_label'] = pd.cut(df_itemmulti['policy_item_value_sum'].astype(float), item_two_group_values,
                                   labels=item_two_group_index_labels)



        df_out = pd.concat([df_itemone,df_itemmulti])
        df_out.reset_index(drop=True,inplace=True)
        return df_out
    def per_item_assignment(self,df_in):
        item_groups = [-1, 0, 499, 999, 1499, 1999, 2999, 4499, 5999, 7999, 11999, 15999, 29999, 999999999999]

        item_groups_labels = ['ND', '1-500', '500-1K', '1-1.5K', '1.5-2K', '2-3K',
                              '3-4.5K', '4.5-6K', '6-8K', '8-12K',
                              '12-16K', '16-30K', '30K+']

        item_groups_index_labels = [-1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        #print(len(item_groups), len(item_groups_labels), len(item_groups_index_labels))

        df_in['item_value_bin'] = pd.cut(df_in['ItemValue'].astype(float), item_groups,
                                         labels=item_groups_labels)

        df_in['item_value_label_base'] = pd.cut(df_in['ItemValue'].astype(float), item_groups,
                                                labels=item_groups_index_labels)

        type_map_dict = {'Wed Rings': 0,
                         'Ladies Item': 1,
                         'Other': 2,
                         'Gents Items': 3,
                         'IDK': -1}

        df_in['ivl_multiplier1'] = df_in['type_classifier'].map(type_map_dict)
        df_in['ivl_multiplier2'] = ((12 * df_in['ivl_multiplier1'].astype(int)))
        df_in['item_value_label'] = df_in['item_value_label_base'].astype(int) + df_in['ivl_multiplier2'].astype(int)
        del df_in['ivl_multiplier1'],df_in['ivl_multiplier2']
        return df_in
    def total_item_count_classification(self,submission_df):
        item_groups = [8, 9, 11, 13, 17, 24, 25]

        item_groups_labels = ['NA', '9-10', '11-13', '14-17', '18-24', '25+']

        item_groups_index_labels = [-1, 9, 10, 11, 12, 13]


        submission_df['item_count_bin'] = pd.cut(submission_df['total_item_count'].astype(float), item_groups,
                                                 labels=item_groups_labels)
        submission_df['item_count_bin'] = submission_df['item_count_bin'].fillna('NA')
        submission_df['item_count_bin'] = np.where(submission_df['item_count_bin'] == 'NA',
                                                   submission_df['total_item_count'], submission_df['item_count_bin'])

        submission_df['item_count_bin_label'] = pd.cut(submission_df['total_item_count'].astype(float), item_groups,
                                                       labels=item_groups_index_labels)
        submission_df['item_count_bin_label'] = submission_df['item_count_bin_label'].fillna(-1)
        submission_df['item_count_bin_label'] = np.where(submission_df['item_count_bin_label'] == -1,
                                                         submission_df['total_item_count'].astype(int),
                                                         submission_df['item_count_bin_label'])





        return submission_df
    def policy_value_assignment_total_value(self,df_in):
        df_itemone = df_in[(df_in['total_item_count'] <= 1)]
        df_itemmulti = df_in[(df_in['total_item_count'] > 1)]


        item_one_group_values = [-1, 0, 499, 999, 1499, 1999,
                                 2999, 4499, 5999, 9999999999]

        item_one_group_value_labels = ['ND', '1-500', '500-1K', '1-1.5K', '1.5-2K', '2-3K',
                                       '3-4.5K', '4.5-6K', '6K+']

        item_one_group_index_labels = ['ND', '1', '2', '3', '4', '5', '6', '7', 'b08']

        # print(len(item_one_group_values),len(item_one_group_value_labels),len(item_one_group_index_labels))

        df_itemone['final_total_value_bin'] = pd.cut(df_itemone['policy_item_value_sum'].astype(float), item_one_group_values,
                                               labels=item_one_group_value_labels)


        df_itemone['final_total_value_label'] = pd.cut(df_itemone['policy_item_value_sum'].astype(float), item_one_group_values,
                                   labels=item_one_group_index_labels)

        df_itemone['final_total_value_counter'] = 1

        item_two_group_values = [-1, 0, 2499,
                                 4499, 5999, 7999, 9999,
                                 15999, 19999, 24999, 29999, 39999, 999999999999]

        item_two_group_value_labels = ['ND', '0-2.5K', '2.5-4.5K', '4.5-6K', '6-8K', '8-10K',
                                       '10-16K', '16-20K', '20-25K', '25-30K',
                                       '30-40K', '40K+']

        item_two_group_index_labels = ['ND', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

        # print(len(item_two_group_values),len(item_two_group_value_labels),len(item_two_group_index_labels))



        df_itemmulti['final_total_value_bin'] = pd.cut(df_itemmulti['policy_item_value_sum'].astype(float), item_two_group_values,
                                               labels=item_two_group_value_labels)


        df_itemmulti['final_total_value_label'] = pd.cut(df_itemmulti['policy_item_value_sum'].astype(float), item_two_group_values,
                                   labels=item_two_group_index_labels)

        df_itemmulti['final_total_value_counter'] = 2


        df_out = pd.concat([df_itemone,df_itemmulti])
        df_out.reset_index(drop=True,inplace=True)
        return df_out
    def age_groups(self,submission_df):
        item_group_values = [-1, 18, 24, 39, 49, 59,
                             69, 9999999999]

        item_group_value_labels = ['miss', '18-24', '25-39', '40-49', '50-59', '60-69',
                                   '70+']

        item_group_index_labels = [7, 1, 2, 3, 4, 5, 6]

        submission_df['first_pass_age_grouping'] = pd.cut(submission_df['Insured_Age'].astype(float), item_group_values,
                                                          labels=item_group_value_labels)

        submission_df['first_pass_age_grouping_label'] = pd.cut(submission_df['Insured_Age'].astype(float),
                                                                item_group_values,
                                                                labels=item_group_index_labels)

        submission_df['final_item_type_classifier'] = submission_df['first_pass_age_grouping_label'].copy(deep=True)
        submission_df['final_item_type_classifier'] = np.where(submission_df['type_classifier'] == 'Ladies Item',
                                                               submission_df['final_item_type_classifier'].astype(
                                                                   float) + 7, submission_df['final_item_type_classifier'])
        submission_df['final_item_type_classifier'] = np.where(submission_df['type_classifier'] == 'Other',
                                                               submission_df['final_item_type_classifier'].astype(
                                                                   float) + 14, submission_df['final_item_type_classifier'])
        submission_df['final_item_type_classifier'] = np.where(submission_df['type_classifier'] == 'Gents Items',
                                                               submission_df['final_item_type_classifier'].astype(
                                                                   float) + 21, submission_df['final_item_type_classifier'])




        return submission_df
    def target_item_check(self,submission_df):
        # Specific item checks
        submission_df['item_and_policy_count_index_label'] = np.where(((submission_df[
                                                                            'type_classifier_base'] == 'Engagement Ring') | (
                                                                               submission_df[
                                                                                   'type_classifier_base'] == 'Ladies Ring')),
                                                                      submission_df['item_count_bin_label'],
                                                                      submission_df['item_count_bin_label'].astype(
                                                                          int) + 13)
        submission_df['item_and_policy_count_index'] = np.where(((submission_df[
                                                                      'type_classifier_base'] == 'Engagement Ring') | (
                                                                             submission_df[
                                                                                 'type_classifier_base'] == 'Ladies Ring')),
                                                                '1+LR', submission_df['item_count_bin'])

        # Per item grouped with total policy value
        submission_df['item_value_label_sub_group'] = ''
        grouping_list = ['12', '24', '36', '48']
        for label in grouping_list:
            submission_df['item_value_label_sub_group'] = np.where(((submission_df['item_value_label'].astype(
                str) == label) & (submission_df['agg_total_value_label'].astype(str) == '24')), label + '>a',
                                                                   submission_df['item_value_label_sub_group'])
            submission_df['item_value_label_sub_group'] = np.where(
                ((submission_df['item_value_label'].astype(str) == label) & (
                            submission_df['agg_total_value_label'].astype(str) == '25')), label + '>b',
                submission_df['item_value_label_sub_group'])

        def grouper_string(x):
            if len(str(x['item_value_label'])) == 1:
                if str(x['item_value_label']) == '6':
                    x['item_value_label_sub_group'] = 'b0' + str(x['item_value_label']) + '>'
                else:
                    x['item_value_label_sub_group'] = '0' + str(x['item_value_label']) + '>'
            else:
                x['item_value_label_sub_group'] = str(x['item_value_label']) + '>'

            x['item_value_label_sub_group'] = x['item_value_label_sub_group'] + x['type_classifier'] + ':' + x[
                'item_value_bin']

            if x['type_classifier'] == 'IDK':
                x['item_value_label_sub_group'] = '99>IDK'

            return x

        #submission_df = submission_df.apply(grouper_string,axis=1)

        submission_df['item_value_label'] = submission_df['item_value_label'].astype(str)
        submission_df['item_value_label_sub_group'] = np.where(((submission_df['item_value_label'].str.len() == 1) & (
                    submission_df['item_value_label'].astype(str) == '6')),
                                                               'b0' + submission_df['item_value_label'] + '>',
                                                               submission_df['item_value_label'] + '>')

        submission_df['item_value_label_sub_group'] = submission_df['item_value_label_sub_group'].astype(str) + submission_df['type_classifier'].astype(str) + ':' + submission_df['item_value_bin'].astype(str)

        submission_df['item_value_label_sub_group'] = np.where(submission_df['type_classifier'].astype(str) == 'IDK','99>IDK',submission_df['item_value_label_sub_group'])

        def group_keyword(x):
            keyword_dict = {
                '1+LR': 'b01>1+LR',
                '1': '14>1',
                '2': '15>2',
                '3': '16>3',
                '4': '17>4',
                '5': '18>5',
                '6': '19>6',
                '7': '20>7',
                '8': '21>8',
                '9-10': '22>9-10',
                '11-13': '23>11-13',
                '14-17': '24>14-17',
                '18-24': '25>18-24',
                '25+': '26>25+',
            }
            return keyword_dict.get(str(x))

        submission_df['item_and_policy_count_index_mapped'] = submission_df['item_and_policy_count_index'].apply(group_keyword)
        return submission_df
    def setup_classifications(self,submission_df):
        def age_grouper(x):

            age_split = str(x['final_item_type_classifier']).split('.')
            x['final_item_type_classifier'] = str(age_split[0])


            if len(str(x['final_item_type_classifier'])) == 1:
                if str(x['final_item_type_classifier']) == '2':
                    x['age_group_index'] = 'b02>' + x['type_classifier'] + ':' + x[
                        'first_pass_age_grouping']
                else:
                    x['age_group_index'] = '0' + str(x['final_item_type_classifier']) + '>' + x['type_classifier'] + ':' + str(x[
                        'first_pass_age_grouping'])

            else:
                x['age_group_index'] = str(x['final_item_type_classifier']) + '>' + x['type_classifier'] + ':' + str(x['first_pass_age_grouping'])

            return x


        submission_df['final_total_value_label'] = submission_df['final_total_value_label'].astype(str)
        submission_df['final_item_type_classifier'] = submission_df['final_item_type_classifier'].astype(str)
        submission_df['age_group_index'] = np.where(((submission_df['final_item_type_classifier'].str.len() == 1) & (submission_df['final_item_type_classifier'].astype(str) == '2')),
                                                    'b02>' + submission_df['type_classifier'].astype(str) + ':' + submission_df['first_pass_age_grouping'].astype(str),
                                                    '0' + submission_df['final_item_type_classifier'].astype(str) + '>' + submission_df['type_classifier'].astype(str) + ':' + submission_df['first_pass_age_grouping'].astype(str))

        submission_df['age_group_index'] = np.where(((submission_df['final_item_type_classifier'].str.len() != 1)),
                                                    submission_df['final_item_type_classifier'].astype(str) + '>' + submission_df['type_classifier'].astype(str) + ':' + submission_df['first_pass_age_grouping'].astype(str),
                                                    submission_df['age_group_index'])

        submission_df['policy_item_index'] = np.where(submission_df['final_total_value_label'].str.len()==1,
                                                      '0' + submission_df['final_total_value_label'].astype(str) + '>i' + submission_df['final_total_value_counter'].astype(str) + ' ' + submission_df['final_total_value_bin'].astype(str),
                                                      submission_df['final_total_value_label'].astype(str) + '>i' + submission_df['final_total_value_counter'].astype(str) + ' ' + submission_df['final_total_value_bin'].astype(str))


        return submission_df

class PolicyProfileGetAccountListMssql(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#57d3e6'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 *args,
                 **kwargs):

        super(PolicyProfileGetAccountListMssql, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count

    def execute(self, context):
        self.sql = '''SELECT DISTINCT
                        dAccount.AccountNumber
                        FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
                        INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAccount
                        ON dAccount.AccountKey = dPol.AccountKey
            '''
        account_cursor = self._query_mssql()
        account_values = account_cursor.fetchall()
        acount_number_df = pd.DataFrame(account_values,columns=['AccountNumber'])
        acount_number_df.sort_values(by=['AccountNumber'], ascending=[True], inplace=True)
        acount_number_df.reset_index(drop=True, inplace=True)
        df_split = np.array_split(acount_number_df, self.chunk_count)
        json_metadata = {}
        group_counter = 0
        for df in df_split:
            df['AccountNumber'] = df['AccountNumber'].str.strip()
            acc_list = "','".join(map(str, df['AccountNumber'].values.tolist()))
            acc_list = "'" + acc_list + "'"
            json_metadata.update({group_counter:str(acc_list)})
            group_counter += 1
        self._account_list_upload(context,json_metadata)
        return

    def _account_list_upload(self, context,json_metadata):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        #df = df.transpose()
        print(df)
        gcs_hook.upload(self.bucket,
                        self.filename.format(ds_nodash=context['ds_nodash']),
                        df.to_json(orient='records', lines='\n'))
        return

    def _query_mssql(self, sql='NA', database='NA'):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn(database=database)
        cursor = conn.cursor()
        if sql == 'NA':
            cursor.execute(self.sql)
            # print(self.sql)
        else:
            cursor.execute(sql)
            # print(sql)
        return cursor


class PolicyProfileGetAccountDataMssql(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#f0c92e'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 *args,
                 **kwargs):

        super(PolicyProfileGetAccountDataMssql, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value

    def execute(self, context):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        file_data = gcs_hook.download(self.bucket, object=self.filename.format(ds_nodash=context['ds_nodash']))
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df_temp = pd.read_json(file_stream, orient='records', lines='\n')
        df_temp.columns = ['AcctNumberList']
        # print(df_temp)
        # print(df_temp.loc[self.chunk_value,'AcctNumberList'])

        self.sql = sql = '''
                        ;WITH DETAILS AS 
                (
                SELECT DISTINCT
                    dAccount.AccountNumber
                    ,dpol.PolicyNumber
                    ,dPol.JobNumber
                    ,dPol.PolicyEffectiveDate																			
                    ,dPol.PolicyStatus
                    ,dPol.SourceOfBusiness
                    ,dPGeo.GeographyStateCode												AS Primary_State
                    ,dPCont.PrimaryAddressPostalCode
                    ,dPCont.MailingAddressPostalCode
                    ,COALESCE(DATEDIFF(YEAR,dPCont.BirthDate,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))
                                ,DATEDIFF(YEAR,dACont.BirthDate,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate)),'') AS Insured_Age
                    ,dRSJ.ItemNumber														
                    ,dIC.ItemClassDescription												
                    ,COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0)						AS ItemValue
                    ,COALESCE(riskGeo.GeographyStateCode,'')								AS Risk_State
                    ,COALESCE(wearerCont.PrimaryAddressPostalCode,'')						AS LocationPostalCode
                    ,COALESCE(riskGeo.GeographyCountryCode,'')								AS Risk_Country
                    ,ISNULL(fQuote.QuoteDeclinationReason,'') AS DeclinationReason
                    ,CASE WHEN fQuote.QuoteDeclined = 1 OR dPol.PolicyStatus = 'Declined' THEN 1 ELSE 0 END AS Declined
                    ,DENSE_RANK() OVER(PARTITION BY dPol.JobNumber
                                        ORDER BY	CASE 
                                                        WHEN dPol.PolicyStatus = 'Bound' THEN 1 
                                                        ELSE 0 
                                                        END DESC
                                                    ,CAST(dPol.PolicyVersion AS INT) 
                                                    ,dPol.JobCloseDate DESC
                                                    ,dPol.PolicyWrittenDate DESC
                                                    ,dPol.JobSubmissionDate DESC
                                                    ,CASE
                                                        WHEN dPol.PolicyStatus = 'Bound' THEN 0
                                                        WHEN dPol.PolicyStatus = 'Renewing' THEN 1 
                                                        WHEN dPol.PolicyStatus = 'Quoted' THEN 2
                                                        WHEN dPol.PolicyStatus = 'Draft' THEN 3
                                                        WHEN dPol.PolicyStatus = 'NonRenewed' THEN 4
                                                        WHEN dPol.PolicyStatus = 'Declined' THEN 5
                                                        WHEN dPol.PolicyStatus = 'NotTaken' THEN 6
                                                        WHEN dPol.PolicyStatus = 'Expired' THEN 7
                                                        WHEN dPol.PolicyStatus = 'Withdrawn' THEN 8
                                                        ELSE 99
                                                        END ASC		
                                                    ,dPol.PolicyKey desc
                                        ) AS JOB_RANK
                FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAccount
                    ON dAccount.AccountKey = dPol.AccountKey
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact dPCont
                    ON dPCont.ContactKey = dPol.PolicyInsuredContactKey 
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography dPGeo
                    ON dPGeo.GeographyKey = dPCont.PrimaryAddressGeographyKey 
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dACont
                    ON dAccount.AccountHolderContactKey = dACont.ContactKey
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentCore AS dRSC
                    ON dRSC.PolicyKey = dPol.PolicyKey
                    LEFT JOIN DW_DDS_CURRENT.bi_dds.DimRiskSegmentJewelryItem AS dRSJ
                    ON dRSJ.RiskSegmentKey = dRSC.RiskSegmentJewelryItemKey 
                    AND dRSC.RiskType = 'ITEM'
                    LEFT JOIN DW_DDS_CURRENT.bi_dds.DimItemClass AS dIC
                    ON dIC.ItemClassKey = dRSJ.ItemClassKey
                    LEFT JOIN DW_DDS_CURRENT.bi_dds.DimContact AS wearerCont
                    ON wearerCont.ContactKey = dRSJ.ItemLocatedWithContactKey
                    LEFT JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS riskGeo
                    ON dRSJ.ItemGeographyKey = riskGeo.GeographyKey
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverage AS dCov 
                    ON dCov.RiskSegmentKey = dRSC.RiskSegmentKey
                    AND CoverageKey<> - 1
                    INNER JOIN DW_DDS_CURRENT.bi_dds.DimCoverageType AS dCType
                    ON dCType.CoverageTypeKey = dCov.CoverageTypeKey
                    LEFT JOIN DW_DDS_CURRENT.bi_dds.FactQuote AS fQuote
                    ON fQuote.PolicyKey = dPol.PolicyKey
                WHERE	1=1
                        AND dPol.AccountSegment = 'Personal Lines'
                        AND dPGeo.GeographyCountryCode = 'USA'
                        AND dPol.JobNumber IS NOT NULL
                        --AND COALESCE(dCov.PerOccurenceLimit,dCov.ItemValue,0) <> 0
                        AND dCType.ConformedCoverageTypeCode = 'SCH'
                        AND dRSC.IsInactive = 0
                        AND dAccount.AccountNumber in ({acc_list})
                )
                SELECT 
                    * 
                    ,DENSE_RANK() OVER(PARTITION BY JobNumber
                                                    ,ItemNumber
                                        ORDER BY	ItemValue desc
                                        ) AS ITEM_RANK
                FROM DETAILS 
                WHERE JOB_RANK = 1
            '''.format(acc_list=df_temp.loc[self.chunk_value,'AcctNumberList'])

        # print(self.sql)

        cursor = self._query_mssql()
        colNameList = []
        for i in range(len(cursor.description)):
            desc = cursor.description[i]
            colNameList.append(desc[0])



        account_values = cursor.fetchall()
        df = pd.DataFrame(account_values)
        if len(df) == 0:
            print('*' * 20)
            print(df_temp.loc[self.chunk_value,'AcctNumberList'])
            print(self.sql)
            print('No Data Processed')
            print('*' * 20)
            return


        df.columns = colNameList

        integer_columns = ['Insured_Age','Declined','JOB_RANK','ITEM_RANK']
        numeric_columns = ['ItemValue','ItemNumber']
        for col in df.columns:
            print('Processing column....',col)
            if col in integer_columns:
                df[col] = df[col].astype(int)
                continue
            if col in numeric_columns:
                df[col] = df[col].astype(float)
                continue

            if 'Date' in col:
                try:
                    df[col] = pd.to_datetime(df[col],format='%Y%m%d')
                    continue
                except:
                    print('Col failed to be processed as date and time....',col)
                    pass

            df[col] = df[col].astype(str)


        self._account_data_upload(context,df)
        if self.chunk_value == 0:
            schema_data = self._generate_schema(df)
            self._upload_schema(schema_data,context)
        return

    def _account_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        extracted_data_file_name = self.extracted_data.format(ds_nodash=context['ds_nodash']) + 'chunk_{chunk_value}.json'.format(chunk_value=self.chunk_value)
        gcs_hook.upload(self.bucket,
                        extracted_data_file_name,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _query_mssql(self, sql='NA', database='NA'):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn(database=database)
        cursor = conn.cursor()
        if sql == 'NA':
            cursor.execute(self.sql)
            # print(self.sql)
        else:
            cursor.execute(sql)
            # print(sql)
        return cursor

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields


    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return


class PolicyProfileScoreGetData(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#a7dbaf'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_source_dataset_table={},
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 *args,
                 **kwargs):

        super(PolicyProfileScoreGetData, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.google_source_dataset_table = google_source_dataset_table

    def execute(self, context):
        print(self.google_source_dataset_table)
        self.sql="SELECT distinct(AccountNumber) from `{project}.{dataset}.{table}`".format(project=self.google_source_dataset_table['project'],
                                                                                            dataset=self.google_source_dataset_table['dataset'],
                                                                                            table=self.google_source_dataset_table['table'])

        bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn_id,use_legacy_sql=False)
        acount_number_df = bq_hook.get_pandas_df(sql=self.sql)
        acount_number_df.sort_values(by=['AccountNumber'], ascending=[True], inplace=True)
        acount_number_df.reset_index(drop=True, inplace=True)
        df_split = np.array_split(acount_number_df, self.chunk_count)
        json_metadata = {}
        group_counter = 0
        for df in df_split:
            df['AccountNumber'] = df['AccountNumber'].str.strip()
            acc_list = "','".join(map(str, df['AccountNumber'].values.tolist()))
            acc_list = "'" + acc_list + "'"
            json_metadata.update({group_counter:str(acc_list)})
            group_counter += 1
        print(json_metadata)
        self._account_list_upload(context,json_metadata)
        return

    def _account_list_upload(self, context,json_metadata):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        df = pd.DataFrame.from_dict(json_metadata, orient='index')
        #df = df.transpose()
        print(df)
        gcs_hook.upload(self.bucket,
                        self.filename.format(ds_nodash=context['ds_nodash']),
                        df.to_json(orient='records', lines='\n'))
        return


class PolicyProfileScoreData(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#71b0e3'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_source_dataset_table={},
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 *args,
                 **kwargs):

        super(PolicyProfileScoreData, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value
        self.google_source_dataset_table = google_source_dataset_table
        self.pp = policy_profile()

    def execute(self, context):

        print('Load Support Data...')
        support_df = self._get_support_data()
        print(support_df)

        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        file_data = gcs_hook.download(self.bucket, object=self.filename.format(ds_nodash=context['ds_nodash']))
        file_stream = io.BufferedReader(io.BytesIO(file_data))
        df_temp = pd.read_json(file_stream, orient='records', lines='\n')
        df_temp.columns = ['AcctNumberList']
        account_list = df_temp.loc[self.chunk_value,'AcctNumberList']

        #Get Item Counts.....
        bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn_id, use_legacy_sql=False)
        score_data_base = self._calculate_item_count(bq_hook,account_list)
        score_data_base = self._calculate_policy_data(score_data_base)
        score_data_base = self._calculate_territory_factors(score_data_base,support_df)

        # Assign types
        print('Start Type Classification....')
        score_data_base = self.pp.type_classification(score_data_base)
        # Assign group values based on total item count and value
        print('Start Segmentation Assignment....')
        score_data_base = self.pp.segmentation_assignment_total_value(score_data_base)
        # Per item segmentation
        print('Start Item Assignment....')
        score_data_base = self.pp.per_item_assignment(score_data_base)
        # Total item segmentation
        print('Start Item Count Classification....')
        score_data_base = self.pp.total_item_count_classification(score_data_base)
        # Final groupings based on total item value and total item values.
        print('Start Policy Value Assignment....')
        score_data_base = self.pp.policy_value_assignment_total_value(score_data_base)
        # Finalization of the age groups.
        print('Start Age Grouping....')
        score_data_base = self.pp.age_groups(score_data_base)
        # Target Item Check and Total Item Count versus Policy
        print('Start Item Check....')
        score_data_base = self.pp.target_item_check(score_data_base)
        # Additional setup functions
        print('Start Setup Classifications....')
        score_data_base = self.pp.setup_classifications(score_data_base)


        score_data_base,decile_ranking_df = self._factor_assignment(score_data_base)
        score_data_base = self._assign_decile_value(score_data_base,decile_ranking_df)
        del decile_ranking_df

        integer_columns = ['Insured_Age', 'Declined', 'JOB_RANK', 'ITEM_RANK']
        numeric_columns = ['ItemValue', 'ItemNumber']
        for col in score_data_base.columns:
            print('Processing column....', col)
            if col in integer_columns:
                score_data_base[col] = score_data_base[col].astype(int)
                continue
            if col in numeric_columns:
                score_data_base[col] = score_data_base[col].astype(float)
                continue

            score_data_base[col] = score_data_base[col].astype(str)

        print(score_data_base)
        self._account_data_upload(context,score_data_base)
        if self.chunk_value == 0:
            schema_data = self._generate_schema(score_data_base)
            self._upload_schema(schema_data,context)
        return


    def _assign_decile_value(self,submission_df,decile_rankings_df):
        submission_df['product'] = np.round(
            submission_df['policy_item_xbeta'] * submission_df['item_x_age_xbeta'] * submission_df['item_count_xbeta'] *
            submission_df['value_x_item_xbeta'] * submission_df['type_class_xbeta'] * submission_df['state_xbeta'] *
            submission_df['territory_factor'], 3)

        submission_df['base_col'] = 'avg_of_product_by_policy'
        sub_pivot = pd.pivot_table(submission_df, index=['AccountNumber', 'JobNumber', 'PolicyEffectiveDate'],
                                   columns=['base_col'], values='product', aggfunc=np.mean)
        sub_pivot.reset_index(drop=False, inplace=True)
        sub_pivot['avg_of_product_by_policy'] = sub_pivot['avg_of_product_by_policy'].round(3)

        submission_df = pd.merge(submission_df, sub_pivot, on=['AccountNumber', 'JobNumber', 'PolicyEffectiveDate'],
                                 how='inner')
        submission_df.reset_index(drop=True, inplace=True)
        del submission_df['base_col']
        submission_df['policy_profile_score'] = submission_df['avg_of_product_by_policy'].apply(self._decile_rank_apply,
                                                                                                args=(
                                                                                                decile_rankings_df,
                                                                                                'dec_rank', -1,))
        submission_df['model_run_date'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        submission_df['model_version'] = 0.1
        return submission_df

    def _get_support_data(self):
        base_ini_file_path = os.path.join(configuration.get('core', 'dags_folder'),r'models\pl_ltv')

        ini_path = 'ini/pp'
        list_files = os.listdir(os.path.join(base_ini_file_path, ini_path))
        for f in list_files:
            if 'cen10data' in f:
                df_sup_cen = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup_cen = df_sup_cen[['zip_code', 'cen10pop']]
                df_sup_cen.columns = ['ZIP_Code', 'cen10pop']
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'population_factors.csv'))
                df_sup_cen['population_factor'] = df_sup_cen['cen10pop'].apply(self._decile_rank_apply,
                                                                               args=(pop_facts, 'den_factor', -1,))

            if 'med_avg_base01' in f:
                df_sup_med = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup_med = df_sup_med[['ZIP_Code', 'med_vehicles', 'med_income']]
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'vehicle_factor.csv'))
                df_sup_med['med_vehicle_factor'] = df_sup_med['med_vehicles'].apply(self._decile_rank_apply,
                                                                                    args=(
                                                                                    pop_facts, 'den_factor', -1,))
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'med_income_factor.csv'))
                df_sup_med['med_income_factor'] = df_sup_med['med_income'].apply(self._decile_rank_apply,
                                                                                 args=(
                                                                                 pop_facts, 'den_factor', -1,))

            if 'giving_base01' in f:
                df_sup_giving = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup_giving = df_sup_giving[['ZIP_Code', 'charity', 'church', 'other_gift']]
                df_sup_giving['total_giving'] = df_sup_giving['charity'] + df_sup_giving['church'] + df_sup_giving[
                    'other_gift']
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'charity_factor.csv'))
                df_sup_giving['charity_factor'] = df_sup_giving['total_giving'].apply(self._decile_rank_apply,
                                                                                      args=(
                                                                                      pop_facts, 'den_factor', 99,))

        df = pd.merge(df_sup_cen, df_sup_med, how='inner', on=['ZIP_Code'])
        df = pd.merge(df, df_sup_giving, how='inner', on=['ZIP_Code'])
        df.reset_index(drop=True, inplace=True)

        df['charity_delta_income'] = df['charity_factor'] - df['med_income_factor']
        df['charity_delta_income_index'] = np.where(df['charity_delta_income'] > 3, 3, df['charity_delta_income'])
        df['charity_delta_income_index'] = np.where(df['charity_delta_income'] < -3, -3,
                                                    df['charity_delta_income_index'])
        df['charity_delta_income_index'] = np.where(
            ((df['charity_factor'] == 99) | (df['med_income_factor'] == 99)), 99,
            df['charity_delta_income_index'])

        pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'charity_delta_income_factor.csv'))
        df['charity_delta_income_factor'] = df['charity_delta_income_index'].apply(self._decile_rank_apply,
                                                                                   args=(
                                                                                   pop_facts, 'den_factor', 99,))

        df['territory_index'] = np.round(
            df['charity_delta_income_factor'] * df['med_vehicle_factor'] * df['population_factor'], 3)
        pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'territory_factors.csv'))
        df['territory_factor'] = df['territory_index'].apply(self._decile_rank_apply, args=(pop_facts, 'den_factor', 99,))

        df.reset_index(drop=True, inplace=True)
        return df

    def _factor_assignment(self,submission_df):
        def remove_spaces(x):
            return str(x).replace(' ', '')

        base_ini_file_path = os.path.join(configuration.get('core', 'dags_folder'), r'models\pl_ltv')
        ## Factor assignment and Decile Score.
        ini_path = 'ini/pp'
        list_files = os.listdir(os.path.join(base_ini_file_path, ini_path))

        decile_rankings_df = pd.DataFrame()
        for f in list_files:
            if 'state_region_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                submission_df['state_xbeta'] = submission_df['region_index'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['state_xbeta'] = submission_df['state_xbeta'].fillna(0)

            if 'item_type_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                submission_df['type_class_base_index'] = submission_df['type_class_base_index'].apply(remove_spaces)
                submission_df['type_class_xbeta'] = submission_df['type_class_base_index'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['type_class_xbeta'] = submission_df['type_class_xbeta'].fillna(0)

            if 'value_x_item_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                submission_df['item_value_label_sub_group'] = submission_df['item_value_label_sub_group'].apply(
                    remove_spaces)
                submission_df['item_value_label_sub_group'] = submission_df['item_value_label_sub_group'].str.upper()
                submission_df['value_x_item_xbeta'] = submission_df['item_value_label_sub_group'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['value_x_item_xbeta'] = submission_df['value_x_item_xbeta'].fillna(0)

            if 'item_count_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup.dropna(inplace=True, how='all')
                submission_df['item_and_policy_count_index_mapped'] = submission_df[
                    'item_and_policy_count_index_mapped'].apply(remove_spaces)
                submission_df['item_count_xbeta'] = submission_df['item_and_policy_count_index_mapped'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['item_count_xbeta'] = submission_df['item_count_xbeta'].fillna(0)

            if 'item_x_age_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                submission_df['age_group_index'] = submission_df['age_group_index'].apply(remove_spaces)
                submission_df['age_group_index'] = submission_df['age_group_index'].str.upper()
                submission_df['item_x_age_xbeta'] = submission_df['age_group_index'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['item_x_age_xbeta'] = submission_df['item_x_age_xbeta'].fillna(0)

            if 'item_count_x_values_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                submission_df['policy_item_index'] = submission_df['policy_item_index'].apply(remove_spaces)
                submission_df['policy_item_index'] = submission_df['policy_item_index'].str.upper()
                submission_df['policy_item_xbeta'] = submission_df['policy_item_index'].map(
                    df_sup.set_index('Variable Level')['Factor_M'])
                submission_df['policy_item_xbeta'] = submission_df['policy_item_xbeta'].fillna(0)

            if 'decile_rankings' in f:
                decile_rankings_df = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
        return submission_df,decile_rankings_df

    def _get_item_counts(self,bq_hook,account_list):
        base_sql = '''SELECT distinct(AccountNumber), PolicyNumber, PolicyEffectiveDate, max(ItemNumber) as total_item_count 
                      FROM `{project}.{dataset}.{table}` 
                      WHERE AccountNumber in ({list_values}) 
                      GROUP BY AccountNumber, PolicyNumber,PolicyEffectiveDate'''.format(project=self.google_source_dataset_table['project'],
                                                                                            dataset=self.google_source_dataset_table['dataset'],
                                                                                            table=self.google_source_dataset_table['table'],
                                                                                            list_values=account_list)
        count_df = bq_hook.get_pandas_df(sql=base_sql)
        return count_df

    def _calculate_item_count(self,bq_hook,account_list):
        count_df = self._get_item_counts(bq_hook, account_list)
        print(count_df)

        # Get Raw Data for Scoring......
        self.sql = "SELECT * from `{project}.{dataset}.{table}` where AccountNumber in ({list_values})".format(
            project=self.google_source_dataset_table['project'],
            dataset=self.google_source_dataset_table['dataset'],
            table=self.google_source_dataset_table['table'],
            list_values=account_list)
        score_data_base = bq_hook.get_pandas_df(sql=self.sql)

        score_data_base = pd.merge(score_data_base, count_df,
                                   on=['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate'],
                                   how='inner')
        score_data_base = score_data_base.sort_values(
            by=['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate'],
            ascending=[True, True, True])
        score_data_base.reset_index(drop=True, inplace=True)

        return score_data_base

    def _calculate_policy_data(self,df):
        ### Calculate total policy value
        sub_df = df[
            ['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate', 'ItemValue', 'total_item_count']]
        for col in sub_df.columns:
            sub_df[col] = sub_df[col].astype(str)
            df[col] = df[col].astype(str)

        sub_df['ItemValue'] = sub_df['ItemValue'].astype(float)
        sub_df['base_col'] = 'policy_item_value_sum'
        sub_pivot = pd.pivot_table(sub_df, index=['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate'],
                                   columns=['base_col'], values='ItemValue', aggfunc=np.sum)
        sub_pivot.reset_index(drop=False, inplace=True)
        df = pd.merge(df, sub_pivot, on=['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate'],
                                 how='inner')
        df = df.sort_values(by=['AccountNumber', 'PolicyNumber', 'PolicyEffectiveDate'],
                                                  ascending=[True, True, True])
        df.reset_index(drop=True, inplace=True)
        return df

    def _calculate_territory_factors(self,df,support_df):
        df['base_col'] = 1
        df['base_col_label'] = 'count'
        sub_pivot = pd.pivot_table(df, index=['LocationPostalCode'],
                                   columns=['base_col_label'], values='base_col', aggfunc=np.sum)
        sub_pivot.reset_index(drop=False, inplace=True)
        #Calculate the Territory Factor

        new = df['LocationPostalCode'].str.split("-", n=1, expand=True)
        df['ZIP_Code'] = pd.to_numeric(new[0],errors='coerce').fillna(-1)
        df = pd.merge(df,support_df,how='inner',on=['ZIP_Code'])
        df = pd.merge(df,sub_pivot,on=['LocationPostalCode'],how='left')
        df.reset_index(drop=True, inplace=True)
        return df

    def _decile_rank(x,decile_df,target_value):
        #Score the decile value
        for i in decile_df.index:
            if (x >= decile_df.loc[i,'Min']) & (x <= decile_df.loc[i,'Max']):
                return decile_df.loc[i,target_value]
        return -1

    def _decile_rank_apply(self,x, decile_df, target_value, return_default):
        for i in decile_df.index:
            if (x >= decile_df.loc[i, 'Min']) & (x <= decile_df.loc[i, 'Max']):
                return decile_df.loc[i, target_value]

        return return_default

    def _account_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        extracted_data_file_name = self.extracted_data.format(ds_nodash=context['ds_nodash']) + 'chunk_{chunk_value}.json'.format(chunk_value=self.chunk_value)
        gcs_hook.upload(self.bucket,
                        extracted_data_file_name,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _query_mssql(self, sql='NA', database='NA'):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn(database=database)
        cursor = conn.cursor()
        if sql == 'NA':
            cursor.execute(self.sql)
            # print(self.sql)
        else:
            cursor.execute(sql)
            # print(sql)
        return cursor

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields

    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return


class PolicyProfileFactorBuild(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#71b0e3'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_source_dataset_table={},
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 *args,
                 **kwargs):

        super(PolicyProfileFactorBuild, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value
        self.google_source_dataset_table = google_source_dataset_table
        self.pp = policy_profile()

    def execute(self, context):

        support_data = self._get_support_data()
        self._base_data_data_upload(context, support_data)

        return


    def _decile_rank_apply(self,x, decile_df, target_value, return_default):
        for i in decile_df.index:
            if (x >= decile_df.loc[i, 'Min']) & (x <= decile_df.loc[i, 'Max']):
                return decile_df.loc[i, target_value]

        return return_default


    def _get_support_data(self):
        base_ini_file_path = os.path.join(configuration.get('core', 'dags_folder'),  'models/pl_ltv')
        print(base_ini_file_path)

        ini_path = 'ini/pp'
        list_files = os.listdir(os.path.join(base_ini_file_path, ini_path))
        print(list_files)
        for f in list_files:
            if 'cen10data' in f:
                df_sup_cen = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup_cen = df_sup_cen[['zip_code', 'cen10pop','landsqmi']]
                df_sup_cen.columns = ['ZIP_Code', 'cen10pop','landsqmi']
                # pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'population_factors.csv'))
                pop_facts_cen10 = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'population_factors.csv'))
                # df_sup_cen['population_factor'] = df_sup_cen['cen10pop'].apply(self._decile_rank_apply,
                #                                                                args=(pop_facts, 'den_factor', -1,))

            if 'med_avg_base01' in f:
                df_sup_med = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                print(df_sup_med.columns)
                df_sup_med = df_sup_med[['ZIP_Code', 'med_vehicles', 'med_income','zip5_pop']]
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'vehicle_factor.csv'))
                print(df_sup_med.columns)
                df_sup_med['med_vehicle_factor'] = df_sup_med['med_vehicles'].apply(self._decile_rank_apply,
                                                                                    args=(
                                                                                    pop_facts, 'den_factor', -1,))
                pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'med_income_factor.csv'))
                df_sup_med['med_income_factor'] = df_sup_med['med_income'].apply(self._decile_rank_apply,
                                                                                 args=(
                                                                                 pop_facts, 'den_factor', -1,))

            if 'giving_base01' in f:
                df_sup_giving = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup_giving = df_sup_giving[['ZIP_Code', 'charity', 'church', 'other_gift']]
                df_sup_giving['charity'] = df_sup_giving['charity'].fillna(0)
                df_sup_giving['church'] = df_sup_giving['church'].fillna(0)
                df_sup_giving['other_gift'] = df_sup_giving['other_gift'].fillna(0)
                df_sup_giving['total_giving'] = df_sup_giving['charity'] + df_sup_giving['church'] + df_sup_giving[
                    'other_gift']
                pop_facts_giving = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'charity_factor.csv'))
                df_sup_giving['charity_factor'] = df_sup_giving['total_giving'].apply(self._decile_rank_apply,
                                                                                      args=(
                                                                                      pop_facts_giving, 'den_factor', 99,))

        df = pd.merge(df_sup_cen, df_sup_med, how='inner', on=['ZIP_Code'])
        df = pd.merge(df, df_sup_giving, how='inner', on=['ZIP_Code'])
        df.reset_index(drop=True, inplace=True)



        df['charity_delta_income'] = df['charity_factor'] - df['med_income_factor']
        df['charity_delta_income_index'] = np.where(df['charity_delta_income'] > 3, 3, df['charity_delta_income'])
        df['charity_delta_income_index'] = np.where(df['charity_delta_income'] < -3, -3,
                                                    df['charity_delta_income_index'])
        df['charity_delta_income_index'] = np.where(
            ((df['charity_factor'] == 99) | (df['med_income_factor'] == 99)), 99,
            df['charity_delta_income_index'])

        pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'charity_delta_income_factor.csv'))
        df['charity_delta_income_factor'] = df['charity_delta_income_index'].apply(self._decile_rank_apply,
                                                                                   args=(
                                                                                   pop_facts, 'den_factor', 99,))

        print(df.columns)
        df['pop_per_squaremile'] = df['zip5_pop'] / df['landsqmi']
        df['pop_per_squaremile'] = df['pop_per_squaremile'].round(decimals=0)
        df['population_factor'] = df['pop_per_squaremile'].apply(self._decile_rank_apply,
                                                                 args=(pop_facts_cen10, 'den_factor', 999,))

        df['population_factor'] = np.where(df['population_factor'] == 0, 0.964, df['population_factor'])
        df['population_factor'] = np.where(df['population_factor'] == 999, 0.964, df['population_factor'])
        df['med_vehicle_factor'] = np.where(df['med_vehicle_factor'] == 999, 1.199, df['med_vehicle_factor'])
        df['charity_delta_income_factor'] = np.where(df['charity_delta_income_factor'] == 999, 1,
                                                     df['charity_delta_income_factor'])

        df['territory_index'] = np.round(
            df['charity_delta_income_factor'] * df['med_vehicle_factor'] * df['population_factor'], 3)
        pop_facts = pd.read_csv(os.path.join(base_ini_file_path, ini_path, 'territory_factors.csv'))
        df['territory_factor'] = df['territory_index'].apply(self._decile_rank_apply, args=(pop_facts, 'den_factor', 99,))

        df.reset_index(drop=True, inplace=True)
        return df

    def _base_data_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        gcs_hook.upload(self.bucket,
                        self.filename,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields

    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return


class PolicyProfileSupportFileScoreBuild(BaseOperator):
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#71b0e3'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_cloud_bq_conn_id='google_cloud_default',
                 google_source_dataset_table={},
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 project = '',
                 dataset='',
                 *args,
                 **kwargs):

        super(PolicyProfileSupportFileScoreBuild, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_bq_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value
        self.google_source_dataset_table = google_source_dataset_table
        self.pp = policy_profile()

        self.project = project
        self.dataset=dataset

    def execute(self, context):

        self._get_support_score_data()

        #self._base_data_data_upload(context, support_data)

        return


    def _decile_rank_apply(self,x, decile_df, target_value, return_default):
        for i in decile_df.index:
            if (x >= decile_df.loc[i, 'Min']) & (x <= decile_df.loc[i, 'Max']):
                return decile_df.loc[i, target_value]

        return return_default


    def _get_support_score_data(self):
        def remove_spaces(x):
            return str(x).replace(' ', '')



        base_ini_file_path = os.path.join(configuration.get('core', 'dags_folder'),  'models/pl_ltv')
        ## Factor assignment and Decile Score.
        ini_path = 'ini/pp'
        list_files = os.listdir(os.path.join(base_ini_file_path, ini_path))
        print(list_files)
        print(self.google_cloud_bq_conn_id)
        print(self.google_cloud_storage_conn_id)

        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn_id)


        submission_df = pd.DataFrame()
        decile_rankings_df = pd.DataFrame()
        for f in list_files:
            if 'state_region_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ','_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup,self.project,self.dataset,'policy_profile_state_region_factors','REPLACE', False,'NA')

            if 'item_type_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ', '_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                    'policy_profile_item_type_factors', 'REPLACE', False, 'NA')

            if 'value_x_item_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ', '_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                    'policy_profile_value_x_item_factors', 'REPLACE', False, 'NA')

            if 'item_count_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup.dropna(inplace=True, how='all')
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ', '_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                    'policy_profile_item_count_factors', 'REPLACE', False, 'NA')

            if 'item_x_age_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ', '_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                    'policy_profile_item_x_age_factors', 'REPLACE', False, 'NA')

            if 'item_count_x_values_factors' in f:
                df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                df_sup['Variable Level'] = df_sup['Variable Level'].apply(remove_spaces)
                df_sup['Variable Level'] = df_sup['Variable Level'].str.upper()
                col_list = []
                for col in df_sup.columns:
                    col_list.append(col.replace(' ', '_'))
                df_sup.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                    'policy_profile_item_count_x_values_factors', 'REPLACE', False, 'NA')


            if 'decile_rankings' in f:
                decile_rankings_df = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
                col_list = []
                for col in decile_rankings_df.columns:
                    col_list.append(col.replace(' ', '_'))
                decile_rankings_df.columns = col_list
                self.gcp_bq_hook.gcp_bq_write_table(decile_rankings_df, self.project, self.dataset,
                                                    'policy_profile_decile_rankings', 'REPLACE', False,
                                                    'NA')

        return

    def _base_data_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        gcs_hook.upload(self.bucket,
                        self.filename,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields

    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return




class LifeTimeValueGetAccountDataMssql(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.
    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#f0c92e'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_three_part_detail = '',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 *args,
                 **kwargs):

        super(LifeTimeValueGetAccountDataMssql, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_storage_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value
        self.mssql_three_part_detail=mssql_three_part_detail

    def execute(self, context):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        self.sql = sql = '''SELECT * from {mssql_three_part_detail}'''.format(mssql_three_part_detail=self.mssql_three_part_detail)
        print(self.sql)
        cursor = self._query_mssql()
        colNameList = []
        for i in range(len(cursor.description)):
            desc = cursor.description[i]
            colNameList.append(desc[0])

        account_values = cursor.fetchall()
        df = pd.DataFrame(account_values)
        if len(df) == 0:
            print('*' * 20)
            print(self.sql)
            print('No Data Processed')
            print('*' * 20)
            return

        df.columns = colNameList

        for column_name, dtype in df.dtypes.iteritems():
         #   print("Column {c}, dtype {d}".format(c=column_name, d=dtype.kind))
            if dtype.kind in ('O', 'S', 'U', 'b'):
                df[column_name] = df[column_name].astype(str)
        print(df)

        self._account_data_upload(context, df)
        if self.chunk_value == 0:
            schema_data = self._generate_schema(df)
            self._upload_schema(schema_data,context)
        return

    def _account_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        extracted_data_file_name = self.extracted_data.format(ds_nodash=context['ds_nodash']) + '{chunk_value}.json'.format(chunk_value=self.chunk_value)
        gcs_hook.upload(self.bucket,
                        extracted_data_file_name,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _query_mssql(self, sql='NA', database='NA'):
        """
        Queries MSSQL and returns a cursor of results.
        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn(database=database)
        cursor = conn.cursor()
        if sql == 'NA':
            cursor.execute(self.sql)
            # print(self.sql)
        else:
            cursor.execute(sql)
            # print(sql)
        return cursor

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields


    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return

class LifeTimeValueSupportFileScoreBuild(BaseOperator):
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#71b0e3'

    @apply_defaults
    def __init__(self,
                 sql = '',
                 bucket = '',
                 filename = '',
                 extracted_data='',
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 google_cloud_bq_conn_id='google_cloud_default',
                 google_source_dataset_table={},
                 delegate_to=None,
                 schema_filename='',
                chunk_count = 100,
                 chunk_value=None,
                 project = '',
                 dataset='',
                 *args,
                 **kwargs):

        super(LifeTimeValueSupportFileScoreBuild, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.extracted_data = extracted_data
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_bq_conn_id = google_cloud_bq_conn_id
        self.schema_filename = schema_filename
        self.table_present = False
        self.chunk_count = chunk_count
        self.chunk_value = chunk_value
        self.google_source_dataset_table = google_source_dataset_table
        self.pp = policy_profile()

        self.project = project
        self.dataset=dataset

    def execute(self, context):

        self._get_support_score_data()
        return


    def _get_support_score_data(self):
        def remove_spaces(x):
            return str(x).replace(' ', '')

        base_ini_file_path = os.path.join(configuration.get('core', 'dags_folder'),  'models/pl_ltv')
        ## Factor assignment and Decile Score.
        ini_path = 'ini/ltv'
        list_files = os.listdir(os.path.join(base_ini_file_path, ini_path))
        print(list_files)
        print(self.google_cloud_bq_conn_id)
        print(self.google_cloud_storage_conn_id)

        self.gcp_bq_hook = BigQueryHook(gcp_conn_id=self.google_cloud_bq_conn_id)

        for f in list_files:
            if '.csv' not in f:
                continue
            file_split = f.split('.')
            df_sup = pd.read_csv(os.path.join(base_ini_file_path, ini_path, f))
            col_list = []
            for col in df_sup.columns:
                df_sup[col] = df_sup[col].apply(remove_spaces)
                col_list.append(col.replace(' ', '_'))
            df_sup.columns = col_list
            self.gcp_bq_hook.gcp_bq_write_table(df_sup, self.project, self.dataset,
                                                'ltv_{table_base}'.format(table_base=file_split[0]), 'REPLACE', False, 'NA')

        return

    def _base_data_data_upload(self, context,df):
        gcs_hook = GCSHook(self.google_cloud_bq_conn_id)
        gcs_hook.upload(self.bucket,
                        self.filename,
                        df.to_json(orient='records', lines='\n',date_format='iso'))
        return

    def _generate_schema(self,df, default_type='STRING'):
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
            #'b': 'BOOLEAN',
            'f': 'NUMERIC',
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

        return fields

    def _upload_schema(self,json_data,context):
        gcs_hook = GCSHook(self.google_cloud_storage_conn_id)
        gcs_hook.upload(self.bucket,
                        self.schema_filename.format(ds_nodash=context['ds_nodash']),
                        str(json_data).replace("'",'"'))
        return
