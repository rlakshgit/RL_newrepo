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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.jm_sftp import SFTPHook
from airflow.models import Variable
import logging


class CDPAuditOperator(BaseOperator):
    """
        Compare the number of files in input folder and output folder to a set variable.
    """
    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                sql_file_count,
                sftp_destination_path = None,
                 sftp_conn_id = "ssh_default",
                 *args,
                 **kwargs):
        super(CDPAuditOperator, self).__init__(*args, **kwargs)
        self.sql_file_count = sql_file_count
        self.sftp_destination_path = sftp_destination_path
        self.sftp_conn_id =sftp_conn_id

    def execute(self, context):
        file_count = Variable.get('cdp_file_list').split(',')
        file_count = file_count[0]
        print(file_count)
        print('_____')
        print(self.sql_file_count)
        if self.sftp_destination_path is None:
            logging.info('auditing number of files in SQL folder')
            if str(file_count) != str(self.sql_file_count):
                logging.info("sql file count doesn't match")
                raise
            else:
                logging.info("sql file count match")

        else:
            logging.info('auditing number of files in SFTP folder')
            sftp_hook = SFTPHook(self.sftp_conn_id)
            sftp_file_list = sftp_hook.list_directory(self.sftp_destination_path)
            filter = context['ds_nodash'] + '.csv'
            print('looking for files ending with ......')
            print(filter)
            # for file in sftp_file_list:
            #     if not file.endswith(filter):
            #         sftp_file_list.remove(file)
            sftp_file_list_2 = [file for file in sftp_file_list if file.endswith(filter)]

            print('__________')
            print(sftp_file_list_2)
            sftp_file_count = len(sftp_file_list_2)

            if str(file_count) != str(sftp_file_count):
                logging.info("SFTP file count doesn't match")
                raise
            else:
                logging.info("SFTP file count match")
        return


