from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import EmptyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import configuration
from plugins.operators.place_key_operator import PlaceKeyOperator
from airflow.hooks.base import BaseHook
from google.oauth2 import service_account
import pandas_gbq
import logging
import pandas as pd
from datetime import timedelta, datetime
from time import sleep
from plugins.operators.gmap_parser import GMapParser
from plugins.operators.gmap_api import GMapAPI
import ast
import os
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('jdp_places_refresh_2',
          max_active_runs=1,
          schedule_interval=None,
          catchup=False,
          default_args=default_args)


# method to run the refresh of place_id tasks
def refresher(**kwargs):
    gmap_api = GMapAPI()
    # place_id_existing = ast.literal_eval(Variable.get('place_id_existing'))

    # receives a dict of task number and the corresponding assigned list of place_ids
    place_keys = list(filter(None, kwargs['keys']))

    #    service_account_file = os.path.join(configuration.get('core', 'dags_folder'), r'common/semi-managed-reporting.json')
    #    credentials_smr = service_account.Credentials.from_service_account_file(service_account_file, )
    #    credentials_smr = service_account.Credentials.from_service_account_file('semi-managed-reporting.json')
    bq_connection = BaseHook.get_connection('prod_edl')
    client_secrets = json.loads(bq_connection.extra_dejson['keyfile_dict'])
    credentials_smr = service_account.Credentials.from_service_account_info(client_secrets)
    project_id_smr = "semi-managed-reporting"

    # for testing
    # place_keys = ['hKLlOr38SZ5kqH0GvdZpp/jBiojuP1CoX/3HH+MEPo0=', 'hJmZmcNcxX7xUVJ9Nzpu9Qq09ZrH5F+EHZRidRdUnYg=']

    sql = f''' SELECT * FROM `semi-managed-reporting.core_JDP.placeKey_table`
                 INNER JOIN (SELECT placeKey, MAX(update_time) update_time 
                    FROM `semi-managed-reporting.core_JDP.placeKey_table` GROUP BY placeKey)
                 USING(placeKey, update_time)
                 WHERE placeKey IN UNNEST({place_keys}) AND current_pid != 'NOT_FOUND'; '''


    df_master_pid = pandas_gbq.read_gbq(sql, project_id=project_id_smr, credentials=credentials_smr, dialect="standard")

    pid_change_columns = ['pid_sent', 'pid_received',
                          'name_sent', 'name_received',
                          'addr_sent', 'addr_received',
                          'json_received', 'update_time',
                          'date_created', 'is_establishment']

    df_pid_change = pd.DataFrame(columns=pid_change_columns)

    # json_change_columns = ['placeKey','json_sent','json_received','what_changed','update_time']
    # df_json_change = pd.DataFrame(columns=json_change_columns)

    # for each key in the place_id list
    warn = 0
    for idx, row in df_master_pid.iterrows():
        sleep(.1)
        # send refresh request to api
        try:
            logging.info("send refresh request to api")
            store_refresh = gmap_api.place_id_refresh(row.current_pid)
        except Exception as e:
            logging.warn(e)
            raise
        # get original values

        # get the status of the refresh response
        logging.info("Getting the status of refresh response.....")
        gmap_parser = GMapParser(store_refresh)
        status = gmap_parser.getStatus()
        if status == 'OK':

            pid_received = gmap_parser.getPlaceID()
            logging.info("pid_received -{}".format(pid_received))
            del gmap_parser

            # compare place_id sent and place_id received
            # if same, it is active
            if row.current_pid == pid_received:
                logging.info('No Change.')
                continue

            # if not the same then process accordingly
            else:
                # TODO update with limited information corresponding to the place_key_operator operator.
                # if it doesn't exist, create a new record

                # get the new json for the new place_id
                # new_json = gmap_api.get_store_details(pid_received)
                # print(new_json)

                # parse the new json
                # gmap_parser = GMapParser(new_json)

                # add entry to the pid_change table
                logging.info('Updated.')
                df_pid_change = df_pid_change.append({"pid_sent": row.current_pid,
                                                      "pid_received": pid_received,
                                                      "name_sent": row.places_name,
                                                      "name_received": row.places_name,
                                                      "addr_sent": row.places_address,
                                                      "addr_received": row.places_address,
                                                      "json_received": row.places_json,
                                                      "update_time": pd.Timestamp.now(),
                                                      "date_created": kwargs['ds'],
                                                      "is_establishment": row.is_place_of_type_establishment,
                                                      }, ignore_index=True)

                # del gmap_parser


       # if refresh response status is NOT_FOUND, set the status of that record to NOT_FOUND
        elif (status == 'NOT_FOUND'):
            logging.info('NOT_FOUND')
            logging.info(row.current_pid)
            df_pid_change = df_pid_change.append({"pid_sent": row.current_pid,
                                                  "pid_received": 'NOT_FOUND',
                                                  "name_sent": row.places_name,
                                                  "name_received": 'NOT_FOUND',
                                                  "addr_sent": row.places_address,
                                                  "addr_received": 'NOT_FOUND',
                                                  "json_received": 'NOT_FOUND',
                                                  "update_time": pd.Timestamp.now(),
                                                  "date_created": datetime.strptime(kwargs['ds'], '%Y-%m-%d').date(),
                                                  "is_establishment":'NOT_FOUND'
                                                  }, ignore_index=True)
        elif status == 'INVALID_REQUEST':
            logging.warning(f'Status is: {status}')
            logging.warning(str(store_refresh))
            logging.warning(f'Sent Payload: {row.current_pid}')
            warn += 1
            if warn == 500: raise

        else:
            logging.error(f'Status is: {status}')
            logging.error(str(store_refresh))
            logging.error(f'Sent Payload: {row.current_pid}')
            raise

    if (len(df_pid_change) > 0):
        df_pid_change['date_created'] = pd.to_datetime(df_pid_change['date_created'])
        pandas_gbq.to_gbq(df_pid_change, f"core_JDP.placeKey_refresh${kwargs['ds_nodash']}", project_id_smr,
                          chunksize=5000, if_exists="append", credentials=credentials_smr)
    else:
        print('NO CHANGES to PIDs...')


# this piece of code generates tasks dynamically and assigns the refresher to 1000 place_ids per task and runs 10 tasks in parallel

# loads data from airflow variable
place_key_tasks = ast.literal_eval(Variable.get('jdp_place_key_tasks'))

# setting the bound
parallel_tasks = 10
iter_list = range(0, len(place_key_tasks), parallel_tasks)
list_tasks = [(k, v) for k, v in place_key_tasks.items()]

td_start = EmptyOperator(task_id="td_start", dag=dag)

# generate tasks dynamically
for i in iter_list:
    td_end = EmptyOperator(task_id="td_{}".format(i + 2), dag=dag)

    for task, place_keys in list_tasks[i:i + parallel_tasks]:
        tn = PythonOperator(
            task_id=f"refresher_{task}",
            python_callable=refresher,
            op_kwargs={'keys': place_keys},
            dag=dag
        )

        tn.set_upstream(td_start)
        td_end.set_upstream(tn)

    td_start = td_end

#
# stale_records_sql = '''SELECT ARRAY_TO_STRING( [ name_sent , addr_sent ], " ") as PlaceID_Lookup
#                            , name_sent as input_name
#                            , addr_sent as input_address
#                            , date_created
#                            FROM `semi-managed-reporting.core_JDP.placeKey_refresh` WHERE pid_received = 'NOT_FOUND' '''
#
#
# stale_records = PlaceKeyOperator(task_id='update_stale_records',
#                                     source_sql=stale_records_sql,
#                                     write_disposition='replace',
#                                     gcp_conn_id='semi_managed_gcp_connection',
#                                     google_places_conn_id='GooglePlacesDefault',
#                                     destination_table='core_JDP.placeKey_stale_records_temp',
#                                     use_legacy_sql=False,
#                                     dag=dag)
#
# update_refresh_table_sql = '''SELECT 'NOT_FOUND' as pid_sent
#                                     , places_pid as pid_received
#                                     , input_name as name_sent
#                                     , places_name as name_received
#                                     , input_address as addr_sent
#                                     , places_address as addr_received
#                                     , places_json as json_received
#                                     , places_update_time as update_time
#                                     , date_created
#
#                             FROM `semi-managed-reporting.core_JDP.placeKey_stale_records_temp` WHERE places_pid != 'NOT_FOUND' '''
#
# update_refresh_table = BigQueryOperator(task_id='stale_update_refesh_table',
# 									  sql=update_refresh_table_sql,
# 									  destination_dataset_table='semi-managed-reporting.GN_JDP.placeKey_refresh',
# 									  write_disposition='WRITE_APPEND',
# 									  gcp_conn_id='semi_managed_gcp_connection',
# 									  use_legacy_sql=False,
# 									  time_partitioning={"type": 'DAY', "field": 'date_created'},
# 									  schema_update_options=('ALLOW_FIELD_RELAXATION',),
# 									  dag=dag)

update_placekey_table_sql = '''WITH source AS (
													SELECT pid_received
                               , pid_sent
														   , name_sent as places_name
														   , addr_sent as places_address
														   , json_received as places_json
														   , update_time
														   , date_created
														   ,is_establishment
													  FROM `semi-managed-reporting.core_JDP.placeKey_refresh`
													  WHERE date_created = '{{ ds }}'),

							  target AS (
													 SELECT placeKey
															, current_pid
                              , previous_pid
															, MAX(update_time) as update_time

													 FROM `semi-managed-reporting.core_JDP.placeKey_table`
                           GROUP BY placeKey
                               , previous_pid
							   , current_pid
							   )

						SELECT t.placeKey
							   , s.places_name
							   , s.places_address
							   , s.pid_received as current_pid
							   , s.pid_sent as previous_pid
							   , s.is_establishment as is_place_of_type_establishment
							   , places_json
							   , DATETIME(s.update_time) as update_time
							   , DATE('{{ ds }}') as date_created
						FROM source s LEFT JOIN target t
							  ON s.pid_sent = t.current_pid'''

update_placekey_table = BigQueryOperator(task_id='update_place_key_table',
                                         sql=update_placekey_table_sql,
                                         destination_dataset_table='semi-managed-reporting.core_JDP.placeKey_table',
                                         write_disposition='WRITE_APPEND',
                                         gcp_conn_id='prod_edl',
                                         use_legacy_sql=False,
                                         time_partitioning={"type": 'DAY', "field": 'date_created'},
                                         schema_update_options=('ALLOW_FIELD_RELAXATION',),
                                         dag=dag)

# td_end >> stale_records >> update_refresh_table >> update_placekey_table
td_end >> update_placekey_table