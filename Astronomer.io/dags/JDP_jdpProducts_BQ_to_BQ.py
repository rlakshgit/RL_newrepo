from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.JDPProductsAuditOperator import JDPProductsAuditOperator
from airflow.models import Variable
from airflow import configuration
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'alangsner@jminsure.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
}

# with open(
#         os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'jdp_products_env_vars.ini')) as f:
#     envJSON = json.loads(f.read())

AIRFLOW_ENV = Variable.get('ENV')
locator_table = 'jeweler_locator_v2_0'
locator_table_old = 'locator_from_JDP_prod_v1'

# Set some project-specific variables that are used in building the tasks.
if AIRFLOW_ENV.lower() == 'dev':
    bq_connection = 'dev_edl'
    smr_project = "semi-managed-reporting"
    project = "dev-edl"
    destination_project = 'dev-edl'
    brandify_project = 'dev-edl'

elif AIRFLOW_ENV.lower() == 'qa':
    bq_connection = 'qa_edl'
    smr_project = "semi-managed-reporting"
    project = "qa-edl"
    destination_project = "qa-edl"
    brandify_project = "external-brandify-test"

else:
    bq_connection = "semi_managed_gcp_connection"
    smr_project = "semi-managed-reporting"
    project = "semi-managed-reporting"
    destination_project = "semi-managed-reporting"
    brandify_project = "external-brandify-prod"

brandify_dataset = "jm_brandify"
jdp_dataset = "core_JDP"
research_dataset = "data_products_t1_insurance_cl"
core_sales_cl_dataset = "core_sales_cl"
core_insurance_cl_dataset = "core_insurance_cl"
data_products_t1_dataset = "data_products_t1_insurance_cl"

dag = DAG('jdp_products',
          max_active_runs=1,
          schedule_interval='0 18 * * *',
          default_args=default_args,
          catchup=False)

# ------------------------------------------------------------------------------
# Google Places Core
# Core table for places data.
# Links to JDP placeKey table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'places_unnest.sql'), 'r') as f:
    sqlFile = f.read()
places_unnest_sql = sqlFile.format(source_project=smr_project,
                                   source_dataset_jdp=jdp_dataset,
                                   placeKey_table='placeKey_table')

google_places_core = BigQueryOperator(task_id='google_places_core',
                                      sql=places_unnest_sql,
                                      destination_dataset_table='{project}.{dataset}.promoted_google_places'.format(
                                          project=destination_project, dataset=core_sales_cl_dataset),
                                      write_disposition='WRITE_TRUNCATE',
                                      gcp_conn_id=bq_connection,
                                      use_legacy_sql=False,
                                      dag=dag)

# Audit Google places core table for duplicate Placekey
audit_google_places_sql = ''' SELECT count(*) as totalcount, Placekey   FROM `{project}.{dataset}.promoted_google_places` group by Placekey having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)

audit_google_places_task = JDPProductsAuditOperator(
    task_id='audit_google_places',
    sql=audit_google_places_sql,
    bigquery_conn_id=bq_connection,
    dag=dag
)

# ------------------------------------------------------------------------------
# Experian brick lookup.
# Links to JDP businessKey table.
# ------------------------------------------------------------------------------

with open(
        os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'experian_brick_lookup.sql'), 'r') as f:
    sql_read_sql = f.read()
experian_brick_lookup_sql = sql_read_sql.format(source_project=smr_project, source_dataset_jdp=jdp_dataset)

experian_business_core = BigQueryOperator(task_id='experian_business_core',
                                          sql=experian_brick_lookup_sql,
                                          destination_dataset_table='{project}.{dataset}.promoted_experian_businesses'.format(
                                              project=destination_project, dataset=core_sales_cl_dataset),
                                          write_disposition='WRITE_TRUNCATE',
                                          gcp_conn_id=bq_connection,
                                          use_legacy_sql=False,
                                          dag=dag)

# Audit Experian business core table for duplicate BusinessKey
audit_experian_business_core_sql = '''SELECT count(*) as totalcount, BusinessKey  FROM `{project}.{dataset}.promoted_experian_businesses` group by BusinessKey having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_experian_business_core_task = JDPProductsAuditOperator(
    task_id='audit_experian_business_core',
    sql=audit_experian_business_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag
)
# ------------------------------------------------------------------------------
# Recent GW locations.
# Links to JDP internal_source_GWPC table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_active_loc.sql'), 'r') as f:
    sql_read_sql = f.read()
most_recent_active_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

active_gwpc_locations = BigQueryOperator(task_id='active_gwpc_locations',
                                         sql=most_recent_active_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_active_gwpc_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)



# ------------------------------------------------------------------------------
# Recent active SF locations.
# Links to JDP internal_source_SF table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_sf.sql'), 'r') as f:
    sql_read_sql = f.read()
recent_active_SF_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

recent_sf_locations = BigQueryOperator(task_id='active_sf_locations',
                                         sql=recent_active_SF_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_sf_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)

# Audit SF core table for duplicate SF_LocationKey
audit_SF_core_sql = '''SELECT count(*) as totalcount, SF_LocationKey, SalesforceID  FROM `{project}.{dataset}.promoted_recent_sf_locations` group by SF_LocationKey, SalesforceID having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_SF_core_task = JDPProductsAuditOperator(
    task_id='audit_SF_core',
    sql=audit_SF_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag)

# ------------------------------------------------------------------------------
# Recent active LNAX locations.
# Links to JDP internal_source_LNAX table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_lnax.sql'), 'r') as f:
    sql_read_sql = f.read()
recent_active_LNAX_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

recent_lnax_locations = BigQueryOperator(task_id='active_lnax_locations',
                                         sql=recent_active_LNAX_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_lnax_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)

# Audit LNAX core table for duplicate LNAX_LocationKey
audit_LNAX_core_sql = '''SELECT count(*) as totalcount, LNAX_LocationKey, customer_number  FROM `{project}.{dataset}.promoted_recent_lnax_locations` group by LNAX_LocationKey, customer_number having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_LNAX_core_task = JDPProductsAuditOperator(
    task_id='audit_LNAX_core',
    sql=audit_LNAX_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag)


# ------------------------------------------------------------------------------
# Recent active WXAX locations.
# Links to JDP internal_source_WXAX table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_wxax.sql'), 'r') as f:
    sql_read_sql = f.read()
recent_active_WXAX_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

recent_wxax_locations = BigQueryOperator(task_id='active_wxax_locations',
                                         sql=recent_active_WXAX_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_wxax_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)

# Audit WXAX core table for duplicate WXAX_LocationKey
audit_WXAX_core_sql = '''SELECT count(*) as totalcount, WXAX_LocationKey, customer_number  FROM `{project}.{dataset}.promoted_recent_wxax_locations` group by WXAX_LocationKey, customer_number having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_WXAX_core_task = JDPProductsAuditOperator(
    task_id='audit_WXAX_core',
    sql=audit_WXAX_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag)

# ------------------------------------------------------------------------------
# Recent active JMAX locations.
# Links to JDP internal_source_JMAX table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_jmax.sql'), 'r') as f:
    sql_read_sql = f.read()
recent_active_JMAX_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

recent_jmax_locations = BigQueryOperator(task_id='active_jmax_locations',
                                         sql=recent_active_JMAX_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_jmax_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)

# Audit JMAX core table for duplicate JMAX_LocationKey
audit_JMAX_core_sql = '''SELECT count(*) as totalcount, JMAX_LocationKey, customer_number  FROM `{project}.{dataset}.promoted_recent_jmax_locations` group by JMAX_LocationKey, customer_number having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_JMAX_core_task = JDPProductsAuditOperator(
    task_id='audit_JMAX_core',
    sql=audit_JMAX_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag)

# ------------------------------------------------------------------------------
# Recent active Zing locations.
# Links to JDP internal_source_Zing_table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'most_recent_zing.sql'), 'r') as f:
    sql_read_sql = f.read()
recent_active_Zing_loc_sql = sql_read_sql.format(source_project=smr_project,
                                                            source_dataset_jdp=jdp_dataset,
                                                            research_dataset=research_dataset,
                                                            transaction_dataset='core_insurance')

recent_zing_locations = BigQueryOperator(task_id='active_zing_locations',
                                         sql=recent_active_Zing_loc_sql,
                                         destination_dataset_table='{project}.{dataset}.promoted_recent_zing_locations'.format(
                                             project=destination_project, dataset=core_sales_cl_dataset),
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id=bq_connection,
                                         use_legacy_sql=False,
                                         dag=dag)

# Audit ZING core table for duplicate ZING_LocationKey
audit_ZING_core_sql = '''SELECT count(*) as totalcount, ZING_LocationKey,  Location_Id, _id  FROM `{project}.{dataset}.promoted_recent_zing_locations` group by ZING_LocationKey,  Location_Id, _id having totalcount > 1'''. \
    format(project=destination_project, dataset=core_sales_cl_dataset)
audit_ZING_core_task = JDPProductsAuditOperator(
    task_id='audit_ZING_core',
    sql=audit_ZING_core_sql,
    bigquery_conn_id=bq_connection,
    dag=dag)


# ------------------------------------------------------------------------------
# Recent active GW locations with Experian data.
# Links to previously created JDP products.
# THIS OUTPUT IS NONPROMOTED.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'gw_recent_with_experian.sql'), 'r') as f:
    sql_read_sql = f.read()

gw_recent_w_experian_sql = sql_read_sql.format(source_project=smr_project,
                                                          source_dataset_jdp=jdp_dataset,
                                                          core_sales_cl_dataset=core_sales_cl_dataset,
                                                          core_insurance_cl_dataset=core_insurance_cl_dataset)

gwpc_locs_w_experian = BigQueryOperator(task_id='gwpc_locs_w_experian',
                                        sql=gw_recent_w_experian_sql,
                                        destination_dataset_table='{project}.{dataset}.nonpromoted_recent_gw_locs_w_experian'.format(
                                            project=destination_project, dataset=data_products_t1_dataset),
                                        write_disposition='WRITE_TRUNCATE',
                                        gcp_conn_id=bq_connection,
                                        use_legacy_sql=False,
                                        dag=dag)

# ------------------------------------------------------------------------------
# Current view master.
# Links to JDP business_master table.
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'master_mapping.sql'), 'r') as f:
    sql_read_sql = f.read()
master_mapping_sql = sql_read_sql.format(source_project=smr_project,
                                                    source_dataset_jdp=jdp_dataset,
                                                    business_master='business_master')
current_view_master = BigQueryOperator(task_id='current_view_master',
                                       sql=master_mapping_sql,
                                       destination_dataset_table='{project}.{dataset}.promoted_current_view_master'.format(
                                           project=destination_project, dataset=core_sales_cl_dataset),
                                       write_disposition='WRITE_TRUNCATE',
                                       gcp_conn_id=bq_connection,
                                       use_legacy_sql=False,
                                       dag=dag)

# ------------------------------------------------------------------------------
# Jeweler locator data output.
# Links to JDP products previously created (google_places, current_view).
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'jeweler_locator.sql'), 'r') as f:
    sql_read_sql = f.read()
with open(
        os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'jeweler_locator_audit.sql'), 'r') as f:
    sql_read_sql_audit = f.read()

# ----------------------------------------------------------------------------
# Read from ini file (for now) to determine exclusions beyond chain stores.
# ----------------------------------------------------------------------------
exclude_list_string = ''
exclude_list = []

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/',
                       'jeweler_locator_exclude_list.ini'), 'r') as f:
    confJSON = json.loads(f.read())

# ---------------------------------------------------------------------------
# Iterate through the dictionary of exclusions to build the SQL clause.
# ---------------------------------------------------------------------------
for k, v in confJSON.items():
    locator_target_field = k
    locator_target_field_values = v
    print(v)

    for locator_target_field_value in locator_target_field_values:
        exclude_list.append(' NOT {field}= "{value}" '.format(field=locator_target_field, value=locator_target_field_value))

if len(exclude_list) > 0:
    exclude_list_string = ' AND ' + '\n AND '.join(exclude_list)

jeweler_locator_sql = sql_read_sql.format(source_project=project,
                                                     source_dataset_jdp=jdp_dataset,
                                                     source_table_tag='promoted',
                                                     core_sales_cl_dataset=core_sales_cl_dataset,
                                                     optional_exclude_list=exclude_list_string)

jeweler_locator_audit_sql = sql_read_sql_audit.format(source_project=project,
                                                                 source_dataset_jdp=jdp_dataset,
                                                                 source_table_tag='promoted',
                                                                 core_sales_cl_dataset=core_sales_cl_dataset,
                                                                 optional_exclude_list=exclude_list_string)

# create_dataset_locator = BigQueryCreateEmptyDatasetOperator(task_id='check_for_locator_dataset',
#                                                             project_id=brandify_project,
#                                                             dataset_id=brandify_dataset,
#                                                             bigquery_conn_id=bq_connection,
#                                                             dag=dag)
# audit Jeweler locator
audit_jeweler_locator_task = JDPProductsAuditOperator(
    task_id='audit_jeweler_locator',
    sql=jeweler_locator_audit_sql,
    bigquery_conn_id=bq_connection,
    dag=dag
)

jeweler_locator = BigQueryOperator(task_id='jeweler_locator',
                                   sql=jeweler_locator_sql,
                                   destination_dataset_table='{project}.{dataset}.{locator_table}'.format(
                                       project=brandify_project, dataset=brandify_dataset, locator_table=locator_table),
                                   create_disposition='CREATE_IF_NEEDED',
                                   write_disposition='WRITE_TRUNCATE',
                                   gcp_conn_id=bq_connection,
                                   use_legacy_sql=False,
                                   dag=dag)

# ------------------------------------------------------------------------------
# OLD (V2) Jeweler locator data output. TODO Delete this when brandify switches to v3
# Links to JDP products previously created (google_places, current_view).
# ------------------------------------------------------------------------------

with open(os.path.join(configuration.get('core', 'dags_folder'), r'JDP/products_SQL/', 'jeweler_locator_old.sql'), 'r') as f:
    sql_read_sql_old = f.read()



jeweler_locator_sql_OLD = sql_read_sql_old.format(source_project=project,
                                                     source_dataset_jdp=jdp_dataset,
                                                     source_table_tag='promoted',
                                                     core_sales_cl_dataset=core_sales_cl_dataset,
                                                     optional_exclude_list=exclude_list_string)

jeweler_locator_audit_sql_OLD = sql_read_sql_audit.format(source_project=project,
                                                     source_dataset_jdp=jdp_dataset,
                                                     source_table_tag='promoted',
                                                     core_sales_cl_dataset=core_sales_cl_dataset,
                                                     optional_exclude_list=exclude_list_string)

# create_dataset_locator = BigQueryCreateEmptyDatasetOperator(task_id='check_for_locator_dataset',
#                                                             project_id=brandify_project,
#                                                             dataset_id=brandify_dataset,
#                                                             bigquery_conn_id=bq_connection,
#                                                             dag=dag)
#audit Jeweler locator
audit_jeweler_locator_task_OLD = JDPProductsAuditOperator(
                                                    task_id = 'audit_jeweler_locator_OLD',
                                                    sql = jeweler_locator_audit_sql_OLD,
                                                    bigquery_conn_id=bq_connection,
                                                    dag = dag
)

jeweler_locator_OLD = BigQueryOperator(task_id='jeweler_locator_OLD',
                                   sql=jeweler_locator_sql_OLD,
                                   destination_dataset_table='{project}.{dataset}.{locator_table}'.format(project=brandify_project,
                                                                                                          dataset=brandify_dataset,
                                                                                                          locator_table=locator_table_old),
                                   write_disposition='WRITE_TRUNCATE',
                                   gcp_conn_id=bq_connection,
                                   use_legacy_sql=False,
                                   dag=dag)

task_start = EmptyOperator(task_id="task_start", dag=dag)
task_end = EmptyOperator(task_id="task_end", dag=dag)

task_start >> experian_business_core >> audit_experian_business_core_task >> gwpc_locs_w_experian >> audit_jeweler_locator_task >> jeweler_locator >> audit_jeweler_locator_task_OLD >> jeweler_locator_OLD >> task_end
task_start >> active_gwpc_locations >> gwpc_locs_w_experian >> audit_jeweler_locator_task >> jeweler_locator >> audit_jeweler_locator_task_OLD >> jeweler_locator_OLD >> task_end
task_start >> current_view_master >> gwpc_locs_w_experian >> audit_jeweler_locator_task >> jeweler_locator >> audit_jeweler_locator_task_OLD >> jeweler_locator_OLD >> task_end
task_start >> google_places_core >> audit_google_places_task >> audit_jeweler_locator_task >> jeweler_locator >> audit_jeweler_locator_task_OLD >> jeweler_locator_OLD >> task_end
task_start >> recent_sf_locations >> audit_SF_core_task >> task_end
task_start >> recent_lnax_locations >> audit_LNAX_core_task >> task_end
task_start >> recent_wxax_locations >> audit_WXAX_core_task >> task_end
task_start >> recent_jmax_locations >> audit_JMAX_core_task >> task_end
task_start >> recent_zing_locations >> audit_ZING_core_task >> task_end

