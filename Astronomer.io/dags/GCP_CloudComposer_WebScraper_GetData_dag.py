from __future__ import print_function


import airflow
from airflow.models import DAG, Variable
from plugins.operators.jm_WebScraperFunctions import WebScraperGenerateTable,WebScraperPopulateTable,WebScraperGroupSites,WebScraperFeeder

AIRFLOW_ENV = Variable.get('ENV')


if AIRFLOW_ENV.lower() == 'dev':
    base_gcp_connector = 'jm_landing_dev'
elif AIRFLOW_ENV.lower() == 'qa':
    base_gcp_connector = 'jm_landing_dev'
elif AIRFLOW_ENV.lower() == 'prod':
    base_gcp_connector = 'jm_landing_prod'   
    
## Run First....

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


with DAG(
        'WebScraper_GetData',
        schedule_interval='@once',  
        catchup=False,
        is_paused_upon_creation = False,
        max_active_runs=1,
        default_args=args) as dag:

    populate_data = WebScraperPopulateTable(
        task_id='populate_table',
        project_id="prospect-model-data-collection",
        google_cloud_bq_conn=base_gcp_connector,
        sql='''WITH 
	all_records as (
		SELECT
		    driver.current_pid as place_id
			,JSON_EXTRACT_SCALAR(places_json, '$.result.website') AS URL

	FROM `semi-managed-reporting.core_sales_JDP.v1_placeKey_table` as driver							
	GROUP BY driver.current_pid
		,places_json
	)
	 
	SELECT distinct place_id
	, URL as website
	, URL as website_url
	FROM all_records
	WHERE URL is not null''',
        #            SELECT * FROM `semi-managed-reporting.wip_prospect_demo.wip_prospect_website`;',
    )

    task_feeder = WebScraperFeeder(
        task_id='feeder_task',
    )

    populate_data >> task_feeder

