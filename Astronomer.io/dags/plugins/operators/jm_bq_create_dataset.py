import json
from typing import Iterable, FrozenSet, Optional

# from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from plugins.hooks.jm_bq import JMBQHook
# from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from plugins.hooks.jm_gcs import GCSHook, _parse_gcs_url
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator #, BaseOperatorLink
# from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults
import logging

class BigQueryCreateEmptyDatasetOperator(BaseOperator):
    """
    This operator is used to create new dataset for your Project in Big query.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_reference: Dataset reference that could be provided with request body.
        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_reference: dict

        **Example**: ::

            create_new_dataset = BigQueryCreateEmptyDatasetOperator(
                                    dataset_id = 'new-dataset',
                                    project_id = 'my-project',
                                    dataset_reference = {"friendlyName": "New Dataset"}
                                    bigquery_conn_id='_my_gcp_conn_',
                                    task_id='newDatasetCreator',
                                    dag=dag)

    """

    template_fields = ('dataset_id', 'project_id')

    ui_color = '#c0FFFc'


    @apply_defaults
    def __init__(self,
                 dataset_id,
                 project_id=None,
                 dataset_reference=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.bigquery_conn_id = bigquery_conn_id
        self.dataset_reference = dataset_reference if dataset_reference else {}
        self.delegate_to = delegate_to

        super(BigQueryCreateEmptyDatasetOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Dataset id: %s Project id: %s', self.dataset_id, self.project_id)

        bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id,
                               project_id=self.project_id,
                               delegate_to=self.delegate_to)

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.create_empty_dataset(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                dataset_reference=self.dataset_reference)
        except Exception as e:
            if 'Already Exists' in str(e):
                logging.info('Dataset already present!')
            else:
                print(e)
                raise
