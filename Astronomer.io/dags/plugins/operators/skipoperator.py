from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults


class TaskSkippingOperator(BaseOperator, SkipMixin):

    @apply_defaults
    def __init__(self,
                 condition,
                 *args,
                 **kwargs):
        super(TaskSkippingOperator,self).__init__(*args, **kwargs)
        self.condition = condition

    def execute(self, context):



        if self.condition == 'True':
           self.log.info('Proceeding with downstream tasks...')
           return

        self.log.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)

        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")