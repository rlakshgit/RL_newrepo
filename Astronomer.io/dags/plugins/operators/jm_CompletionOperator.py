
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import json

class CompletionOperator(BaseOperator):
    """
    Write Audit data for api from landing metadata and bigquery

    :param source: The source(s) that should be either retrieved or set
    :type source: semi-colon delimited list
    :param mode: The mode will be used for reseting, setting, or retrieving the source setting value.
    :type mode: str
    :param reset: If passed in as True all the sources and not use the passed in sources will be set to 0
    :type reset: boleen

    """


    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 source,
                 mode,
                 *args,
                 **kwargs):

        super(CompletionOperator, self).__init__(*args, **kwargs)

        self.source = source.split(';')
        self.mode = mode.upper()


    def execute(self, context):
        try:
            SOURCE_LIST_STATUS = Variable.get('SOURCE_LIST_STATUS')
        except:
            Variable.set('SOURCE_LIST_STATUS', {})
            SOURCE_LIST_STATUS = Variable.get('SOURCE_LIST_STATUS')

        if self.mode == 'RESET':
            self._reset_completion_status()

        if self.mode == 'GET':
            return self._completion_status_check()

        if self.mode == 'SET':
            self._completion_status_set_bit()
        return

    def _reset_completion_status(self):
        SOURCE_LIST_STATUS = json.loads(Variable.get('SOURCE_LIST_STATUS'))
        for k in SOURCE_LIST_STATUS:
            SOURCE_LIST_STATUS[k] = False

        Variable.set('SOURCE_LIST_STATUS', json.dumps(SOURCE_LIST_STATUS))
        return


    def _completion_status_check(self):
        SOURCE_LIST_STATUS = json.loads(Variable.get('SOURCE_LIST_STATUS'))
        return_value = True
        for source in self.source:
            try:
                if SOURCE_LIST_STATUS[source.lower()] == True:
                    continue
                else:
                    print('Source not DAG not completed....',source)
                    return_value = False
            except:
                print('Missing Source Information for ',source)
                return_value = False

        if return_value:
            print('All sources ready return a True statement')
        
        return return_value

    def _completion_status_set_bit(self):
        SOURCE_LIST_STATUS = json.loads(Variable.get('SOURCE_LIST_STATUS'))
        for source in self.source:
            SOURCE_LIST_STATUS[source.lower()] = True
        Variable.set('SOURCE_LIST_STATUS', json.dumps(SOURCE_LIST_STATUS))
        return


