from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import json

class CTResetOperator(BaseOperator):
    """
    To reset Change tracking values for sql sources

    """

    ui_color = '#e0aFFc'

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):

        super(CTResetOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        try:
            SQL_SOURCE_CT_STATUS = json.loads(Variable.get('SQL_SOURCE_CT_STATUS'))
            for k in SQL_SOURCE_CT_STATUS:
                SQL_SOURCE_CT_STATUS[k] = 'TRUE.' + str(k)
            Variable.set('SQL_SOURCE_CT_STATUS', json.dumps(SQL_SOURCE_CT_STATUS))

        except:
            source_list = ['BillingCenter', 'ClaimCenter', 'ContactManager', 'PolicyCenter', 'PLEcom']
            jsonvalue = {}
            for source in source_list:
                jsonvalue[source] = 'TRUE.' + source
            Variable.set('SQL_SOURCE_CT_STATUS', json.dumps(jsonvalue))



