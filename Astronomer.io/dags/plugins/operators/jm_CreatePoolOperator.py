from plugins.hooks.jm_pool import get_pool, create_pool,delete_pool
from airflow.exceptions import PoolNotFound
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreatePoolOperator(BaseOperator):
    # its pool blue, get it?
    ui_color = '#b8e9ee'

    @apply_defaults
    def __init__(
            self,
            name,
            slots,
            description='',
            *args, **kwargs):
        super(CreatePoolOperator, self).__init__(*args, **kwargs)
        self.description = description
        self.slots = slots
        self.name = name

    def execute(self, context):
        try:
            pool = get_pool(name=self.name)
            if pool:
                print('Pool Exists: {pool}'.format(pool=self.name))
                return
        except PoolNotFound:
            # create the pool
            pool = create_pool(name=self.name, slots=self.slots, description=self.description)
            print('Created pool: {pool}'.format(pool=self.name))

class DeletePoolOperator(BaseOperator):
    # its pool blue, get it?
    ui_color = '#b8e9ee'

    @apply_defaults
    def __init__(
            self,
            name,
            *args, **kwargs):
        super(DeletePoolOperator, self).__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        try:
            pool = get_pool(name=self.name)
            if pool:
                print('Deleting Pool: {pool}'.format(pool=self.name))
                delete_pool(name=self.name)
                return
        except PoolNotFound:
            # create the pool
            print('No pool: {pool}'.format(pool=self.name))