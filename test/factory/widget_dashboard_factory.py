import factory
from spaceone.core import utils

from spaceone.dashboard.model import Widget


class WidgetFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = Widget

    widget_id = factory.LazyAttribute(lambda o: utils.generate_id('widget'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    view_mode = 'AUTO'
    variables = {
        'group_by': 'product',
        'project_id': []
    }
    schema = {}
    tags = {'type': 'test',
            'env': 'dev'}
    labels = ['a', 'b', 'c']
    user_id = 'cloudforet@gmail.com'
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id('domain'))
    created_at = factory.Faker('date_time')
    updated_at = factory.Faker('date_time')
