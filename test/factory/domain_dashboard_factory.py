import factory
from spaceone.core import utils

from spaceone.dashboard.model import DomainDashboard


class DomainDashboardFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = DomainDashboard

    domain_dashboard_id = factory.LazyAttribute(lambda o: utils.generate_id('domain-dash'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    layouts = []
    dashboard_options = {
        'group_by': 'product',
        'project_id': []
    }
    settings = {
        'date_range': {'enabled': True},
        'currency': {'enabled': True},
    }
    dashboard_options_schema = {}
    labels = ['a', 'b', 'c']
    tags = {'type': 'test',
            'env': 'dev'}
    user_id = 'cloudforet@gmail.com'
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id('domain'))
    created_at = factory.Faker('date_time')
    updated_at = factory.Faker('date_time')
