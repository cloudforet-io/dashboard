import factory
from spaceone.core import utils

from spaceone.dashboard.model import DomainDashboard


class DomainDashboardFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = DomainDashboard

    domain_dashboard_id = factory.LazyAttribute(lambda o: utils.generate_id('domain-dash'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    layouts = []
    # options = {'currency': {'enabled': True},
    #            'date_range': {'enabled': True,
    #                           'period': {'end': '2021-12',
    #                                      'start': '2021-11'},
    #                           'period_type': 'FIXED'}},
    default_variables = {
        'group_by': 'product',
        'project_id': []
    }
    tags = {'type': 'test',
            'env': 'dev'}
    labels = ['a', 'b', 'c']
    user_id = 'cloudforet@gmail.com'
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id('domain'))
    created_at = factory.Faker('date_time')
    updated_at = factory.Faker('date_time')
