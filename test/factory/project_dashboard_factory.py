import factory
from spaceone.core import utils

from spaceone.dashboard.model import ProjectDashboard


class ProjectDashboardFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = ProjectDashboard

    project_dashboard_id = factory.LazyAttribute(lambda o: utils.generate_id('project-dash'))
    project_id = factory.LazyAttribute(lambda o: utils.generate_id('project'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    layouts = []
    variables = {
        'group_by': 'product',
        'project_id': []
    }
    settings = {
        'date_range': {'enabled': True},
        'currency': {'enabled': True},
    }
    variables_schema = {}
    labels = ['a', 'b', 'c']
    tags = {'type': 'test',
            'env': 'dev'}
    user_id = 'cloudforet@gmail.com'
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id('domain'))
    created_at = factory.Faker('date_time')
    updated_at = factory.Faker('date_time')
