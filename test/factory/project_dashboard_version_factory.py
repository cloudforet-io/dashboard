import factory
from spaceone.core import utils

from spaceone.dashboard.model import ProjectDashboardVersion


class ProjectDashboardVersionFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = ProjectDashboardVersion

    # project_dashboard_id = factory.LazyAttribute(lambda o: utils.generate_id('project-dash'))
    layouts = [{'name': 'widget1'}, {'name': 'widget2'}, {'name': 'widget3'}]
    dashboard_options = {
        'group_by': 'product',
        'project_id': []
    }
    settings = {
        'date_range': {'enabled': True},
        'currency': {'enabled': True},
    }
    dashboard_options_schema = {'a': 'b'}
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id('domain'))
    created_at = factory.Faker('date_time')
