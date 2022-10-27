import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data
from parameterized import parameterized

from spaceone.dashboard.info import ProjectDashboardInfo, ProjectDashboardsInfo, StatisticsInfo
from spaceone.dashboard.model import ProjectDashboard
from spaceone.dashboard.service import ProjectDashboardService
from test.factory import ProjectDashboardFactory
from test.lib import *


class TestProjectDashboardService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.dashboard')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'dashboard',
            'api_class': 'ProjectDashboard'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all data_sources')
        project_dashboard_vos = ProjectDashboard.objects.filter()
        project_dashboard_vos.delete()

    @parameterized.expand([['user_id', None], ['user_id', 'cloudforet@gmail.com']], name_func=key_value_name_func)
    def test_create_project_dashboard(self, key, value):
        params = {
            'name': 'test',
            'domain_id': 'domain-12345',
            'options': {
                'date_range': {
                    'enabled': True,
                    'period_type': 'AUTO',
                    'period': {'start': '2021-11', 'end': '2021-12'}
                },
                'currency': {
                    'enabled': True,
                }
            }
        }

        if key and value:
            params.update({key: value})

        self.transaction.method = 'create'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.create(params.copy())

        print_data(project_dashboard_vo.to_dict(), 'test_create_project_dashboard')
        ProjectDashboardInfo(project_dashboard_vo)

        self.assertIsInstance(project_dashboard_vo, ProjectDashboard)
        self.assertEqual(params['name'], project_dashboard_vo.name)
        self.assertEqual(params['options']['currency']['enabled'], project_dashboard_vo.options.currency.enabled)

    def test_update_project_dashboard(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'name': 'update project dashboard test',
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.update(params.copy())
        print_data(project_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        self.assertIsInstance(project_dashboard_vo, ProjectDashboard)
        self.assertEqual(params['name'], project_dashboard_vo.name)

    def test_get_project_dashboard(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        get_project_dashboard_vo = project_dashboard_svc.get(params)

        print_data(get_project_dashboard_vo.to_dict(), 'test_get_project_dashboard')
        ProjectDashboardInfo(get_project_dashboard_vo)

        self.assertIsInstance(get_project_dashboard_vo, ProjectDashboard)
        self.assertEqual(project_dashboard_vo.name, get_project_dashboard_vo.name)
        self.assertEqual(project_dashboard_vo.project_dashboard_id, get_project_dashboard_vo.project_dashboard_id)

    def test_list_project_dashboards(self):
        project_dashboard_vos = ProjectDashboardFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), project_dashboard_vos))

        print_data(project_dashboard_vos[4].to_dict(), "5th project_dashboard_vo")

        params = {
            'project_dashboard_id': project_dashboard_vos[4].project_dashboard_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        list_project_dashboard_vos, total_count = project_dashboard_svc.list(params)

        ProjectDashboardsInfo(list_project_dashboard_vos, total_count)

        self.assertEqual(len(list_project_dashboard_vos), 1)
        self.assertEqual(list_project_dashboard_vos[0].project_dashboard_id, params.get('project_dashboard_id'))
        self.assertIsInstance(list_project_dashboard_vos[0], ProjectDashboard)
        self.assertEqual(total_count, 1)


if __name__ == '__main__':
    unittest.main()
