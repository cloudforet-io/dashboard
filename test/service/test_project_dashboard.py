import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data
from parameterized import parameterized

from spaceone.dashboard.info import ProjectDashboardInfo, ProjectDashboardsInfo, StatisticsInfo
from spaceone.dashboard.info.project_dashboard_version_info import ProjectDashboardVersionsInfo
from spaceone.dashboard.model import ProjectDashboard, ProjectDashboardVersion
from spaceone.dashboard.service import ProjectDashboardService, DomainDashboardService
from spaceone.dashboard.error import *
from test.factory import ProjectDashboardFactory
from test.factory.project_dashboard_version_factory import ProjectDashboardVersionFactory
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
            'project_id': 'project-12345',
            'name': 'test',
            'viewers': 'PUBLIC',
            'domain_id': 'domain-12345',
            'dashboard_options': {
                'project_id': 'project-1234'
            },
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            }
        }

        if key and value:
            params.update({key: value})

        self.transaction.method = 'create'
        self.transaction.set_meta('user_id', 'cloudforet@gmail.com')
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.create(params.copy())

        print_data(project_dashboard_vo.to_dict(), 'test_create_project_dashboard')
        ProjectDashboardInfo(project_dashboard_vo)

        self.assertIsInstance(project_dashboard_vo, ProjectDashboard)
        self.assertEqual(params['name'], project_dashboard_vo.name)
        self.assertEqual(params['dashboard_options']['project_id'],
                         project_dashboard_vo.dashboard_options.get('project_id'))

    def test_update_project_dashboard(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'name': 'update project dashboard test',
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.update(params.copy())
        print_data(project_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        self.assertIsInstance(project_dashboard_vo, ProjectDashboard)
        self.assertEqual(params['name'], project_dashboard_vo.name)

    def test_update_project_dashboard_permission_error(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id,
                                                       viewers='PRIVATE',
                                                       user_id='cloudforet2@gmail.com')

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'name': 'update project dashboard test',
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        self.transaction.set_meta('user_id', 'cloudforet@gmail.com')
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        with self.assertRaises(ERROR_PERMISSION_DENIED):
            project_dashboard_svc.update(params.copy())

    def test_delete_project_dashboard(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        project_dashboard_version_vos = ProjectDashboardVersionFactory.build_batch(5,
                                                                                   project_dashboard_id=project_dashboard_vo.project_dashboard_id,
                                                                                   domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), project_dashboard_version_vos))

        for idx, project_dashboard_version in enumerate(project_dashboard_version_vos, 1):
            print_data(project_dashboard_version.to_dict(), f'{idx}th project_dashboard_version')

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'domain_id': project_dashboard_vo.domain_id
        }

        self.transaction.method = 'delete'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_svc.delete(params.copy())

        with self.assertRaises(ERROR_NOT_FOUND):
            project_dashboard_svc.list_versions(params)

    def test_delete_version(self):
        # Create 1 domain_dashboard instance / Create 1 domain_dashboard_version instance
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        ProjectDashboardVersionFactory(
            project_dashboard_id=project_dashboard_vo.project_dashboard_id,
            layouts=project_dashboard_vo.layouts,
            dashboard_options=project_dashboard_vo.dashboard_options,
            version=1,
            dashboard_options_schema=project_dashboard_vo.dashboard_options_schema,
            domain_id=self.domain_id
        )

        # When updating domain_dashboard, version=2 is created
        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'name': 'update domain dashboard test',
            'layouts': [{'name': 'widget4'}],
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }
        self.transaction.method = 'update'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.update(params.copy())
        print_data(project_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'version': 1,
            'domain_id': project_dashboard_vo.domain_id
        }

        # After that, delete version=1 with the delete_version method
        project_dashboard_svc.delete_version(params.copy())

    def test_delete_latest_version(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'version': 1,
            'domain_id': project_dashboard_vo.domain_id
        }

        self.transaction.method = 'delete_version'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        with self.assertRaises(ERROR_LATEST_VERSION):
            project_dashboard_svc.delete_version(params.copy())

    def test_revert_version(self):
        # Create 1 domain_dashboard instance / Create 1 domain_dashboard_version instance
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        first_version = ProjectDashboardVersionFactory(
            project_dashboard_id=project_dashboard_vo.project_dashboard_id,
            layouts=project_dashboard_vo.layouts,
            dashboard_options=project_dashboard_vo.dashboard_options,
            version=1,
            dashboard_options_schema=project_dashboard_vo.dashboard_options_schema,
            domain_id=self.domain_id
        )

        # When updating domain_dashboard, version=2 is created
        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'name': 'update project dashboard test',
            'layouts': [{'name': 'widget4'}],
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }
        self.transaction.method = 'update'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_vo = project_dashboard_svc.update(params.copy())
        print_data(project_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'version': 1,
            'domain_id': project_dashboard_vo.domain_id
        }

        # After that, revert version=1 with the revert_version method
        project_dashboard_vo = project_dashboard_svc.revert_version(params.copy())
        print_data(project_dashboard_vo.to_dict(), 'test_reverted_project_dashboard')

        self.assertIsInstance(project_dashboard_vo, ProjectDashboard)
        self.assertEqual(project_dashboard_vo.version, 3)
        self.assertEqual(project_dashboard_vo.layouts, first_version.layouts)
        self.assertEqual(project_dashboard_vo.dashboard_options, first_version.dashboard_options)
        self.assertEqual(project_dashboard_vo.dashboard_options_schema, first_version.dashboard_options_schema)

    def test_get_version(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        ProjectDashboardVersionFactory(
            project_dashboard_id=project_dashboard_vo.project_dashboard_id,
            layouts=project_dashboard_vo.layouts,
            dashboard_options=project_dashboard_vo.dashboard_options,
            version=1,
            dashboard_options_schema=project_dashboard_vo.dashboard_options_schema,
            domain_id=self.domain_id
        )

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'version': 1,
            'domain_id': project_dashboard_vo.domain_id
        }

        self.transaction.method = 'get_version'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        project_dashboard_version_vo = project_dashboard_svc.get_version(params.copy())

        print_data(project_dashboard_version_vo.to_dict(), 'test_project_dashboard_version_by_get_version')

        self.assertIsInstance(project_dashboard_version_vo, ProjectDashboardVersion)
        self.assertEqual(project_dashboard_version_vo.version, 1)
        self.assertEqual(project_dashboard_version_vo.project_dashboard_id, project_dashboard_vo.project_dashboard_id)
        self.assertEqual(project_dashboard_version_vo.layouts, project_dashboard_vo.layouts)
        self.assertEqual(project_dashboard_version_vo.dashboard_options, project_dashboard_vo.dashboard_options)
        self.assertEqual(project_dashboard_version_vo.dashboard_options_schema,
                         project_dashboard_vo.dashboard_options_schema)

    def test_list_versions(self):
        project_dashboard_vo = ProjectDashboardFactory(domain_id=self.domain_id)
        project_dashboard_version_vos = ProjectDashboardVersionFactory.build_batch(5,
                                                                                   project_dashboard_id=project_dashboard_vo.project_dashboard_id,
                                                                                   domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), project_dashboard_version_vos))

        params = {
            'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
            'domain_id': project_dashboard_vo.domain_id
        }

        self.transaction.method = 'list_versions'
        project_dashboard_svc = ProjectDashboardService(transaction=self.transaction)
        list_project_dashboard_version_vos, total_count, current_version = project_dashboard_svc.list_versions(
            params.copy())

        ProjectDashboardVersionsInfo(list_project_dashboard_version_vos, total_count)

        for idx, domain_dashboard_version in enumerate(list_project_dashboard_version_vos, 1):
            print_data(domain_dashboard_version.to_dict(), f'{idx}th project_dashboard_version')

        self.assertEqual(len(list_project_dashboard_version_vos), 5)
        self.assertIsInstance(list_project_dashboard_version_vos[0], ProjectDashboardVersion)
        self.assertEqual(total_count, 5)

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
