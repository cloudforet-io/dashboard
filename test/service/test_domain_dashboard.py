import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data
from parameterized import parameterized

from spaceone.dashboard.info import DomainDashboardInfo, DomainDashboardsInfo
from spaceone.dashboard.info.domain_dashboard_version_info import DomainDashboardVersionsInfo
from spaceone.dashboard.model import DomainDashboard, DomainDashboardVersion
from spaceone.dashboard.service.domain_dashboard_service import DomainDashboardService
from spaceone.dashboard.error import *
from test.factory import DomainDashboardFactory
from test.factory.domain_dashboard_version_factory import DomainDashboardVersionFactory
from test.lib import *


class TestDomainDashboardService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.dashboard')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'dashboard',
            'api_class': 'DomainDashboard'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all data_sources')
        domain_dashboard_vos = DomainDashboard.objects.filter()
        domain_dashboard_vos.delete()

    @parameterized.expand([['user_id', None], ['user_id', 'cloudforet@gmail.com']], name_func=key_value_name_func)
    def test_create_domain_dashboard(self, key, value):
        params = {
            'name': 'test',
            'viewers': 'PUBLIC',
            'domain_id': 'domain-12345',
            'variables': {
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
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_vo = domain_dashboard_svc.create(params.copy())

        print_data(domain_dashboard_vo.to_dict(), 'test_create_domain_dashboard')
        DomainDashboardInfo(domain_dashboard_vo)

        self.assertIsInstance(domain_dashboard_vo, DomainDashboard)
        self.assertEqual(params['name'], domain_dashboard_vo.name)
        self.assertEqual(params['variables']['project_id'],
                         domain_dashboard_vo.variables.get('project_id'))

    def test_update_domain_dashboard(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'name': 'update domain dashboard test',
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_vo = domain_dashboard_svc.update(params.copy())
        print_data(domain_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        self.assertIsInstance(domain_dashboard_vo, DomainDashboard)
        self.assertEqual(params['name'], domain_dashboard_vo.name)

    def test_update_domain_dashboard_permission_error(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id,
                                                     viewers='PRIVATE',
                                                     user_id='cloudforet2@gmail.com')

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'name': 'update domain dashboard test',
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        self.transaction.set_meta('user_id', 'cloudforet@gmail.com')
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        with self.assertRaises(ERROR_PERMISSION_DENIED):
            domain_dashboard_svc.update(params.copy())

    def test_delete_domain_dashboard(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        domain_dashboard_version_vos = DomainDashboardVersionFactory.build_batch(5,
                                                                                 domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id,
                                                                                 domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), domain_dashboard_version_vos))

        for idx, domain_dashboard_version in enumerate(domain_dashboard_version_vos, 1):
            print_data(domain_dashboard_version.to_dict(), f'{idx}th domain_dashboard_version')

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'domain_id': domain_dashboard_vo.domain_id
        }

        self.transaction.method = 'delete'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_svc.delete(params.copy())

        with self.assertRaises(ERROR_NOT_FOUND):
            domain_dashboard_svc.list_versions(params)

    def test_delete_version(self):
        # Create 1 domain_dashboard instance / Create 1 domain_dashboard_version instance
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        DomainDashboardVersionFactory(
            domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id,
            layouts=domain_dashboard_vo.layouts,
            variables=domain_dashboard_vo.variables,
            version=1,
            variables_schema=domain_dashboard_vo.variables_schema,
            domain_id=self.domain_id
        )

        # When updating domain_dashboard, version=2 is created
        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'name': 'update domain dashboard test',
            'layouts': [[{'name': 'widget4'}]],
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }
        self.transaction.method = 'update'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_vo = domain_dashboard_svc.update(params.copy())
        print_data(domain_dashboard_vo.to_dict(), 'test_update_project_dashboard')

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'version': 1,
            'domain_id': domain_dashboard_vo.domain_id
        }

        # After that, delete version=1 with the delete_version method
        domain_dashboard_svc.delete_version(params.copy())

    def test_delete_latest_version(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'version': 1,
            'domain_id': domain_dashboard_vo.domain_id
        }
        print(params)

        self.transaction.method = 'delete_version'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        with self.assertRaises(ERROR_LATEST_VERSION):
            domain_dashboard_svc.delete_version(params.copy())

    def test_revert_version(self):
        # Create 1 domain_dashboard instance / Create 1 domain_dashboard_version instance
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        first_version = DomainDashboardVersionFactory(
            domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id,
            layouts=domain_dashboard_vo.layouts,
            variables=domain_dashboard_vo.variables,
            version=1,
            variables_schema=domain_dashboard_vo.variables_schema,
            domain_id=self.domain_id
        )

        # When updating domain_dashboard, version=2 is created
        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'name': 'update domain dashboard test',
            'layouts': [[{'name': 'widget4'}]],
            'settings': {
                'date_range': {'enabled': False},
                'currency': {'enabled': False}
            },
            'tags': {'a': 'b'},
            'domain_id': self.domain_id
        }
        self.transaction.method = 'update'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_vo = domain_dashboard_svc.update(params.copy())
        print_data(domain_dashboard_vo.to_dict(), 'test_update_domain_dashboard')

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'version': 1,
            'domain_id': domain_dashboard_vo.domain_id
        }

        # After that, revert version=1 with the revert_version method
        domain_dashboard_vo = domain_dashboard_svc.revert_version(params.copy())
        print_data(domain_dashboard_vo.to_dict(), 'test_reverted_domain_dashboard')

        self.assertIsInstance(domain_dashboard_vo, DomainDashboard)
        self.assertEqual(domain_dashboard_vo.version, 3)
        self.assertEqual(domain_dashboard_vo.layouts, first_version.layouts)
        self.assertEqual(domain_dashboard_vo.variables, first_version.variables)
        self.assertEqual(domain_dashboard_vo.variables_schema, first_version.variables_schema)

    def test_get_version(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        DomainDashboardVersionFactory(
            domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id,
            layouts=domain_dashboard_vo.layouts,
            variables=domain_dashboard_vo.variables,
            version=1,
            variables_schema=domain_dashboard_vo.variables_schema,
            domain_id=self.domain_id
        )

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'version': 1,
            'domain_id': domain_dashboard_vo.domain_id
        }

        self.transaction.method = 'get_version'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        domain_dashboard_version_vo = domain_dashboard_svc.get_version(params.copy())

        print_data(domain_dashboard_version_vo.to_dict(), 'test_domain_dashboard_version_by_get_version')

        self.assertIsInstance(domain_dashboard_version_vo, DomainDashboardVersion)
        self.assertEqual(domain_dashboard_version_vo.version, 1)
        self.assertEqual(domain_dashboard_version_vo.domain_dashboard_id, domain_dashboard_vo.domain_dashboard_id)
        self.assertEqual(domain_dashboard_version_vo.layouts, domain_dashboard_vo.layouts)
        self.assertEqual(domain_dashboard_version_vo.variables, domain_dashboard_vo.variables)
        self.assertEqual(domain_dashboard_version_vo.variables_schema,
                         domain_dashboard_vo.variables_schema)

    def test_list_versions(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)
        domain_dashboard_version_vos = DomainDashboardVersionFactory.build_batch(5,
                                                                                 domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id,
                                                                                 domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), domain_dashboard_version_vos))

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'domain_id': domain_dashboard_vo.domain_id
        }

        self.transaction.method = 'get'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        list_domain_dashboard_version_vos, total_count, current_version = domain_dashboard_svc.list_versions(
            params.copy())

        DomainDashboardVersionsInfo(list_domain_dashboard_version_vos, total_count)

        for idx, domain_dashboard_version in enumerate(list_domain_dashboard_version_vos, 1):
            print_data(domain_dashboard_version.to_dict(), f'{idx}th domain_dashboard_version')

        self.assertEqual(len(list_domain_dashboard_version_vos), 5)
        self.assertIsInstance(list_domain_dashboard_version_vos[0], DomainDashboardVersion)
        self.assertEqual(total_count, 5)

    def test_get_domain_dashboard(self):
        domain_dashboard_vo = DomainDashboardFactory(domain_id=self.domain_id)

        params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        get_domain_dashboard_vo = domain_dashboard_svc.get(params)

        print_data(get_domain_dashboard_vo.to_dict(), 'test_get_domain_dashboard')
        DomainDashboardInfo(get_domain_dashboard_vo)

        self.assertIsInstance(get_domain_dashboard_vo, DomainDashboard)
        self.assertEqual(domain_dashboard_vo.name, get_domain_dashboard_vo.name)
        self.assertEqual(domain_dashboard_vo.domain_dashboard_id, get_domain_dashboard_vo.domain_dashboard_id)

    def test_list_domain_dashboards(self):
        domain_dashboard_vos = DomainDashboardFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), domain_dashboard_vos))

        print_data(domain_dashboard_vos[4].to_dict(), "5th domain_dashboard_vo")

        params = {
            'domain_dashboard_id': domain_dashboard_vos[4].domain_dashboard_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        domain_dashboard_svc = DomainDashboardService(transaction=self.transaction)
        list_domain_dashboard_vos, total_count = domain_dashboard_svc.list(params)

        DomainDashboardsInfo(list_domain_dashboard_vos, total_count)

        self.assertEqual(len(list_domain_dashboard_vos), 1)
        self.assertEqual(list_domain_dashboard_vos[0].domain_dashboard_id, params.get('domain_dashboard_id'))
        self.assertIsInstance(list_domain_dashboard_vos[0], DomainDashboard)
        self.assertEqual(total_count, 1)


if __name__ == '__main__':
    unittest.main()
