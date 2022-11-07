import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data

from spaceone.dashboard.info import CustomWidgetInfo, CustomWidgetsInfo, StatisticsInfo
from spaceone.dashboard.model import CustomWidget
from spaceone.dashboard.service import CustomWidgetService
from test.factory import CustomWidgetFactory
from test.lib import *


class TestCustomWidgetService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.dashboard')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'dashboard',
            'api_class': 'CustomWidget'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all data_sources')
        custom_widget_vos = CustomWidget.objects.filter()
        custom_widget_vos.delete()

    def test_create_custom_widget(self):
        params = {
            'widget_name': 'widget-name',
            'title': 'test',
            'version': 'v1',
            'widget_options': {'group_by': 'product'},
            'inherit_options': {'a': {'enabled': True}},
            'tags': {'type': 'test'},
            'domain_id': 'domain-12345'
        }

        self.transaction.method = 'create'
        custom_widget_svc = CustomWidgetService(transaction=self.transaction)
        custom_widget_vo = custom_widget_svc.create(params.copy())

        print_data(custom_widget_vo.to_dict(), 'test_create_custom_widget')
        CustomWidgetInfo(custom_widget_vo)

        self.assertIsInstance(custom_widget_vo, CustomWidget)
        self.assertEqual(params['title'], custom_widget_vo.title)

    def test_update_custom_widget(self):
        custom_widget_vo = CustomWidgetFactory(domain_id=self.domain_id)

        params = {
            'custom_widget_id': custom_widget_vo.custom_widget_id,
            'title': 'update widget test',
            'widget_options': {'group_by': 'product2'},
            'inherit_options': {'b': {'enabled': True}},
            'tags': {'type': 'test from params'},
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        custom_widget_svc = CustomWidgetService(transaction=self.transaction)
        updated_custom_widget_vo = custom_widget_svc.update(params.copy())
        print_data(updated_custom_widget_vo.to_dict(), 'test_update_custom_widget')

        self.assertIsInstance(updated_custom_widget_vo, CustomWidget)
        self.assertEqual(params['title'], updated_custom_widget_vo.title)

    def test_get_custom_widget(self):
        custom_widget_vo = CustomWidgetFactory(domain_id=self.domain_id)

        params = {
            'custom_widget_id': custom_widget_vo.custom_widget_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        custom_widget_svc = CustomWidgetService(transaction=self.transaction)
        get_custom_widget_vo = custom_widget_svc.get(params)

        print_data(get_custom_widget_vo.to_dict(), 'test_get_custom_widget')
        CustomWidgetInfo(get_custom_widget_vo)

        self.assertIsInstance(get_custom_widget_vo, CustomWidget)
        self.assertEqual(custom_widget_vo.title, get_custom_widget_vo.title)
        self.assertEqual(custom_widget_vo.custom_widget_id, get_custom_widget_vo.custom_widget_id)

    def test_list_custom_widgets(self):
        custom_widget_vos = CustomWidgetFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), custom_widget_vos))

        print_data(custom_widget_vos[4].to_dict(), "5th custom_widget_vo")

        params = {
            'custom_widget_id': custom_widget_vos[4].custom_widget_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        custom_widget_svc = CustomWidgetService(transaction=self.transaction)
        list_custom_widget_vos, total_count = custom_widget_svc.list(params)

        CustomWidgetsInfo(list_custom_widget_vos, total_count)

        self.assertEqual(len(list_custom_widget_vos), 1)
        self.assertEqual(list_custom_widget_vos[0].custom_widget_id, params.get('custom_widget_id'))
        self.assertIsInstance(list_custom_widget_vos[0], CustomWidget)
        self.assertEqual(total_count, 1)


if __name__ == '__main__':
    unittest.main()
