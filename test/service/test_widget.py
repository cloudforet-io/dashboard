import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data

from spaceone.dashboard.info import WidgetInfo, WidgetsInfo, StatisticsInfo
from spaceone.dashboard.model import Widget
from spaceone.dashboard.service import WidgetService
from test.factory import WidgetFactory
from test.lib import *


class TestWidgetService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.dashboard')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'dashboard',
            'api_class': 'Widget'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all data_sources')
        widget_vos = Widget.objects.filter()
        widget_vos.delete()

    def test_create_widget(self):
        params = {
            'name': 'test',
            'view_mode': 'FULL',
            'domain_id': 'domain-12345'
        }

        self.transaction.method = 'create'
        widget_svc = WidgetService(transaction=self.transaction)
        widget_vo = widget_svc.create(params.copy())

        print_data(widget_vo.to_dict(), 'test_create_widget')
        print(">>>>>>")
        print(widget_vo.tags)
        WidgetInfo(widget_vo)

        self.assertIsInstance(widget_vo, Widget)
        self.assertEqual(params['name'], widget_vo.name)

    def test_update_widget(self):
        widget_vo = WidgetFactory(domain_id=self.domain_id)

        params = {
            'widget_id': widget_vo.widget_id,
            'name': 'update widget test',
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        widget_svc = WidgetService(transaction=self.transaction)
        updated_widget_vo = widget_svc.update(params.copy())
        print_data(updated_widget_vo.to_dict(), 'test_update_widget')

        self.assertIsInstance(updated_widget_vo, Widget)
        self.assertEqual(params['name'], updated_widget_vo.name)

    def test_get_widget(self):
        widget_vo = WidgetFactory(domain_id=self.domain_id)

        params = {
            'widget_id': widget_vo.widget_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        widget_svc = WidgetService(transaction=self.transaction)
        get_widget_vo = widget_svc.get(params)

        print_data(get_widget_vo.to_dict(), 'test_get_widget')
        WidgetInfo(get_widget_vo)

        self.assertIsInstance(get_widget_vo, Widget)
        self.assertEqual(widget_vo.name, get_widget_vo.name)
        self.assertEqual(widget_vo.widget_id, get_widget_vo.widget_id)

    def test_list_widgets(self):
        widget_vos = WidgetFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), widget_vos))

        print_data(widget_vos[4].to_dict(), "5th widget_vo")

        params = {
            'widget_id': widget_vos[4].widget_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        widget_svc = WidgetService(transaction=self.transaction)
        list_widget_vos, total_count = widget_svc.list(params)

        WidgetsInfo(list_widget_vos, total_count)

        self.assertEqual(len(list_widget_vos), 1)
        self.assertEqual(list_widget_vos[0].widget_id, params.get('widget_id'))
        self.assertIsInstance(list_widget_vos[0], Widget)
        self.assertEqual(total_count, 1)


if __name__ == '__main__':
    unittest.main()
