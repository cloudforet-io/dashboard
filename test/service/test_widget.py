import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data

from spaceone.dashboard.info import WidgetInfo, WidgetsInfo, StatisticsInfo
from spaceone.dashboard.model import Widget
from spaceone.dashboard.service import WidgetService


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
            'domain_id': 'domain-12345'
        }

        self.transaction.method = 'create'
        widget_svc = WidgetService(transaction=self.transaction)
        widget_vo = widget_svc.create(params.copy())

        print_data(widget_vo.to_dict(), 'test_create_widget')
        WidgetInfo(widget_vo)

        self.assertIsInstance(widget_vo, Widget)
        self.assertEqual(params['name'], widget_vo.name)

    def test_update_domain_dashboard(self):
        pass

    def test_get_domain_dashboard(self):
        pass

    def test_list_domain_dashboards(self):
        pass

    def test_stat_domain_dashboard(self):
        pass
