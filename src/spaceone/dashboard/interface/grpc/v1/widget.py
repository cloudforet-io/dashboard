from spaceone.api.dashboard.v1 import widget_pb2, widget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Widget(BaseAPI, widget_pb2_grpc.WidgetServicer):
    pb2 = widget_pb2
    pb2_grpc = widget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            return self.locator.get_info('WidgetInfo', widget_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            return self.locator.get_info('WidgetInfo', widget_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            widget_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            return self.locator.get_info('WidgetInfo', widget_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            widget_vos, total_count = widget_service.list(params)
            return self.locator.get_info('WidgetsInfo',
                                         widget_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('WidgetService', metadata) as widget_service:
            return self.locator.get_info('StatisticsInfo', widget_service.stat(params))
