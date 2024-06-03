from spaceone.api.dashboard.v1 import private_widget_pb2, private_widget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.private_widget_service import PrivateWidgetService


class PrivateWidget(BaseAPI, private_widget_pb2_grpc.PrivateWidgetServicer):
    pb2 = private_widget_pb2
    pb2_grpc = private_widget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        response: dict = pri_widget_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        response: dict = pri_widget_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        pri_widget_svc.delete(params)
        return self.empty()

    def load(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        response: dict = pri_widget_svc.load(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        response: dict = pri_widget_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_widget_svc = PrivateWidgetService(metadata)
        response: dict = pri_widget_svc.list(params)
        return self.dict_to_message(response)
