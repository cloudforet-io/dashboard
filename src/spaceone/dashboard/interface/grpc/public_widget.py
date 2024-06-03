from spaceone.api.dashboard.v1 import public_widget_pb2, public_widget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.public_widget_service import PublicWidgetService


class PublicWidget(BaseAPI, public_widget_pb2_grpc.PublicWidgetServicer):
    pb2 = public_widget_pb2
    pb2_grpc = public_widget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        response: dict = pub_widget_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        response: dict = pub_widget_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        pub_widget_svc.delete(params)
        return self.empty()

    def load(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        response: dict = pub_widget_svc.load(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        response: dict = pub_widget_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_widget_svc = PublicWidgetService(metadata)
        response: dict = pub_widget_svc.list(params)
        return self.dict_to_message(response)
