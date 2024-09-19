from spaceone.api.dashboard.v1 import private_dashboard_pb2, private_dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.private_dashboard_service import PrivateDashboardService


class PrivateDashboard(BaseAPI, private_dashboard_pb2_grpc.PrivateDashboardServicer):
    pb2 = private_dashboard_pb2
    pb2_grpc = private_dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        response: dict = pri_dash_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        response: dict = pri_dash_svc.update(params)
        return self.dict_to_message(response)

    def change_folder(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        response: dict = pri_dash_svc.change_folder(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        pri_dash_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_dash_svc = PrivateDashboardService(metadata)
        response: dict = pub_dash_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        response: dict = pri_dash_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_dash_svc = PrivateDashboardService(metadata)
        response: dict = pri_dash_svc.stat(params)
        return self.dict_to_message(response)
