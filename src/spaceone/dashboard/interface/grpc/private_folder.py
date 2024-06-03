from spaceone.api.dashboard.v1 import private_folder_pb2, private_folder_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.private_folder_service import PrivateFolderService


class PrivateFolder(BaseAPI, private_folder_pb2_grpc.PrivateFolderServicer):
    pb2 = private_folder_pb2
    pb2_grpc = private_folder_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_folder_svc = PrivateFolderService(metadata)
        response: dict = pri_folder_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_folder_svc = PrivateFolderService(metadata)
        response: dict = pri_folder_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_folder_svc = PrivateFolderService(metadata)
        pri_folder_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_dash_svc = PrivateFolderService(metadata)
        response: dict = pub_dash_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_folder_svc = PrivateFolderService(metadata)
        response: dict = pri_folder_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_folder_svc = PrivateFolderService(metadata)
        response: dict = pri_folder_svc.stat(params)
        return self.dict_to_message(response)
